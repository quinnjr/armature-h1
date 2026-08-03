//! The hyper service bridge: converts heads and responses between dialects
//! and enforces the parity checks hyper does not.

use crate::bytestr::ByteStr;
use crate::conn::ConnConfig;
use crate::header::{self, HeaderId, HeaderVec};
use crate::service::{Body, H1Service, Request};
use crate::{Head, Limits, Method, Version};
use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use super::body::{HyperBodyIo, HyperOutBody};

/// Approximate request-line overhead ("METHOD  HTTP/1.1\r\n" scaffolding) for
/// the cumulative head-bytes cap. The wire-exact cap is enforced by hyper's
/// `max_buf_size`; this check exists so a head that is *cumulatively* huge
/// without any single oversized piece is still rejected at 431 parity.
#[allow(dead_code)] // wired in a later task, via `convert_head`
const HEAD_OVERHEAD: usize = 26;

/// Convert a hyper request head into the bespoke `Head`, enforcing the parity
/// checks hyper itself does not (header count, cumulative head bytes,
/// declared body size). `Err` carries the rejection status.
// No production caller exists yet (`Bridge` is wired into the connection
// driver in a later task); keep it from tripping dead-code lints.
#[allow(dead_code)]
pub(crate) fn convert_head(
    parts: &::hyper::http::request::Parts,
    limits: &Limits,
) -> Result<Head, u16> {
    let version = match parts.version {
        ::hyper::http::Version::HTTP_11 => Version::Http11,
        ::hyper::http::Version::HTTP_10 => Version::Http10,
        _ => return Err(505),
    };

    let method = Method::from_bytes(parts.method.as_str().as_bytes()).unwrap_or_else(|| {
        Method::Other(
            ByteStr::from_utf8(Bytes::copy_from_slice(parts.method.as_str().as_bytes()))
                .expect("method tokens are ASCII"),
        )
    });

    let target_str = match (parts.uri.scheme(), parts.uri.path_and_query()) {
        (None, Some(pq)) => pq.as_str().to_owned(),
        _ => parts.uri.to_string(),
    };

    if parts.headers.len() > limits.max_headers {
        return Err(431);
    }

    let mut cumulative = target_str.len() + HEAD_OVERHEAD;
    let mut headers = HeaderVec::new();
    let mut declared_len: Option<u64> = None;
    for (name, value) in parts.headers.iter() {
        cumulative += name.as_str().len() + value.as_bytes().len() + 4;
        let id = HeaderId::from_bytes(name.as_str().as_bytes())
            .unwrap_or_else(|| header::intern(name.as_str()));
        if id == HeaderId::ContentLength
            && let Ok(s) = std::str::from_utf8(value.as_bytes())
            && let Ok(n) = s.trim().parse::<u64>()
        {
            declared_len = Some(n);
        }
        headers.push((id, Bytes::copy_from_slice(value.as_bytes())));
    }
    if cumulative > limits.max_head_bytes {
        return Err(431);
    }
    if declared_len.is_some_and(|n| n > limits.max_body_bytes) {
        return Err(413);
    }

    let target =
        ByteStr::from_utf8(Bytes::from(target_str.into_bytes())).expect("http::Uri is valid UTF-8");

    Ok(Head {
        method,
        target,
        version,
        headers,
    })
}

/// Adapts an [`H1Service`] to hyper's `Service` trait over `Incoming`
/// request bodies and [`HyperOutBody`] response bodies.
///
/// `phase` (connection-lifecycle coordination for graceful shutdown / the
/// upgrade handoff) is added in a later task; this bridge only carries what
/// per-request conversion needs.
// No production caller exists yet (the connection driver constructs a
// `Bridge` and passes it to `hyper::server::conn::http1` in a later task);
// keep it from tripping dead-code lints in the meantime.
#[allow(dead_code)]
pub(crate) struct Bridge<S> {
    pub(crate) service: Rc<S>,
    pub(crate) cfg: Rc<ConnConfig>,
    /// Filled only when a 101 on a request that asked for an upgrade goes out.
    pub(crate) upgrade_slot: Rc<RefCell<Option<::hyper::upgrade::OnUpgrade>>>,
    pub(crate) sent_101: Rc<Cell<bool>>,
}

// wired in a later task, along with `Bridge` itself
#[allow(dead_code)]
type BridgeResponse = ::hyper::http::Response<HyperOutBody>;

impl<S: H1Service + 'static>
    ::hyper::service::Service<::hyper::http::Request<::hyper::body::Incoming>> for Bridge<S>
{
    type Response = BridgeResponse;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<BridgeResponse, Self::Error>>>>;

    fn call(&self, mut req: ::hyper::http::Request<::hyper::body::Incoming>) -> Self::Future {
        let service = self.service.clone();
        let cfg = self.cfg.clone();
        let upgrade_slot = self.upgrade_slot.clone();
        let sent_101 = self.sent_101.clone();

        Box::pin(async move {
            let on_upgrade = ::hyper::upgrade::on(&mut req);
            let (parts, incoming) = req.into_parts();

            let head = match convert_head(&parts, &cfg.limits) {
                Ok(h) => h,
                Err(status) => return Ok(reject(status)),
            };

            // Reuse the bespoke framing decision table so anything hyper let
            // through is still rejected exactly where the native stack would.
            let kind = match crate::framing::decide(&head, &cfg.limits) {
                Ok(k) => k,
                Err(e) => return Ok(reject(e.status())),
            };

            let wants_upgrade =
                head.count(&HeaderId::Upgrade) > 0 && head.connection_has_token("upgrade");
            let expects_continue = head
                .get_str(&HeaderId::Expect)
                .is_some_and(|v| v.eq_ignore_ascii_case("100-continue"));

            let fully_read = Rc::new(Cell::new(false));
            let trailers_slot = Rc::new(RefCell::new(None));
            let body_io: Rc<RefCell<dyn crate::service::BodyIo>> = Rc::new(RefCell::new(
                HyperBodyIo::new(incoming, trailers_slot.clone()),
            ));
            let body = Body::from_backend(
                kind,
                body_io,
                expects_continue,
                cfg.limits.max_body_bytes,
                fully_read.clone(),
                trailers_slot,
            );

            let resp = service.call(Request { head, body }).await;

            let upgrading = resp.status == 101 && wants_upgrade;
            if upgrading {
                *upgrade_slot.borrow_mut() = Some(on_upgrade);
                sent_101.set(true);
            }

            // Native parity: an unread body means undelimited bytes on the
            // wire under the bespoke stack; the observable contract is that
            // the connection is not reused. hyper *can* drain, but strict
            // parity wins: force close.
            let force_close = !upgrading && !fully_read.get();

            Ok(convert_response(resp, &cfg, force_close))
        })
    }
}

/// An empty rejection response that also closes the connection, matching the
/// native rule that any framing rejection closes.
#[allow(dead_code)] // wired in a later task
fn reject(status: u16) -> BridgeResponse {
    let mut r = ::hyper::http::Response::builder()
        .status(::hyper::http::StatusCode::from_u16(status).expect("known status"))
        .body(HyperOutBody::new(crate::service::ResponseBody::Empty))
        .expect("static response");
    r.headers_mut().insert(
        ::hyper::http::header::CONNECTION,
        ::hyper::http::HeaderValue::from_static("close"),
    );
    r
}

#[allow(dead_code)] // wired in a later task
fn convert_response(
    resp: crate::service::Response,
    cfg: &ConnConfig,
    force_close: bool,
) -> BridgeResponse {
    let crate::service::Response {
        status,
        headers,
        body,
    } = resp;

    let status = ::hyper::http::StatusCode::from_u16(status)
        .unwrap_or(::hyper::http::StatusCode::INTERNAL_SERVER_ERROR);

    // Parity with the native writer: bodies on 204/304/1xx are dropped, not
    // written, so a handler mistake degrades identically under both backends.
    let body_forbidden = matches!(status.as_u16(), 204 | 304) || status.is_informational();
    let body = if body_forbidden {
        crate::service::ResponseBody::Empty
    } else {
        body
    };

    let mut builder = ::hyper::http::Response::builder().status(status);
    let mut has_server = false;
    for (id, value) in headers.into_iter() {
        if id == HeaderId::Server {
            has_server = true;
        }
        let Ok(name) = ::hyper::http::HeaderName::from_bytes(id.as_str().as_bytes()) else {
            continue;
        };
        let Ok(value) = ::hyper::http::HeaderValue::from_maybe_shared(value) else {
            continue;
        };
        builder = builder.header(name, value);
    }
    if !has_server
        && let Some(name) = &cfg.server_name
        && let Ok(v) = ::hyper::http::HeaderValue::from_maybe_shared(name.clone())
    {
        builder = builder.header(::hyper::http::header::SERVER, v);
    }
    if force_close {
        builder = builder.header(
            ::hyper::http::header::CONNECTION,
            ::hyper::http::HeaderValue::from_static("close"),
        );
    }

    builder
        .body(HyperOutBody::new(body))
        .expect("converted response is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::HeaderId;
    use crate::{Limits, Method, Version};

    fn parts(builder: ::hyper::http::request::Builder) -> ::hyper::http::request::Parts {
        builder.body(()).unwrap().into_parts().0
    }

    #[test]
    fn converts_method_target_version_and_headers() {
        let p = parts(
            ::hyper::http::Request::builder()
                .method("POST")
                .uri("/a/b?q=1")
                .version(::hyper::http::Version::HTTP_11)
                .header("host", "a")
                .header("x-custom", "v"),
        );
        let head = convert_head(&p, &Limits::default()).unwrap();
        assert_eq!(head.method, Method::Post);
        assert_eq!(head.target.as_str(), "/a/b?q=1");
        assert_eq!(head.version, Version::Http11);
        assert_eq!(head.get_str(&HeaderId::Host), Some("a"));
        assert_eq!(head.path(), "/a/b");
        assert_eq!(head.query(), Some("q=1"));
    }

    #[test]
    fn preserves_absolute_form_targets() {
        let p = parts(
            ::hyper::http::Request::builder()
                .uri("http://a/x")
                .header("host", "a"),
        );
        let head = convert_head(&p, &Limits::default()).unwrap();
        assert_eq!(head.target.as_str(), "http://a/x");
    }

    #[test]
    fn too_many_headers_rejects_431() {
        let mut b = ::hyper::http::Request::builder()
            .uri("/")
            .header("host", "a");
        for i in 0..Limits::default().max_headers {
            b = b.header(format!("x-h-{i}"), "v");
        }
        assert_eq!(convert_head(&parts(b), &Limits::default()), Err(431));
    }

    #[test]
    fn cumulative_header_bytes_reject_431() {
        let limits = Limits {
            max_head_bytes: 64,
            ..Default::default()
        };
        let p = parts(
            ::hyper::http::Request::builder()
                .uri("/")
                .header("host", "a")
                .header("x-big", "v".repeat(100)),
        );
        assert_eq!(convert_head(&p, &limits), Err(431));
    }

    #[test]
    fn oversized_declared_body_rejects_413() {
        let limits = Limits {
            max_body_bytes: 4,
            ..Default::default()
        };
        let p = parts(
            ::hyper::http::Request::builder()
                .method("POST")
                .uri("/")
                .header("host", "a")
                .header("content-length", "10"),
        );
        assert_eq!(convert_head(&p, &limits), Err(413));
    }

    #[test]
    fn unknown_method_becomes_other() {
        let p = parts(
            ::hyper::http::Request::builder()
                .method("PURGE")
                .uri("/")
                .header("host", "a"),
        );
        let head = convert_head(&p, &Limits::default()).unwrap();
        assert!(matches!(head.method, Method::Other(ref t) if t.as_str() == "PURGE"));
    }
}
