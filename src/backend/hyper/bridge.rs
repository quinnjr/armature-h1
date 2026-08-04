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
use super::{Phase, PhaseClock};

/// Approximate request-line overhead ("METHOD  HTTP/1.1\r\n" scaffolding) for
/// the cumulative head-bytes cap. The wire-exact cap is enforced by hyper's
/// `max_buf_size`; this check exists so a head that is *cumulatively* huge
/// without any single oversized piece is still rejected at 431 parity.
const HEAD_OVERHEAD: usize = 26;

/// Convert a hyper request head into the bespoke `Head`, enforcing the parity
/// checks hyper itself does not (header count, cumulative head bytes,
/// declared body size). `Err` carries the rejection status.
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
        // Read, not validated: hyper has already parsed and checked this field
        // (rejecting a non-numeric, negative or conflicting `Content-Length`
        // during head parsing and writing its own `400`), so a value reaching
        // here is well-formed. This is only where the *declared* size is
        // recovered for the `max_body_bytes` check below, and a parse that
        // somehow fails simply leaves the check to the body reader. The `trim`
        // is defensive against surrounding whitespace, not an enforcement
        // point.
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
pub(crate) struct Bridge<S> {
    pub(crate) service: Rc<S>,
    pub(crate) cfg: Rc<ConnConfig>,
    /// Filled only when a 101 on a request that asked for an upgrade goes out.
    pub(crate) upgrade_slot: Rc<RefCell<Option<::hyper::upgrade::OnUpgrade>>>,
    pub(crate) sent_101: Rc<Cell<bool>>,
    /// The watchdog's phase clock: `Handler` while the handler runs, `Write`
    /// once a response is handed to hyper.
    pub(crate) phase: Rc<PhaseClock>,
    /// The version of the request being served, for the bare 408 the watchdog
    /// writes on a body-phase expiry: the native loop echoes the request's
    /// version there (`conn.rs`, `write_error(version, 408)`).
    pub(crate) req_version: Rc<Cell<Version>>,
}

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
        let phase = self.phase.clone();
        let req_version = self.req_version.clone();

        Box::pin(async move {
            // The head is parsed by the time hyper calls us: everything from
            // here to the response is the native loop's handler/body window.
            phase.set(Phase::Handler);
            // Recorded before anything can fail so a 408 raised anywhere in
            // this window carries the request's version, as native's does.
            req_version.set(match req.version() {
                ::hyper::http::Version::HTTP_10 => Version::Http10,
                _ => Version::Http11,
            });
            let on_upgrade = ::hyper::upgrade::on(&mut req);
            let (parts, incoming) = req.into_parts();

            let head = match convert_head(&parts, &cfg.limits) {
                Ok(h) => h,
                Err(status) => {
                    phase.set(Phase::Write);
                    // Anything hyper parsed as neither 1.0 nor 1.1 gets a 1.1
                    // status line, exactly as the native loop answers a head it
                    // could not version.
                    let v = match parts.version {
                        ::hyper::http::Version::HTTP_10 => Version::Http10,
                        _ => Version::Http11,
                    };
                    return Ok(reject(status, v, phase));
                }
            };

            // Reuse the bespoke framing decision table so anything hyper let
            // through is still rejected exactly where the native stack would.
            let kind = match crate::framing::decide(&head, &cfg.limits) {
                Ok(k) => k,
                Err(e) => {
                    phase.set(Phase::Write);
                    return Ok(reject(e.status(), head.version, phase));
                }
            };

            let wants_upgrade =
                head.count(&HeaderId::Upgrade) > 0 && head.connection_has_token("upgrade");
            let expects_continue = head
                .get_str(&HeaderId::Expect)
                .is_some_and(|v| v.eq_ignore_ascii_case("100-continue"));

            // Captured before `head` moves into the `Request`: hyper writes the
            // status line from the response's own version, so a 1.0 request
            // whose response defaulted to 1.1 would both mislabel the status
            // line and let hyper apply 1.1 framing (chunked) to a 1.0 peer.
            let version = head.version;
            // Also captured before the move: hyper forces `Encoder::length(0)`
            // for a `HEAD` response and never polls the response body, so the
            // body's own end-of-stream signal can never arrive. See below.
            let is_head_request = head.method == Method::Head;
            // hyper only keeps a 1.0 connection alive if the response says so
            // explicitly, and the native writer emits exactly this field for
            // the same case (`write::write_head`, Http10 + keep_alive).
            let keep_alive = head.is_keep_alive();

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

            // The handler window is over. Response writing is hyper's, under
            // `write_timeout` (see BACKENDS.md on write_timeout granularity).
            phase.set(Phase::Write);
            // For a `HEAD` response hyper writes the head and closes the
            // message out without ever polling the body, so `HyperOutBody`
            // would never report end-of-stream and the clock would never leave
            // `Phase::Write` — parking a keep-alive connection under
            // `write_timeout` instead of `idle_timeout`. hyper's behaviour here
            // is guaranteed (it forces a zero-length encoder for `HEAD`), so
            // signalling end-of-stream on its behalf is exact, not a guess.
            // Must follow `set(Phase::Write)`: entering `Handler` or `Idle`
            // clears the flag.
            if is_head_request {
                phase.note_body_end();
            }
            Ok(convert_response(
                resp,
                &cfg,
                version,
                phase,
                keep_alive && !force_close,
            ))
        })
    }
}

/// The wire version hyper should write this response's status line in.
fn wire_version(version: Version) -> ::hyper::http::Version {
    match version {
        Version::Http10 => ::hyper::http::Version::HTTP_10,
        Version::Http11 => ::hyper::http::Version::HTTP_11,
    }
}

/// An empty rejection response that also closes the connection, matching the
/// native rule that any framing rejection closes.
fn reject(status: u16, version: Version, clock: Rc<PhaseClock>) -> BridgeResponse {
    let mut r = ::hyper::http::Response::builder()
        .status(::hyper::http::StatusCode::from_u16(status).expect("known status"))
        .version(wire_version(version))
        .body(HyperOutBody::new(
            crate::service::ResponseBody::Empty,
            clock,
        ))
        .expect("static response");
    r.headers_mut().insert(
        ::hyper::http::header::CONNECTION,
        ::hyper::http::HeaderValue::from_static("close"),
    );
    r
}

fn convert_response(
    resp: crate::service::Response,
    cfg: &ConnConfig,
    version: Version,
    clock: Rc<PhaseClock>,
    // Whether the connection persists after this response — the native writer's
    // `keep_alive`: the request asked for it *and* nothing (an unread body, a
    // rejection) forces a close.
    keep_alive: bool,
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

    // hyper writes the status line — and picks its framing — from the
    // response's version, so it has to carry the request's.
    let mut builder = ::hyper::http::Response::builder()
        .status(status)
        .version(wire_version(version));
    let mut has_server = false;
    let mut has_connection = false;
    for (id, value) in headers.into_iter() {
        // A forced close is not the handler's field to override here. hyper
        // decides whether to reuse the connection from the response's
        // `Connection` field, so honouring a handler `keep-alive` on a request
        // whose body went unread would reuse a connection the native loop
        // closes unconditionally (it closes on `body_consumed` regardless of
        // what the handler wrote). Dropping the handler's value is the only
        // lever the bridge has over hyper's accounting.
        if id == HeaderId::Connection && !keep_alive {
            continue;
        }
        let Ok(name) = ::hyper::http::HeaderName::from_bytes(id.as_str().as_bytes()) else {
            continue;
        };
        let Ok(value) = ::hyper::http::HeaderValue::from_maybe_shared(value) else {
            continue;
        };
        // Only a field that survived validation suppresses the one below: the
        // native writer's `emitted` applies the same writability filter, so a
        // handler value hyper rejects must not leave the response with no
        // `Connection` field at all.
        if id == HeaderId::Server {
            has_server = true;
        }
        if id == HeaderId::Connection {
            has_connection = true;
        }
        builder = builder.header(name, value);
    }
    if !has_server
        && let Some(name) = &cfg.server_name
        && let Ok(v) = ::hyper::http::HeaderValue::from_maybe_shared(name.clone())
    {
        builder = builder.header(::hyper::http::header::SERVER, v);
    }
    // The same table `write::write_head` uses, so the field matches the native
    // writer's byte for byte: a handler that set `Connection` itself owns the
    // field; HTTP/1.1 persistence is the default and saying so would be noise;
    // HTTP/1.0 persistence has to be stated, because hyper closes a 1.0
    // connection whose response does not; and a connection that will not
    // persist says so, which hyper leaves implicit on 1.0.
    if !has_connection {
        let field = match (version, keep_alive) {
            (Version::Http11, true) => None,
            (Version::Http10, true) => Some("keep-alive"),
            (_, false) => Some("close"),
        };
        if let Some(field) = field {
            builder = builder.header(
                ::hyper::http::header::CONNECTION,
                ::hyper::http::HeaderValue::from_static(field),
            );
        }
    }

    builder
        .body(HyperOutBody::new(body, clock))
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
