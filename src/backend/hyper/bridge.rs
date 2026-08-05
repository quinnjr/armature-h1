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
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;

use super::body::{HyperBodyIo, HyperOutBody};
use super::{Phase, PhaseClock};

/// The fixed part of a request line: the SP after the method, the SP after the
/// target, `HTTP/1.1` (8 bytes, the same width for either version this crate
/// serves) and the terminating CRLF.
const REQUEST_LINE_FIXED: usize = 1 + 1 + 8 + 2;

/// The blank line that ends the header section.
const HEAD_TERMINATOR: usize = 2;

/// Convert a hyper request head into the bespoke `Head`, enforcing the parity
/// checks hyper itself does not (header count, cumulative head bytes,
/// declared body size). `Err` carries the rejection status.
pub(crate) fn convert_head(
    parts: &::hyper::http::request::Parts,
    limits: &Limits,
) -> Result<Head, u16> {
    // Defensive, and unreachable for `conn::http1` input: hyper's parser
    // rejects any other version during head parsing and writes its own `400`
    // before the service is called, so this rejection never fires under the
    // hyper backend. See BACKENDS.md, the parsing row for the status of an
    // unsupported HTTP version.
    let version = from_wire(parts.version).ok_or(505u16)?;

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

    // The wire request line, byte for byte: `METHOD SP target SP HTTP/1.x CRLF`.
    // The wire-exact cap on any *single* piece is hyper's `max_buf_size`; this
    // check exists so a head that is only *cumulatively* huge — no one piece
    // oversized — is still rejected at 431 parity with the native reader.
    //
    // One residual inexactness remains, and is recorded in BACKENDS.md: hyper
    // hands over field values already trimmed of optional whitespace, so a head
    // that padded its values carried a few more bytes than this counts. The
    // undercount is bounded by hyper's `max_buf_size`, which caps the head on
    // the wire regardless.
    let mut cumulative =
        parts.method.as_str().len() + target_str.len() + REQUEST_LINE_FIXED + HEAD_TERMINATOR;
    let mut headers = HeaderVec::new();
    let mut declared_len: Option<u64> = None;
    for (name, value) in parts.headers.iter() {
        // `name` + ": " + `value` + CRLF.
        cumulative += name.as_str().len() + 2 + value.as_bytes().len() + 2;
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

    Ok(Head::new(method, target, version, headers))
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
    /// Stamped onto every bridged request, matching the native loop's
    /// [`Request::peer`](crate::Request::peer). Taken from the accept call
    /// rather than from hyper, which does not carry the address.
    pub(crate) peer: Option<SocketAddr>,
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
        let peer = self.peer;

        Box::pin(async move {
            // The head is parsed by the time hyper calls us: everything from
            // here to the response is the native loop's handler/body window.
            phase.set(Phase::Handler);
            // Recorded before anything can fail so a 408 raised anywhere in
            // this window carries the request's version, as native's does.
            req_version.set(from_wire(req.version()).unwrap_or(Version::Http11));
            let on_upgrade = ::hyper::upgrade::on(&mut req);
            let (parts, incoming) = req.into_parts();

            let head = match convert_head(&parts, &cfg.limits) {
                Ok(h) => h,
                Err(status) => {
                    phase.set(Phase::Write);
                    let v = from_wire(parts.version).unwrap_or(Version::Http11);
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

            let resp = service.call(Request { head, body, peer }).await;

            // `fully_read` gates the handoff for the same reason it gates reuse
            // below, and on exactly the signal `conn::write_response` calls
            // `body_consumed`. Bytes of an unread request body are still on the
            // wire, and whatever hyper has buffered past the head reaches the
            // upgrade consumer as `Upgraded::buffered` — which that consumer is
            // documented to treat as the peer's first post-upgrade frames.
            // Handing it body bytes under that contract is the smuggling shape
            // pointed at the consumer instead of at the parser, so an unread
            // body forfeits the handoff; `force_close` then closes, as native
            // does.
            let upgrading = resp.status == 101 && wants_upgrade && fully_read.get();
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

/// The crate [`Version`] a hyper wire version denotes, or `None` for a version
/// this crate does not serve. The inverse of [`wire_version`], and the single
/// place the hyper -> crate direction of that mapping is written.
///
/// Callers split on what `None` means to them:
///
/// * `convert_head` turns it into a `505` — a request this crate cannot serve.
/// * The response paths fall back to [`Version::Http11`]. There, `None` means
///   there is no request version to echo (hyper parsed something it will not
///   name, or the head was rejected before a version was recovered), and the
///   native loop answers such a head with a `1.1` status line; matching it
///   keeps the two backends' error responses byte-identical.
fn from_wire(v: ::hyper::http::Version) -> Option<Version> {
    match v {
        ::hyper::http::Version::HTTP_11 => Some(Version::Http11),
        ::hyper::http::Version::HTTP_10 => Some(Version::Http10),
        _ => None,
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

    let status = ::hyper::http::StatusCode::from_u16(status).unwrap_or_else(|_| {
        // `http::StatusCode` has no representation outside `100..=999`, so the
        // response has to be degraded. Saying so is the difference between an
        // operator seeing a bug in their handler and seeing an unexplained 500.
        tracing::warn!(
            status,
            "handler status outside the valid range; degrading to 500"
        );
        ::hyper::http::StatusCode::INTERNAL_SERVER_ERROR
    });

    // Parity with the native writer: bodies on 204/304/1xx are dropped, not
    // written, so a handler mistake degrades identically under both backends.
    // The same predicate the native writer suppresses its own framing with.
    let body_forbidden = crate::write::forbids_body_framing(status.as_u16());
    let body = if body_forbidden {
        crate::service::ResponseBody::Empty
    } else {
        body
    };

    // Parity with `write::write_head`'s stale-`Content-Length` rule: a handler
    // length that disagrees with the body it framed is dropped, so the true
    // length is the one that reaches the wire. Under hyper the stakes are
    // higher than under the native writer, which merely reclaims framing:
    // hyper *honours* the field, truncating the body at a short length and
    // aborting the connection at a long one.
    //
    // `None` means no length a handler could have agreed with — a `Stream`,
    // whose length is not known until it ends — so any field is stale.
    let expected_len = match &body {
        crate::service::ResponseBody::Empty => Some(0u64),
        crate::service::ResponseBody::Full(b) => Some(b.len() as u64),
        crate::service::ResponseBody::Stream(_) => None,
    };
    // Exempt on exactly the status the native writer exempts, and no other:
    // `304`, where RFC 9110 section 15.4.5 makes the field descriptive of the
    // representation that *would* have been sent rather than a framing claim.
    // hyper writes no body there and picks the framing itself, so the
    // descriptive value cannot desynchronize anything.
    //
    // 204 and 1xx get no such exemption — RFC 9110 section 8.6 forbids the
    // field outright — even though their body was replaced with `Empty` just
    // above. Exempting them would let a `204` carry a handler
    // `Content-Length: 1234` under hyper that the native writer strips, and an
    // intermediary that honours the field then waits for a body that never
    // comes: a keep-alive desync reached through a backend divergence.
    let check_content_length = !crate::write::content_length_is_descriptive(status.as_u16());

    // hyper writes the status line — and picks its framing — from the
    // response's version, so it has to carry the request's.
    let mut builder = ::hyper::http::Response::builder()
        .status(status)
        .version(wire_version(version));
    let mut has_server = false;
    let mut has_connection = false;
    for (id, value) in headers.into_iter() {
        // `expected_len.is_none()` is tested first and on its own: a `Stream`
        // has no length any field could agree with, and an *unparseable* value
        // also reads as `None`, so comparing the two directly would let
        // `content-length: abc` through on a stream body — the one combination
        // where hyper has neither a length of its own to fall back on nor a
        // reason to trust the field.
        if check_content_length
            && id == HeaderId::ContentLength
            && (expected_len.is_none() || crate::write::parse_len(&value) != expected_len)
        {
            tracing::warn!("dropping response content-length that disagrees with the body length");
            continue;
        }
        // Always dropped. hyper picks the response's framing from the body's
        // `SizeHint` and writes the framing field itself, so a handler
        // `Transfer-Encoding` is at best a duplicate of what hyper is about to
        // write and at worst a coding hyper is not applying. The native writer
        // reaches the same place from the other direction — it never duplicates
        // a handler field — so exactly one framing field is written either way.
        if id == HeaderId::TransferEncoding {
            continue;
        }
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
        // Dropped rather than written, for the reason the native writer gives:
        // there is no error left to return once a response is being built, and
        // dropping is the only outcome that does not put attacker-chosen bytes
        // on the wire. The message is the native writer's word for word so log
        // alerting on it fires the same under either backend.
        let Ok(name) = ::hyper::http::HeaderName::from_bytes(id.as_str().as_bytes()) else {
            tracing::warn!(
                field = id.as_str(),
                "dropping response header with an invalid field name or value"
            );
            continue;
        };
        let Ok(value) = ::hyper::http::HeaderValue::from_maybe_shared(value) else {
            tracing::warn!(
                field = id.as_str(),
                "dropping response header with an invalid field name or value"
            );
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
    // The same decision the native writer emits from, taken from the same
    // function rather than restated here, so the two backends cannot drift: a
    // handler that set `Connection` itself owns the field; HTTP/1.1
    // persistence is the default and saying so would be noise; HTTP/1.0
    // persistence has to be stated, because hyper closes a 1.0 connection
    // whose response does not; and a connection that will not persist says so,
    // which hyper leaves implicit on 1.0.
    if !has_connection && let Some(field) = crate::write::connection_field(version, keep_alive) {
        builder = builder.header(
            ::hyper::http::header::CONNECTION,
            ::hyper::http::HeaderValue::from_static(field),
        );
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
        assert_eq!(head.target().as_str(), "/a/b?q=1");
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
        assert_eq!(head.target().as_str(), "http://a/x");
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

    /// The cumulative count is the request line and header section byte for
    /// byte, so a head that fits must not be rejected and one that does not
    /// must be. The head below is exactly 41 bytes on the wire:
    /// `GET / HTTP/1.1\r\n` (16) + `host: a\r\n` (9) + `x: yy\r\n` (7) +
    /// `\r\n` (2) — 34.
    #[test]
    fn cumulative_head_bytes_are_counted_exactly() {
        let build = || {
            parts(
                ::hyper::http::Request::builder()
                    .method("GET")
                    .uri("/")
                    .header("host", "a")
                    .header("x", "yy"),
            )
        };
        const WIRE: usize = 16 + 9 + 7 + 2;
        let at_cap = Limits {
            max_head_bytes: WIRE,
            ..Default::default()
        };
        assert!(
            convert_head(&build(), &at_cap).is_ok(),
            "a head of exactly {WIRE} bytes is within a {WIRE}-byte cap"
        );
        let under_cap = Limits {
            max_head_bytes: WIRE - 1,
            ..Default::default()
        };
        assert_eq!(convert_head(&build(), &under_cap), Err(431));
    }

    /// Convert with the connection persisting, the common case.
    fn conv(resp: crate::service::Response, version: Version) -> BridgeResponse {
        conv_full(resp, version, true, None)
    }

    /// Convert with every input the conversion actually branches on exposed:
    /// `keep_alive` gates the forced-close arm and the `Connection` table, and
    /// `server_name` gates the default `Server` field.
    fn conv_full(
        resp: crate::service::Response,
        version: Version,
        keep_alive: bool,
        server_name: Option<Bytes>,
    ) -> BridgeResponse {
        let cfg = ConnConfig {
            limits: Limits::default(),
            tick: std::time::Duration::from_millis(10),
            server_name,
        };
        let clock = Rc::new(PhaseClock::new(Phase::Write));
        convert_response(resp, &cfg, version, clock, keep_alive)
    }

    fn header_of(r: &BridgeResponse, name: &str) -> Option<String> {
        r.headers()
            .get(name)
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
    }

    /// A handler `Content-Length` that disagrees with the body must not reach
    /// hyper: hyper *honours* the field, truncating the body at a short length
    /// and aborting the connection at a long one. Dropping it puts hyper's
    /// `SizeHint`-derived true length on the wire instead.
    #[test]
    fn a_wrong_handler_content_length_is_dropped() {
        let resp = crate::service::Response::new(200)
            .header(HeaderId::ContentLength, Bytes::from_static(b"5"))
            .with_body(crate::service::ResponseBody::Full(Bytes::from_static(
                b"hello world",
            )));
        let out = conv(resp, Version::Http11);
        assert_eq!(header_of(&out, "content-length"), None);
    }

    /// A handler telling the truth stays in charge of its own framing, exactly
    /// as it does under the native writer.
    #[test]
    fn a_correct_handler_content_length_is_preserved() {
        let resp = crate::service::Response::new(200)
            .header(HeaderId::ContentLength, Bytes::from_static(b"11"))
            .with_body(crate::service::ResponseBody::Full(Bytes::from_static(
                b"hello world",
            )));
        let out = conv(resp, Version::Http11);
        assert_eq!(header_of(&out, "content-length").as_deref(), Some("11"));
    }

    #[test]
    fn a_content_length_on_an_empty_body_survives_only_at_zero() {
        let zero = crate::service::Response::new(200)
            .header(HeaderId::ContentLength, Bytes::from_static(b"0"));
        assert_eq!(
            header_of(&conv(zero, Version::Http11), "content-length").as_deref(),
            Some("0")
        );
        let nonzero = crate::service::Response::new(200)
            .header(HeaderId::ContentLength, Bytes::from_static(b"7"));
        assert_eq!(
            header_of(&conv(nonzero, Version::Http11), "content-length"),
            None
        );
    }

    /// A stream's length is not known until it ends, so no handler value can
    /// agree with it. The native writer's `OutBody::Chunked` arm says the same.
    #[test]
    fn a_content_length_on_a_stream_body_is_always_dropped() {
        struct Never;
        impl crate::service::futures_stream::Stream for Never {
            fn poll_next(
                self: Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Result<Bytes, crate::BodyError>>> {
                std::task::Poll::Ready(None)
            }
        }
        let resp = crate::service::Response::new(200)
            .header(HeaderId::ContentLength, Bytes::from_static(b"3"))
            .with_body(crate::service::ResponseBody::Stream(Box::pin(Never)));
        assert_eq!(
            header_of(&conv(resp, Version::Http11), "content-length"),
            None
        );
    }

    /// An unparseable value is not a length the body can agree with, and on a
    /// `Stream` the expected length is `None` too — so the two must not be
    /// compared to each other. hyper has no length of its own to fall back on
    /// for a stream, which makes passing `abc` through the worst outcome.
    #[test]
    fn an_unparseable_content_length_on_a_stream_body_is_dropped() {
        struct Never;
        impl crate::service::futures_stream::Stream for Never {
            fn poll_next(
                self: Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Result<Bytes, crate::BodyError>>> {
                std::task::Poll::Ready(None)
            }
        }
        let resp = crate::service::Response::new(200)
            .header(HeaderId::ContentLength, Bytes::from_static(b"abc"))
            .with_body(crate::service::ResponseBody::Stream(Box::pin(Never)));
        assert_eq!(
            header_of(&conv(resp, Version::Http11), "content-length"),
            None
        );
    }

    /// The exemption is `304` and nothing else, exactly as in `write.rs`'s
    /// `a_handler_content_length_survives_on_304_but_not_204`. A `304` may
    /// carry a `Content-Length` describing the representation that would have
    /// been sent; a `204` may not carry the field at all, and letting one
    /// through here where the native writer strips it is a backend divergence
    /// an intermediary can read as a body that never arrives.
    #[test]
    fn a_handler_content_length_survives_on_304_but_not_204() {
        let ok = crate::service::Response::new(304)
            .header(HeaderId::ContentLength, Bytes::from_static(b"1234"));
        assert_eq!(
            header_of(&conv(ok, Version::Http11), "content-length").as_deref(),
            Some("1234")
        );
        let bad = crate::service::Response::new(204)
            .header(HeaderId::ContentLength, Bytes::from_static(b"1234"));
        assert_eq!(
            header_of(&conv(bad, Version::Http11), "content-length"),
            None
        );
    }

    /// A status that forbids a body drops the handler's body, not just its
    /// framing: hyper would otherwise write bytes after a head that promises
    /// none. `size_hint().exact()` is what hyper reads the length off.
    #[test]
    fn a_body_on_a_204_is_replaced_with_an_empty_one() {
        use ::hyper::body::Body as _;
        let resp = crate::service::Response::new(204).with_body(
            crate::service::ResponseBody::Full(Bytes::from_static(b"hello world")),
        );
        let out = conv(resp, Version::Http11);
        assert_eq!(out.body().size_hint().exact(), Some(0));
    }

    /// A forced close is not the handler's to override. hyper decides reuse
    /// from the response's `Connection` field, so a handler `keep-alive` on a
    /// connection the native loop closes unconditionally has to be dropped —
    /// and the table below then states the close.
    #[test]
    fn a_handler_connection_is_dropped_when_the_connection_must_close() {
        let resp = crate::service::Response::new(200)
            .header(HeaderId::Connection, Bytes::from_static(b"keep-alive"));
        let out = conv_full(resp, Version::Http11, false, None);
        assert_eq!(header_of(&out, "connection").as_deref(), Some("close"));
    }

    /// HTTP/1.0 persistence has to be stated: hyper closes a 1.0 connection
    /// whose response does not say otherwise, and the native writer emits
    /// exactly this field for the same case.
    #[test]
    fn http10_keep_alive_is_stated_and_http11_is_not() {
        let out = conv_full(
            crate::service::Response::new(200),
            Version::Http10,
            true,
            None,
        );
        assert_eq!(header_of(&out, "connection").as_deref(), Some("keep-alive"));
        let out = conv_full(
            crate::service::Response::new(200),
            Version::Http11,
            true,
            None,
        );
        assert_eq!(header_of(&out, "connection"), None);
    }

    /// The configured `Server` fills in only where the handler left the field
    /// open, matching the native writer's `has_server` suppression.
    #[test]
    fn the_configured_server_name_fills_in_but_never_overrides() {
        let name = Some(Bytes::from_static(b"armature-h1"));
        let out = conv_full(
            crate::service::Response::new(200),
            Version::Http11,
            true,
            name.clone(),
        );
        assert_eq!(header_of(&out, "server").as_deref(), Some("armature-h1"));
        let resp = crate::service::Response::new(200)
            .header(HeaderId::Server, Bytes::from_static(b"handler"));
        let out = conv_full(resp, Version::Http11, true, name);
        assert_eq!(header_of(&out, "server").as_deref(), Some("handler"));
    }

    /// hyper chooses framing from the body's `SizeHint` and writes the field
    /// itself, so any handler `Transfer-Encoding` is a duplicate or a lie.
    #[test]
    fn a_handler_transfer_encoding_is_never_echoed() {
        let resp = crate::service::Response::new(200)
            .header(HeaderId::TransferEncoding, Bytes::from_static(b"chunked"))
            .with_body(crate::service::ResponseBody::Full(Bytes::from_static(
                b"hi",
            )));
        let out = conv(resp, Version::Http11);
        assert_eq!(header_of(&out, "transfer-encoding"), None);
    }

    /// `http::StatusCode` has no representation for a status outside
    /// `100..=999`, so the bridge degrades to `500`. See BACKENDS.md.
    #[test]
    fn an_out_of_range_handler_status_becomes_500() {
        assert!(
            ::hyper::http::StatusCode::from_u16(1000).is_err(),
            "1000 is the case under test"
        );
        let out = conv(crate::service::Response::new(1000), Version::Http11);
        assert_eq!(out.status().as_u16(), 500);
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
