//! RFC 9110/9111/9112 conformance, over real sockets.
//!
//! The pure functions are unit-tested where they live. This suite exists to prove
//! the *wiring*: that a rejection `framing::decide` reaches actually produces the
//! right bytes on the wire, and actually closes the connection.
//!
//! A raw `TcpStream` is used rather than an HTTP client library, because a client
//! would normalize away the very malformations under test.
//!
//! Every `_close` assertion checks **both** the status and that the socket closed.
//! A correct status on a connection left open is still a smuggling vector, so
//! asserting only the status would let the real defect through.

use armature_h1::{Config, Limits, Request, Response, Server};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The outcome of one raw exchange.
struct Exchange {
    /// Everything the server wrote.
    body: String,
    /// Whether the server closed the connection.
    closed: bool,
}

impl Exchange {
    fn status(&self) -> Option<u16> {
        self.body
            .strip_prefix("HTTP/1.1 ")
            .or_else(|| self.body.strip_prefix("HTTP/1.0 "))
            .and_then(|rest| rest.get(..3))
            .and_then(|s| s.parse().ok())
    }

    /// Assert this exchange was rejected with `status` and the socket closed.
    fn assert_rejected_and_closed(&self, status: u16) {
        assert_eq!(
            self.status(),
            Some(status),
            "expected {status}, got: {:?}",
            &self.body[..self.body.len().min(200)]
        );
        assert!(
            self.closed,
            "connection must close after a rejection; leaving it open is the \
             smuggling vector: {:?}",
            &self.body[..self.body.len().min(200)]
        );
        assert!(
            self.body.contains("connection: close"),
            "a rejection must advertise the close: {:?}",
            &self.body[..self.body.len().min(200)]
        );
    }

    fn responses(&self) -> usize {
        self.body.matches("HTTP/1.").count()
    }
}

async fn echo(mut req: Request) -> Response {
    match req.body.collect(1024 * 1024).await {
        Ok(b) => {
            let mut r = Response::new(200);
            r.body = armature_h1::ResponseBody::Full(b);
            r
        }
        Err(e) => Response::status_only(e.status()),
    }
}

fn config(limits: Limits) -> Config {
    Config::new(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
        .workers(1)
        .limits(limits)
        .pin_cores(false)
}

fn quick_limits() -> Limits {
    Limits {
        // Short enough that a timeout test finishes fast, long enough that a
        // healthy request is never a flake.
        idle_timeout: Duration::from_millis(400),
        header_timeout: Duration::from_millis(400),
        body_timeout: Duration::from_millis(400),
        ..Default::default()
    }
}

/// Send `raw` on a fresh connection and collect the reply.
fn raw_exchange_with(limits: Limits, raw: &'static [u8]) -> Exchange {
    raw_exchange_slow(limits, vec![raw.to_vec()], Duration::ZERO)
}

fn raw_exchange(raw: &'static [u8]) -> Exchange {
    raw_exchange_with(quick_limits(), raw)
}

/// Send `pieces` with `gap` between them to a server built from `make`, then
/// read the reply to EOF.
///
/// Generalized over the service so tests that need a handler other than
/// `echo` still get the bind/thread/runtime/shutdown/join wiring — and the
/// `Exchange::closed` signal — for free, instead of open-coding their own
/// copy of it.
fn raw_exchange_with_service<S>(
    limits: Limits,
    make: impl Fn() -> S + Send + Clone + 'static,
    pieces: Vec<Vec<u8>>,
    gap: Duration,
) -> Exchange
where
    S: armature_h1::H1Service + 'static,
{
    let server = Server::bind(config(limits)).expect("bind");
    let addr = server.local_addr();
    let handle = server.handle();

    let client = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
            for (i, piece) in pieces.iter().enumerate() {
                if i > 0 && !gap.is_zero() {
                    tokio::time::sleep(gap).await;
                }
                if s.write_all(piece).await.is_err() {
                    // The server may have rejected and closed already, which is a
                    // valid outcome rather than a test failure.
                    break;
                }
            }
            let mut buf = Vec::new();
            // read_to_end returning Ok means the server closed its side.
            let closed = matches!(
                tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut buf)).await,
                Ok(Ok(_))
            );
            Exchange {
                body: String::from_utf8_lossy(&buf).into_owned(),
                closed,
            }
        });
        handle.shutdown();
        out
    });

    server.serve(make).expect("serve");
    client.join().expect("client thread")
}

/// Send `pieces` with `gap` between them, then read the reply to EOF.
fn raw_exchange_slow(limits: Limits, pieces: Vec<Vec<u8>>, gap: Duration) -> Exchange {
    raw_exchange_with_service(limits, || echo, pieces, gap)
}

// ============================ framing ============================

/// RFC 9112 section 6.1: both present is unresolvable ambiguity.
#[test]
fn content_length_and_transfer_encoding_400_close() {
    raw_exchange(
        b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\nhello",
    )
    .assert_rejected_and_closed(400);
}

#[test]
fn conflicting_content_length_400_close() {
    raw_exchange(
        b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\nhello",
    )
    .assert_rejected_and_closed(400);
}

#[test]
fn comma_list_content_length_conflict_400_close() {
    raw_exchange(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5, 6\r\n\r\nhello")
        .assert_rejected_and_closed(400);
}

#[test]
fn malformed_content_length_400_close() {
    raw_exchange(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: abc\r\n\r\nhello")
        .assert_rejected_and_closed(400);
}

#[test]
fn chunked_not_final_400_close() {
    raw_exchange(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked, gzip\r\n\r\n")
        .assert_rejected_and_closed(400);
}

#[test]
#[cfg_attr(
    feature = "hyper-backend",
    ignore = "see BACKENDS.md: status for an unsupported transfer coding \
              (hyper rejects at parse with 400, before framing::decide runs)"
)]
fn unsupported_transfer_coding_501_close() {
    raw_exchange(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip\r\n\r\n")
        .assert_rejected_and_closed(501);
}

/// Positive twin of `unsupported_transfer_coding_501_close` for the hyper
/// backend: BACKENDS.md documents hyper rejecting an unsupported transfer
/// coding with `400` at head-parse time, before `framing::decide` runs. A
/// hyper version that starts answering this `501`, or with something else
/// entirely, should fail the build rather than pass an ignored test silently.
#[test]
#[cfg(feature = "hyper-backend")]
fn hyper_unsupported_transfer_coding_400_close() {
    raw_exchange(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip\r\n\r\n")
        .assert_rejected_and_closed(400);
}

/// RFC 9112 section 6.1: a server must not reuse a connection after receiving
/// `Transfer-Encoding` in an HTTP/1.0 request. Accepting it is the TE-downgrade
/// smuggling vector — a hop that reads the body as unframed while this one reads
/// it as chunked.
#[test]
fn transfer_encoding_on_http_10_400_close() {
    raw_exchange(b"POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n")
        .assert_rejected_and_closed(400);
}

#[test]
fn missing_host_400_close() {
    raw_exchange(b"GET / HTTP/1.1\r\n\r\n").assert_rejected_and_closed(400);
}

#[test]
fn multiple_host_400_close() {
    raw_exchange(b"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n").assert_rejected_and_closed(400);
}

/// RFC 9112 section 6.3 permits treating identical duplicates as one.
#[test]
fn identical_duplicate_content_length_accepted() {
    let e = raw_exchange(
        b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
    );
    assert_eq!(e.status(), Some(200), "{}", e.body);
    assert!(e.body.ends_with("hello"), "{}", e.body);
}

#[test]
fn chunked_request_round_trips() {
    let e = raw_exchange(
        b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
    );
    assert_eq!(e.status(), Some(200), "{}", e.body);
    assert!(e.body.ends_with("hello"), "{}", e.body);
}

#[test]
fn chunked_with_trailers_round_trips() {
    let e = raw_exchange(
        b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\nEtag: v1\r\n\r\n",
    );
    assert_eq!(e.status(), Some(200), "{}", e.body);
    assert!(e.body.ends_with("hello"), "{}", e.body);
}

/// RFC 9110 section 6.5.1. Framing was decided before the trailer section was
/// read, so honoring one from there is a smuggling vector.
#[test]
fn forbidden_trailer_rejected() {
    let e = raw_exchange(
        b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nContent-Length: 99\r\n\r\n",
    );
    assert_eq!(e.status(), Some(400), "{}", e.body);
    assert!(e.closed, "must close: {}", e.body);
}

// ============================ syntax ============================

#[test]
fn bare_cr_400_close() {
    raw_exchange(b"GET / HTTP/1.1\rHost: a\r\n\r\n").assert_rejected_and_closed(400);
}

/// RFC 9112 section 2.2 permits accepting a bare LF. This crate declines:
/// leniency that differs from a peer's is the smuggling vector.
#[test]
#[cfg_attr(
    feature = "hyper-backend",
    ignore = "see BACKENDS.md: bare LF as a head line terminator \
              (httparse accepts it and hyper offers no strictness knob)"
)]
fn bare_lf_400_close() {
    raw_exchange(b"GET / HTTP/1.1\nHost: a\r\n\r\n").assert_rejected_and_closed(400);
}

/// Positive twin of `bare_lf_400_close` for the hyper backend: BACKENDS.md
/// documents httparse accepting a bare LF as a head line terminator, with no
/// server-side knob to turn that off, so hyper serves this request `200`
/// instead of rejecting it. If a future httparse/hyper exposes strictness
/// here and this starts failing, that is exactly the signal the ignored
/// original test is watching for.
#[test]
#[cfg(feature = "hyper-backend")]
fn hyper_bare_lf_accepted() {
    let e = raw_exchange(b"GET / HTTP/1.1\nHost: a\r\n\r\n");
    assert_eq!(e.status(), Some(200), "{}", e.body);
}

/// RFC 9112 section 5.2.
#[test]
fn obs_fold_400_close() {
    raw_exchange(b"GET / HTTP/1.1\r\nHost: a\r\n b\r\n\r\n").assert_rejected_and_closed(400);
}

/// RFC 9112 section 5.1.
#[test]
fn whitespace_before_colon_400_close() {
    raw_exchange(b"GET / HTTP/1.1\r\nHost : a\r\n\r\n").assert_rejected_and_closed(400);
}

#[test]
fn bad_request_line_400_close() {
    raw_exchange(b"GET\r\nHost: a\r\n\r\n").assert_rejected_and_closed(400);
}

#[test]
#[cfg_attr(
    feature = "hyper-backend",
    ignore = "see BACKENDS.md: status for an unsupported HTTP version \
              (hyper's on_error maps Parse::Version to 400, not 505)"
)]
fn http_12_505_close() {
    raw_exchange(b"GET / HTTP/1.2\r\nHost: a\r\n\r\n").assert_rejected_and_closed(505);
}

/// Positive twin of `http_12_505_close` for the hyper backend: BACKENDS.md
/// documents `Server::on_error` mapping `Parse::Version` to `400` rather than
/// `505`, and that the mapping is not configurable. If a hyper release
/// starts writing `505` for this, the ignored original passes and this one
/// should be the thing that notices first.
#[test]
#[cfg(feature = "hyper-backend")]
fn hyper_http_12_400_close() {
    raw_exchange(b"GET / HTTP/1.2\r\nHost: a\r\n\r\n").assert_rejected_and_closed(400);
}

#[test]
fn absolute_form_target_accepted() {
    let e = raw_exchange(
        b"GET http://a.example/x HTTP/1.1\r\nHost: a.example\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(e.status(), Some(200), "{}", e.body);
}

/// RFC 9110 section 7.1: the fragment is not part of the request target, and no
/// RFC 9112 section 3.2 form contains one. Accepting it would route on bytes an
/// upstream hop would have stripped.
#[test]
#[cfg_attr(
    feature = "hyper-backend",
    ignore = "see BACKENDS.md: fragment in the request target \
              (http::uri truncates at '#' before the bridge sees the target)"
)]
fn fragment_in_target_400_close() {
    raw_exchange(b"GET /a#frag HTTP/1.1\r\nHost: a\r\n\r\n").assert_rejected_and_closed(400);
}

/// Positive twin of `fragment_in_target_400_close` for the hyper backend:
/// BACKENDS.md documents `http::uri::PathAndQuery::from_shared` truncating
/// at `#` before hyper builds the `Request`, so the bridge never sees the
/// fragment and the request is served on the truncated target. The handler
/// echoes the target it actually received, so this pins the truncation
/// itself, not just the status code.
#[test]
#[cfg(feature = "hyper-backend")]
fn hyper_fragment_in_target_truncated_and_served() {
    async fn echo_target(req: Request) -> Response {
        let mut r = Response::new(200);
        r.body = armature_h1::ResponseBody::Full(bytes::Bytes::copy_from_slice(
            req.head.target().as_bytes(),
        ));
        r
    }

    let e = raw_exchange_with_service(
        quick_limits(),
        || echo_target,
        vec![b"GET /a#frag HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec()],
        Duration::ZERO,
    );

    assert_eq!(e.status(), Some(200), "{}", e.body);
    assert!(
        e.body.ends_with("/a"),
        "the fragment must be gone by the time the handler sees the target: {}",
        e.body
    );
}

#[test]
fn asterisk_form_options_accepted() {
    let e = raw_exchange(b"OPTIONS * HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
    assert_eq!(e.status(), Some(200), "{}", e.body);
}

// ============================ limits ============================

#[test]
fn oversized_head_431_close() {
    let limits = Limits {
        max_head_bytes: 60,
        ..quick_limits()
    };
    let long = b"GET /aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\nHost: a\r\n\r\n";
    raw_exchange_with(limits, long).assert_rejected_and_closed(431);
}

#[test]
fn too_many_headers_431_close() {
    let limits = Limits {
        max_headers: 2,
        ..quick_limits()
    };
    raw_exchange_with(
        limits,
        b"GET / HTTP/1.1\r\nHost: a\r\nA: 1\r\nB: 2\r\nC: 3\r\n\r\n",
    )
    .assert_rejected_and_closed(431);
}

#[test]
fn oversized_declared_body_413_close() {
    let limits = Limits {
        max_body_bytes: 2,
        ..quick_limits()
    };
    raw_exchange_with(
        limits,
        b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello",
    )
    .assert_rejected_and_closed(413);
}

#[test]
fn oversized_chunked_body_413() {
    let limits = Limits {
        max_body_bytes: 2,
        ..quick_limits()
    };
    let e = raw_exchange_with(
        limits,
        b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
    );
    assert_eq!(e.status(), Some(413), "{}", e.body);
    assert!(e.closed, "must close: {}", e.body);
}

/// Slowloris: a head that starts and stalls must not hold the connection.
#[test]
fn header_timeout_408_close() {
    let e = raw_exchange_slow(
        quick_limits(),
        vec![b"GET / HTTP/1.1\r\nHost: a\r\n".to_vec(), b"\r\n".to_vec()],
        Duration::from_millis(1200),
    );
    assert_eq!(e.status(), Some(408), "{}", e.body);
    assert!(e.closed, "must close: {}", e.body);
}

/// The body deadline must actually fire, not merely be armed.
///
/// Asserting only `closed` would pass with `body_timeout` entirely unenforced:
/// the remaining bytes eventually arrive, the echo handler completes, and the
/// connection then closes on the *idle* deadline instead. The status is the
/// discriminator — an unenforced body deadline yields 200 with `hello`.
#[test]
fn body_timeout_408_close() {
    // Declares five body bytes, sends three, then stalls past the body deadline.
    let e = raw_exchange_slow(
        quick_limits(),
        vec![
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhel".to_vec(),
            b"lo".to_vec(),
        ],
        Duration::from_millis(1200),
    );
    e.assert_rejected_and_closed(408);
    assert_eq!(
        e.responses(),
        1,
        "the request must not also be served once the rest arrives: {}",
        e.body
    );
    assert!(
        !e.body.contains("hello"),
        "the reply precedes the stalled bytes, so the handler never echoed them: {}",
        e.body
    );
}

/// An idle keep-alive connection is closed silently: no response is owed.
#[test]
fn idle_timeout_closes_without_response() {
    let e = raw_exchange_slow(quick_limits(), vec![Vec::new()], Duration::ZERO);
    assert!(e.closed, "idle connection must be closed");
    assert!(
        e.body.is_empty(),
        "no response is owed on an idle close: {}",
        e.body
    );
}

// ============================ semantics ============================

#[test]
fn keep_alive_serves_sequential_requests() {
    let e = raw_exchange(
        b"GET /a HTTP/1.1\r\nHost: a\r\n\r\nGET /b HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(e.responses(), 2, "{}", e.body);
}

#[test]
fn pipelined_requests_answered_in_order() {
    let e = raw_exchange(
        b"POST /1 HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\n1\
          POST /2 HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\n2\
          POST /3 HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nConnection: close\r\n\r\n3",
    );
    assert_eq!(e.responses(), 3, "{}", e.body);
    let p1 = e.body.find("\r\n\r\n1").expect("first echo");
    let p2 = e.body.find("\r\n\r\n2").expect("second echo");
    let p3 = e.body.find("\r\n\r\n3").expect("third echo");
    assert!(p1 < p2 && p2 < p3, "must be in request order: {}", e.body);
}

#[test]
fn expect_100_continue_interim_then_final() {
    let e = raw_exchange(
        b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nExpect: 100-continue\r\nConnection: close\r\n\r\nhello",
    );
    assert!(
        e.body.starts_with("HTTP/1.1 100 Continue\r\n\r\n"),
        "the interim response must come first: {}",
        e.body
    );
    assert!(e.body.contains("HTTP/1.1 200 OK"), "{}", e.body);
    assert!(e.body.ends_with("hello"), "{}", e.body);
}

#[test]
fn head_returns_headers_without_body() {
    let e = raw_exchange(b"HEAD / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
    assert_eq!(e.status(), Some(200), "{}", e.body);
    assert!(
        e.body.ends_with("\r\n\r\n"),
        "no body may follow a HEAD response: {:?}",
        e.body
    );
}

#[test]
fn http_10_closes_by_default() {
    let e = raw_exchange(b"GET / HTTP/1.0\r\n\r\n");
    assert!(e.body.starts_with("HTTP/1.0 200 OK"), "{}", e.body);
    assert!(e.body.contains("connection: close"), "{}", e.body);
    assert!(e.closed);
}

#[test]
fn http_10_keep_alive_honored() {
    let e =
        raw_exchange(b"GET /a HTTP/1.0\r\nConnection: keep-alive\r\n\r\nGET /b HTTP/1.0\r\n\r\n");
    assert_eq!(e.responses(), 2, "{}", e.body);
}

/// The core anti-smuggling property, asserted end to end: bytes following a
/// rejected request must never be interpreted as a new request.
#[test]
fn does_not_resynchronize_after_a_rejection() {
    let e = raw_exchange(
        b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n\
          helloGET /smuggled HTTP/1.1\r\nHost: a\r\n\r\n",
    );
    assert_eq!(
        e.responses(),
        1,
        "the smuggled request must not be served: {}",
        e.body
    );
    assert_eq!(e.status(), Some(400));
}

/// Same property for the unread-body case, which is not an error at all — just a
/// handler that ignored the body.
#[test]
fn unread_body_is_not_mined_for_a_second_request() {
    async fn ignore(_req: Request) -> Response {
        Response::status_only(404)
    }

    let server = Server::bind(config(quick_limits())).expect("bind");
    let addr = server.local_addr();
    let handle = server.handle();
    let client = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = rt.block_on(async move {
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            let _ = s
                .write_all(
                    b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\n\
                      helloGET /smuggled HTTP/1.1\r\nHost: a\r\n\r\n",
                )
                .await;
            let mut buf = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut buf)).await;
            String::from_utf8_lossy(&buf).into_owned()
        });
        handle.shutdown();
        out
    });
    server.serve(|| ignore).expect("serve");
    let body = client.join().expect("client");

    assert_eq!(
        body.matches("HTTP/1.").count(),
        1,
        "an unread body must not be mined for a second request: {body}"
    );
    assert!(body.contains("connection: close"), "{body}");
}

/// RFC 9110 section 9.3.6. This crate does not establish CONNECT tunnels
/// itself: a handler's response to a `CONNECT` is written like any other
/// response — framed, with `Content-Length: 0` for an empty body — and the
/// connection carries on under the ordinary keep-alive rules, so a pipelined
/// follow-up is served.
#[test]
#[cfg_attr(
    feature = "hyper-backend",
    ignore = "see BACKENDS.md: a 2xx response to CONNECT \
              (hyper hijacks it as a tunnel: no framing field is written and \
               the connection is not reused)"
)]
fn connect_2xx_is_an_ordinary_response_and_the_connection_continues() {
    async fn ok(_req: Request) -> Response {
        Response::status_only(200)
    }

    let e = raw_exchange_with_service(
        quick_limits(),
        || ok,
        vec![
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n\
              GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"
                .to_vec(),
        ],
        Duration::ZERO,
    );

    assert!(e.body.starts_with("HTTP/1.1 200 OK"), "{}", e.body);
    assert!(
        e.body.to_lowercase().contains("content-length: 0"),
        "the response is framed like any other, not left undelimited as a \
         tunnel would be: {}",
        e.body
    );
    assert_eq!(
        e.responses(),
        2,
        "no tunnel was established, so the pipelined request is served too: \
         {}",
        e.body
    );
    assert!(
        e.closed,
        "the pipelined request carried `Connection: close`, so the \
         connection must end, not continue as a tunnel would: {}",
        e.body
    );
}

/// Positive twin of `connect_2xx_is_an_ordinary_response_and_the_connection_continues`
/// for the hyper backend: BACKENDS.md documents hyper's role/encoder hijacking
/// a 2xx response to `CONNECT` as a tunnel-establishment upgrade — no framing
/// field is written, the connection is not reused, and the pipelined request
/// behind it is never served.
#[test]
#[cfg(feature = "hyper-backend")]
fn hyper_connect_2xx_is_hijacked_as_a_tunnel() {
    async fn ok(_req: Request) -> Response {
        Response::status_only(200)
    }

    let e = raw_exchange_with_service(
        quick_limits(),
        || ok,
        vec![
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n\
              GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"
                .to_vec(),
        ],
        Duration::ZERO,
    );

    assert!(e.body.starts_with("HTTP/1.1 200 OK"), "{}", e.body);
    assert!(
        !e.body.to_lowercase().contains("content-length"),
        "a tunnel hijack writes no framing field: {}",
        e.body
    );
    assert_eq!(
        e.responses(),
        1,
        "the tunnel swallows the connection, so the pipelined request behind \
         it is never served: {}",
        e.body
    );
}

/// A handler `Content-Length` that disagrees with the body it framed must
/// never reach the wire: the peer would read the difference as the start of
/// the next response. Both backends replace it with the true length — the
/// native writer by reclaiming framing, the hyper bridge by dropping the field
/// so hyper frames from the body's own size.
#[test]
fn a_wrong_handler_content_length_is_replaced_with_the_true_one() {
    async fn lying(_req: Request) -> Response {
        Response::new(200)
            .header(
                armature_h1::HeaderId::ContentLength,
                bytes::Bytes::from_static(b"5"),
            )
            .with_body(armature_h1::ResponseBody::Full(bytes::Bytes::from_static(
                b"hello world",
            )))
    }

    let e = raw_exchange_with_service(
        quick_limits(),
        || lying,
        vec![b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n".to_vec()],
        Duration::ZERO,
    );

    let lower = e.body.to_lowercase();
    assert_eq!(
        lower.matches("content-length:").count(),
        1,
        "exactly one framing field: {}",
        e.body
    );
    assert!(lower.contains("content-length: 11"), "{}", e.body);
    assert!(
        e.body.ends_with("hello world"),
        "the whole body goes out, untruncated: {}",
        e.body
    );
    assert!(
        e.closed,
        "`Connection: close` on the request means the connection ends: {}",
        e.body
    );
}
