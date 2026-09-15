//! Backend differential fuzzing: the native connection loop against the hyper
//! backend, in one process.
//!
//! `framing_differential` compares this crate's *parser* against hyper's. This
//! target compares this crate's two *backends* against each other: the same
//! bytes, the same service, once through `Connection::with_buffered(..).serve()`
//! (the bespoke loop) and once through `armature_h1::serve_connection(..)`
//! (hyper's `conn::http1`, because this target is built with `hyper-backend`
//! on). The `hyper-backend` feature swaps only what `serve_connection` selects;
//! the native `Connection` stays public and compiled, which is what makes the
//! side-by-side possible without two binaries.
//!
//! The property under test is the one the feature promises: swapping backends
//! must not change *what gets served*. A build that answers a request the other
//! build refuses — or answers it with different body bytes — is a behavioural
//! fork between two builds of the same crate, and in a proxy chain that fork is
//! the request-smuggling primitive.
//!
//! ## Why the comparison has this exact shape
//!
//! `BACKENDS.md` is the exhaustive list of what could not be shimmed, and every
//! one of its rows lives in the *rejection* path or in the *fields* of a
//! response. So the comparison deliberately avoids both:
//!
//! - **No assertion about rejection statuses.** `BACKENDS.md` "Parsing and
//!   rejection" documents four cases where the two backends refuse the same
//!   input with different statuses (`505` vs `400` for `HTTP/1.2`, `501` vs
//!   `400` for `Transfer-Encoding: gzip`, `505` vs *no bytes at all* for an h2c
//!   preface) and one where native refuses and hyper serves (bare LF, fragment
//!   in the target). Asserting on non-200 responses would fail on the first
//!   trivial input and prove nothing.
//! - **No assertion about headers, field order, or `Connection`.** `Date`
//!   differs by construction, header order is hyper's to choose, and the
//!   "Responses" table documents that hyper rewrites `Connection` and drops
//!   fields whose values hold control bytes.
//! - **No assertion about interim responses.** `Expect: 100-continue` is
//!   documented as eager in hyper and lazy in native, so `1xx` responses are
//!   parsed for framing purposes and then ignored.
//!
//! What is left is the part that must not diverge:
//!
//! 1. **Neither backend panics.** Implicit, and the reason this target is worth
//!    running even with the assertions below fully satisfied.
//! 2. **Agreement on served payload.** If both backends answered the first
//!    request with `200` under content-length framing, the body bytes must be
//!    identical. The service is a deterministic echo, so the response body *is*
//!    the request body the backend decided on — a mismatch means the two
//!    backends framed the same request body differently, which is the smuggling
//!    condition stated in payload terms.
//! 3. **hyper must not out-serve native.** When *both* backends produced only
//!    `200`s (nobody rejected anything), hyper's response count must not exceed
//!    native's. Native is the stricter parser; hyper finding one more request in
//!    the same bytes means it accepted something native did not, which is the
//!    smuggling-adjacent direction worth catching. The reverse — hyper serving
//!    fewer — is allowed, because every documented divergence that changes the
//!    count is a hyper rejection.
//!
//!    Two guards on that count are load-bearing, not defensive, and both were
//!    put there by inputs that fired:
//!
//!    - **Both sides must have produced only `200`s.** Otherwise a documented
//!      rejection divergence (bare LF, fragment, `HTTP/1.2`, unsupported
//!      transfer coding — all of which reject on exactly one side) shows up as
//!      a count difference for a reason `BACKENDS.md` already accounts for.
//!    - **The input must be free of bare LF.** The first real run of this
//!      target found `POST / HTTP/1.1\r\nHost:tn,,,\n\n\r\nhello` in about a
//!      minute: hyper serves it `200`, and native writes *nothing at all* —
//!      its head scanner only ever terminates on CRLFCRLF, so the request is
//!      still incomplete when the stream ends and a connection that closes
//!      before a complete head owes no response. That is the same
//!      `BACKENDS.md` bare-LF row as the `400` case, just reached through the
//!      EOF path instead of the reject path, so the previous guard did not
//!      catch it. Excluding bare LF from the count comparison is the honest
//!      fix: on any input where hyper's extra leniency is documented, the
//!      count cannot mean what the assertion claims. On strictly CRLF-framed
//!      input the comparison keeps its teeth, and that is where an *undocumented*
//!      over-serve would live.

#![no_main]

use armature_h1::{ConnConfig, Connection, DateCache, Limits, Request, Response, ResponseBody};
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

thread_local! {
    static RUNTIME: RefCell<tokio::runtime::Runtime> = RefCell::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    );
}

/// The shared service: a deterministic echo, identical on both sides.
///
/// Determinism is what makes body comparison meaningful — any difference in the
/// bytes that come back is a difference in the bytes the backend decided the
/// request body contained.
async fn echo(mut req: Request) -> Response {
    match req.body.collect(64 * 1024).await {
        Ok(b) => {
            let mut r = Response::new(200);
            r.body = ResponseBody::Full(b);
            r
        }
        Err(e) => Response::status_only(e.status()),
    }
}

/// Deadlines short enough that no pathological input can stall an iteration.
///
/// A healthy case ends at EOF long before any of these fire; they exist only so
/// a case that *would* hang (an announced body that never arrives, say) ends in
/// milliseconds instead of seconds.
fn cfg() -> Rc<ConnConfig> {
    Rc::new(ConnConfig {
        limits: Limits {
            idle_timeout: Duration::from_millis(80),
            header_timeout: Duration::from_millis(80),
            body_timeout: Duration::from_millis(80),
            write_timeout: Duration::from_millis(80),
            ..Default::default()
        },
        tick: Duration::from_millis(10),
        server_name: None,
    })
}

/// One parsed response off the wire.
#[derive(Debug)]
struct Resp {
    status: u16,
    body: Vec<u8>,
    /// Whether the framing was an explicit `content-length`, as opposed to
    /// implied-empty (interim, `204`, `304`).
    content_length: bool,
    /// The response's HTTP version, as carried on its status line
    /// (`HTTP/1.0` or `HTTP/1.1`). commit b990f8e's headline fix is carrying
    /// the request version into the response — 1.0 framing, `connection:
    /// keep-alive` written on a 1.0 response, the 1.0 bare `408` — so a
    /// parser that only recognised `HTTP/1.1` status lines would bail before
    /// any assertion ran on exactly the new code.
    version_1_0: bool,
}

/// Split a response stream into messages.
///
/// `None` means the stream could not be walked end to end — a truncated head, a
/// body shorter than its announced length, or framing this parser does not
/// model (chunked). Every comparison below is skipped in that case rather than
/// guessed at: a half-understood stream cannot support an assertion about how
/// many requests were served.
///
/// Counting `HTTP/1.` occurrences would be simpler and wrong here: the service
/// echoes the request body, and fuzzed input contains response-looking bytes.
/// Walking the framing is the only way to tell a served response from a payload
/// that resembles one.
fn parse_responses(mut buf: &[u8]) -> Option<Vec<Resp>> {
    let mut out = Vec::new();
    while !buf.is_empty() {
        let end = find(buf, b"\r\n\r\n")?;
        let head = &buf[..end];
        let rest = &buf[end + 4..];

        let line_end = find(head, b"\r\n").unwrap_or(head.len());
        let line = &head[..line_end];
        let (version_1_0, rest_of_line) = if let Some(r) = line.strip_prefix(b"HTTP/1.1 ") {
            (false, r)
        } else {
            let r = line.strip_prefix(b"HTTP/1.0 ")?;
            (true, r)
        };
        let status: u16 = std::str::from_utf8(rest_of_line)
            .ok()?
            .get(..3)?
            .parse()
            .ok()?;

        // Chunked responses are not modelled; the echo service never produces
        // one, so seeing one means this parser is out of sync with the stream.
        if header_present(head, b"transfer-encoding") {
            return None;
        }

        let len = match content_length(head) {
            Some(n) => n,
            // Interim and bodiless statuses carry no framing field and no body.
            None if status < 200 || status == 204 || status == 304 => 0,
            // Anything else without content-length is framed by connection
            // close: it runs to the end of the stream and nothing follows.
            None => {
                out.push(Resp {
                    status,
                    body: rest.to_vec(),
                    content_length: false,
                    version_1_0,
                });
                return Some(out);
            }
        };
        if rest.len() < len {
            // Announced more body than arrived: the stream was cut mid-message.
            return None;
        }
        out.push(Resp {
            status,
            body: rest[..len].to_vec(),
            content_length: true,
            version_1_0,
        });
        buf = &rest[len..];
    }
    Some(out)
}

/// Whether `data` holds an LF that is not part of a CRLF.
///
/// Native terminates head lines on CRLF only; hyper (via httparse) also accepts
/// a bare LF. See `BACKENDS.md`, "Parsing and rejection", row 1.
fn has_bare_lf(data: &[u8]) -> bool {
    data.iter()
        .enumerate()
        .any(|(i, &b)| b == b'\n' && (i == 0 || data[i - 1] != b'\r'))
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Whether `head` carries a field with this (lowercase) name.
fn header_present(head: &[u8], name: &[u8]) -> bool {
    field_value(head, name).is_some()
}

fn field_value<'a>(head: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    for line in head.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            continue;
        };
        let (n, v) = line.split_at(colon);
        if n.eq_ignore_ascii_case(name) {
            return Some(trim(&v[1..]));
        }
    }
    None
}

fn content_length(head: &[u8]) -> Option<usize> {
    std::str::from_utf8(field_value(head, b"content-length")?)
        .ok()?
        .parse()
        .ok()
}

fn trim(mut v: &[u8]) -> &[u8] {
    while let Some((f, r)) = v.split_first() {
        if f.is_ascii_whitespace() {
            v = r;
        } else {
            break;
        }
    }
    while let Some((l, r)) = v.split_last() {
        if l.is_ascii_whitespace() {
            v = r;
        } else {
            break;
        }
    }
    v
}

/// Feed `data` to one backend over an in-memory duplex and collect its output.
///
/// The client half-closes after writing, so a backend that is waiting for more
/// input sees EOF immediately instead of sitting on a deadline. The outer
/// timeout is a backstop for the pathological cases the short `Limits` above are
/// there to bound.
async fn drive<F, Fut>(data: &[u8], serve: F) -> Vec<u8>
where
    F: FnOnce(tokio::io::DuplexStream) -> Fut,
    Fut: std::future::Future<Output = std::io::Result<Option<armature_h1::Upgraded>>>,
{
    let (mut client, server) = tokio::io::duplex(64 << 10);
    let conn = serve(server);

    let exchange = async {
        let _ = client.write_all(data).await;
        let _ = client.flush().await;
        let _ = client.shutdown().await;
        let mut out = Vec::new();
        let _ = client.read_to_end(&mut out).await;
        out
    };

    // Both halves run in one task: neither future is `Send`, and the connection
    // is driven only while the client is also being polled, which is exactly the
    // interleaving a real peer produces.
    let (result, out) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(2), conn),
        exchange
    );
    let _ = result;
    out
}

fn native(data: &[u8]) -> Vec<u8> {
    RUNTIME.with(|rt| {
        rt.borrow_mut().block_on(drive(data, |io| async move {
            Connection::with_buffered(
                io,
                echo,
                cfg(),
                Rc::new(RefCell::new(DateCache::new())),
                Bytes::new(),
            )
            .serve()
            .await
        }))
    })
}

fn hyper(data: &[u8]) -> Vec<u8> {
    RUNTIME.with(|rt| {
        rt.borrow_mut().block_on(drive(data, |io| {
            // Built with `hyper-backend`, so this resolves to hyper's h1 stack.
            armature_h1::serve_connection(
                io,
                Rc::new(echo),
                cfg(),
                Rc::new(RefCell::new(DateCache::new())),
                Bytes::new(),
            )
        }))
    })
}

fuzz_target!(|data: &[u8]| {
    // Bounded like `framing_differential`: each case spins two connections on a
    // real runtime, so large inputs buy coverage at a poor rate.
    if data.is_empty() || data.len() > 2048 {
        return;
    }

    let native_out = native(data);
    let hyper_out = hyper(data);

    // Invariant 1 (no panic) has already held by reaching this line.

    let (Some(n), Some(h)) = (parse_responses(&native_out), parse_responses(&hyper_out)) else {
        // A stream this parser cannot walk supports no claim about it.
        return;
    };

    // Invariant 2: agreement on served payload.
    //
    // Restricted to a first response that is `200` under content-length framing
    // on both sides. Non-200s are where every documented divergence lives, and
    // close-framed or interim responses carry no length to compare against.
    if let (Some(a), Some(b)) = (n.first(), h.first())
        && a.status == 200
        && b.status == 200
        && a.content_length
        && b.content_length
        && a.body != b.body
    {
        panic!(
            "SERVED PAYLOAD DIVERGENCE — native {} bytes, hyper {} bytes\n\
             Both backends answered this request with 200 and echoed different \
             bodies, so they framed the same request body differently. Same \
             crate, same service, two builds: this is a smuggling vector.\n\
             native: {:?}\nhyper:  {:?}\ninput:  {:?}",
            a.body.len(),
            b.body.len(),
            String::from_utf8_lossy(&a.body),
            String::from_utf8_lossy(&b.body),
            String::from_utf8_lossy(data),
        );
    }

    // Invariant 2b: agreement on response version.
    //
    // Both backends are expected to carry the request's HTTP version into the
    // response status line (native's half of this is commit b990f8e: 1.0
    // framing, `connection: keep-alive` on a 1.0 response, the 1.0 bare
    // `408`). Restricted to the same first-response-is-200 case as invariant
    // 2, for the same reason: a non-200 is where the documented divergences
    // live.
    if let (Some(a), Some(b)) = (n.first(), h.first())
        && a.status == 200
        && b.status == 200
        && a.version_1_0 != b.version_1_0
    {
        panic!(
            "RESPONSE VERSION DIVERGENCE — native wrote HTTP/1.{}, hyper wrote \
             HTTP/1.{}\n\
             Both backends answered this request with 200 but disagree on which \
             HTTP version they echoed in the status line.\ninput: {:?}",
            if a.version_1_0 { 0 } else { 1 },
            if b.version_1_0 { 0 } else { 1 },
            String::from_utf8_lossy(data),
        );
    }

    // Invariant 3: hyper must not serve more requests than native accepted.
    //
    // Bare LF is hyper's documented extra leniency (`BACKENDS.md` row 1), and it
    // reaches the count through two paths, not one: native either answers `400`
    // or — when the LF-terminated head is the last thing on the stream — writes
    // nothing and closes, because a connection that ends before a complete head
    // owes no response. The second path leaves native's response list *clean and
    // empty*, so the "no rejections" guard below does not exclude it. Skipping
    // bare-LF input entirely is what keeps this assertion about undocumented
    // divergence.
    if has_bare_lf(data) {
        return;
    }

    // Interim `1xx` responses are dropped first — hyper writes `100 Continue`
    // eagerly and native lazily (`BACKENDS.md`, "Responses"), so counting them
    // would compare a documented divergence. Both sides must then be free of
    // any non-200 final response: a rejection on either side puts the case in
    // the documented-divergence space (fragment, `HTTP/1.2`, unsupported
    // transfer coding all reject on exactly one side), and the count comparison
    // stops meaning anything. What survives the guard is the case worth having:
    // native walked the stream cleanly and stopped, and hyper found another
    // request in the same bytes.
    let finals = |v: &[Resp]| -> Vec<u16> {
        v.iter()
            .map(|r| r.status)
            .filter(|s| *s >= 200)
            .collect()
    };
    let (nf, hf) = (finals(&n), finals(&h));
    if nf.iter().all(|s| *s == 200) && hf.iter().all(|s| *s == 200) && hf.len() > nf.len() {
        panic!(
            "OVER-SERVE — native served {} requests, hyper served {}\n\
             Neither backend rejected anything, yet hyper found more requests in \
             the same bytes than the stricter native parser accepted. hyper \
             accepted a request native did not.\ninput: {:?}",
            nf.len(),
            hf.len(),
            String::from_utf8_lossy(data),
        );
    }
});
