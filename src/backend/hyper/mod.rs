//! Serving through hyper's `conn::http1`. See BACKENDS.md for divergences.
//!
//! hyper owns parsing, framing and writing; this module owns *timing*. The
//! native loop's four deadlines (idle, header, body, write) have no equivalent
//! in hyper's connection future — `header_read_timeout` is the only one it
//! offers, and it covers a different window. So the deadlines are produced from
//! the outside: a [`PhaseClock`] that the IO adapter and the service bridge
//! advance, and a watchdog future that races hyper's connection future and
//! reproduces the native reaction to each expiry.
//!
//! The reconstruction is exact for `idle_timeout`, `header_timeout` and
//! `body_timeout`. Where it is not — `write_timeout` granularity, the keep-alive
//! wait after a `HEAD` response, a pipelined head arriving mid-write — and for
//! every non-timing divergence (hyper's parser permissiveness, its own status
//! choices for heads it rejects, the `max_buf_size` floor under
//! `max_head_bytes`), BACKENDS.md is the exhaustive list, with the reason each
//! one cannot be shimmed. Every `#[cfg_attr(feature = "hyper-backend", ignore)]`
//! in the test suite points at a row there.

pub(crate) mod body;
pub(crate) mod bridge;
pub(crate) mod io;

use super::Backend;
use crate::Version;
use crate::conn::ConnConfig;
use crate::deadline::ConnDeadline;
use crate::header::HeaderVec;
use crate::limits::Limits;
use crate::service::{H1Service, Upgraded};
use crate::write::{self, DateCache, OutBody, ResponseHead};
use bridge::Bridge;
use bytes::Bytes;
use io::{HyperIo, IoShared, SharedIo};
use std::cell::{Cell, RefCell};
use std::io as stdio;
use std::rc::Rc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

/// What the connection is waiting on, which picks the deadline that applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// No request in flight: `idle_timeout` (and no response is owed on expiry).
    Idle,
    /// A head has started arriving but the handler has not been called yet:
    /// `header_timeout`.
    Head,
    /// The handler is running, which is where body reads happen:
    /// `body_timeout`.
    Handler,
    /// A response has been handed to hyper: `write_timeout`, re-armed on every
    /// byte that reaches the transport, so a stream that keeps making progress
    /// never expires and one that stalls dies on schedule.
    Write,
}

/// The connection's phase, plus a change notification for the watchdog.
///
/// `generation` exists so an expiry that races a phase change loses: the
/// watchdog snapshots the generation before arming and discards an expiry whose
/// generation moved, rather than answering 408 for a phase that already ended.
pub(crate) struct PhaseClock {
    phase: Cell<Phase>,
    generation: Cell<u64>,
    /// Whether any byte of the current response has reached the transport.
    /// Set on every nonzero write, reset whenever a fresh response begins.
    /// Gates the bare-408 write: bytes already on the wire mean a 408 would be
    /// spliced into a response the peer is mid-parse of.
    wrote_bytes: Cell<bool>,
    /// Whether the current response body has reported end-of-stream. Half of
    /// the "response finished" signal; the other half is a drained write
    /// buffer, which is a successful flush.
    body_done: Cell<bool>,
    notify: tokio::sync::Notify,
}

impl PhaseClock {
    pub(crate) fn new(phase: Phase) -> Self {
        Self {
            phase: Cell::new(phase),
            generation: Cell::new(0),
            wrote_bytes: Cell::new(false),
            body_done: Cell::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn phase(&self) -> Phase {
        self.phase.get()
    }

    /// Whether the current response has put anything on the wire.
    pub(crate) fn wrote_bytes(&self) -> bool {
        self.wrote_bytes.get()
    }

    /// Enter `p`, bumping the generation and waking the watchdog.
    ///
    /// Entering [`Phase::Idle`] or [`Phase::Handler`] starts a fresh response,
    /// resetting the per-response write and body-end flags. Both are safe
    /// points to do it: reaching `Idle` already required the body to report
    /// end-of-stream *and* the write buffer to drain, so nothing of the
    /// previous response is still in flight. Resetting at `Handler` alone would
    /// leave `wrote_bytes` latched for the life of a keep-alive connection, and
    /// every 408 after the first response would be suppressed as if it were
    /// about to be spliced into a write.
    pub(crate) fn set(&self, p: Phase) {
        if matches!(p, Phase::Idle | Phase::Handler) {
            self.wrote_bytes.set(false);
            self.body_done.set(false);
        }
        self.phase.set(p);
        self.bump();
    }

    /// Record that bytes reached the transport, re-arming a write deadline.
    ///
    /// Only [`Phase::Write`] re-arms: elsewhere the write is a `100-continue`
    /// or hyper's own error response, neither of which should extend the
    /// header or body deadline it happened under.
    pub(crate) fn note_write(&self) {
        self.wrote_bytes.set(true);
        if self.phase.get() == Phase::Write {
            self.bump();
        }
    }

    /// Record that the response body has no more frames to give.
    pub(crate) fn note_body_end(&self) {
        self.body_done.set(true);
    }

    /// Record a successful flush: hyper's write buffer is drained.
    ///
    /// Body exhausted *and* buffer drained is the one observable "this response
    /// is finished" moment, and it is what returns the connection to
    /// [`Phase::Idle`] so a keep-alive wait is bounded by `idle_timeout` rather
    /// than `write_timeout`.
    pub(crate) fn note_flush(&self) {
        if self.phase.get() == Phase::Write && self.body_done.get() {
            self.set(Phase::Idle);
        }
    }

    fn bump(&self) {
        self.generation.set(self.generation.get().wrapping_add(1));
        self.notify.notify_waiters();
    }
}

/// The deadline that applies while waiting in `phase`.
fn phase_timeout(phase: Phase, limits: &Limits) -> Duration {
    match phase {
        Phase::Idle => limits.idle_timeout,
        Phase::Head => limits.header_timeout,
        Phase::Handler => limits.body_timeout,
        Phase::Write => limits.write_timeout,
    }
}

/// Wait until some phase outlives its deadline, and report which one.
///
/// Never returns otherwise: every phase change re-arms and loops.
async fn watchdog(clock: &PhaseClock, limits: &Limits, deadline: &mut ConnDeadline) -> Phase {
    loop {
        let generation = clock.generation.get();
        let phase = clock.phase.get();
        deadline.arm(phase_timeout(phase, limits));
        tokio::select! {
            biased;
            () = clock.notify.notified() => continue,
            () = deadline.expired() => {
                // A change that landed between the snapshot and the expiry
                // means this deadline was armed for a phase already over.
                if clock.generation.get() != generation {
                    continue;
                }
                return phase;
            }
        }
    }
}

/// Write a native-formatted bare error response after hyper's future is gone.
///
/// Byte-for-byte what `Connection::write_error` emits: same writer, same
/// header set, same `Connection: close`.
async fn write_error_close<IO: AsyncRead + AsyncWrite + Unpin>(
    shared: &Rc<RefCell<IoShared<IO>>>,
    date: &Rc<RefCell<DateCache>>,
    version: Version,
    status: u16,
) {
    let mut out = bytes::BytesMut::new();
    {
        let mut date = date.borrow_mut();
        let date_bytes = date.get(std::time::SystemTime::now());
        write::write_head(
            &mut out,
            version,
            &ResponseHead {
                status,
                headers: HeaderVec::new(),
            },
            &OutBody::None,
            date_bytes,
            false,
        );
    }
    let mut io = SharedIo(shared.clone());
    use tokio::io::AsyncWriteExt;
    let _ = io.write_all(&out).await;
    let _ = io.flush().await;
}

/// Walk the error source chain for an `io::Error` to preserve error-kind
/// parity with the native loop where possible.
fn find_io_error(e: &::hyper::Error) -> Option<stdio::Error> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(s) = source {
        if let Some(io_err) = s.downcast_ref::<stdio::Error>() {
            return Some(stdio::Error::new(io_err.kind(), io_err.to_string()));
        }
        source = s.source();
    }
    None
}

/// Serving via `hyper::server::conn::http1`, with native timing semantics
/// layered on from outside.
pub(crate) struct HyperBackend;

impl Backend for HyperBackend {
    async fn serve<IO, S>(
        io: IO,
        service: Rc<S>,
        cfg: Rc<ConnConfig>,
        date: Rc<RefCell<DateCache>>,
        buffered: Bytes,
    ) -> stdio::Result<Option<Upgraded>>
    where
        IO: AsyncRead + AsyncWrite + Unpin + 'static,
        S: H1Service + 'static,
    {
        let clock = Rc::new(PhaseClock::new(Phase::Idle));
        let (hyper_io, shared) = HyperIo::new(io, buffered, clock.clone());
        let upgrade_slot = Rc::new(RefCell::new(None));
        let sent_101 = Rc::new(Cell::new(false));
        let req_version = Rc::new(Cell::new(Version::Http11));
        let bridge = Bridge {
            service,
            cfg: cfg.clone(),
            upgrade_slot: upgrade_slot.clone(),
            sent_101: sent_101.clone(),
            phase: clock.clone(),
            req_version: req_version.clone(),
        };

        let mut builder = ::hyper::server::conn::http1::Builder::new();
        builder
            // Timing policy is ours: the watchdog owns every deadline.
            .header_read_timeout(None)
            // Wire-level head cap; the cumulative cap lives in convert_head.
            // hyper refuses to go below its own 8 KiB floor, so a smaller
            // configured cap is enforced only by `convert_head`.
            .max_buf_size(cfg.limits.max_head_bytes.max(MIN_HYPER_BUF))
            // Native semantics: EOF on read closes; responses flush per
            // response, not batched across pipelined requests.
            //
            // `pipeline_flush(false)` is load-bearing beyond parity:
            // `PhaseClock::note_flush` reads a successful flush as "hyper owes
            // the wire nothing more". With pipelined flushing enabled hyper's
            // `flush_pipeline` path returns early without draining the write
            // buffer, which would report a completion that had not happened.
            .half_close(false)
            .pipeline_flush(false);
        let mut conn = builder.serve_connection(hyper_io, bridge);

        let mut deadline = ConnDeadline::new(cfg.tick);
        // `poll_without_shutdown` rather than awaiting the connection future:
        // hyper's `with_upgrades()` requires `I: Send`, which this crate's
        // `Rc`-shared IO can never be. The old-style path keeps the transport
        // recoverable through `into_parts` instead, which is what the upgrade
        // handoff needs anyway. Skipping hyper's `poll_shutdown` is also the
        // closer parity: the native loop never shuts the transport down
        // explicitly either, it just drops it. The borrow is scoped so the
        // connection can be dropped (408 path) or consumed (upgrade path)
        // afterwards.
        let result = {
            let fut = std::future::poll_fn(|cx| conn.poll_without_shutdown(cx));
            tokio::pin!(fut);
            tokio::select! {
                biased;
                expired = watchdog(&clock, &cfg.limits, &mut deadline) => Err(expired),
                r = &mut fut => Ok(r),
            }
        };

        match result {
            // Idle expiry, and write-stall expiry: silent close. Native owes no
            // response on an idle close, and writes nothing when a write stalls
            // — a bare 408 there would land inside the body the peer is already
            // parsing.
            Err(Phase::Idle | Phase::Write) => {
                drop(conn);
                Ok(None)
            }
            // Header/body expiry: native writes a bare 408 and closes.
            Err(phase @ (Phase::Head | Phase::Handler)) => {
                // Release hyper's borrow of the shared IO before writing.
                drop(conn);
                // Native answers a head it never finished parsing in 1.1 (it
                // has no version to echo yet) and a body-phase timeout in the
                // request's own version.
                let version = match phase {
                    Phase::Handler => req_version.get(),
                    _ => Version::Http11,
                };
                // Unless something already went out for this response — a
                // `100-continue`, or a response hyper is mid-write of. Appending
                // a 408 to those corrupts the framing.
                if !clock.wrote_bytes() {
                    write_error_close(&shared, &date, version, 408).await;
                }
                Ok(None)
            }
            Ok(Ok(())) => {
                if sent_101.get() && upgrade_slot.borrow_mut().take().is_some() {
                    let parts = conn.into_parts();
                    return Ok(Some(Upgraded {
                        // hyper's read-ahead satisfies the `buffered`
                        // contract: bytes the peer sent past the 101 head,
                        // which must not be dropped.
                        buffered: parts.read_buf,
                        io: Box::new(SharedIo(parts.io.0)),
                    }));
                }
                Ok(None)
            }
            // hyper handles and reports protocol errors itself (it writes its
            // own 4xx for malformed requests); at this level the connection is
            // simply over. IO errors map like the native loop's.
            Ok(Err(e)) => {
                if let Some(io_err) = find_io_error(&e) {
                    Err(io_err)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// hyper's hard floor for `max_buf_size`; passing less panics.
const MIN_HYPER_BUF: usize = 8 * 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::DateCache;
    use crate::{ConnConfig, Limits, Request, Response};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn hello(_req: Request) -> Response {
        Response::text("hi")
    }

    async fn echo(mut req: Request) -> Response {
        match req.body.collect(1024 * 1024).await {
            Ok(b) => Response::ok().with_body(crate::ResponseBody::Full(b)),
            Err(e) => Response::status_only(e.status()),
        }
    }

    fn quick(mut limits: Limits) -> Limits {
        limits.idle_timeout = Duration::from_millis(200);
        limits.header_timeout = Duration::from_millis(200);
        limits
    }

    fn cfg(limits: Limits) -> Rc<ConnConfig> {
        Rc::new(ConnConfig {
            limits: quick(limits),
            tick: Duration::from_millis(10),
            server_name: None,
        })
    }

    /// A response body that yields `count` chunks, one every `gap`.
    ///
    /// `gap` of zero yields as fast as hyper drains it, which is what fills a
    /// small transport buffer and produces a genuine write stall.
    struct TickStream {
        remaining: usize,
        gap: Duration,
        sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
        chunk: Bytes,
    }

    impl TickStream {
        fn body(count: usize, gap: Duration, chunk: Bytes) -> crate::ResponseBody {
            crate::ResponseBody::Stream(Box::pin(TickStream {
                remaining: count,
                gap,
                sleep: Box::pin(tokio::time::sleep(gap)),
                chunk,
            }))
        }
    }

    impl crate::service::futures_stream::Stream for TickStream {
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<Bytes, crate::BodyError>>> {
            use std::future::Future;
            use std::task::Poll;
            let this = self.get_mut();
            if this.remaining == 0 {
                return Poll::Ready(None);
            }
            if this.gap > Duration::ZERO {
                match this.sleep.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        this.sleep
                            .as_mut()
                            .reset(tokio::time::Instant::now() + this.gap);
                    }
                }
            }
            this.remaining -= 1;
            Poll::Ready(Some(Ok(this.chunk.clone())))
        }
    }

    async fn exchange<S>(input: &'static [u8], service: S, limits: Limits) -> String
    where
        S: crate::H1Service + 'static,
    {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(HyperBackend::serve(
            server,
            Rc::new(service),
            cfg(limits),
            Rc::new(RefCell::new(DateCache::new())),
            Bytes::new(),
        ));
        local
            .run_until(async move {
                client.write_all(input).await.unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                    .await;
                let _ = task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await
    }

    #[tokio::test]
    async fn serves_a_single_request() {
        let out = exchange(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            hello,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(out.ends_with("hi"), "{out}");
    }

    #[tokio::test]
    async fn full_body_uses_content_length_framing() {
        let out = exchange(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            hello,
            Limits::default(),
        )
        .await;
        assert!(
            out.to_ascii_lowercase().contains("content-length: 2"),
            "{out}"
        );
        assert!(
            !out.to_ascii_lowercase().contains("transfer-encoding"),
            "{out}"
        );
    }

    #[tokio::test]
    async fn echoes_content_length_and_chunked_bodies() {
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
            echo,
            Limits::default(),
        )
        .await;
        assert!(out.ends_with("hello"), "{out}");

        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
            echo,
            Limits::default(),
        )
        .await;
        assert!(out.ends_with("hello"), "{out}");
    }

    /// A finished response must hand the connection back to `idle_timeout`,
    /// not leave it parked under `write_timeout`: `Phase::Write` has to end
    /// when the body is exhausted and the write buffer drains.
    #[tokio::test]
    async fn a_quiet_keep_alive_connection_closes_on_the_idle_deadline() {
        let started = tokio::time::Instant::now();
        // idle_timeout is 200ms (via `quick`), write_timeout the 30s default:
        // only an actual return to `Phase::Idle` closes this in time.
        let out = exchange(
            b"GET / HTTP/1.1\r\nHost: a\r\n\r\n",
            hello,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "closed after {:?}, so the connection never left Phase::Write",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn idle_timeout_closes_silently() {
        let out = exchange(b"", hello, Limits::default()).await;
        assert!(out.is_empty(), "no response owed on idle close: {out}");
    }

    #[tokio::test]
    async fn header_timeout_writes_408() {
        // A started-but-never-finished head.
        let out = exchange(b"GET / HTT", hello, Limits::default()).await;
        assert!(out.starts_with("HTTP/1.1 408"), "{out}");
    }

    /// The 408 is owed on *every* request, not just the first.
    ///
    /// `wrote_bytes` suppresses the 408 so it is never spliced into a response
    /// in flight; latched for the life of the connection it would instead
    /// suppress every 408 after the first response, silently closing where
    /// native answers. Only a second request on a live connection catches that.
    #[tokio::test]
    async fn header_timeout_writes_408_on_a_reused_connection() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(HyperBackend::serve(
            server,
            Rc::new(hello),
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
            Bytes::new(),
        ));
        let (first, second) = local
            .run_until(async move {
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n")
                    .await
                    .unwrap();
                // Drain the whole first response before sending anything else,
                // so the second head cannot arrive while the write is in
                // flight — that is the other, already-covered case.
                let mut first = String::new();
                let mut buf = [0u8; 256];
                while !first.ends_with("hi") {
                    let n = client.read(&mut buf).await.unwrap();
                    assert!(n > 0, "server closed early: {first}");
                    first.push_str(&String::from_utf8_lossy(&buf[..n]));
                }
                // A second head that never finishes.
                client.write_all(b"GET / HTT").await.unwrap();
                let mut rest = Vec::new();
                let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut rest))
                    .await;
                let _ = task.await;
                (first, String::from_utf8_lossy(&rest).into_owned())
            })
            .await;
        assert!(first.starts_with("HTTP/1.1 200 OK"), "{first}");
        assert!(
            second.starts_with("HTTP/1.1 408"),
            "the second request is owed a 408 too, cleanly after the first \
             response rather than spliced into it: {second:?}"
        );
    }

    #[tokio::test]
    async fn body_timeout_writes_408() {
        let limits = Limits {
            body_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhel",
            echo,
            limits,
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 408"), "{out}");
    }

    /// hyper writes the status line — and picks its framing — from the
    /// response's version, so the request's has to be carried across.
    #[tokio::test]
    async fn response_echoes_the_request_version() {
        let out = exchange(
            b"GET / HTTP/1.0\r\nHost: a\r\n\r\n",
            hello,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.0 200 OK"), "{out}");
    }

    /// A stream that keeps making progress must never expire, however long it
    /// runs. Under the earlier `Phase::Idle`-during-write scheme this died at
    /// `idle_timeout` (200ms here) with a truncated body.
    #[tokio::test]
    async fn a_progressing_stream_outlives_every_deadline() {
        async fn stream(_req: Request) -> Response {
            Response::ok().with_body(TickStream::body(
                10,
                Duration::from_millis(30),
                Bytes::from_static(b"chunk"),
            ))
        }
        let limits = Limits {
            write_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        // ~300ms of streaming under a 100ms write_timeout, a 200ms
        // idle_timeout and a 200ms header_timeout: only per-write re-arming
        // gets the whole body out.
        let out = exchange(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            stream,
            limits,
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        // Counted as framed chunks: "chunked" in the Transfer-Encoding field
        // would otherwise inflate a bare `matches("chunk")`.
        assert_eq!(out.matches("5\r\nchunk\r\n").count(), 10, "{out}");
    }

    /// A peer that stops reading stalls the write. Native writes nothing and
    /// closes; a bare 408 here would land inside the body already in flight.
    #[tokio::test]
    async fn a_stalled_write_closes_silently_without_splicing_a_408() {
        async fn stream(_req: Request) -> Response {
            Response::ok().with_body(TickStream::body(
                64,
                Duration::ZERO,
                Bytes::from(vec![b'x'; 4096]),
            ))
        }
        let limits = Limits {
            write_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        // A 512-byte transport the client deliberately does not drain: the head
        // fits, the 256 KiB body cannot, so hyper's write goes Pending and
        // stays there. The request is pipelined so bytes also arrive *during*
        // the write, which is the case that used to re-phase to `Head` and
        // splice a 408 into the body.
        let (mut client, server) = tokio::io::duplex(512);
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(HyperBackend::serve(
            server,
            Rc::new(stream),
            cfg(limits),
            Rc::new(RefCell::new(DateCache::new())),
            Bytes::new(),
        ));
        let out = local
            .run_until(async move {
                client
                    .write_all(
                        b"GET / HTTP/1.1\r\nHost: a\r\n\r\nGET /two HTTP/1.1\r\nHost: a\r\n\r\n",
                    )
                    .await
                    .unwrap();
                // Read nothing until well past write_timeout.
                tokio::time::sleep(Duration::from_millis(400)).await;
                let mut out = Vec::new();
                let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                    .await;
                let _ = task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(
            !out.contains("408"),
            "a 408 must never be spliced into a response in flight: {out}"
        );
        assert!(out.len() < 4096, "the stall must cut the body short: {out}");
    }

    #[tokio::test]
    async fn upgrade_hands_back_transport_and_buffered_bytes() {
        async fn switching(_req: Request) -> Response {
            Response::new(101)
                .header(crate::HeaderId::Upgrade, Bytes::from_static(b"raw"))
                .header(crate::HeaderId::Connection, Bytes::from_static(b"upgrade"))
        }
        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(HyperBackend::serve(
            server,
            Rc::new(switching),
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
            Bytes::new(),
        ));
        let upgraded = local
            .run_until(async move {
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\nConnection: upgrade\r\nUpgrade: raw\r\n\r\nFIRSTFRAME")
                    .await
                    .unwrap();
                let mut buf = [0u8; 256];
                let n = client.read(&mut buf).await.unwrap();
                let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                assert!(head.starts_with("HTTP/1.1 101"), "{head}");
                task.await.expect("join").expect("serve")
            })
            .await;
        let upgraded = upgraded.expect("transport handed back");
        assert_eq!(&upgraded.buffered[..], b"FIRSTFRAME");
    }

    #[tokio::test]
    async fn unread_body_forces_close() {
        async fn ignore(_req: Request) -> Response {
            Response::status_only(404)
        }
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhelloGET / HTTP/1.1\r\nHost: a\r\n\r\n",
            ignore,
            Limits::default(),
        )
        .await;
        assert_eq!(
            out.matches("HTTP/1.1").count(),
            1,
            "unread body must not enable reuse: {out}"
        );
    }

    /// hyper reads keep-alive off the response's `Connection` field, so a
    /// handler that sets one must not be able to keep a connection the native
    /// loop closes unconditionally on an unread body. Before the bridge dropped
    /// the handler's field under a forced close, hyper drained the small body
    /// and served the pipelined request too.
    #[tokio::test]
    async fn a_handler_connection_header_cannot_defeat_the_unread_body_close() {
        async fn ignore(_req: Request) -> Response {
            Response::status_only(404).header(
                crate::HeaderId::Connection,
                Bytes::from_static(b"keep-alive"),
            )
        }
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhelloGET / HTTP/1.1\r\nHost: a\r\n\r\n",
            ignore,
            Limits::default(),
        )
        .await;
        assert_eq!(
            out.matches("HTTP/1.1").count(),
            1,
            "unread body must not enable reuse: {out}"
        );
        assert!(
            out.to_ascii_lowercase().contains("connection: close"),
            "{out}"
        );
    }

    /// A handler `Connection` value hyper's `HeaderValue` rejects is dropped;
    /// it must not also suppress the bridge's own field, or the response goes
    /// out with no `Connection` at all where native emits one. The native
    /// writer's `emitted` applies the same writability filter for the same
    /// reason.
    #[tokio::test]
    async fn an_invalid_handler_connection_header_does_not_suppress_ours() {
        async fn bad(_req: Request) -> Response {
            Response::status_only(200).header(
                crate::HeaderId::Connection,
                Bytes::from_static(b"keep\r\nalive"),
            )
        }
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhelloGET / HTTP/1.1\r\nHost: a\r\n\r\n",
            bad,
            Limits::default(),
        )
        .await;
        assert!(
            out.to_ascii_lowercase().contains("connection: close"),
            "an unwritable handler field must not leave the response without \
             one: {out}"
        );
        assert_eq!(out.matches("HTTP/1.1").count(), 1, "{out}");
    }

    /// The body-phase 408 carries the request's version, as the native loop's
    /// `write_error(version, 408)` does. (A header-phase 408 has no version to
    /// echo yet, and stays 1.1 under both backends.)
    #[tokio::test]
    async fn the_body_timeout_408_echoes_an_http_10_request_version() {
        let limits = Limits {
            body_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let out = exchange(
            b"POST / HTTP/1.0\r\nHost: a\r\nContent-Length: 5\r\n\r\nhel",
            echo,
            limits,
        )
        .await;
        assert!(out.starts_with("HTTP/1.0 408"), "{out}");
    }
}
