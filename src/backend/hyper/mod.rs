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
//! `body_timeout` when they apply to an otherwise-idle connection. Where it is
//! not — `write_timeout` granularity, and a pipelined head that arrives
//! *during* a response write — and for every non-timing divergence (hyper's
//! parser permissiveness, its own status choices for heads it rejects, the
//! `max_buf_size` floor under `max_head_bytes`, its status-code type),
//! BACKENDS.md is the exhaustive list, with the reason each one cannot be
//! shimmed. A partial head arriving mid-write is one such case: `note_read`
//! only re-phases from `Phase::Idle` (see its doc), so those bytes sit
//! buffered without starting `header_timeout`; once the response finishes and
//! the connection returns to `Phase::Idle`, the already-buffered partial head
//! does not produce another read to re-arm anything, so the wait is bounded
//! by `idle_timeout` with a silent close rather than `header_timeout` and a
//! 408. That is the safe direction — no unbounded wait — just not the native
//! reaction; pinned by the `tests` module's
//! `a_partial_head_arriving_mid_write_is_bounded_by_the_idle_deadline`.
//! Every `#[cfg_attr(feature = "hyper-backend", ignore)]`
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
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

/// hyper's hard floor for `max_buf_size`; passing less panics.
const MIN_HYPER_BUF: usize = 8 * 1024;

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
    /// Whether any byte of the current *final* response has reached the
    /// transport. Set on every nonzero write taken in [`Phase::Write`], reset
    /// whenever a fresh response begins. Gates the bare-408 write: bytes of a
    /// final response already on the wire mean a 408 would be spliced into a
    /// message the peer is mid-parse of.
    ///
    /// Also set on writes taken in [`Phase::Head`], which are hyper's *own*
    /// final error response (400/431/505) for a head it refused to parse: a
    /// bare 408 appended to a truncated 400 is two final responses for one
    /// request, which poisons the response queue behind a pipelining proxy.
    ///
    /// Writes taken in [`Phase::Handler`] are *interim* — hyper's eager
    /// `100 Continue` is the only one this crate can produce — and must not
    /// latch the flag: an interim response is explicitly followed by a final
    /// one, so a 408 after it is correct framing, and it is what the native
    /// loop writes (`conn.rs`, the `write_error(version, 408)` after the
    /// handler-phase deadline, which is unconditional).
    wrote_bytes: Cell<bool>,
    /// Whether the current response body has reported end-of-stream. Half of
    /// the "response finished" signal; the other half is a drained write
    /// buffer, which is a successful flush.
    body_done: Cell<bool>,
    /// Set once the transport has been handed to an upgrade consumer. From
    /// that point nobody is watching this clock — the watchdog is gone with
    /// the serve future — but the handed-out `SharedIo` still routes through
    /// `IoShared`, so post-101 traffic would keep calling `note_write` and
    /// `note_flush`. Detaching makes both no-ops rather than pointless
    /// `notify_waiters()` calls and phase flips on a dead clock.
    detached: Cell<bool>,
    notify: tokio::sync::Notify,
}

impl PhaseClock {
    pub(crate) fn new(phase: Phase) -> Self {
        Self {
            phase: Cell::new(phase),
            generation: Cell::new(0),
            wrote_bytes: Cell::new(false),
            body_done: Cell::new(false),
            detached: Cell::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Stop tracking: the transport has left for an upgrade consumer.
    pub(crate) fn detach(&self) {
        self.detached.set(true);
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
        if self.detached.get() {
            return;
        }
        if matches!(p, Phase::Idle | Phase::Handler) {
            self.wrote_bytes.set(false);
            self.body_done.set(false);
        }
        self.phase.set(p);
        self.bump();
    }

    /// Record that bytes reached the transport, re-arming a write deadline.
    ///
    /// The two effects are scoped differently.
    ///
    /// *Re-arming* is a [`Phase::Write`] concept only: it is response progress
    /// buying more `write_timeout`. A write taken under `header_timeout` or
    /// `body_timeout` must not extend the deadline the peer is being measured
    /// against.
    ///
    /// *Latching* `wrote_bytes` covers [`Phase::Write`] **and**
    /// [`Phase::Head`]. The bridge enters `Phase::Write` before it hands a
    /// response back — success path and both rejection paths — so every byte
    /// of *this crate's* final responses is taken there. But hyper writes its
    /// own final 4xx/5xx (400, 431, 505) for a head it refuses to parse, and
    /// it does that while the clock is still in `Phase::Head`. Those bytes are
    /// a final response too: a head-phase expiry racing them would truncate
    /// the 400 and append a bare 408, giving the peer two final responses for
    /// one request. Writes in [`Phase::Handler`] are the interim
    /// `100 Continue`, which deliberately does not latch (see `wrote_bytes`).
    pub(crate) fn note_write(&self) {
        if self.detached.get() {
            return;
        }
        if matches!(self.phase.get(), Phase::Write | Phase::Head) {
            self.wrote_bytes.set(true);
        }
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
        if self.detached.get() {
            return;
        }
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
///
/// The error cannot be moved out of the chain (it is borrowed), so it is
/// rebuilt. An OS-level error is rebuilt from its errno, which keeps
/// `raw_os_error()` intact: the native loop propagates the original error, and
/// an embedder matching on errno must get the same answer under both backends.
/// Errors with no errno — hyper's own synthesized IO errors — fall back to
/// kind plus message, which is all there is to preserve.
fn find_io_error(e: &::hyper::Error) -> Option<stdio::Error> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(s) = source {
        if let Some(io_err) = s.downcast_ref::<stdio::Error>() {
            return Some(match io_err.raw_os_error() {
                Some(code) => stdio::Error::from_raw_os_error(code),
                None => stdio::Error::new(io_err.kind(), io_err.to_string()),
            });
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
        peer: Option<SocketAddr>,
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
            peer,
        };

        let mut builder = ::hyper::server::conn::http1::Builder::new();
        builder
            // Timing policy is ours: the watchdog owns every deadline.
            .header_read_timeout(None)
            // Wire-level head cap; the cumulative cap lives in convert_head.
            // hyper refuses to go below its own 8 KiB floor, so a smaller
            // configured cap is enforced only by `convert_head`.
            .max_buf_size(cfg.limits.max_head_bytes.max(MIN_HYPER_BUF))
            // Field-count cap, pushed down so hyper's parser and
            // `convert_head` agree on where "too many headers" starts.
            // hyper's own default is 100 while `Limits::max_headers` may be
            // configured up to `MAX_HEADERS_CEILING` (128), so without this a
            // 101..=128 configuration has hyper rejecting heads this crate
            // accepts. Both rejections are a 431, so pushing the real cap down
            // costs no status divergence. There is no floor on this setter
            // (unlike `max_buf_size`), but setting it at all moves hyper's
            // header scratch from the stack to the heap — one allocation per
            // request, which hyper documents as roughly a 5% parse cost. That
            // is the price of the two parsers agreeing.
            .max_headers(cfg.limits.max_headers)
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
                // Unless bytes of a *final* response are already on the wire,
                // which a 408 would be spliced into. In `Phase::Head` that is
                // reachable: hyper writes its own 400/431/505 for a head it
                // refuses, under this very phase, and a head-phase expiry can
                // race it. In `Phase::Handler` it is not — entering `Handler`
                // clears the flag and the only write taken there is hyper's
                // interim `100 Continue`, which `note_write` deliberately does
                // not latch (it is defined to be followed by a final response,
                // and native writes its 408 after one too).
                debug_assert!(
                    phase != Phase::Handler || !clock.wrote_bytes(),
                    "no final-response bytes can be on the wire in Phase::Handler; \
                     a new transition into it, or latching interim writes, would \
                     silently start suppressing the body-timeout 408",
                );
                if !clock.wrote_bytes() {
                    // Deadline the write too. This is the one serving-path
                    // await reached *because* the peer misbehaved, and a peer
                    // with a zero receive window can leave it Pending forever
                    // — pinning the task, the fd and the shared IO with no
                    // timeout at all. `write_timeout` is the deadline that
                    // governs every other write on the connection.
                    deadline.arm(cfg.limits.write_timeout);
                    tokio::select! {
                        biased;
                        () = deadline.expired() => {}
                        () = write_error_close(&shared, &date, version, 408) => {}
                    }
                }
                Ok(None)
            }
            Ok(Ok(())) => {
                if sent_101.get() && upgrade_slot.borrow_mut().take().is_some() {
                    let parts = conn.into_parts();
                    // The transport outlives the watchdog from here on; stop
                    // the handed-out `SharedIo` from driving a clock nobody
                    // watches.
                    clock.detach();
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

    /// Everything the server wrote, and whether it closed the connection.
    ///
    /// The `closed` half is load-bearing, not decoration: without it a server
    /// that *hangs* is indistinguishable from one that closed silently, and
    /// every "closes silently" assertion degenerates into "wrote nothing
    /// within two seconds" — which a deleted timeout also satisfies. Same
    /// shape as `tests/rfc9112.rs`'s `Exchange`.
    async fn exchange<S>(input: &'static [u8], service: S, limits: Limits) -> (String, bool)
    where
        S: crate::H1Service + 'static,
    {
        exchange_with_cfg(input, service, cfg(limits)).await
    }

    async fn exchange_with_cfg<S>(
        input: &'static [u8],
        service: S,
        config: Rc<ConnConfig>,
    ) -> (String, bool)
    where
        S: crate::H1Service + 'static,
    {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(HyperBackend::serve(
            server,
            Rc::new(service),
            config,
            Rc::new(RefCell::new(DateCache::new())),
            Bytes::new(),
            None,
        ));
        local
            .run_until(async move {
                client.write_all(input).await.unwrap();
                let mut out = Vec::new();
                // `read_to_end` returning `Ok` means the server closed its
                // side; the timeout elapsing means it is still holding on.
                let closed = matches!(
                    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                        .await,
                    Ok(Ok(_))
                );
                // Bound the join as well. A serve future that never resolves
                // has to surface as a failed `closed` assertion in the caller,
                // not as a test binary that hangs until CI's own timeout —
                // which is the same "hang looks like success" hole `closed`
                // exists to close.
                let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
                (String::from_utf8_lossy(&out).into_owned(), closed)
            })
            .await
    }

    #[tokio::test]
    async fn serves_a_single_request() {
        let (out, _closed) = exchange(
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
        let (out, _closed) = exchange(
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
        let (out, _closed) = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
            echo,
            Limits::default(),
        )
        .await;
        assert!(out.ends_with("hello"), "{out}");

        let (out, _closed) = exchange(
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
        let (out, _closed) = exchange(
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
        let (out, closed) = exchange(b"", hello, Limits::default()).await;
        assert!(out.is_empty(), "no response owed on idle close: {out}");
        // Both halves are needed: "wrote nothing" alone is also what a server
        // that never times out at all produces.
        assert!(
            closed,
            "the idle deadline must actually close the connection"
        );
    }

    #[tokio::test]
    async fn header_timeout_writes_408() {
        // A started-but-never-finished head.
        let (out, _closed) = exchange(b"GET / HTT", hello, Limits::default()).await;
        assert!(out.starts_with("HTTP/1.1 408"), "{out}");
    }

    /// The 408 write needs a deadline of its own.
    ///
    /// It is the one serving-path await reached *because* the peer misbehaved,
    /// and it is aimed straight back at that peer. A slowloris that advertises
    /// a zero receive window leaves the write Pending forever, and an
    /// un-deadlined write there pins the task, the fd and the shared IO for
    /// the life of the process — no timeout, no close, no bound.
    #[tokio::test]
    async fn the_408_write_is_bounded_when_the_peer_never_reads() {
        let limits = Limits {
            write_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        // An 8-byte transport the client never drains: far too small for the
        // 408 head, so the write parks after the first few bytes.
        let (mut client, server) = tokio::io::duplex(8);
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(HyperBackend::serve(
            server,
            Rc::new(hello),
            cfg(limits),
            Rc::new(RefCell::new(DateCache::new())),
            Bytes::new(),
            None,
        ));
        let finished = local
            .run_until(async move {
                // A head that never finishes, so `header_timeout` (200ms via
                // `quick`) fires and the 408 path is taken.
                client.write_all(b"GET / HTT").await.unwrap();
                // `client` stays alive for the whole wait — dropping it would
                // fail the write with a broken pipe and prove nothing. It is
                // simply never read from.
                let r = tokio::time::timeout(Duration::from_secs(2), task).await;
                // Keep the read half open until the assertion window closes.
                drop(client);
                r
            })
            .await;
        // ~200ms header_timeout + ~100ms write_timeout, bounded well under the
        // 2s outer timeout. Without the deadline on the write this never
        // resolves.
        assert!(
            finished.is_ok(),
            "an un-deadlined 408 write pins the connection forever"
        );
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
        // A deliberately roomy `idle_timeout`: the whole first exchange —
        // request write, handler, response write, and the client's read loop —
        // has to fit inside it on a CI box running four feature rows at once,
        // and 200ms is not the margin that buys. What this test discriminates
        // is unaffected: the 408 comes from `header_timeout` (200ms) starting
        // when the *partial second head* arrives, and 1s of idle grace before
        // it can only make a wrongly-suppressed 408 easier to see, not harder.
        let config = Rc::new(ConnConfig {
            limits: Limits {
                idle_timeout: Duration::from_secs(1),
                header_timeout: Duration::from_millis(200),
                ..Default::default()
            },
            tick: Duration::from_millis(10),
            server_name: None,
        });
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(HyperBackend::serve(
            server,
            Rc::new(hello),
            config,
            Rc::new(RefCell::new(DateCache::new())),
            Bytes::new(),
            None,
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
        let (out, _closed) = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhel",
            echo,
            limits,
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 408"), "{out}");
    }

    /// hyper answers `Expect: 100-continue` eagerly, at head-parse time. The
    /// interim response must not suppress the body-phase 408: an interim
    /// response is *defined* to be followed by a final one, so `100` then `408`
    /// is correct framing, and it is exactly what the native loop writes (its
    /// handler-phase `write_error(version, 408)` is unconditional).
    ///
    /// Discriminating: with `wrote_bytes` latched on any write rather than only
    /// on writes taken in `Phase::Write`, hyper's eager `100` sets the flag and
    /// the connection closes silently where native answers.
    #[tokio::test]
    async fn an_interim_100_continue_does_not_suppress_the_body_timeout_408() {
        let limits = Limits {
            body_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        // A declared body that never arrives, after an `Expect` hyper answers.
        let (out, _closed) = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n",
            echo,
            limits,
        )
        .await;
        assert!(
            out.starts_with("HTTP/1.1 100 Continue"),
            "hyper answers the Expect eagerly: {out:?}"
        );
        assert!(
            out.contains("HTTP/1.1 408"),
            "the interim response must not suppress the 408: {out:?}"
        );
    }

    /// hyper never polls a `HEAD` response's body, so the body's own
    /// end-of-stream can never fire and the clock would stay in `Phase::Write`
    /// forever — parking the keep-alive wait under `write_timeout` (30s) rather
    /// than `idle_timeout`. The bridge signals end-of-stream on hyper's behalf;
    /// this checks the connection really does return to `Phase::Idle`.
    #[tokio::test]
    async fn a_head_response_returns_the_connection_to_the_idle_deadline() {
        async fn head_hello(_req: Request) -> Response {
            // A non-empty body, so nothing else could report end-of-stream:
            // `HyperOutBody::new` only pre-marks empty bodies as done.
            Response::text("hi")
        }
        let started = tokio::time::Instant::now();
        // idle_timeout is 200ms (via `quick`), write_timeout the 30s default.
        let (out, _closed) = exchange(
            b"HEAD / HTTP/1.1\r\nHost: a\r\n\r\n",
            head_hello,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out:?}");
        assert!(
            !out.ends_with("hi"),
            "a HEAD response carries no body: {out:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "closed after {:?}, so the connection never left Phase::Write",
            started.elapsed()
        );
    }

    /// The same shim, asserted structurally rather than by timing: a keep-alive
    /// `HEAD` must leave the connection reusable for a pipelined follow-up.
    #[tokio::test]
    async fn a_head_response_keeps_the_connection_reusable() {
        let (out, _closed) = exchange(
            b"HEAD / HTTP/1.1\r\nHost: a\r\n\r\nGET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            hello,
            Limits::default(),
        )
        .await;
        assert_eq!(
            out.matches("HTTP/1.1 200 OK").count(),
            2,
            "the follow-up request must be served too: {out:?}"
        );
        assert!(out.ends_with("hi"), "only the GET carries a body: {out:?}");
    }

    /// hyper writes the status line — and picks its framing — from the
    /// response's version, so the request's has to be carried across.
    #[tokio::test]
    async fn response_echoes_the_request_version() {
        let (out, _closed) = exchange(
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
            write_timeout: Duration::from_millis(300),
            ..Default::default()
        };
        // ~300ms of streaming under a 300ms write_timeout, a 200ms
        // idle_timeout and a 200ms header_timeout: only per-write re-arming
        // gets the whole body out, because the *total* run outlives both
        // 200ms deadlines. The write_timeout is 10x the 30ms inter-chunk gap
        // rather than 3.3x so a scheduling hiccup on a loaded CI box cannot
        // expire a stream that is in fact progressing.
        let (out, _closed) = exchange(
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
            None,
        ));
        let (out, closed) = local
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
                let closed = matches!(
                    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                        .await,
                    Ok(Ok(_))
                );
                let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
                (String::from_utf8_lossy(&out).into_owned(), closed)
            })
            .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(
            !out.contains("408"),
            "a 408 must never be spliced into a response in flight: {out}"
        );
        assert!(out.len() < 4096, "the stall must cut the body short: {out}");
        // The silent half of the claim: the write deadline has to actually
        // close the connection. Without this, a server that simply parked
        // forever would satisfy every assertion above.
        assert!(closed, "the stalled write must close the connection");
    }

    /// A pipelined head split across a response write is bounded by
    /// `idle_timeout`, not `header_timeout`: `note_read` only re-phases from
    /// `Phase::Idle` (deliberately — see its doc, and
    /// `reads_during_a_write_do_not_re_phase` in `io.rs`), so a partial head
    /// landing in `Phase::Write` never starts the header clock. Once the
    /// response finishes and the connection returns to `Phase::Idle`, those
    /// same bytes are already buffered — no further read arrives to start
    /// the clock there either — so the wait is bounded by `idle_timeout` with
    /// a silent close, not `header_timeout` and a 408. Pins the module doc's
    /// caveat on the "exact for idle/header/body" claim.
    #[tokio::test]
    async fn a_partial_head_arriving_mid_write_is_bounded_by_the_idle_deadline() {
        async fn stream(_req: Request) -> Response {
            // 10 chunks rather than 5: the client's partial second head has to
            // land while the response is still going out, and a ~600ms stream
            // leaves that window wide even when the runtime is contended.
            Response::ok().with_body(TickStream::body(
                10,
                Duration::from_millis(60),
                Bytes::from_static(b"chunk"),
            ))
        }
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(HyperBackend::serve(
            server,
            Rc::new(stream),
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
            Bytes::new(),
            None,
        ));
        let (first, rest, closed) = local
            .run_until(async move {
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n")
                    .await
                    .unwrap();
                // Wait for the response write to be under way, then send half
                // a second head while it is still going out.
                let mut buf = [0u8; 256];
                let n = client.read(&mut buf).await.unwrap();
                assert!(n > 0, "no response bytes arrived");
                client.write_all(b"GET /two HTT").await.unwrap();
                // Drain the rest of the (chunked, short) response.
                let mut first = String::from_utf8_lossy(&buf[..n]).into_owned();
                while !first.ends_with("0\r\n\r\n") {
                    let n = client.read(&mut buf).await.unwrap();
                    assert!(n > 0, "server closed before the response finished: {first}");
                    first.push_str(&String::from_utf8_lossy(&buf[..n]));
                }
                // Now stall: the second head is never completed. idle_timeout
                // (200ms via `quick`) should close the connection silently,
                // not answer a 408 for the buffered partial head.
                let mut rest = Vec::new();
                let closed = matches!(
                    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut rest))
                        .await,
                    Ok(Ok(_))
                );
                let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
                (first, String::from_utf8_lossy(&rest).into_owned(), closed)
            })
            .await;
        assert!(first.starts_with("HTTP/1.1 200 OK"), "{first}");
        assert!(
            rest.is_empty(),
            "a partial head buffered during Phase::Write must not produce a \
             408 once the connection returns to idle: {rest:?}"
        );
        // "Silently" means closed, not hung: the bound has to be `idle_timeout`
        // and not "no deadline applies here at all".
        assert!(
            closed,
            "the buffered partial head must still be bounded by idle_timeout"
        );
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
            None,
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
        let (out, _closed) = exchange(
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
        let (out, _closed) = exchange(
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
    ///
    /// The case has to be a *keep-alive* one to discriminate: under a forced
    /// close the bridge drops any handler `Connection` before the
    /// `HeaderValue` conversion is reached, so the value's validity never
    /// matters. HTTP/1.0 + keep-alive is the shape where the bridge owes a
    /// field of its own (`connection: keep-alive`) that a wrongly-latched
    /// `has_connection` would suppress.
    #[tokio::test]
    async fn an_invalid_handler_connection_header_does_not_suppress_ours() {
        async fn bad(_req: Request) -> Response {
            Response::status_only(200).header(
                crate::HeaderId::Connection,
                Bytes::from_static(b"keep\r\nalive"),
            )
        }
        let (out, _closed) = exchange(
            b"GET / HTTP/1.0\r\nHost: a\r\nConnection: keep-alive\r\n\r\n",
            bad,
            Limits::default(),
        )
        .await;
        assert!(
            out.to_ascii_lowercase().contains("connection: keep-alive"),
            "an unwritable handler field must not leave the response without \
             one: {out}"
        );
    }

    /// The `has_server` half of the same rule: a handler `Server` value hyper
    /// rejects must not suppress the configured `server_name`.
    #[tokio::test]
    async fn an_invalid_handler_server_header_does_not_suppress_the_configured_one() {
        async fn bad(_req: Request) -> Response {
            Response::status_only(200)
                .header(crate::HeaderId::Server, Bytes::from_static(b"ba\r\nd"))
        }
        let config = Rc::new(ConnConfig {
            limits: quick(Limits::default()),
            tick: Duration::from_millis(10),
            server_name: Some(Bytes::from_static(b"armature")),
        });
        let (out, _closed) = exchange_with_cfg(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            bad,
            config,
        )
        .await;
        assert!(
            out.to_ascii_lowercase().contains("server: armature"),
            "{out}"
        );
        assert!(!out.contains("ba\r\nd"), "{out}");
    }

    /// An explicit `Content-Length: 0` body is already exhausted, so a handler
    /// that never reads it has still "fully read" it and the connection is
    /// reusable — as it is under the native loop, whose constructor treats
    /// `Length(0)` as done. Before `from_backend` agreed, this forced a close
    /// and the pipelined follow-up went unanswered.
    #[tokio::test]
    async fn an_unread_content_length_zero_body_still_allows_reuse() {
        async fn ignore(_req: Request) -> Response {
            Response::status_only(204)
        }
        let (out, _closed) = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\nGET / HTTP/1.1\r\nHost: a\r\n\r\n",
            ignore,
            Limits::default(),
        )
        .await;
        assert_eq!(
            out.matches("HTTP/1.1 204").count(),
            2,
            "an already-exhausted body must not force a close: {out}"
        );
        assert!(
            !out.to_ascii_lowercase().contains("connection: close"),
            "{out}"
        );
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
        let (out, _closed) = exchange(
            b"POST / HTTP/1.0\r\nHost: a\r\nContent-Length: 5\r\n\r\nhel",
            echo,
            limits,
        )
        .await;
        assert!(out.starts_with("HTTP/1.0 408"), "{out}");
    }

    /// The hyper counterpart of `conn.rs`'s
    /// `retained_body_across_an_upgrade_closes_instead_of_panicking`.
    ///
    /// A handler that stashes the request `Body` in state outliving the
    /// response still holds a handle on the transport when the 101 handoff is
    /// attempted. The native loop forfeits the handoff for that reason
    /// (`into_parts` returns `None` while another handle is live) and answers
    /// `Ok(None)`. hyper's stack has no such shared-handle check to fail: the
    /// request body is `Incoming`, which is a channel endpoint rather than a
    /// borrow of the socket, so `conn.into_parts()` succeeds and the transport
    /// *is* handed back. That divergence is deliberate and recorded in
    /// BACKENDS.md; what both backends must guarantee — and what this pins —
    /// is that the serve future resolves, without panicking and without
    /// hanging.
    #[tokio::test]
    async fn a_retained_body_across_an_upgrade_neither_panics_nor_hangs() {
        thread_local! {
            static LEAKED: RefCell<Option<crate::Body>> = const { RefCell::new(None) };
        }

        async fn switching(req: Request) -> Response {
            LEAKED.with(|slot| *slot.borrow_mut() = Some(req.body));
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
            None,
        ));
        let served = local
            .run_until(async move {
                client
                    .write_all(
                        b"GET / HTTP/1.1\r\nHost: a\r\nConnection: upgrade\r\nUpgrade: raw\r\n\r\n",
                    )
                    .await
                    .unwrap();
                let mut buf = [0u8; 256];
                let n = client.read(&mut buf).await.unwrap();
                let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                assert!(head.starts_with("HTTP/1.1 101"), "{head}");
                tokio::time::timeout(Duration::from_secs(2), task).await
            })
            .await;
        let served = served
            .expect("the serve future must resolve, not hang")
            .expect("the worker task must not panic")
            .expect("serve");
        // Where native forfeits the handoff, hyper completes it.
        assert!(
            served.is_some(),
            "hyper's request body does not borrow the transport, so the \
             handoff still happens — the divergence from native's Ok(None)"
        );
        LEAKED.with(|slot| slot.borrow_mut().take());
    }

    /// `PhaseClock`'s rules, direct.
    ///
    /// Everything else that exercises them does so through the timing tests,
    /// which are the slowest and least precise instruments in the suite: a
    /// broken rule shows up there as a wrong status or a wall-clock margin,
    /// several hundred milliseconds later. These are the rules themselves.
    mod phase_clock {
        use super::super::{Phase, PhaseClock};

        /// `note_write` latches only where a *final* response can be in
        /// flight: `Write` (this crate's responses) and `Head` (hyper's own
        /// 400/431/505 for a head it refuses). `Idle` cannot carry a write at
        /// all, and `Handler` carries only the interim `100 Continue`.
        #[test]
        fn note_write_latches_only_in_the_write_and_head_phases() {
            for (phase, expected) in [
                (Phase::Idle, false),
                (Phase::Head, true),
                (Phase::Handler, false),
                (Phase::Write, true),
            ] {
                let c = PhaseClock::new(phase);
                assert!(!c.wrote_bytes(), "{phase:?} starts clean");
                c.note_write();
                assert_eq!(
                    c.wrote_bytes(),
                    expected,
                    "note_write in {phase:?} should {} latch",
                    if expected { "" } else { "not" }
                );
            }
        }

        /// Re-arming is narrower than latching: only `Write` progress buys
        /// more time. A write under `header_timeout` must not extend it.
        #[test]
        fn only_a_write_phase_write_re_arms_the_deadline() {
            for (phase, expected) in [
                (Phase::Idle, false),
                (Phase::Head, false),
                (Phase::Handler, false),
                (Phase::Write, true),
            ] {
                let c = PhaseClock::new(phase);
                let before = c.generation.get();
                c.note_write();
                assert_eq!(
                    c.generation.get() > before,
                    expected,
                    "note_write in {phase:?} bumped the generation unexpectedly"
                );
            }
        }

        /// Entering `Idle` or `Handler` starts a fresh response. Latched for
        /// the life of a keep-alive connection instead, `wrote_bytes` would
        /// suppress every 408 after the first response.
        #[test]
        fn entering_idle_or_handler_clears_the_per_response_flags() {
            for reset in [Phase::Idle, Phase::Handler] {
                let c = PhaseClock::new(Phase::Write);
                c.note_write();
                c.note_body_end();
                assert!(c.wrote_bytes());
                c.set(reset);
                assert!(!c.wrote_bytes(), "{reset:?} must clear wrote_bytes");
                // `body_done` is private; observe it through `note_flush`,
                // which only returns to `Idle` when it is set.
                c.set(Phase::Write);
                c.note_flush();
                assert_eq!(
                    c.phase(),
                    Phase::Write,
                    "{reset:?} must clear body_done, so a flush alone cannot \
                     end the response"
                );
            }
        }

        /// Entering `Head` or `Write` is not a fresh response and must not
        /// clear anything — `Head` in particular, or hyper's own error
        /// response would stop suppressing the spliced 408.
        #[test]
        fn entering_head_or_write_preserves_the_per_response_flags() {
            for keep in [Phase::Head, Phase::Write] {
                let c = PhaseClock::new(Phase::Write);
                c.note_write();
                c.set(keep);
                assert!(c.wrote_bytes(), "{keep:?} must not clear wrote_bytes");
            }
        }

        /// "Response finished" is body-exhausted *and* buffer-drained, in
        /// `Phase::Write`. Any weaker rule returns a live connection to the
        /// idle deadline mid-response.
        #[test]
        fn note_flush_returns_to_idle_only_on_a_finished_write() {
            // Flush with no end-of-stream: still writing.
            let c = PhaseClock::new(Phase::Write);
            c.note_flush();
            assert_eq!(c.phase(), Phase::Write);

            // End-of-stream and a flush: finished.
            let c = PhaseClock::new(Phase::Write);
            c.note_body_end();
            c.note_flush();
            assert_eq!(c.phase(), Phase::Idle);

            // The same signals outside `Phase::Write` change nothing.
            for phase in [Phase::Idle, Phase::Head, Phase::Handler] {
                let c = PhaseClock::new(phase);
                c.note_body_end();
                c.note_flush();
                assert_eq!(c.phase(), phase, "note_flush must be inert in {phase:?}");
            }
        }

        /// Every phase change has to bump: the watchdog snapshots the
        /// generation before arming and discards an expiry whose generation
        /// moved, which is what stops a 408 for a phase that already ended.
        #[test]
        fn every_set_bumps_the_generation() {
            let c = PhaseClock::new(Phase::Idle);
            let mut last = c.generation.get();
            for phase in [
                Phase::Head,
                Phase::Handler,
                Phase::Write,
                Phase::Idle,
                // Re-entering the same phase counts too.
                Phase::Idle,
            ] {
                c.set(phase);
                assert!(c.generation.get() > last, "set({phase:?}) must bump");
                last = c.generation.get();
            }
        }

        /// After the transport leaves for an upgrade consumer nobody is
        /// watching, so post-101 traffic must not drive the clock.
        #[test]
        fn a_detached_clock_ignores_everything() {
            let c = PhaseClock::new(Phase::Write);
            c.note_body_end();
            c.detach();
            let generation = c.generation.get();
            c.note_write();
            c.note_flush();
            c.set(Phase::Idle);
            assert_eq!(c.phase(), Phase::Write);
            assert!(!c.wrote_bytes());
            assert_eq!(c.generation.get(), generation, "a detached clock is quiet");
        }
    }
}
