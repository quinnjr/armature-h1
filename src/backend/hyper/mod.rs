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
//! Known timing divergences, for BACKENDS.md:
//!
//! - `write_timeout` has no phase. The per-flush boundary the native writer
//!   re-arms on is invisible from outside hyper, so a response in flight sits
//!   in `Phase::Idle`: a peer that stops reading mid-response is cut off by
//!   `idle_timeout` rather than `write_timeout` (75s vs 30s by default).
//! - `max_head_bytes` below hyper's 8 KiB `max_buf_size` floor is enforced only
//!   by the cumulative check in `bridge::convert_head`, not on the wire.

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
}

/// The connection's phase, plus a change notification for the watchdog.
///
/// `generation` exists so an expiry that races a phase change loses: the
/// watchdog snapshots the generation before arming and discards an expiry whose
/// generation moved, rather than answering 408 for a phase that already ended.
pub(crate) struct PhaseClock {
    phase: Cell<Phase>,
    generation: Cell<u64>,
    notify: tokio::sync::Notify,
}

impl PhaseClock {
    pub(crate) fn new(phase: Phase) -> Self {
        Self {
            phase: Cell::new(phase),
            generation: Cell::new(0),
            notify: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn phase(&self) -> Phase {
        self.phase.get()
    }

    /// Enter `p`, bumping the generation and waking the watchdog.
    pub(crate) fn set(&self, p: Phase) {
        self.phase.set(p);
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
    status: u16,
) {
    let mut out = bytes::BytesMut::new();
    {
        let mut date = date.borrow_mut();
        let date_bytes = date.get(std::time::SystemTime::now());
        write::write_head(
            &mut out,
            Version::Http11,
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
        let bridge = Bridge {
            service,
            cfg: cfg.clone(),
            upgrade_slot: upgrade_slot.clone(),
            sent_101: sent_101.clone(),
            phase: clock.clone(),
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
            // Idle expiry: silent close, no response owed (native parity).
            Err(Phase::Idle) => {
                drop(conn);
                Ok(None)
            }
            // Header/body expiry: native writes a bare 408 and closes.
            Err(Phase::Head | Phase::Handler) => {
                // Release hyper's borrow of the shared IO before writing.
                drop(conn);
                write_error_close(&shared, &date, 408).await;
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
}
