//! The thread-per-core server.
//!
//! N pinned OS threads, each running a `current_thread` runtime. On Unix each
//! thread owns its own `SO_REUSEPORT` listener, so the kernel load-balances
//! accepts and a connection never migrates cores — which is what makes per-core
//! date caches and service state safe to keep non-atomic.

use crate::Limits;
use crate::conn::ConnConfig;
use crate::service::{H1Service, Transport};
use crate::tls::{H2Fallback, Preface, UpgradeConsumer, is_h2c_preface};
use crate::write::DateCache;
use bytes::{Bytes, BytesMut};
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::watch;

/// How long a worker stands down after `accept` returns an error.
///
/// See the accept arm of `worker_loop` for why standing down at all is the
/// point.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(10);

/// Socket-level tuning.
#[derive(Clone, Debug)]
pub struct TcpConfig {
    /// Disable Nagle's algorithm. On for a request/response protocol, where
    /// delaying a small response to coalesce it helps nobody.
    pub nodelay: bool,
    /// Listen backlog.
    pub backlog: i32,
    /// Use `SO_REUSEPORT` so each worker owns its own listener.
    ///
    /// Ignored on platforms without it, which fall back to one shared listener.
    pub reuse_port: bool,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            nodelay: true,
            backlog: 1024,
            reuse_port: true,
        }
    }
}

/// Server configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Address to bind.
    pub addr: SocketAddr,
    /// Worker threads. Defaults to the available parallelism.
    pub workers: usize,
    /// Per-connection limits and deadlines.
    pub limits: Limits,
    /// Socket tuning.
    pub tcp: TcpConfig,
    /// Deadline coarsening granularity.
    pub tick: Duration,
    /// Pin each worker to a core.
    pub pin_cores: bool,
    /// Value for the `Server` field, or none to omit it.
    pub server_name: Option<Bytes>,
    /// How long to let in-flight connections finish after a shutdown signal.
    pub shutdown_grace: Duration,
    /// Detect the h2c prior-knowledge preface on plaintext connections and route
    /// it to the HTTP/2 fallback.
    ///
    /// Costs one small read before the first request is parsed, so it is opt-in.
    pub detect_h2c: bool,
    /// TLS, when serving HTTPS.
    #[cfg(feature = "tls")]
    pub tls: Option<std::sync::Arc<rustls::ServerConfig>>,
}

impl Config {
    /// A default configuration for `addr`.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            workers: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            limits: Limits::default(),
            tcp: TcpConfig::default(),
            tick: Duration::from_millis(100),
            pin_cores: true,
            server_name: None,
            shutdown_grace: Duration::from_secs(10),
            detect_h2c: false,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Detect the h2c prior-knowledge preface on plaintext connections.
    pub fn detect_h2c(mut self, on: bool) -> Self {
        self.detect_h2c = on;
        self
    }

    /// Serve TLS with this rustls configuration.
    #[cfg(feature = "tls")]
    pub fn with_tls(mut self, tls: std::sync::Arc<rustls::ServerConfig>) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Set the worker count.
    pub fn workers(mut self, n: usize) -> Self {
        self.workers = n.max(1);
        self
    }

    /// Set the per-connection limits.
    ///
    /// `limits.max_headers` above [`crate::limits::MAX_HEADERS_CEILING`] is
    /// clamped to that ceiling, with a `tracing::warn!` — the parser's fixed
    /// scratch array cannot serve more than that regardless of configuration.
    pub fn limits(mut self, mut limits: Limits) -> Self {
        limits.clamp_max_headers();
        self.limits = limits;
        self
    }

    /// Set the `Server` field value.
    pub fn server_name(mut self, name: Bytes) -> Self {
        self.server_name = Some(name);
        self
    }

    /// Enable or disable core pinning.
    pub fn pin_cores(mut self, on: bool) -> Self {
        self.pin_cores = on;
        self
    }
}

/// A handle for stopping a running server.
///
/// `Clone + Send`, since it must cross into the thread that decides to stop.
#[derive(Clone, Debug)]
pub struct ServerHandle {
    tx: watch::Sender<bool>,
}

impl ServerHandle {
    /// Signal every worker to stop accepting and drain.
    ///
    /// Safe to call before [`Server::serve`] starts: the flag is set on the
    /// channel itself, and each worker checks its current value before its
    /// first accept, so a server told to stop before it began stops without
    /// ever accepting.
    pub fn shutdown(&self) {
        // `send_replace`, not `send`. `send` fails and **leaves the value
        // unchanged** when no receiver exists, which is precisely the state
        // between `bind` and `serve` — the workers have not subscribed yet, so
        // a shutdown in that window would be silently discarded and
        // `is_shutting_down` would go on reporting false. `send_replace` sets
        // the value regardless of who is listening.
        //
        // Idempotent by construction: replacing `true` with `true` is the same
        // state.
        let _ = self.tx.send_replace(true);
    }

    /// Whether shutdown has been signalled.
    pub fn is_shutting_down(&self) -> bool {
        *self.tx.borrow()
    }
}

/// How listeners are distributed across workers.
enum Listeners {
    /// One listener per worker, via `SO_REUSEPORT`.
    PerWorker(Vec<std::net::TcpListener>),
    /// One shared listener, for platforms without `SO_REUSEPORT`.
    ///
    /// `TcpListener::accept` takes `&self`, so this needs no lock.
    Shared(Arc<std::net::TcpListener>),
}

/// A bound, not-yet-serving server.
pub struct Server {
    cfg: Config,
    listeners: Listeners,
    local_addr: SocketAddr,
    tx: watch::Sender<bool>,
}

impl Server {
    /// Bind according to `cfg`.
    ///
    /// Binding happens here rather than in [`serve`](Self::serve) so that
    /// [`local_addr`](Self::local_addr) is available before serving starts —
    /// which is what lets a test bind port 0 and then connect to it.
    pub fn bind(cfg: Config) -> io::Result<Self> {
        let (listeners, local_addr) = if cfg.tcp.reuse_port && reuse_port_supported() {
            let mut v = Vec::with_capacity(cfg.workers);
            let mut addr = cfg.addr;
            for i in 0..cfg.workers {
                let l = bind_one(addr, &cfg.tcp, true)?;
                if i == 0 {
                    // With port 0, the first bind picks the port; the rest must
                    // join that same port or they would each get their own.
                    addr = l.local_addr()?;
                }
                v.push(l);
            }
            (Listeners::PerWorker(v), addr)
        } else {
            let l = bind_one(cfg.addr, &cfg.tcp, false)?;
            let addr = l.local_addr()?;
            (Listeners::Shared(Arc::new(l)), addr)
        };

        let (tx, _) = watch::channel(false);
        Ok(Self {
            cfg,
            listeners,
            local_addr,
            tx,
        })
    }

    /// The concrete bound address, with port 0 resolved.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// A handle for stopping this server.
    pub fn handle(&self) -> ServerHandle {
        ServerHandle {
            tx: self.tx.clone(),
        }
    }

    /// Serve until shutdown, blocking the calling thread.
    ///
    /// Connections this crate will not serve — a negotiated `h2`, or an h2c
    /// preface — are closed, as is one a handler upgrades with status 101. Use
    /// [`serve_with_fallback`](Self::serve_with_fallback) to route HTTP/2
    /// somewhere, and [`serve_with`](Self::serve_with) to also consume upgrades.
    ///
    /// `make` runs once per worker thread to produce that worker's service. Note
    /// the bounds: the *factory* is `Send`, because it crosses thread boundaries
    /// at startup; the *service* it produces is not, because it never does. That
    /// asymmetry is what lets per-core service state be non-atomic.
    pub fn serve<F, S>(self, make: F) -> io::Result<()>
    where
        F: Fn() -> S + Send + Clone + 'static,
        S: H1Service + 'static,
    {
        self.serve_with_fallback(make, || CloseH2)
    }

    /// Serve until shutdown, routing HTTP/2 connections to a fallback.
    ///
    /// `make_fallback` runs once per worker, like `make`, and for the same reason:
    /// the factory crosses thread boundaries at startup, the fallback it produces
    /// never does — so a fallback may hold non-`Send` state.
    ///
    /// An upgraded connection is still closed; use [`serve_with`](Self::serve_with)
    /// to consume those too.
    pub fn serve_with_fallback<F, S, G, H>(self, make: F, make_fallback: G) -> io::Result<()>
    where
        F: Fn() -> S + Send + Clone + 'static,
        S: H1Service + 'static,
        G: Fn() -> H + Send + Clone + 'static,
        H: H2Fallback + 'static,
    {
        self.serve_with(make, make_fallback, || CloseUpgrade)
    }

    /// Serve until shutdown, with both exits off the HTTP/1 path plugged.
    ///
    /// `make_upgrade` produces this worker's [`UpgradeConsumer`], which receives
    /// the transport whenever a handler answers an upgrade request with 101 —
    /// the WebSocket handoff. It runs once per worker with the same `Send`
    /// factory / non-`Send` product asymmetry as `make` and `make_fallback`.
    ///
    /// A handler that never reads the request body to its end forfeits the
    /// handoff and the connection closes instead, on **both** backends: the
    /// unread bytes are still on the wire, and the consumer would read them as
    /// the peer's first post-upgrade frames.
    ///
    /// A handler that *retains* a still-live [`Body`](crate::service::Body)
    /// past its response forfeits it too, but only on the default (native)
    /// backend, where the body holds a second handle on the transport and two
    /// readers on one socket is not a state this crate will produce. Under
    /// `hyper-backend` the body is a channel endpoint rather than a borrow of
    /// the socket, so there is no live handle to detect and the upgrade
    /// proceeds; `BACKENDS.md` records that divergence, and this crate's
    /// rendered documentation is built with that feature enabled. Drain the
    /// body and drop it before answering 101 and neither rule can bite.
    /// See [`Connection::serve`](crate::conn::Connection::serve).
    ///
    /// # Examples
    ///
    /// The shape to copy is the three factories: each is a `Fn` that is `Send`
    /// and `Clone` because it is handed to every worker thread, while what it
    /// returns stays on one worker and need not be either.
    ///
    /// ```no_run
    /// use armature_h1::{CloseH2, Config, HeaderId, Request, Response, Server};
    /// use armature_h1::{UpgradeConsumer, Upgraded};
    /// use bytes::Bytes;
    /// use std::future::Future;
    /// use std::pin::Pin;
    /// use std::rc::Rc;
    /// use tokio::io::AsyncWriteExt;
    ///
    /// async fn handler(req: Request) -> Response {
    ///     // Drop the body before answering 101, or the handoff is forfeited.
    ///     drop(req.body);
    ///     Response::new(101)
    ///         .header(HeaderId::Connection, Bytes::from_static(b"upgrade"))
    ///         .header(HeaderId::Upgrade, Bytes::from_static(b"raw"))
    /// }
    ///
    /// /// Non-`Send` on purpose: an upgrade consumer never leaves its worker.
    /// struct Sessions {
    ///     count: Rc<std::cell::Cell<u64>>,
    /// }
    ///
    /// impl UpgradeConsumer for Sessions {
    ///     fn handle(&self, upgraded: Upgraded) -> Pin<Box<dyn Future<Output = ()>>> {
    ///         self.count.set(self.count.get() + 1);
    ///         Box::pin(async move {
    ///             let Upgraded { mut io, buffered, peer: _ } = upgraded;
    ///             // `buffered` before `io`, always: see `UpgradeConsumer`.
    ///             let _ = io.write_all(&buffered).await;
    ///         })
    ///     }
    /// }
    ///
    /// let server = Server::bind(Config::new("127.0.0.1:8080".parse().unwrap()))?;
    /// // Blocks until `server.handle().shutdown()` is called.
    /// server.serve_with(
    ///     || handler,
    ///     || CloseH2,
    ///     || Sessions { count: Rc::new(std::cell::Cell::new(0)) },
    /// )?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn serve_with<F, S, G, H, U, C>(
        self,
        make: F,
        make_fallback: G,
        make_upgrade: U,
    ) -> io::Result<()>
    where
        F: Fn() -> S + Send + Clone + 'static,
        S: H1Service + 'static,
        G: Fn() -> H + Send + Clone + 'static,
        H: H2Fallback + 'static,
        U: Fn() -> C + Send + Clone + 'static,
        C: UpgradeConsumer + 'static,
    {
        let Server {
            cfg, listeners, tx, ..
        } = self;

        let core_ids = if cfg.pin_cores {
            core_affinity::get_core_ids().unwrap_or_default()
        } else {
            Vec::new()
        };

        // Each worker gets its own listener under SO_REUSEPORT, or a dup of the
        // one shared listener where that option does not exist.
        let mut per_worker: Vec<Option<std::net::TcpListener>> = match listeners {
            Listeners::PerWorker(v) => v.into_iter().map(Some).collect(),
            Listeners::Shared(shared) => (0..cfg.workers)
                .map(|_| match shared.try_clone() {
                    Ok(l) => Some(l),
                    Err(e) => {
                        // A missing worker beats a crashed server, so this
                        // stays a skip rather than a hard failure — but it
                        // must be visible, or a fleet silently runs short.
                        tracing::warn!(error = %e, "failed to clone the shared listener for a worker; that worker will not start");
                        None
                    }
                })
                .collect(),
        };

        // Subscribed once, before any worker starts, and cloned per worker. A
        // `subscribe()` inside the loop would mark the sender's *current* value
        // as already seen, so a `shutdown()` landing midway through the loop
        // would stop the workers subscribed before it and leave the rest
        // accepting forever — `serve` would then block in `join` with
        // `is_shutting_down()` reporting true. A clone inherits the seen
        // version from this one, taken before anything can be signalled.
        let base_rx = tx.subscribe();

        let mut handles = Vec::with_capacity(cfg.workers);
        for (worker, slot) in per_worker.iter_mut().enumerate() {
            let cfg = cfg.clone();
            let make = make.clone();
            let make_fallback = make_fallback.clone();
            let make_upgrade = make_upgrade.clone();
            let rx = base_rx.clone();
            let core = core_ids.get(worker).copied();
            let Some(std_listener) = slot.take() else {
                continue;
            };

            let spawned = std::thread::Builder::new()
                .name(format!("h1-{worker}"))
                .spawn(move || {
                    if let Some(core) = core {
                        // Best effort: a container may forbid it, and failing
                        // to pin costs locality, not correctness.
                        core_affinity::set_for_current(core);
                    }
                    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    else {
                        return;
                    };
                    rt.block_on(worker_loop(
                        std_listener,
                        make,
                        make_fallback,
                        make_upgrade,
                        cfg,
                        rx,
                    ));
                });

            match spawned {
                Ok(h) => handles.push(h),
                Err(e) => {
                    // The workers spawned before this one are already accepting
                    // on the bound port. Returning the error on its own would
                    // leave them there — invisible, unjoinable, and holding the
                    // port — so a caller that logs the failure and retries
                    // `bind` would end up with two generations of workers
                    // serving one address. Stop them and wait for them out
                    // before admitting the failure.
                    tracing::error!(error = %e, worker, "failed to spawn a worker; stopping the ones already started");
                    let _ = tx.send_replace(true);
                    for h in handles {
                        let _ = h.join();
                    }
                    return Err(e);
                }
            }
        }

        for h in handles {
            let _ = h.join();
        }
        Ok(())
    }
}

/// One worker's accept loop.
async fn worker_loop<F, S, G, H, U, C>(
    std_listener: std::net::TcpListener,
    make: F,
    make_fallback: G,
    make_upgrade: U,
    cfg: Config,
    mut rx: watch::Receiver<bool>,
) where
    F: Fn() -> S,
    S: H1Service + 'static,
    G: Fn() -> H,
    H: H2Fallback + 'static,
    U: Fn() -> C,
    C: UpgradeConsumer + 'static,
{
    std_listener.set_nonblocking(true).ok();
    let Ok(listener) = TcpListener::from_std(std_listener) else {
        return;
    };

    // Per-core state: created once here, shared by every connection on this
    // thread, and never touched by another. No atomics, no locks.
    let mut limits = cfg.limits.clone();
    let cfg = Rc::new(cfg);
    // Defense in depth: `Config::limits` already clamps on the way in, but
    // `cfg.limits` can also be set directly since `Config`'s fields are
    // public, so the parser's fixed scratch array still needs protecting
    // here.
    limits.clamp_max_headers();
    let conn_cfg = Rc::new(ConnConfig {
        limits,
        tick: cfg.tick,
        server_name: cfg.server_name.clone(),
    });
    let ctx = Rc::new(WorkerCtx {
        service: Rc::new(make()),
        fallback: Rc::new(make_fallback()),
        upgrades: Rc::new(make_upgrade()),
        conn_cfg,
        date: Rc::new(RefCell::new(DateCache::new())),
        cfg: cfg.clone(),
    });

    let local = tokio::task::LocalSet::new();

    local
        .run_until(async {
            loop {
                // Checked at the top of every iteration rather than only on
                // `changed()`. A shutdown signalled *before* this worker's
                // receiver existed is already the channel's current value and
                // will never produce a transition, so a transition-only loop
                // would accept forever on a server the caller has already
                // stopped. `changed()` still covers the signal arriving while
                // this worker is parked in `select!`.
                if *rx.borrow() {
                    break;
                }
                tokio::select! {
                    changed = rx.changed() => {
                        // `Err` means every sender is gone, which can only
                        // happen once nobody can ever signal shutdown again;
                        // treat it as the signal rather than spinning on a
                        // channel that will never yield.
                        if changed.is_err() || *rx.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let (stream, peer) = match accepted {
                            Ok(pair) => pair,
                            Err(e) => {
                                // Not a `continue`. A failed `accept` — the fd
                                // table full, `EMFILE`/`ENFILE` — leaves the
                                // listener readable, so re-polling it straight
                                // away yields the same error at whatever rate
                                // this core can manage. Thread-per-core means
                                // one such loop per core, and they would starve
                                // the `LocalSet` tasks serving the connections
                                // already accepted on the same current-thread
                                // runtime: a leaked-fd burst would take the box
                                // down rather than merely refuse new work. The
                                // pause is short enough that transient failures
                                // cost nothing measurable and long enough that
                                // a persistent one leaves the core to its real
                                // job.
                                tracing::warn!(error = %e, "accept failed; pausing before the next attempt");
                                tokio::time::sleep(ACCEPT_BACKOFF).await;
                                continue;
                            }
                        };
                        if cfg.tcp.nodelay {
                            let _ = stream.set_nodelay(true);
                        }
                        let ctx = ctx.clone();
                        tokio::task::spawn_local(async move {
                            dispatch(stream, ctx, peer).await;
                        });
                    }
                }
            }
        })
        .await;

    // Drain: give in-flight connections a bounded window to finish.
    let _ = tokio::time::timeout(cfg.shutdown_grace, local).await;
}

/// Everything one worker holds for the lifetime of the thread.
///
/// These six were once six arguments to `dispatch`, cloned one by one on every
/// accept. None of them varies per connection — they are the worker, not the
/// connection — so they live behind a single `Rc` and an accept costs one
/// refcount bump and one pointer move rather than six of each. The inner `Rc`s
/// stay because [`crate::backend::serve_connection`] is public and takes them
/// individually.
struct WorkerCtx<S, H, C> {
    service: Rc<S>,
    fallback: Rc<H>,
    upgrades: Rc<C>,
    conn_cfg: Rc<ConnConfig>,
    date: Rc<RefCell<DateCache>>,
    cfg: Rc<Config>,
}

/// Decide what protocol a connection speaks, then serve or hand it off.
///
/// With TLS off and h2c detection off — the default — this reduces to serving
/// HTTP/1 directly, with no extra read on the fast path.
async fn dispatch<S, H, C>(
    stream: tokio::net::TcpStream,
    ctx: Rc<WorkerCtx<S, H, C>>,
    peer: SocketAddr,
) where
    S: H1Service + 'static,
    H: H2Fallback + 'static,
    C: UpgradeConsumer + 'static,
{
    #[cfg(feature = "tls")]
    if let Some(tls) = ctx.cfg.tls.clone() {
        let acceptor = tokio_rustls::TlsAcceptor::from(tls);
        // The handshake is deadlined by the same limit that governs the request
        // head, because until it completes there is no `TlsStream` for the
        // connection's own timeouts to be armed against. Without this, a peer
        // that connects and then says nothing — or stops halfway through a
        // ClientHello — holds a task and an fd for as long as it likes, and
        // under `SO_REUSEPORT` it can aim every such socket at one worker.
        let handshake =
            tokio::time::timeout(ctx.cfg.limits.header_timeout, acceptor.accept(stream));
        let Ok(Ok(tls_stream)) = handshake.await else {
            // Neither a failed handshake nor an expired one is a protocol error
            // we can report over HTTP; the peer gets a TLS alert from rustls, or
            // nothing at all, and the socket closes.
            return;
        };
        let is_h2 = crate::tls::negotiated_h2(tls_stream.get_ref().1);
        if is_h2 {
            // Nothing has been read past the handshake, so there is no buffered
            // application data to forward.
            ctx.fallback
                .handle(Box::new(tls_stream), Bytes::new(), Some(peer))
                .await;
        } else {
            serve_h1(tls_stream, &ctx, Bytes::new(), peer).await;
        }
        return;
    }

    if ctx.cfg.detect_h2c {
        match peek_preface(stream, &ctx.cfg).await {
            Some((stream, buffered, Preface::Http2)) => {
                // The preface is part of the HTTP/2 stream and cannot be re-read
                // from the socket, so it must travel with the connection.
                ctx.fallback
                    .handle(Box::new(stream), buffered, Some(peer))
                    .await;
            }
            Some((stream, buffered, _)) => {
                serve_h1(stream, &ctx, buffered, peer).await;
            }
            None => {}
        }
        return;
    }

    serve_h1(stream, &ctx, Bytes::new(), peer).await;
}

/// Read just enough to classify a plaintext connection.
///
/// Returns `None` if the peer closed or errored before deciding. `NeedMore` at
/// EOF is reported as `Http1`, so a client that opens a connection and says
/// nothing is handled by the HTTP/1 path's idle timeout rather than being routed
/// to a fallback that has no bytes to work with.
async fn peek_preface(
    mut stream: tokio::net::TcpStream,
    cfg: &Config,
) -> Option<(tokio::net::TcpStream, Bytes, Preface)> {
    let mut buf = BytesMut::with_capacity(crate::tls::H2C_PREFACE.len());
    loop {
        match is_h2c_preface(&buf) {
            Preface::NeedMore => {}
            decided => return Some((stream, buf.freeze(), decided)),
        }
        let deadline = tokio::time::timeout(cfg.limits.header_timeout, stream.read_buf(&mut buf));
        match deadline.await {
            Ok(Ok(0)) => {
                // EOF mid-prefix: let the HTTP/1 path deal with it.
                return Some((stream, buf.freeze(), Preface::Http1));
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) | Err(_) => return None,
        }
    }
}

/// Serve one HTTP/1 connection to completion.
async fn serve_h1<IO, S, H, C>(io: IO, ctx: &WorkerCtx<S, H, C>, buffered: Bytes, peer: SocketAddr)
where
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
    S: H1Service + 'static,
    C: UpgradeConsumer + 'static,
{
    // A clean close and an I/O error are the same outcome here: the connection is
    // over and there is nobody left to tell.
    //
    // An upgraded transport goes to the connection consumer, which is
    // `CloseUpgrade` unless the caller plugged one in via `serve_with`.
    if let Ok(Some(upgraded)) = crate::backend::serve_connection(
        io,
        ctx.service.clone(),
        ctx.conn_cfg.clone(),
        ctx.date.clone(),
        buffered,
        Some(peer),
    )
    .await
    {
        ctx.upgrades.handle(upgraded).await;
    }
}

/// An [`H2Fallback`] that closes the connection.
///
/// The default. Closing is the honest outcome: mis-serving an HTTP/2 stream
/// through an HTTP/1 parser would produce nonsense, and there is nothing useful
/// to reply with over a protocol we do not speak.
#[derive(Clone, Copy, Debug, Default)]
pub struct CloseH2;

impl H2Fallback for CloseH2 {
    fn handle(
        &self,
        io: Box<dyn Transport>,
        buffered: Bytes,
        _peer: Option<SocketAddr>,
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async move {
            drop(buffered);
            drop(io);
        })
    }
}

/// An [`UpgradeConsumer`] that closes the transport.
///
/// The default, and what [`Server`] did unconditionally before `serve_with`
/// existed. Closing is the honest outcome for a handoff with no destination:
/// the alternative is a socket left open that nothing will ever read.
#[derive(Clone, Copy, Debug, Default)]
pub struct CloseUpgrade;

impl UpgradeConsumer for CloseUpgrade {
    fn handle(&self, upgraded: crate::service::Upgraded) -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async move {
            drop(upgraded);
        })
    }
}

/// Whether this platform load-balances accepts across `SO_REUSEPORT` sockets.
fn reuse_port_supported() -> bool {
    cfg!(all(
        unix,
        not(target_os = "solaris"),
        not(target_os = "illumos")
    ))
}

/// Bind one listener.
fn bind_one(
    addr: SocketAddr,
    tcp: &TcpConfig,
    reuse_port: bool,
) -> io::Result<std::net::TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => socket2::Domain::IPV4,
        SocketAddr::V6(_) => socket2::Domain::IPV6,
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    socket.set_reuse_address(true)?;

    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    #[cfg(not(all(unix, not(target_os = "solaris"), not(target_os = "illumos"))))]
    let _ = reuse_port;

    // Nagle is disabled on each accepted stream in `worker_loop`, which is where
    // it actually governs response latency.
    socket.bind(&addr.into())?;
    socket.listen(tcp.backlog)?;
    Ok(socket.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Request, Response};
    use std::net::{Ipv4Addr, SocketAddrV4};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn loopback() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
    }

    fn test_config(workers: usize) -> Config {
        // Short deadlines so a test that ends on a timeout finishes in
        // milliseconds rather than waiting out the production default.
        let limits = Limits {
            idle_timeout: Duration::from_millis(300),
            header_timeout: Duration::from_millis(300),
            ..Default::default()
        };
        Config::new(loopback())
            .workers(workers)
            .limits(limits)
            // Pinning is pointless in a test and fails inside some containers.
            .pin_cores(false)
    }

    async fn hello(_req: Request) -> Response {
        Response::text("hi")
    }

    /// Send one request over a fresh connection and return the response bytes.
    async fn request(addr: SocketAddr, raw: &[u8]) -> io::Result<String> {
        let mut s = tokio::net::TcpStream::connect(addr).await?;
        s.write_all(raw).await?;
        let mut out = Vec::new();
        s.read_to_end(&mut out).await?;
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    const GET: &[u8] = b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";

    /// Run `body` against a live server, then shut it down.
    fn with_server<Fut, T>(
        workers: usize,
        body: impl FnOnce(SocketAddr) -> Fut + Send + 'static,
    ) -> T
    where
        Fut: std::future::Future<Output = T>,
        T: Send + 'static,
    {
        let server = Server::bind(test_config(workers)).expect("bind");
        let addr = server.local_addr();
        let handle = server.handle();

        let client = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let out = rt.block_on(body(addr));
            handle.shutdown();
            out
        });

        server.serve(|| hello).expect("serve");
        client.join().expect("client thread")
    }

    #[test]
    fn binds_and_serves_on_an_ephemeral_port() {
        let out = with_server(1, |addr| async move {
            assert_ne!(addr.port(), 0, "port 0 must resolve to a real port");
            request(addr, GET).await.unwrap()
        });
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(out.ends_with("hi"), "{out}");
    }

    #[test]
    fn serves_concurrent_connections() {
        let results = with_server(2, |addr| async move {
            let mut set = tokio::task::JoinSet::new();
            for _ in 0..64 {
                set.spawn(async move { request(addr, GET).await });
            }
            let mut ok = 0;
            while let Some(r) = set.join_next().await {
                if r.unwrap().unwrap().starts_with("HTTP/1.1 200 OK") {
                    ok += 1;
                }
            }
            ok
        });
        assert_eq!(results, 64);
    }

    #[test]
    fn serves_across_multiple_workers() {
        let results = with_server(4, |addr| async move {
            let mut ok = 0;
            for _ in 0..40 {
                if request(addr, GET)
                    .await
                    .unwrap()
                    .starts_with("HTTP/1.1 200 OK")
                {
                    ok += 1;
                }
            }
            ok
        });
        assert_eq!(results, 40);
    }

    #[test]
    fn single_worker_config_works() {
        let out = with_server(1, |addr| async move { request(addr, GET).await.unwrap() });
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
    }

    #[test]
    fn shutdown_stops_accepting() {
        let server = Server::bind(test_config(1)).expect("bind");
        let addr = server.local_addr();
        let handle = server.handle();

        let client = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            // One good request proves the server is up.
            let first = rt.block_on(request(addr, GET));
            assert!(first.unwrap().starts_with("HTTP/1.1 200 OK"));

            handle.shutdown();
            assert!(handle.is_shutting_down());
            // Idempotent.
            handle.shutdown();

            // After shutdown, connections stop being served. Whether the OS
            // refuses the connect or accepts it into a closed backlog is
            // platform-dependent, so assert on the absence of a response rather
            // than on a specific errno.
            std::thread::sleep(Duration::from_millis(300));
            rt.block_on(async {
                match tokio::time::timeout(Duration::from_millis(500), request(addr, GET)).await {
                    Err(_) => true,
                    Ok(Err(_)) => true,
                    Ok(Ok(body)) => body.is_empty(),
                }
            })
        });

        server.serve(|| hello).expect("serve");
        assert!(
            client.join().unwrap(),
            "no request may be served after shutdown"
        );
    }

    /// A shutdown signalled before `serve` runs must still be honoured.
    ///
    /// `watch::Sender::subscribe` marks the sender's *current* value as already
    /// seen, so a receiver created after the signal never observes a
    /// transition. A loop that waited only on `changed()` would accept forever
    /// on a server the caller had already stopped, and `serve` would never
    /// return.
    #[test]
    fn shutdown_before_serve_returns_immediately() {
        let server = Server::bind(test_config(2)).expect("bind");
        let handle = server.handle();

        handle.shutdown();
        assert!(handle.is_shutting_down());

        // The failure mode is a hang, so the assertion is that this returns at
        // all. A watchdog thread turns a regression into a failure rather than
        // a suite that never finishes.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            server.serve(|| hello).expect("serve");
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(Duration::from_secs(10)).is_ok(),
            "serve must return when shutdown was signalled before it started"
        );
    }

    /// A shutdown signalled after the server has been serving traffic must stop
    /// every worker, not merely the one the last request landed on.
    ///
    /// This is the easy half of the property: by the time eight requests have
    /// been answered, every worker has long since subscribed, so no receiver
    /// can miss the transition. The startup race is the hard half and has its
    /// own test below.
    #[test]
    fn shutdown_after_serving_traffic_stops_every_worker() {
        let server = Server::bind(test_config(4)).expect("bind");
        let addr = server.local_addr();
        let handle = server.handle();

        let client = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            // Enough requests that every worker has almost certainly accepted
            // at least one, so none is merely idle when the signal lands. Each
            // response is checked: a discarded result would let eight refused
            // connections stand in for eight served requests, and the premise
            // that the workers were busy would be asserted nowhere.
            let mut served = 0;
            for _ in 0..8 {
                if rt
                    .block_on(request(addr, GET))
                    .is_ok_and(|r| r.starts_with("HTTP/1.1 200 OK"))
                {
                    served += 1;
                }
            }
            handle.shutdown();
            served
        });

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            server.serve(|| hello).expect("serve");
            let _ = tx.send(());
        });
        let served = client.join().expect("client thread");
        assert_eq!(
            served, 8,
            "every request before the signal must have been answered, or the \
             workers were never busy and the shutdown proves nothing"
        );
        assert!(
            rx.recv_timeout(Duration::from_secs(10)).is_ok(),
            "serve must return once every worker has stopped; a worker that \
             missed the signal keeps accepting and join blocks forever"
        );
    }

    /// Every worker must observe a shutdown, not just the ones that happened to
    /// subscribe before it landed.
    ///
    /// With the subscription taken inside the spawn loop, a signal arriving
    /// midway stopped the workers already subscribed and left the rest
    /// accepting, so `serve` blocked in `join` while `is_shutting_down()`
    /// reported true. Sampling that window means signalling *while* `serve` is
    /// still spawning threads — a shutdown sent after the server is up cannot
    /// reach it, because by then every receiver exists and every one of them
    /// sees the transition.
    ///
    /// The window is a few hundred microseconds wide and cannot be observed
    /// from outside, so this is a race and is treated as one: eight workers to
    /// widen it, a delay swept across the range rather than one guessed value,
    /// and the whole thing repeated. A single pass that happened to signal too
    /// late would prove nothing and say nothing about it.
    ///
    /// The failure mode is a hang, not a wrong answer, so every pass is bounded
    /// by a watchdog on the channel `serve` reports through.
    #[test]
    fn shutdown_during_worker_startup_stops_every_worker() {
        for attempt in 0..20u64 {
            let server = Server::bind(test_config(8)).expect("bind");
            let handle = server.handle();

            // Swept from "before `serve` was even called" up past the far end
            // of the spawn loop, so the interesting middle is covered whatever
            // this machine's thread-spawn cost turns out to be. A single fixed
            // sleep would be tuned to one machine and silently stop sampling
            // anything on another.
            let delay = Duration::from_micros(attempt * 75);
            let signal = std::thread::spawn(move || {
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                handle.shutdown();
            });

            let (tx, rx) = std::sync::mpsc::channel();
            let serving = std::thread::spawn(move || {
                server.serve(|| hello).expect("serve");
                let _ = tx.send(());
            });

            // Generous rather than tight: the assertion is that `serve` returns
            // at all. A bound close to the real duration would turn a loaded CI
            // box into a failure, which is the one thing a race test must not
            // do.
            assert!(
                rx.recv_timeout(Duration::from_secs(10)).is_ok(),
                "attempt {attempt} (signal after {delay:?}): serve must return \
                 once every worker has stopped. A worker whose receiver was \
                 created after the signal never sees a transition, so it \
                 accepts forever and join blocks on it"
            );
            signal.join().expect("signal thread");
            serving.join().expect("serve thread");
        }
    }

    #[test]
    fn config_defaults_are_sane() {
        let c = Config::new(loopback());
        assert!(c.workers >= 1);
        assert!(c.tcp.nodelay);
        assert!(c.tcp.reuse_port);
        assert_eq!(c.tick, Duration::from_millis(100));
        assert_eq!(c.shutdown_grace, Duration::from_secs(10));
        assert_eq!(Config::new(loopback()).workers(0).workers, 1, "never zero");
    }

    #[test]
    fn limits_clamps_max_headers_to_the_parser_ceiling() {
        let c = Config::new(loopback()).limits(Limits {
            max_headers: 200,
            ..Default::default()
        });
        assert_eq!(c.limits.max_headers, crate::limits::MAX_HEADERS_CEILING);
        assert_eq!(crate::limits::MAX_HEADERS_CEILING, 128);
    }

    #[test]
    fn bind_reports_the_resolved_port() {
        let s = Server::bind(test_config(3)).expect("bind");
        let addr = s.local_addr();
        assert_ne!(addr.port(), 0);
        // Every per-worker listener must share the one resolved port, or three
        // workers would be listening on three different ports.
        if let Listeners::PerWorker(v) = &s.listeners {
            for l in v {
                assert_eq!(l.local_addr().unwrap().port(), addr.port());
            }
        }
    }
}
