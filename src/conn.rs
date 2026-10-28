//! The per-connection state machine.
//!
//! One `Connection` owns one transport and serves requests on it until the peer
//! closes, a deadline expires, framing goes wrong, or the connection is upgraded.
//!
//! The controlling rule: **any framing rejection closes the connection.** The
//! stream is never resynchronized after a malformed or ambiguous request,
//! because the bytes following one are exactly what a smuggling attack wants
//! interpreted as a fresh request.

use crate::deadline::ConnDeadline;
use crate::header::{self, HeaderId, HeaderVec};
use crate::service::{BodyIo, H1Service, ResponseBody, Upgraded};
use crate::write::{self, DateCache, OutBody, ResponseHead};
use crate::{Body, Limits, Method, Request, Response, Version, framing, parse};
use bytes::{Bytes, BytesMut};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncRead, AsyncWrite};

/// How much to read per syscall.
const READ_CHUNK: usize = 8 * 1024;

/// How many bytes of chunk frames to accumulate before flushing a streamed
/// response body.
///
/// Matches [`READ_CHUNK`] so a stream and a read move data in comparable units.
const STREAM_FLUSH_BYTES: usize = 8 * 1024;

/// How many bytes of finished responses may wait in the write buffer for the
/// next pipelined one.
///
/// A response whose successor is already buffered is not flushed on its own:
/// pipelined requests are answered with one write instead of one per request.
/// The cap bounds how much a client can make the server hold back by
/// pipelining.
const PIPELINE_FLUSH_BYTES: usize = 32 * 1024;

/// The interim response for `Expect: 100-continue`.
const CONTINUE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

/// Per-connection configuration.
#[derive(Clone, Debug)]
pub struct ConnConfig {
    /// Resource limits and deadlines.
    pub limits: Limits,
    /// Deadline coarsening granularity.
    pub tick: Duration,
    /// Value for the `Server` field, or none to omit it.
    pub server_name: Option<Bytes>,
}

impl Default for ConnConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            tick: Duration::from_millis(100),
            server_name: None,
        }
    }
}

/// What to do with the connection after a response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Serve another request.
    KeepAlive,
    /// Close.
    Close,
    /// Hand the transport to an upgrade consumer.
    Upgrade,
}

/// Transport plus read buffer, shared between the serve loop and request bodies.
///
/// Shared through `Rc<RefCell<_>>` rather than split, because HTTP/1 serializes
/// a request against its response: while a handler holds a body, the loop is
/// awaiting that handler and touches nothing.
struct IoState<IO> {
    io: IO,
    /// Unconsumed bytes: a partial head, or body bytes read past one.
    buf: BytesMut,
    /// How much of `buf` the head scanner has already searched.
    ///
    /// A head that arrives in `k` reads would otherwise be rescanned from byte 0
    /// after every one of them — O(k · head_len) for a head the peer can split
    /// as finely as it likes. The cursor makes the scan resume where it left
    /// off. It is a pure optimization and is reset to zero by every operation
    /// that consumes or rewrites `buf`, so a stale value can never hide a
    /// terminator: correctness only ever depends on `buf` itself.
    scanned: usize,
    /// Where the buffered head ends, once the scanner has found its terminator.
    ///
    /// Kept so the head is not searched again to split it off and again to
    /// parse it. Cleared alongside `scanned`, for the same reason.
    head_end: Option<usize>,
    /// How many bytes of a `100 Continue` are owed to the peer and not yet
    /// written; zero when none is.
    ///
    /// A count rather than a flag because a write can be short: one that
    /// accepted half the interim response must not count as having sent it.
    pending_continue: usize,
}

impl<IO> IoState<IO> {
    /// Forget the head-scan cursor.
    ///
    /// Called wherever `buf` shrinks or its front moves, because the cursor is
    /// an offset into a buffer that no longer exists in that form.
    fn reset_scan(&mut self) {
        self.scanned = 0;
        self.head_end = None;
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> IoState<IO> {
    /// Read once into `buf`, appending. `Ok(0)` means EOF.
    fn poll_read_more(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        // Read straight into the buffer's spare capacity. This used to grow the
        // buffer with zeroed bytes first, because filling uninitialized
        // capacity needs `unsafe` this crate forbids. That was an 8 KiB memset
        // on every poll, the `Pending` one included, and it was the single
        // largest cost in a profile of the keep-alive path. Tokio's `read_buf`
        // does the uninitialized-read bookkeeping on its side of the boundary.
        //
        // Reserving first is what keeps this a read of up to a whole chunk:
        // with no spare capacity, `BytesMut` would hand `read_buf` a 64-byte
        // sliver. Once the previous request's slices are dropped, the reserve
        // reclaims the existing allocation rather than growing it.
        self.buf.reserve(READ_CHUNK);
        let read = std::pin::pin!(tokio::io::AsyncReadExt::read_buf(
            &mut self.io,
            &mut self.buf
        ));
        read.poll(cx)
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> BodyIo for IoState<IO> {
    fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Bytes>> {
        let before = self.buf.len();
        match self.poll_read_more(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(0)) => Poll::Ready(Ok(Bytes::new())),
            Poll::Ready(Ok(_)) => {
                // Hand the new bytes to the body as a slice of this same
                // allocation.
                let fresh = self.buf.split_off(before).freeze();
                self.reset_scan();
                Poll::Ready(Ok(fresh))
            }
        }
    }

    fn take_buffered(&mut self, max: usize) -> Bytes {
        let n = self.buf.len().min(max);
        let taken = self.buf.split_to(n).freeze();
        self.reset_scan();
        taken
    }

    fn push_back(&mut self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        self.reset_scan();
        // These bytes precede whatever is already buffered, and the head scanner
        // needs the result contiguous — the next pipelined request's head can
        // straddle the join — so a second "pending" slot consulted ahead of
        // `buf` is not an option here.
        //
        // In practice `buf` is empty at every push-back: a body only has residue
        // to return when it over-read through `poll_fill`, which splits the new
        // bytes off and leaves `buf` at the length it had before the read — and
        // that length is zero, because the body drained `buf` through
        // `take_buffered` first. Appending into the existing allocation is
        // therefore the normal path, and it costs one copy and no allocation
        // instead of the rebuild's two copies and an allocation. The general
        // case is kept honest below rather than asserted away.
        if self.buf.is_empty() {
            self.buf.extend_from_slice(&bytes);
            return;
        }
        let mut joined = BytesMut::with_capacity(bytes.len() + self.buf.len());
        joined.extend_from_slice(&bytes);
        joined.extend_from_slice(&self.buf);
        self.buf = joined;
    }

    fn poll_send_continue(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.pending_continue > 0 {
            let rest = &CONTINUE[CONTINUE.len() - self.pending_continue..];
            match std::pin::Pin::new(&mut self.io).poll_write(cx, rest) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(n)) => self.pending_continue -= n.min(self.pending_continue),
            }
        }
        // A buffering transport (TLS) would otherwise sit on it while the
        // client waits for it before sending the body.
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }
}

/// A single HTTP/1 connection.
pub struct Connection<IO, S> {
    shared: Rc<RefCell<IoState<IO>>>,
    service: S,
    cfg: Rc<ConnConfig>,
    date: Rc<RefCell<DateCache>>,
    deadline: ConnDeadline,
    out: BytesMut,
    /// Reused across requests: allocating a fresh flag per request would put an
    /// allocation back on the steady-state path.
    body_read: Rc<Cell<bool>>,
    /// Stamped onto every [`Request`] served here. A `SocketAddr` is `Copy` and
    /// 32 bytes at worst, so this is carried by value rather than behind an
    /// `Rc`: sharing it would cost a pointer chase per request to save nothing.
    peer: Option<SocketAddr>,
}

impl<IO, S> Connection<IO, S>
where
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
    S: H1Service,
{
    /// Wrap `io`, serving requests through `service`.
    ///
    /// Must be called inside a tokio runtime context: the connection's reusable
    /// deadline timer is allocated here.
    pub fn new(io: IO, service: S, cfg: Rc<ConnConfig>, date: Rc<RefCell<DateCache>>) -> Self {
        Self::with_buffered(io, service, cfg, date, Bytes::new())
    }

    /// Wrap `io` with `buffered` bytes already read from it.
    ///
    /// Used by protocol dispatch, which must read the first bytes of a connection
    /// to decide whether this crate should serve it at all. Those bytes are part
    /// of the request and cannot be re-read from the socket, so they are handed
    /// back here rather than dropped.
    pub fn with_buffered(
        io: IO,
        service: S,
        cfg: Rc<ConnConfig>,
        date: Rc<RefCell<DateCache>>,
        buffered: Bytes,
    ) -> Self {
        let deadline = ConnDeadline::new(cfg.tick);
        let mut buf = BytesMut::with_capacity(READ_CHUNK.max(buffered.len()));
        buf.extend_from_slice(&buffered);
        Self {
            shared: Rc::new(RefCell::new(IoState {
                io,
                buf,
                scanned: 0,
                head_end: None,
                pending_continue: 0,
            })),
            service,
            cfg,
            date,
            deadline,
            out: BytesMut::with_capacity(1024),
            body_read: Rc::new(Cell::new(false)),
            peer: None,
        }
    }

    /// Report `peer` as the connection's remote address on every request.
    ///
    /// A builder rather than a constructor parameter because the address is
    /// optional and the constructors already take four arguments; a fifth that
    /// is `None` at most call sites is noise. Without this the requests this
    /// connection serves carry [`Request::peer`](crate::Request::peer) of
    /// `None`, which downstream code must read as "unknown", not "local".
    ///
    /// # Examples
    ///
    /// ```
    /// use armature_h1::{ConnConfig, Connection, DateCache, Request, Response};
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    ///
    /// async fn handler(_req: Request) -> Response {
    ///     Response::text("hi")
    /// }
    ///
    /// let rt = tokio::runtime::Builder::new_current_thread()
    ///     .enable_all()
    ///     .build()
    ///     .unwrap();
    /// // A connection allocates a reusable timer up front, so it has to be
    /// // built inside a runtime context — in the real server that is the
    /// // worker's own runtime, here it is entered explicitly.
    /// let _guard = rt.enter();
    ///
    /// let (_client, server) = tokio::io::duplex(8 * 1024);
    /// let conn = Connection::new(
    ///     server,
    ///     handler,
    ///     Rc::new(ConnConfig::default()),
    ///     Rc::new(RefCell::new(DateCache::new())),
    /// )
    /// .with_peer(Some("203.0.113.7:54321".parse().unwrap()));
    ///
    /// // Every request `conn.serve()` now dispatches carries that address in
    /// // `Request::peer`; without the builder it would carry `None`.
    /// ```
    #[must_use]
    pub fn with_peer(mut self, peer: Option<SocketAddr>) -> Self {
        self.peer = peer;
        self
    }

    /// Serve requests until the connection ends.
    ///
    /// `Ok(Some(_))` means the connection was upgraded and the caller must hand
    /// it to the upgrade consumer: a handler signals an upgrade by answering a
    /// request that asked for one with status 101, and whoever drove this
    /// `Connection` receives the socket here. Under [`crate::Server`], pass an
    /// [`UpgradeConsumer`](crate::UpgradeConsumer) to
    /// [`serve_with`](crate::Server::serve_with) and it receives the same
    /// thing; [`serve`](crate::Server::serve) and
    /// [`serve_with_fallback`](crate::Server::serve_with_fallback) default to
    /// [`CloseUpgrade`](crate::CloseUpgrade), which closes.
    ///
    /// Two things forfeit the handoff, and both end the connection with
    /// `Ok(None)` after the response is written as usual. A handler that
    /// *retains* the request [`Body`] past its response forfeits it, because
    /// the body holds a handle on the same transport and two readers on one
    /// socket is not a state this crate will produce. A handler that drops the
    /// body *unread* forfeits it too, because the bytes it never read are
    /// still on the wire and would reach the consumer as `Upgraded::buffered`
    /// — which is documented as the peer's first post-upgrade frames.
    ///
    /// So an upgrading handler must read the body to its end and then drop it
    /// before answering 101. Dropping it unread is not enough.
    pub async fn serve(mut self) -> io::Result<Option<Upgraded>> {
        loop {
            // Wait for the next request to begin. An idle keep-alive connection
            // that never sends again is closed silently — no response is owed.
            match self.read_until_head_or_idle().await? {
                HeadOutcome::Eof => return Ok(None),
                HeadOutcome::IdleTimeout => return Ok(None),
                HeadOutcome::HeaderTimeout => {
                    self.write_error(Version::Http11, 408).await?;
                    return Ok(None);
                }
                HeadOutcome::Failed(status) => {
                    self.write_error(Version::Http11, status).await?;
                    return Ok(None);
                }
                HeadOutcome::Ready => {}
            }

            // Split the head off rather than cloning the buffer. `BytesMut::clone`
            // would copy every head byte and allocate to do it; `split_to` hands
            // back a view of the same allocation, and the remainder stays put for
            // the body or the next pipelined request.
            let head_bytes = {
                let mut st = self.shared.borrow_mut();
                // `read_until_head_or_idle` only returns Ready once a terminator
                // is present, so the fallback search is unreachable in practice.
                let Some(end) = st.head_end.or_else(|| parse::find_head_end(&st.buf)) else {
                    return Ok(None);
                };
                let head = st.buf.split_to(end).freeze();
                st.reset_scan();
                head
            };
            let head_len = head_bytes.len();
            let head = match parse::parse_head_region(&head_bytes, head_len, &self.cfg.limits) {
                Ok(head) => head,
                Err(e) => {
                    self.write_error(Version::Http11, e.status()).await?;
                    return Ok(None);
                }
            };

            let version = head.version;
            let keep_alive = head.is_keep_alive();

            // Framing before anything else touches the body.
            let kind = match framing::decide(&head, &self.cfg.limits) {
                Ok(k) => k,
                Err(e) => {
                    self.write_error(version, e.status()).await?;
                    return Ok(None);
                }
            };

            let wants_upgrade =
                head.count(&HeaderId::Upgrade) > 0 && head.connection_has_token("upgrade");
            // HTTP/1.1 only: a 1.0 client cannot be sent a 1xx (RFC 9110
            // section 15.2) and the expectation is to be ignored from one
            // (section 10.1.1). It would read the interim response as final.
            let expects_continue = version == Version::Http11
                && head
                    .get_str(&HeaderId::Expect)
                    .is_some_and(|v| v.eq_ignore_ascii_case("100-continue"));
            let is_head_request = head.method == Method::Head;

            self.shared.borrow_mut().pending_continue =
                if expects_continue { CONTINUE.len() } else { 0 };

            let dyn_io: Rc<RefCell<dyn BodyIo>> = self.shared.clone();
            // Shared with the body so the loop can tell, after the handler
            // returns, whether the body was consumed to its end.
            self.body_read.set(false);
            let body_read = self.body_read.clone();
            let body = Body::new(
                kind,
                dyn_io,
                expects_continue,
                &self.cfg.limits,
                body_read.clone(),
            );

            // The body deadline covers the handler's read of it. Nothing else
            // polls this deadline, so the handler call itself has to race it:
            // arming a timer nobody awaits would leave a handler blocked on body
            // bytes the peer never sends holding the connection forever.
            //
            // Cancelling the handler mid-await drops its `Body`, and with it the
            // last borrow of the transport, so the 408 can be written here.
            let mut call = std::pin::pin!(self.service.call(Request {
                head,
                body,
                peer: self.peer,
            }));

            // Earlier responses may still be waiting in `out` for this one (see
            // `PIPELINE_FLUSH_BYTES`). They go out before anything else can be
            // written — a `100 Continue` is written straight to the transport —
            // and before a handler that does not finish at once can hold them
            // back for as long as it takes.
            let mut early = None;
            if !self.out.is_empty() {
                if !expects_continue
                    && let Poll::Ready(resp) =
                        std::future::poll_fn(|cx| Poll::Ready(call.as_mut().poll(cx))).await
                {
                    early = Some(resp);
                }
                if early.is_none() {
                    self.flush().await?;
                }
            }
            // `poll_fn` over the pinned call rather than `select!`, which takes
            // its futures by value: with the request's inline header array
            // inside, the handler's future is large enough that moving it into
            // and out of the `select!` state cost more than parsing the head.
            // Deadline first, as `biased` had it.
            //
            // `body_timeout` bounds the body's arrival, not the handler: once the
            // body has been read to its end — or when there never was one — an
            // expiry means nothing, and a handler may take as long as it needs.
            // A body still incomplete at the deadline is a stalled peer (or a
            // handler sitting on bytes it never reads), and gets the 408.
            let resp = if let Some(resp) = early {
                Some(resp)
            } else if body_read.get() {
                Some(call.as_mut().await)
            } else {
                self.deadline.arm(self.cfg.limits.body_timeout);
                let deadline = &mut self.deadline;
                let resp = std::future::poll_fn(|cx| {
                    if !body_read.get() && deadline.poll_expired(cx).is_ready() {
                        return Poll::Ready(None);
                    }
                    call.as_mut().poll(cx).map(Some)
                })
                .await;
                self.deadline.disarm();
                resp
            };
            let Some(resp) = resp else {
                self.write_error(version, 408).await?;
                return Ok(None);
            };

            // An unconsumed or errored body leaves an unknown number of bytes on
            // the wire. Reusing the connection would mean looking for a request
            // line inside a message body — the smuggling scenario itself — so
            // reuse requires that the body reached its end cleanly.
            //
            // Draining the remainder instead would preserve keep-alive for
            // handlers that ignore bodies, but it hands an attacker a way to make
            // the server read bytes it has no use for. Closing is the safe
            // default; a handler that wants keep-alive on a request with a body
            // must read that body.
            let body_consumed = body_read.get();
            let keep_alive = keep_alive && body_consumed;

            let disposition = self
                .write_response(
                    version,
                    resp,
                    keep_alive,
                    is_head_request,
                    wants_upgrade,
                    body_consumed,
                )
                .await?;

            match disposition {
                Disposition::KeepAlive => continue,
                Disposition::Close => return Ok(None),
                Disposition::Upgrade => {
                    // A handler that stashed the request `Body` somewhere
                    // outliving the response still holds a handle on the
                    // transport. Handing the socket to an upgrade consumer while
                    // that handle can read from it would interleave two readers,
                    // so the handoff is forfeited and the connection closes.
                    // Read before `into_parts` consumes `self`.
                    let peer = self.peer;
                    let Some((io, buffered)) = self.into_parts() else {
                        return Ok(None);
                    };
                    return Ok(Some(Upgraded {
                        peer,
                        io: Box::new(io),
                        buffered,
                    }));
                }
            }
        }
    }

    /// Consume the connection, yielding the transport and unread bytes.
    ///
    /// `None` when the transport is still shared — a handler kept the request
    /// `Body` alive past its response, and that `Body` holds a handle on the
    /// very state being unwrapped here. The transport cannot be handed off while
    /// a second owner could still read from it, so the caller closes instead.
    fn into_parts(self) -> Option<(IO, Bytes)> {
        let state = Rc::try_unwrap(self.shared).ok().map(RefCell::into_inner)?;
        Some((state.io, state.buf.freeze()))
    }

    /// Read until a complete head is buffered, or a deadline or EOF intervenes.
    async fn read_until_head_or_idle(&mut self) -> io::Result<HeadOutcome> {
        // A complete head may already be buffered from a pipelined write.
        if self.has_complete_head() {
            return Ok(HeadOutcome::Ready);
        }
        // Responses are only held back while a successor head is buffered, so
        // this cannot find any; it is what guarantees none ever waits on a
        // read.
        if !self.out.is_empty() {
            self.flush().await?;
        }

        let had_bytes = !self.shared.borrow().buf.is_empty();
        // Idle applies before the first byte; the header deadline applies once
        // the request has begun. Conflating them would let a client hold a
        // connection open indefinitely mid-head.
        self.deadline.arm(if had_bytes {
            self.cfg.limits.header_timeout
        } else {
            self.cfg.limits.idle_timeout
        });
        let mut started = had_bytes;

        loop {
            let read = {
                let shared = self.shared.clone();
                let deadline = &mut self.deadline;
                tokio::select! {
                    biased;
                    () = deadline.expired() => None,
                    r = std::future::poll_fn(|cx| shared.borrow_mut().poll_read_more(cx)) => Some(r),
                }
            };

            match read {
                None => {
                    return Ok(if started {
                        HeadOutcome::HeaderTimeout
                    } else {
                        HeadOutcome::IdleTimeout
                    });
                }
                Some(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return Ok(HeadOutcome::Eof);
                }
                Some(Err(e)) => return Err(e),
                Some(Ok(0)) => return Ok(HeadOutcome::Eof),
                Some(Ok(_)) => {
                    // Checked before arming the header deadline: a head that
                    // arrives whole in one read — nearly every one — has no
                    // use for it, and arming is not free.
                    if self.has_complete_head() {
                        return Ok(HeadOutcome::Ready);
                    }
                    if !started {
                        started = true;
                        self.deadline.arm(self.cfg.limits.header_timeout);
                    }
                    // Refuse to buffer an unterminated head forever.
                    if self.shared.borrow().buf.len() > self.cfg.limits.max_head_bytes {
                        return Ok(HeadOutcome::Failed(431));
                    }
                }
            }
        }
    }

    /// Whether a complete head is buffered, resuming the search where the last
    /// call left off.
    ///
    /// `parse::find_head_end` is a plain search for `\r\n\r\n`, so restricting
    /// it to a suffix is exact as long as the suffix starts three bytes before
    /// the last scanned position — the shortest overlap that can still hold a
    /// terminator straddling the boundary. Nothing else about head validation
    /// happens here; the bare-CR/LF and obs-fold rules are `parse::prescan`'s,
    /// and it always sees the whole head region.
    fn has_complete_head(&self) -> bool {
        let mut st = self.shared.borrow_mut();
        // RFC 9112 section 2.2: a server SHOULD ignore empty lines received
        // before a request line. Only whole CRLFs, matching the strict line
        // endings everywhere else, and only at the front, where a request line
        // can never begin with CR — so this can never eat part of a head. A
        // peer streaming nothing but CRLFs is bounded by the header deadline,
        // and the discarded bytes never accumulate toward `max_head_bytes`.
        let blank = st
            .buf
            .as_chunks::<2>()
            .0
            .iter()
            .take_while(|pair| *pair == b"\r\n")
            .count()
            * 2;
        if blank > 0 {
            bytes::Buf::advance(&mut st.buf, blank);
            st.scanned = st.scanned.saturating_sub(blank);
        }
        // A miss leaves `scanned == buf.len()`, so this also covers "called
        // twice without an intervening read".
        if st.buf.len() <= st.scanned {
            return false;
        }
        let from = st.scanned.saturating_sub(3);
        if let Some(end) = parse::find_head_end(&st.buf[from..]) {
            st.head_end = Some(from + end);
            return true;
        }
        st.scanned = st.buf.len();
        false
    }

    /// Serialize and write a response, returning what to do next.
    async fn write_response(
        &mut self,
        version: Version,
        resp: Response,
        req_keep_alive: bool,
        is_head_request: bool,
        wants_upgrade: bool,
        body_consumed: bool,
    ) -> io::Result<Disposition> {
        // Status 101 alone does not take the connection: the peer must have
        // asked for the upgrade, or the transport would be handed off to a
        // protocol the client is not speaking.
        //
        // `body_consumed` gates it for the same reason it gates reuse. Bytes of
        // an unread request body are still on the wire, and `into_parts` hands
        // whatever is buffered to the upgrade consumer as
        // `Upgraded::buffered` — which that consumer is documented to treat as
        // the peer's first post-upgrade frames. Handing it body bytes under
        // that contract is the smuggling shape pointed at the consumer instead
        // of at the parser, so an unread body forfeits the handoff and closes.
        let upgrading = resp.status == 101 && wants_upgrade && body_consumed;
        // An unread request body means the next bytes on the wire are body
        // bytes, not a request line. Rather than guess where the body ended,
        // close.
        let mut keep_alive = req_keep_alive && !upgrading;

        let Response {
            mut status,
            mut headers,
            mut body,
        } = resp;

        // A final response needs a final status. Anything outside 200..=599
        // cannot be one — `1000` is not a status line a peer can parse, and a
        // 1xx other than 101 is an interim response the client would keep
        // waiting past for a final one that never comes, as is a 101 the
        // client never asked for. (A 101 it did ask for but that is not
        // honoured — the body was left unread — is still written, then the
        // connection closes; see `upgrading`.) The handler's fields and body
        // went with that status, so neither is kept.
        if !write::is_final_status(status, wants_upgrade) {
            tracing::warn!(
                status,
                "handler returned a status that cannot be a final response; sending 500"
            );
            status = 500;
            headers.clear();
            body = ResponseBody::Empty;
            keep_alive = false;
        }

        // A handler that says `Connection: close` is taken at its word. The
        // writer states persistence itself and drops such a field, so the
        // socket's fate and what the response says about it cannot disagree.
        if header::all(&headers, &HeaderId::Connection).any(|v| header::value_has_token(v, "close"))
        {
            keep_alive = false;
        }

        // HTTP/1.0 has no chunked coding, so a streamed body there is
        // close-delimited: written unframed, and the close marks its end.
        let close_delimited = version == Version::Http10 && matches!(body, ResponseBody::Stream(_));
        if close_delimited {
            keep_alive = false;
        }

        if let Some(name) = &self.cfg.server_name
            && header::get(&headers, &HeaderId::Server).is_none()
        {
            headers.push((HeaderId::Server, name.clone()));
        }

        let out_body = match &body {
            ResponseBody::Empty => OutBody::None,
            ResponseBody::Full(b) => OutBody::Fixed(b.clone()),
            ResponseBody::Stream(_) => OutBody::Chunked,
        };

        self.settle_continue().await?;
        // Not cleared: it may hold earlier pipelined responses still owed to
        // the peer, and this one goes out after them.
        {
            let mut date = self.date.borrow_mut();
            let now = SystemTime::now();
            let date_bytes = date.get(now);
            write::write_head_for(
                &mut self.out,
                version,
                &ResponseHead { status, headers },
                &out_body,
                date_bytes,
                keep_alive,
                is_head_request,
            );
        }

        // A HEAD response carries the headers a GET would, and no body
        // (RFC 9112 section 6.3). 204, 304 and 1xx likewise forbid one, and the
        // writer already suppresses their framing field — so writing a body
        // anyway would put bytes on the wire that no `Content-Length` or chunk
        // framing accounts for, and a keep-alive peer reads them as the start of
        // the next response. A handler that attaches a body to a 304 should lose
        // the body, not desync the connection.
        let body_forbidden = matches!(status, 204 | 304) || (100..200).contains(&status);
        if !is_head_request && !upgrading && !body_forbidden {
            match body {
                ResponseBody::Empty => {}
                ResponseBody::Full(b) => self.out.extend_from_slice(&b),
                ResponseBody::Stream(mut s) => {
                    // Every write in the stream loop is deadline-guarded, not
                    // just the last one: a peer that stops reading mid-stream
                    // would otherwise pin the connection and its buffer
                    // indefinitely. The deadline is re-armed per flush, so a
                    // slow but progressing consumer is never cut off. That is
                    // unchanged by the coalescing below — it changes how many
                    // flushes there are, never whether one is guarded.
                    //
                    // Flush the head first so the peer can begin processing.
                    self.flush().await?;
                    loop {
                        // Poll once without committing to a wait. If the stream
                        // is not ready, whatever has been coalesced so far is
                        // flushed *before* parking — otherwise a handler that
                        // emits a small frame and then goes quiet would sit
                        // unwritten until enough later frames arrived to cross
                        // the byte threshold, which for an SSE or long-poll body
                        // can be minutes. Re-polling after `Pending` is sound:
                        // the first poll registered the waker.
                        let next =
                            match std::future::poll_fn(|cx| Poll::Ready(s.as_mut().poll_next(cx)))
                                .await
                            {
                                Poll::Ready(item) => item,
                                Poll::Pending => {
                                    if !self.out.is_empty() {
                                        self.flush().await?;
                                    }
                                    std::future::poll_fn(|cx| s.as_mut().poll_next(cx)).await
                                }
                            };
                        match next {
                            None => break,
                            Some(Ok(chunk)) => {
                                // Accumulate frames and pay for one write+flush
                                // per `STREAM_FLUSH_BYTES` rather than per
                                // chunk: a stream yielding small pieces would
                                // otherwise turn each one into its own pair of
                                // syscalls. The threshold bounds bytes, not
                                // time — coalescing only ever spans chunks that
                                // are ready back to back without the stream
                                // parking. The moment it returns `Pending` the
                                // buffer goes out, so an idle stream never holds
                                // a frame back regardless of how little it has
                                // produced.
                                if close_delimited {
                                    self.out.extend_from_slice(&chunk);
                                } else {
                                    write::write_chunk(&mut self.out, &chunk);
                                }
                                if self.out.len() >= STREAM_FLUSH_BYTES {
                                    self.flush().await?;
                                }
                            }
                            Some(Err(_)) => {
                                // The body failed mid-stream. The head is
                                // already sent, so the only honest signal left
                                // is to close without a terminating chunk.
                                //
                                // Frames the stream already yielded are written
                                // first. Coalescing must not turn a mid-stream
                                // failure into silent data loss: every chunk the
                                // body handed over successfully goes on the wire
                                // exactly as it did when each was flushed
                                // immediately.
                                self.flush().await?;
                                return Ok(Disposition::Close);
                            }
                        }
                    }
                    if !close_delimited {
                        write::write_last_chunk(&mut self.out, &HeaderVec::new());
                    }
                }
            }
        }

        // Hold a keep-alive response back while the next request is already
        // here, so a pipelined batch goes out in one write. Whichever response
        // finds no successor buffered flushes the batch; nothing waits on a
        // read, since the successor needs none.
        let coalesce = keep_alive
            && !upgrading
            && self.out.len() < PIPELINE_FLUSH_BYTES
            && self.has_complete_head();
        if !coalesce {
            self.flush().await?;
        }

        if upgrading {
            return Ok(Disposition::Upgrade);
        }

        Ok(if keep_alive {
            Disposition::KeepAlive
        } else {
            Disposition::Close
        })
    }

    /// Write the pending buffer under the write deadline, then clear it.
    ///
    /// The deadline is armed here rather than by the caller so that every write
    /// on the response path is covered. A streaming body flushes once per chunk,
    /// and guarding only the final flush would leave all the others unbounded —
    /// which is exactly the position a peer that stops reading mid-stream puts
    /// the connection in.
    async fn flush(&mut self) -> io::Result<()> {
        self.deadline.arm(self.cfg.limits.write_timeout);
        let flushed = tokio::select! {
            biased;
            () = self.deadline.expired() => Err(io::Error::from(io::ErrorKind::TimedOut)),
            r = Self::write_all_from(&self.shared, &self.out) => r,
        };
        self.deadline.disarm();
        self.out.clear();
        flushed
    }

    /// Write `out` in full, then flush, then clear it for reuse.
    ///
    /// Writes straight from the buffer rather than `split()`ing it off. `split`
    /// leaves the buffer with zero capacity, so the next response would have to
    /// reallocate — one allocation per request, purely to avoid a borrow.
    ///
    /// Each `borrow_mut` is scoped to a single poll rather than held across the
    /// await. Holding it across would deadlock the moment anything else — a body
    /// read, say — needed the same transport, and that is the kind of latent
    /// hazard that only surfaces under a specific interleaving.
    async fn write_all_from(shared: &Rc<RefCell<IoState<IO>>>, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let mut written = 0;
        while written < bytes.len() {
            let n = std::future::poll_fn(|cx| {
                let mut st = shared.borrow_mut();
                std::pin::Pin::new(&mut st.io).poll_write(cx, &bytes[written..])
            })
            .await?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            written += n;
        }
        std::future::poll_fn(|cx| {
            let mut st = shared.borrow_mut();
            std::pin::Pin::new(&mut st.io).poll_flush(cx)
        })
        .await
    }

    /// Resolve an owed `100 Continue` before a final response goes out.
    ///
    /// Once a final response is on its way an interim one is no longer owed,
    /// and writing it later would land inside that response — in the middle
    /// of a body a streaming handler produces by reading the request. So one
    /// not yet begun is dropped; one a short write left half-sent is finished,
    /// because the peer is already parsing it.
    async fn settle_continue(&mut self) -> io::Result<()> {
        let pending = self.shared.borrow().pending_continue;
        if pending == CONTINUE.len() {
            self.shared.borrow_mut().pending_continue = 0;
            return Ok(());
        }
        if pending == 0 {
            return Ok(());
        }
        let shared = self.shared.clone();
        self.deadline.arm(self.cfg.limits.write_timeout);
        let deadline = &mut self.deadline;
        let sent = std::future::poll_fn(|cx| {
            if deadline.poll_expired(cx).is_ready() {
                return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
            }
            shared.borrow_mut().poll_send_continue(cx)
        })
        .await;
        self.deadline.disarm();
        sent
    }

    /// Write a bare error response and close.
    ///
    /// Always `Connection: close`: a connection whose framing we do not fully
    /// agree on is never reused.
    async fn write_error(&mut self, version: Version, status: u16) -> io::Result<()> {
        if self.settle_continue().await.is_err() {
            return Ok(());
        }
        // Appended rather than written alone: earlier pipelined responses may
        // still be waiting in `out`, and they are owed first.
        {
            let mut date = self.date.borrow_mut();
            let now = SystemTime::now();
            let date_bytes = date.get(now);
            write::write_head(
                &mut self.out,
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
        // Best effort: the peer may already be gone, and there is nothing
        // further to do about it either way. Deadline-guarded like every other
        // write, so a peer that stops reading cannot hold the connection open
        // on an error response.
        let _ = self.flush().await;
        Ok(())
    }
}

/// Why head reading stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeadOutcome {
    /// A complete head is buffered.
    Ready,
    /// The peer closed cleanly between requests.
    Eof,
    /// No request began within the idle deadline.
    IdleTimeout,
    /// A request began but its head never completed.
    HeaderTimeout,
    /// The head was rejected before parsing, with this status.
    Failed(u16),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::HeaderId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Shorten the idle and header deadlines so a test that ends on a timeout
    /// finishes in milliseconds rather than waiting out the 75s production
    /// default.
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

    /// Drive a connection with `input`, returning everything written back.
    ///
    /// Runs on a `LocalSet` because nothing in this crate is `Send` — which is
    /// itself part of what these tests hold in place.
    async fn exchange<S>(input: &'static [u8], service: S, limits: Limits) -> String
    where
        S: H1Service + 'static,
    {
        exchange_raw(input, service, limits).await.0
    }

    async fn exchange_raw<S>(input: &'static [u8], service: S, limits: Limits) -> (String, bool)
    where
        S: H1Service + 'static,
    {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let local = tokio::task::LocalSet::new();

        let conn = Connection::new(
            server,
            service,
            cfg(limits),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let server_task = local.spawn_local(async move { conn.serve().await });

        local
            .run_until(async move {
                client.write_all(input).await.unwrap();
                let mut out = Vec::new();
                // Read to EOF or until the server stops writing.
                let read =
                    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                        .await;
                let closed = matches!(read, Ok(Ok(_)));
                let _ = server_task.await;
                (String::from_utf8_lossy(&out).into_owned(), closed)
            })
            .await
    }

    /// Serve one request and report what the handler saw in
    /// [`Request::peer`](crate::Request::peer).
    ///
    /// `with_peer` outer-`None` leaves the builder uncalled, which is what
    /// `Connection::new` alone produces; `Some(p)` calls it with `p`. The
    /// distinction is the point of the two tests below: the default and the
    /// populated case reach the handler by different routes, and only the
    /// accept loop exercises the second one otherwise — which under
    /// `hyper-backend` never runs, leaving this public escape hatch with no
    /// coverage at all in that feature row.
    async fn peer_seen_by_handler(with_peer: Option<Option<SocketAddr>>) -> Option<SocketAddr> {
        let seen: Rc<Cell<Option<SocketAddr>>> = Rc::new(Cell::new(None));
        let recorder = seen.clone();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let local = tokio::task::LocalSet::new();

        let conn = Connection::new(
            server,
            move |req: Request| {
                recorder.set(req.peer);
                async move { Response::text("hi") }
            },
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let conn = match with_peer {
            Some(peer) => conn.with_peer(peer),
            None => conn,
        };
        let server_task = local.spawn_local(async move { conn.serve().await });

        local
            .run_until(async move {
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                    .await;
                let _ = server_task.await;
                // Without this a handler that never ran would report `None` and
                // pass the default case for the wrong reason.
                assert!(
                    String::from_utf8_lossy(&out).starts_with("HTTP/1.1 200 OK"),
                    "the handler must have run"
                );
            })
            .await;

        seen.get()
    }

    #[tokio::test]
    async fn with_peer_reaches_the_handler() {
        let addr: SocketAddr = "203.0.113.7:54321".parse().expect("addr");
        assert_eq!(peer_seen_by_handler(Some(Some(addr))).await, Some(addr));
    }

    #[tokio::test]
    async fn peer_is_none_without_with_peer() {
        // `None` here means unknown, not local: a `duplex` pair has no address
        // to report, and nothing downstream may read the absence as trust.
        assert_eq!(peer_seen_by_handler(None).await, None);
    }

    async fn ok_service(_req: Request) -> Response {
        Response::text("hi")
    }

    async fn echo_service(mut req: Request) -> Response {
        match req.body.collect(1024 * 1024).await {
            Ok(b) => Response::ok().with_body(ResponseBody::Full(b)),
            Err(e) => Response::status_only(e.status()),
        }
    }

    /// A handler that never touches the body — the common 404-on-POST shape.
    async fn ignore_body_service(_req: Request) -> Response {
        Response::status_only(404)
    }

    #[tokio::test]
    async fn serves_a_single_request() {
        let out = exchange(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 200 OK\r\n"), "{out}");
        assert!(out.contains("content-length: 2\r\n"), "{out}");
        assert!(out.ends_with("hi"), "{out}");
    }

    #[tokio::test]
    async fn keeps_the_connection_alive_for_sequential_requests() {
        // Both requests are written up front; the second is served on the same
        // connection.
        let out = exchange(
            b"GET /a HTTP/1.1\r\nHost: a\r\n\r\nGET /b HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert_eq!(out.matches("HTTP/1.1 200 OK").count(), 2, "{out}");
    }

    #[tokio::test]
    async fn serves_pipelined_requests_in_order() {
        async fn echo_path(req: Request) -> Response {
            let mut r = Response::new(200);
            r.body = ResponseBody::Full(Bytes::from(req.head.path().to_owned()));
            r
        }
        let out = exchange(
            b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\nGET /2 HTTP/1.1\r\nHost: a\r\n\r\nGET /3 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            echo_path,
            Limits::default(),
        )
        .await;
        let p1 = out.find("/1").expect("first response");
        let p2 = out.find("/2").expect("second response");
        let p3 = out.find("/3").expect("third response");
        assert!(
            p1 < p2 && p2 < p3,
            "responses must be in request order: {out}"
        );
    }

    /// A transport that counts the writes made to it.
    struct CountingIo {
        inner: tokio::io::DuplexStream,
        writes: Rc<Cell<usize>>,
    }

    impl AsyncRead for CountingIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for CountingIo {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.writes.set(self.writes.get() + 1);
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// A pipelined batch is answered with one write, not one per request.
    #[tokio::test]
    async fn pipelined_responses_go_out_in_one_write() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let writes = Rc::new(Cell::new(0));
        let io = CountingIo {
            inner: server,
            writes: writes.clone(),
        };
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            io,
            ok_service,
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let server_task = local.spawn_local(async move { conn.serve().await });
        let out = local
            .run_until(async move {
                client
                    .write_all(
                        b"GET / HTTP/1.1\r\nHost: a\r\n\r\nGET / HTTP/1.1\r\nHost: a\r\n\r\n\
                          GET / HTTP/1.1\r\nHost: a\r\n\r\nGET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
                let mut out = Vec::new();
                tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                    .await
                    .expect("server closes after the last request")
                    .unwrap();
                let _ = server_task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await;
        assert_eq!(out.matches("HTTP/1.1 200 OK").count(), 4, "{out}");
        assert_eq!(writes.get(), 1, "four pipelined responses, one write");
    }

    /// Held-back responses must not wait on a handler that does not finish at
    /// once: the client gets them while the next handler is still running.
    #[tokio::test]
    async fn a_slow_handler_does_not_hold_back_earlier_pipelined_responses() {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let release_rx = Rc::new(RefCell::new(Some(release_rx)));
        let service = move |req: Request| {
            let wait = (req.head.path() == "/slow")
                .then(|| release_rx.borrow_mut().take())
                .flatten();
            async move {
                if let Some(rx) = wait {
                    let _ = rx.await;
                }
                let mut r = Response::new(200);
                r.body = ResponseBody::Full(Bytes::from(req.head.path().to_owned()));
                r
            }
        };
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            server,
            service,
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let server_task = local.spawn_local(async move { conn.serve().await });
        local
            .run_until(async move {
                client
                    .write_all(
                        b"GET /fast HTTP/1.1\r\nHost: a\r\n\r\n\
                          GET /slow HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
                let mut first = vec![0u8; 4096];
                let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut first))
                    .await
                    .expect("the first response must not wait for the slow handler")
                    .unwrap();
                let first = String::from_utf8_lossy(&first[..n]).into_owned();
                assert!(first.ends_with("/fast"), "{first}");
                release_tx.send(()).unwrap();
                let mut rest = Vec::new();
                tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut rest))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(String::from_utf8_lossy(&rest).ends_with("/slow"));
                let _ = server_task.await;
            })
            .await;
    }

    /// A `100 Continue` is written straight to the transport, so a response
    /// held back for the batch must go out ahead of it.
    #[tokio::test]
    async fn a_held_back_response_precedes_the_next_requests_100_continue() {
        let out = exchange(
            b"GET / HTTP/1.1\r\nHost: a\r\n\r\n\
              POST / HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
            echo_service,
            Limits::default(),
        )
        .await;
        let ok = out.find("HTTP/1.1 200 OK").expect("first response");
        let cont = out.find("HTTP/1.1 100 Continue").expect("interim response");
        let echoed = out.rfind("hello").expect("echoed body");
        assert!(ok < cont && cont < echoed, "{out}");
    }

    /// Drive one connection over a duplex of `capacity` bytes.
    async fn exchange_with_capacity<S>(input: &'static [u8], service: S, capacity: usize) -> String
    where
        S: H1Service + 'static,
    {
        let (mut client, server) = tokio::io::duplex(capacity);
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            server,
            service,
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let server_task = local.spawn_local(async move { conn.serve().await });
        local
            .run_until(async move {
                client.write_all(input).await.unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                    .await;
                let _ = server_task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await
    }

    /// A body that streams the request body back.
    struct EchoStream(Body);

    impl crate::service::futures_stream::Stream for EchoStream {
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Bytes, crate::BodyError>>> {
            self.0.poll_chunk(cx)
        }
    }

    /// A handler that streams the request body back reads it only after its
    /// response head is out; the `100 Continue` it would trigger then is no
    /// longer owed, and writing it would land inside the chunked body.
    #[tokio::test]
    async fn no_100_continue_inside_a_streamed_response() {
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
            |req: Request| async move {
                Response::ok().with_body(ResponseBody::Stream(Box::pin(EchoStream(req.body))))
            },
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(!out.contains("100 Continue"), "{out}");
        assert!(out.contains("5\r\nhello\r\n"), "{out}");
    }

    /// A body stream yielding two fixed chunks.
    struct TwoChunks(u8);

    impl crate::service::futures_stream::Stream for TwoChunks {
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Bytes, crate::BodyError>>> {
            self.0 += 1;
            Poll::Ready(match self.0 {
                1 => Some(Ok(Bytes::from_static(b"ab"))),
                2 => Some(Ok(Bytes::from_static(b"cd"))),
                _ => None,
            })
        }
    }

    /// RFC 9112 section 6.1: no chunked coding to an HTTP/1.0 client. A
    /// streamed body there is close-delimited instead.
    #[tokio::test]
    async fn a_streamed_body_to_http_10_is_close_delimited() {
        let (out, closed) = exchange_raw(
            b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
            |_req: Request| async {
                Response::ok().with_body(ResponseBody::Stream(Box::pin(TwoChunks(0))))
            },
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.0 200 OK\r\n"), "{out}");
        assert!(
            !out.to_ascii_lowercase().contains("transfer-encoding"),
            "{out}"
        );
        assert!(out.contains("connection: close\r\n"), "{out}");
        assert!(out.ends_with("\r\n\r\nabcd"), "unframed body: {out}");
        assert!(closed, "the close is the framing");
    }

    /// The server decides persistence and says so; a handler's
    /// `Connection: keep-alive` must not contradict a close it then performs.
    #[tokio::test]
    async fn a_handler_keep_alive_does_not_mask_the_servers_close() {
        let (out, closed) = exchange_raw(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello",
            |_req: Request| async {
                Response::text("hi").header(HeaderId::Connection, Bytes::from_static(b"keep-alive"))
            },
            Limits::default(),
        )
        .await;
        assert!(!out.contains("keep-alive"), "{out}");
        assert!(out.contains("connection: close\r\n"), "{out}");
        assert!(closed);
    }

    /// And a handler's `Connection: close` is acted on, not just echoed.
    #[tokio::test]
    async fn a_handler_close_closes_the_connection() {
        let (out, closed) = exchange_raw(
            b"GET / HTTP/1.1\r\nHost: a\r\n\r\nGET / HTTP/1.1\r\nHost: a\r\n\r\n",
            |_req: Request| async {
                Response::text("hi").header(HeaderId::Connection, Bytes::from_static(b"close"))
            },
            Limits::default(),
        )
        .await;
        assert_eq!(out.matches("HTTP/1.1 200").count(), 1, "{out}");
        assert_eq!(out.matches("connection").count(), 1, "{out}");
        assert!(closed);
    }

    /// RFC 9110 section 9.3.2: a HEAD response's length describes the GET body.
    #[tokio::test]
    async fn a_head_response_keeps_the_handlers_content_length() {
        let out = exchange(
            b"HEAD / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            |_req: Request| async {
                Response::ok().header(HeaderId::ContentLength, Bytes::from_static(b"1234"))
            },
            Limits::default(),
        )
        .await;
        assert!(out.contains("content-length: 1234\r\n"), "{out}");
        assert!(out.ends_with("\r\n\r\n"), "no body: {out}");
    }

    /// A status no final response can carry becomes a 500 and a close.
    #[tokio::test]
    async fn an_impossible_final_status_becomes_500() {
        for status in [1000u16, 99, 102, 101] {
            let (out, closed) = exchange_raw(
                b"GET / HTTP/1.1\r\nHost: a\r\n\r\n",
                move |_req: Request| async move { Response::new(status) },
                Limits::default(),
            )
            .await;
            assert!(
                out.starts_with("HTTP/1.1 500 Internal Server Error\r\n"),
                "{status}: {out}"
            );
            assert!(closed, "{status}");
        }
    }

    /// RFC 9110 sections 10.1.1 and 15.2: no 1xx to an HTTP/1.0 client.
    #[tokio::test]
    async fn no_100_continue_for_http_10() {
        let out = exchange(
            b"POST / HTTP/1.0\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\nhello",
            echo_service,
            Limits::default(),
        )
        .await;
        assert!(!out.contains("100 Continue"), "{out}");
        assert!(out.ends_with("hello"), "{out}");
    }

    /// A short write of the interim response must not count as sending it.
    #[tokio::test]
    async fn a_short_write_of_100_continue_is_completed() {
        let out = exchange_with_capacity(
            b"POST / HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
            echo_service,
            8,
        )
        .await;
        assert!(
            out.starts_with("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n"),
            "{out}"
        );
    }

    /// RFC 9112 section 2.2: empty lines before a request line are ignored,
    /// including between pipelined requests.
    #[tokio::test]
    async fn empty_lines_before_a_request_line_are_ignored() {
        let out = exchange(
            b"\r\n\r\nGET / HTTP/1.1\r\nHost: a\r\n\r\n\r\nGET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert_eq!(out.matches("HTTP/1.1 200 OK").count(), 2, "{out}");
    }

    /// Only whole CRLFs: a bare LF before the request line is still refused,
    /// as a bare LF anywhere in the head is.
    #[tokio::test]
    async fn a_bare_lf_before_a_request_line_is_still_refused() {
        let out = exchange(
            b"\nGET / HTTP/1.1\r\nHost: a\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 400"), "{out}");
    }

    /// An error response for a later pipelined request is appended to the
    /// held-back batch, not written over it.
    #[tokio::test]
    async fn an_error_after_a_held_back_response_keeps_both() {
        let out = exchange(
            b"GET / HTTP/1.1\r\nHost: a\r\n\r\nGET / HTTP/1.1\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        let ok = out.find("HTTP/1.1 200 OK").expect("first response");
        let bad = out.find("HTTP/1.1 400").expect("missing-Host rejection");
        assert!(ok < bad, "{out}");
    }

    #[tokio::test]
    async fn closes_on_connection_close() {
        let (out, closed) = exchange_raw(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert!(out.contains("connection: close"), "{out}");
        assert!(closed, "server must close the socket");
    }

    #[tokio::test]
    async fn http_10_closes_by_default() {
        let out = exchange(b"GET / HTTP/1.0\r\n\r\n", ok_service, Limits::default()).await;
        assert!(out.starts_with("HTTP/1.0 200 OK"), "{out}");
        assert!(out.contains("connection: close"), "{out}");
    }

    #[tokio::test]
    async fn echoes_a_content_length_body() {
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
            echo_service,
            Limits::default(),
        )
        .await;
        assert!(out.ends_with("hello"), "{out}");
    }

    #[tokio::test]
    async fn echoes_a_chunked_body() {
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
            echo_service,
            Limits::default(),
        )
        .await;
        assert!(out.ends_with("hello"), "{out}");
    }

    #[tokio::test]
    async fn head_response_has_headers_but_no_body() {
        let out = exchange(
            b"HEAD / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert!(
            out.contains("content-length: 2"),
            "HEAD keeps the length a GET would report: {out}"
        );
        assert!(out.ends_with("\r\n\r\n"), "no body may follow: {out}");
    }

    #[tokio::test]
    async fn parse_error_yields_400_and_closes() {
        // Bare LF: rejected by the prescan.
        let (out, closed) = exchange_raw(
            b"GET / HTTP/1.1\nHost: a\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 400 Bad Request"), "{out}");
        assert!(out.contains("connection: close"), "{out}");
        assert!(closed);
    }

    #[tokio::test]
    async fn framing_conflict_yields_400_and_closes() {
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\nhello",
            echo_service,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 400 Bad Request"), "{out}");
        assert!(out.contains("connection: close"), "{out}");
    }

    #[tokio::test]
    async fn missing_host_yields_400() {
        let out = exchange(b"GET / HTTP/1.1\r\n\r\n", ok_service, Limits::default()).await;
        assert!(out.starts_with("HTTP/1.1 400 Bad Request"), "{out}");
    }

    #[tokio::test]
    async fn oversized_declared_body_yields_413() {
        let limits = Limits {
            max_body_bytes: 2,
            ..Default::default()
        };
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello",
            echo_service,
            limits,
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 413"), "{out}");
    }

    #[tokio::test]
    async fn unsupported_version_yields_505() {
        let out = exchange(
            b"GET / HTTP/1.2\r\nHost: a\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 505"), "{out}");
    }

    /// The bytes after a rejected request must never be read as a new request.
    #[tokio::test]
    async fn does_not_resynchronize_after_a_framing_error() {
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\nhelloGET /second HTTP/1.1\r\nHost: a\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert_eq!(
            out.matches("HTTP/1.1").count(),
            1,
            "exactly one response; the trailing request must not be served: {out}"
        );
        assert!(out.starts_with("HTTP/1.1 400"), "{out}");
    }

    /// A handler that ignores the body leaves undelimited bytes on the wire, so
    /// the connection must not be reused.
    #[tokio::test]
    async fn unread_body_forces_a_close() {
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhelloGET / HTTP/1.1\r\nHost: a\r\n\r\n",
            ignore_body_service,
            Limits::default(),
        )
        .await;
        assert_eq!(
            out.matches("HTTP/1.1").count(),
            1,
            "the unread body must not be mined for a second request: {out}"
        );
        assert!(out.contains("connection: close"), "{out}");
    }

    #[tokio::test]
    async fn sends_100_continue_when_body_is_read() {
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nExpect: 100-continue\r\nConnection: close\r\n\r\nhello",
            echo_service,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 100 Continue\r\n\r\n"), "{out}");
        assert!(out.contains("HTTP/1.1 200 OK"), "{out}");
    }

    /// The value of lazy 100-continue: a rejection costs no interim response and
    /// no body transfer.
    #[tokio::test]
    async fn omits_100_continue_when_body_is_ignored() {
        let out = exchange(
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\nhello",
            ignore_body_service,
            Limits::default(),
        )
        .await;
        assert!(!out.contains("100 Continue"), "{out}");
        assert_eq!(out.matches("HTTP/1.1").count(), 1, "{out}");
        assert!(out.starts_with("HTTP/1.1 404"), "{out}");
    }

    #[tokio::test]
    async fn oversized_head_yields_431() {
        let limits = Limits {
            max_head_bytes: 40,
            ..Default::default()
        };
        let out = exchange(
            b"GET /aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\nHost: a\r\n\r\n",
            ok_service,
            limits,
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 431"), "{out}");
    }

    /// No `Server` field unless one is configured: it only fingerprints the
    /// implementation for an attacker.
    #[tokio::test]
    async fn server_field_is_omitted_by_default() {
        let out = exchange(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            ok_service,
            Limits::default(),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        assert!(!out.to_ascii_lowercase().contains("\r\nserver:"), "{out}");
    }

    #[tokio::test]
    async fn server_name_is_emitted_when_configured() {
        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            server,
            ok_service,
            Rc::new(ConnConfig {
                limits: Limits::default(),
                tick: Duration::from_millis(10),
                server_name: Some(Bytes::from_static(b"armature-h1")),
            }),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let task = local.spawn_local(async move { conn.serve().await });
        let out = local
            .run_until(async move {
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                    .await;
                let _ = task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await;
        assert!(out.contains("server: armature-h1"), "{out}");
    }

    #[tokio::test]
    async fn response_headers_from_the_handler_are_emitted() {
        async fn tagged(_req: Request) -> Response {
            Response::ok().header(HeaderId::Etag, Bytes::from_static(b"v1"))
        }
        let out = exchange(
            b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
            tagged,
            Limits::default(),
        )
        .await;
        assert!(out.contains("etag: v1"), "{out}");
    }

    /// A peer that stops reading mid-stream must not pin the connection and its
    /// buffer. Every flush in the streaming loop is deadline-guarded, so this
    /// fails if the guard is ever narrowed back to the final write.
    #[tokio::test]
    async fn write_timeout_cuts_off_a_stalled_streamed_body() {
        const CHUNK: &[u8] = &[b'x'; 1024];

        struct Endless;

        impl crate::service::futures_stream::Stream for Endless {
            fn poll_next(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Bytes, crate::BodyError>>> {
                Poll::Ready(Some(Ok(Bytes::from_static(CHUNK))))
            }
        }

        async fn streamer(_req: Request) -> Response {
            Response::ok().with_body(ResponseBody::Stream(Box::pin(Endless)))
        }

        let limits = Limits {
            write_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        // Small enough that the unread transport backs up within a chunk or two.
        let (client, server) = tokio::io::duplex(256);
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            server,
            streamer,
            cfg(limits),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let task = local.spawn_local(async move { conn.serve().await });

        let served = local
            .run_until(async move {
                let mut client = client;
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n")
                    .await
                    .unwrap();
                // Deliberately never read. The client half is held open so the
                // failure comes from the deadline rather than from a closed peer.
                tokio::time::timeout(Duration::from_secs(2), task)
                    .await
                    .expect("the connection must not hang on a peer that stopped reading")
                    .expect("join")
            })
            .await;

        assert_eq!(
            served.expect_err("a stalled write must fail").kind(),
            io::ErrorKind::TimedOut
        );
    }

    /// A handler awaiting body bytes the peer never sends must be cut off. The
    /// deadline being armed is not enough — nothing else polls it, so the
    /// handler call has to lose a race against it.
    #[tokio::test]
    async fn body_timeout_yields_408_and_closes() {
        let limits = Limits {
            body_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            server,
            echo_service,
            cfg(limits),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let task = local.spawn_local(async move { conn.serve().await });

        let out = local
            .run_until(async move {
                // Declares five body bytes and sends three, then never sends the
                // rest.
                client
                    .write_all(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhel")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let read =
                    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                        .await;
                assert!(matches!(read, Ok(Ok(_))), "the server must close");
                let _ = task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await;

        assert!(out.starts_with("HTTP/1.1 408 Request Timeout"), "{out}");
        assert!(out.contains("connection: close"), "{out}");
    }

    /// Status 101 on a request that asked for an upgrade hands the transport
    /// back to whoever drove the connection, together with the bytes the peer
    /// already sent past the head. Those bytes are the first frames of the new
    /// protocol and cannot be re-read from the socket.
    #[tokio::test]
    async fn upgrade_hands_back_the_transport_and_buffered_bytes() {
        async fn switching(_req: Request) -> Response {
            Response::new(101)
                .header(HeaderId::Upgrade, Bytes::from_static(b"raw"))
                .header(HeaderId::Connection, Bytes::from_static(b"upgrade"))
        }

        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            server,
            switching,
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let task = local.spawn_local(async move { conn.serve().await });

        let (head, upgraded) = local
            .run_until(async move {
                client
                    .write_all(
                        b"GET / HTTP/1.1\r\nHost: a\r\nConnection: upgrade\r\nUpgrade: raw\r\n\r\nFIRSTFRAME",
                    )
                    .await
                    .unwrap();
                let mut buf = [0u8; 256];
                let n = client.read(&mut buf).await.unwrap();
                let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                let upgraded = task.await.expect("join").expect("serve");
                (head, upgraded)
            })
            .await;

        assert!(
            head.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
            "{head}"
        );
        assert!(
            !head.contains("content-length"),
            "101 frames no body: {head}"
        );
        let upgraded = upgraded.expect("the transport must be handed back");
        assert_eq!(
            &upgraded.buffered[..],
            b"FIRSTFRAME",
            "bytes past the head belong to the upgraded protocol"
        );
    }

    /// A handler that keeps the request body alive past its 101 still owns a
    /// handle on the transport, so the handoff cannot happen. It must not panic
    /// the connection task either: the retained body is ordinary safe code, and
    /// a per-core worker that unwinds on it takes every other connection on that
    /// core with it.
    #[tokio::test]
    async fn retained_body_across_an_upgrade_closes_instead_of_panicking() {
        thread_local! {
            static LEAKED: RefCell<Option<Body>> = const { RefCell::new(None) };
        }

        async fn switching(req: Request) -> Response {
            // Stash the body in state that outlives the response — the shape a
            // handler holding per-core state would produce.
            LEAKED.with(|slot| *slot.borrow_mut() = Some(req.body));
            Response::new(101)
                .header(HeaderId::Upgrade, Bytes::from_static(b"raw"))
                .header(HeaderId::Connection, Bytes::from_static(b"upgrade"))
        }

        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            server,
            switching,
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let task = local.spawn_local(async move { conn.serve().await });

        let served = local
            .run_until(async move {
                client
                    .write_all(
                        b"GET / HTTP/1.1\r\nHost: a\r\nConnection: upgrade\r\nUpgrade: raw\r\n\r\n",
                    )
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                    .await;
                task.await.expect("the worker task must not panic")
            })
            .await;

        // The 101 head is already on the wire by the time the handoff is
        // attempted — the response is written before the disposition is acted
        // on. What matters is that no upgrade is handed back and no panic
        // escapes.
        assert!(
            served.expect("serve").is_none(),
            "a retained body forfeits the handoff"
        );
        LEAKED.with(|slot| slot.borrow_mut().take());
    }

    /// 101 alone must not take the connection. Handing the transport to a
    /// protocol the client never negotiated loses every subsequent request on
    /// it.
    #[tokio::test]
    async fn status_101_without_a_client_upgrade_request_does_not_upgrade() {
        async fn switching(_req: Request) -> Response {
            Response::new(101)
        }

        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            server,
            switching,
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let task = local.spawn_local(async move { conn.serve().await });

        let upgraded = local
            .run_until(async move {
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut out))
                    .await;
                task.await.expect("join").expect("serve")
            })
            .await;

        assert!(upgraded.is_none(), "no upgrade was negotiated");
    }

    /// A stream that emits one small frame and then goes quiet must deliver
    /// that frame promptly. Coalescing is allowed to span chunks that are ready
    /// together, never to hold bytes across a park — otherwise an SSE body
    /// trickling 50-byte events would stay invisible until 8 KiB had piled up.
    #[tokio::test]
    async fn a_streamed_frame_is_flushed_when_the_stream_goes_idle() {
        struct OneThenIdle {
            sent: bool,
        }

        impl crate::service::futures_stream::Stream for OneThenIdle {
            fn poll_next(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Bytes, crate::BodyError>>> {
                let me = self.get_mut();
                if me.sent {
                    // No waker is registered: this stream is idle forever, the
                    // way a long-poll handler is between events.
                    return Poll::Pending;
                }
                me.sent = true;
                Poll::Ready(Some(Ok(Bytes::from_static(b"hello"))))
            }
        }

        async fn trickle(_req: Request) -> Response {
            Response::ok().with_body(ResponseBody::Stream(Box::pin(OneThenIdle { sent: false })))
        }

        let (client, server) = tokio::io::duplex(64 * 1024);
        let local = tokio::task::LocalSet::new();
        let conn = Connection::new(
            server,
            trickle,
            cfg(Limits::default()),
            Rc::new(RefCell::new(DateCache::new())),
        );
        let task = local.spawn_local(async move { conn.serve().await });

        let out = local
            .run_until(async move {
                let mut client = client;
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                // The stream never completes, so read until the frame shows up
                // or the bound expires — never to EOF.
                let _ = tokio::time::timeout(Duration::from_millis(500), async {
                    let mut buf = [0u8; 1024];
                    loop {
                        let n = client.read(&mut buf).await.unwrap();
                        if n == 0 {
                            break;
                        }
                        out.extend_from_slice(&buf[..n]);
                        if out.ends_with(b"5\r\nhello\r\n") {
                            break;
                        }
                    }
                })
                .await;
                task.abort();
                String::from_utf8_lossy(&out).into_owned()
            })
            .await;

        assert!(
            out.contains("transfer-encoding: chunked"),
            "the head must go out first: {out}"
        );
        assert!(
            out.ends_with("5\r\nhello\r\n"),
            "an idle stream must not hold its frame in the coalescing buffer: {out}"
        );
    }

    /// `Connection` must never become `Send`. The per-core model rests on it:
    /// the `Rc` and `RefCell` in per-core state are only sound because nothing
    /// migrates threads. Asserting a *negative* impl on stable takes the
    /// ambiguity trick — if `Connection` were `Send`, both impls below would
    /// apply and this would stop compiling.
    #[test]
    fn connection_is_not_send() {
        trait AmbiguousIfSend<A> {
            fn assert() {}
        }
        impl<T: ?Sized> AmbiguousIfSend<()> for T {}
        impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}

        type Svc = fn(Request) -> std::future::Ready<Response>;
        let _ = <Connection<tokio::io::DuplexStream, Svc> as AmbiguousIfSend<_>>::assert;
    }

    #[tokio::test]
    async fn eof_between_requests_closes_silently() {
        let (out, _) = exchange_raw(b"", ok_service, Limits::default()).await;
        assert!(
            out.is_empty(),
            "no response is owed on a silent close: {out}"
        );
    }
}
