> **Historical artifact.** This plan documents the design as it stood when the
> `hyper-backend` feature was first built, including build-time scaffolding
> instructions for a now-completed task list. It is kept for history, not as
> current guidance. In particular, the Task 8 phase-clock section describes an
> `idle_timeout`-based approximation that commit `b990f8e` later replaced with
> a real `Phase::Write`; treat that section, and any other decision described
> here, as superseded wherever it conflicts with the current source or with
> `BACKENDS.md`.

# Hyper Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `hyper-backend` cargo feature that serves connections through hyper's `conn::http1` instead of the bespoke protocol stack, behind the same public API, with strict behavioral parity.

**Architecture:** A private `backend` module defines the per-connection serving seam with two impls — `backend::native` (the existing `Connection` loop) and `backend::hyper` (hyper's `serve_connection_with_upgrades` plus adapters). A cfg-gated alias `ActiveBackend` selects one at compile time; `server.rs` and a new public `serve_connection` free function call through it. The bespoke protocol modules stay compiled and exported under both features.

**Tech Stack:** Rust (edition from workspace), tokio current-thread runtimes, `hyper = "1"` (features `server`, `http1`, optional). No hyper-util — the tokio↔hyper IO adapter is hand-rolled.

**Spec:** `docs/superpowers/specs/2026-08-03-hyper-backend-design.md`

## Global Constraints

- Branch: `feature/hyper-backend` (create from `develop` if not already on it).
- The crate is `#![forbid(unsafe_code)]` — the hyper IO adapter must fill `hyper::rt::ReadBufCursor` via its safe `put_slice`, never via the unsafe uninit-slice API.
- Dependency: `hyper = { version = "1", features = ["server", "http1"], optional = true }`. **No hyper-util, no new `http`/`http-body` direct deps** — use hyper's re-exports (`hyper::http`, `hyper::body::{Body, Frame, Incoming, SizeHint}`).
- Feature is **non-additive by design**: `hyper-backend` swaps the serving path for the whole binary. Public API is identical either way.
- Nothing in the serving path may require `Send`: everything runs on `LocalSet` with `Rc`/`RefCell`. hyper's http1 conn supports `!Send` services — do not add an Executor or http2 support.
- Strict parity: limits, deadlines, body caps, and upgrades are enforced identically wherever the adapter can enforce them. The conformance suite (`tests/rfc9112.rs`, `tests/dispatch.rs`, `tests/tls_dispatch.rs`) runs unchanged under both backends.
- Every `#[cfg_attr(feature = "hyper-backend", ignore = "...")]` needs a matching row in `BACKENDS.md`. Empty ignore list is the goal.
- Out of scope: runtime backend selection, hyper http2 (h2c still routes to `H2Fallback`), response trailer support, armature-core changes.
- Inside the `backend::hyper` module, refer to the hyper crate as `::hyper` (the module name shadows it).
- Run tests with `cargo test` and `cargo test --features hyper-backend` (plus `--features tls` combos where relevant). Commit after every green task.

## File Structure

- `Cargo.toml` — feature + optional dep.
- `src/backend/mod.rs` — `Backend` trait, `ActiveBackend` alias, public `serve_connection` wrapper.
- `src/backend/native.rs` — trait impl delegating to `Connection` (always compiled; it IS the native backend and stays the impl behind `Connection`'s public API).
- `src/backend/hyper/mod.rs` — hyper `Backend` impl: builder config, phase watchdog, upgrade recovery, error mapping.
- `src/backend/hyper/io.rs` — `HyperIo<IO>`: tokio↔`hyper::rt` adapter with initial-buffered-bytes support and shared inner handle.
- `src/backend/hyper/body.rs` — `HyperBodyIo` (request body in, `BodyIo` over an `http_body::Body`) and `HyperOutBody` (response body out, `hyper::body::Body` over `ResponseBody`).
- `src/backend/hyper/bridge.rs` — the `hyper::service::Service` impl: head conversion, parity checks, response conversion.
- `src/service.rs` — add cfg-gated `Body::from_backend` (raw-frames body constructor).
- `src/server.rs` — `serve_h1` rewired to `ActiveBackend::serve`.
- `src/lib.rs` — module wiring + `pub use backend::serve_connection`.
- `benches/e2e.rs` — switch from `Connection::new` to `serve_connection` so the A/B harness actually A/Bs.
- `BACKENDS.md` — divergence table.
- `.github/workflows/ci.yml` — new; explicit feature-combo jobs.

---

### Task 1: Cargo feature and dependency

**Files:**
- Modify: `Cargo.toml`

**Interfaces:**
- Produces: cargo feature `hyper-backend`, optional dep `hyper`. All later `#[cfg(feature = "hyper-backend")]` gates hang off this.

- [ ] **Step 1: Add the dependency and feature**

In `Cargo.toml`, after the `tokio-rustls` line in `[dependencies]`:

```toml
hyper = { version = "1", features = ["server", "http1"], optional = true }
```

And in `[features]`:

```toml
hyper-backend = ["dep:hyper"]
```

- [ ] **Step 2: Verify both configurations compile**

Run: `cargo check && cargo check --features hyper-backend`
Expected: both succeed (the feature does nothing yet).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "feat: add hyper-backend cargo feature with optional hyper dep"
```

---

### Task 2: Backend seam — trait, native impl, server rewire, public entry point

**Files:**
- Create: `src/backend/mod.rs`, `src/backend/native.rs`
- Modify: `src/lib.rs`, `src/server.rs:458-481` (`serve_h1`), `benches/e2e.rs`

**Interfaces:**
- Consumes: `Connection::with_buffered`, `RcService` (currently private in `server.rs` — move it into `backend/native.rs`), `ConnConfig`, `DateCache`, `Upgraded`, `H1Service`.
- Produces:
  - `pub(crate) trait Backend` with associated fn `serve<IO, S>(io, service: Rc<S>, cfg: Rc<ConnConfig>, date: Rc<RefCell<DateCache>>, buffered: Bytes) -> impl Future<Output = io::Result<Option<Upgraded>>>` where `IO: AsyncRead + AsyncWrite + Unpin + 'static`, `S: H1Service + 'static`.
  - `pub(crate) struct NativeBackend;` implementing it.
  - `pub(crate) type ActiveBackend` — `NativeBackend` when the feature is off (Task 8 flips it).
  - `pub async fn serve_connection<IO, S>(io, service: Rc<S>, cfg: Rc<ConnConfig>, date: Rc<RefCell<DateCache>>, buffered: Bytes) -> io::Result<Option<Upgraded>>` — public wrapper over `ActiveBackend::serve`, exported from `lib.rs`. This is what makes the e2e bench (an external target that only sees the public API) a real A/B harness.

- [ ] **Step 1: Write the failing test**

In `src/backend/mod.rs` (created below with the test in place), a unit test proving the seam serves a request end to end:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::DateCache;
    use crate::{ConnConfig, Request, Response};
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn hello(_req: Request) -> Response {
        Response::text("hi")
    }

    #[tokio::test]
    async fn serve_connection_serves_through_the_active_backend() {
        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(serve_connection(
            server,
            Rc::new(hello),
            Rc::new(ConnConfig::default()),
            Rc::new(RefCell::new(DateCache::new())),
            bytes::Bytes::new(),
        ));
        let out = local
            .run_until(async move {
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    client.read_to_end(&mut out),
                )
                .await;
                let _ = task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(out.ends_with("hi"), "{out}");
    }

    /// Dispatch read-ahead bytes (h2c sniffing) must reach the backend intact.
    #[tokio::test]
    async fn serve_connection_honors_pre_buffered_bytes() {
        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        // First 4 bytes of the request arrive via `buffered`, the rest on the wire.
        let task = local.spawn_local(serve_connection(
            server,
            Rc::new(hello),
            Rc::new(ConnConfig::default()),
            Rc::new(RefCell::new(DateCache::new())),
            bytes::Bytes::from_static(b"GET "),
        ));
        let out = local
            .run_until(async move {
                client
                    .write_all(b"/ HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    client.read_to_end(&mut out),
                )
                .await;
                let _ = task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib backend -- --nocapture`
Expected: FAIL to compile — module `backend` / `serve_connection` don't exist yet.

- [ ] **Step 3: Implement the seam**

`src/backend/mod.rs`:

```rust
//! The per-connection serving seam.
//!
//! Two implementations exist: [`native`] (the bespoke `Connection` loop) and,
//! behind the `hyper-backend` feature, `hyper` (hyper's `conn::http1`).
//! `ActiveBackend` selects one at compile time. The feature changes which
//! backend the server *uses*, not the API: the bespoke protocol modules stay
//! compiled and exported either way.

pub(crate) mod native;

use crate::conn::ConnConfig;
use crate::service::{H1Service, Upgraded};
use crate::write::DateCache;
use bytes::Bytes;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::rc::Rc;
use tokio::io::{AsyncRead, AsyncWrite};

/// One way of serving an HTTP/1 connection to completion.
pub(crate) trait Backend {
    /// Serve `io` until close, error, or upgrade.
    ///
    /// `buffered` holds bytes protocol dispatch already read (h2c sniffing);
    /// they are part of the first request and must be consumed before `io`.
    fn serve<IO, S>(
        io: IO,
        service: Rc<S>,
        cfg: Rc<ConnConfig>,
        date: Rc<RefCell<DateCache>>,
        buffered: Bytes,
    ) -> impl Future<Output = io::Result<Option<Upgraded>>>
    where
        IO: AsyncRead + AsyncWrite + Unpin + 'static,
        S: H1Service + 'static;
}

#[cfg(not(feature = "hyper-backend"))]
pub(crate) type ActiveBackend = native::NativeBackend;

/// Serve one connection through whichever backend this build selected.
///
/// This is the same entry the server's accept loop uses; it exists publicly so
/// out-of-crate harnesses — the `e2e` bench in particular — measure the
/// selected backend rather than always the native `Connection`.
pub async fn serve_connection<IO, S>(
    io: IO,
    service: Rc<S>,
    cfg: Rc<ConnConfig>,
    date: Rc<RefCell<DateCache>>,
    buffered: Bytes,
) -> io::Result<Option<Upgraded>>
where
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
    S: H1Service + 'static,
{
    ActiveBackend::serve(io, service, cfg, date, buffered).await
}
```

(plus the `#[cfg(test)] mod tests` from Step 1 at the bottom.)

`src/backend/native.rs`:

```rust
//! The bespoke `Connection` loop, behind the backend seam.
//!
//! Always compiled: `Connection` is public API regardless of feature, and the
//! differential story depends on the bespoke stack existing under both builds.

use super::Backend;
use crate::conn::{ConnConfig, Connection};
use crate::service::{H1Service, Upgraded};
use crate::write::DateCache;
use bytes::Bytes;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use tokio::io::{AsyncRead, AsyncWrite};

/// Shares one service across every connection on a worker.
pub(crate) struct RcService<S>(pub(crate) Rc<S>);

impl<S: H1Service> H1Service for RcService<S> {
    type Future = S::Future;

    #[inline]
    fn call(&self, req: crate::Request) -> Self::Future {
        self.0.call(req)
    }
}

pub(crate) struct NativeBackend;

impl Backend for NativeBackend {
    async fn serve<IO, S>(
        io: IO,
        service: Rc<S>,
        cfg: Rc<ConnConfig>,
        date: Rc<RefCell<DateCache>>,
        buffered: Bytes,
    ) -> io::Result<Option<Upgraded>>
    where
        IO: AsyncRead + AsyncWrite + Unpin + 'static,
        S: H1Service + 'static,
    {
        Connection::with_buffered(io, RcService(service), cfg, date, buffered)
            .serve()
            .await
    }
}
```

In `src/lib.rs`: add `mod backend;` to the module list and `pub use backend::serve_connection;` to the re-exports.

In `src/server.rs`: delete the private `RcService` (lines 500-510) and rewrite `serve_h1` to delegate:

```rust
/// Serve one HTTP/1 connection to completion.
async fn serve_h1<IO, S>(
    io: IO,
    service: Rc<S>,
    conn_cfg: Rc<ConnConfig>,
    date: Rc<RefCell<DateCache>>,
    buffered: Bytes,
) where
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
    S: H1Service + 'static,
{
    // (keep the existing comment block about dropped upgrades verbatim)
    if let Ok(Some(upgraded)) = crate::backend::serve_connection(io, service, conn_cfg, date, buffered).await {
        drop(upgraded);
    }
}
```

- [ ] **Step 4: Run the full suite**

Run: `cargo test && cargo test --features tls`
Expected: everything green — pure refactor, no behavior change.

- [ ] **Step 5: Switch `benches/e2e.rs` to the seam**

In `benches/e2e.rs`, replace the `Connection::new(...)` + `conn.serve()` construction inside `spawn_server` with:

```rust
armature_h1::serve_connection(
    server_io,
    Rc::new(service),
    Rc::new(cfg),
    Rc::new(RefCell::new(DateCache::new())),
    bytes::Bytes::new(),
)
```

awaited the same way `conn.serve()` was (adapt to the file's existing structure; the `ConnConfig`/`DateCache` setup already exists there). Do NOT touch `benches/parse.rs`, `benches/write.rs`, or `tests/alloc_regression.rs` — those measure the bespoke modules by design and keep driving `Connection` directly.

Run: `cargo bench --bench e2e -- --test`
Expected: compiles and completes in test mode.

- [ ] **Step 6: Commit**

```bash
git add src/backend src/lib.rs src/server.rs benches/e2e.rs
git commit -m "refactor: introduce backend seam; native Connection becomes the default backend"
```

---

### Task 3: Hyper IO adapter (`HyperIo`)

Everything from here to Task 8 is `#[cfg(feature = "hyper-backend")]`. Gate the module once in `src/backend/mod.rs`:

```rust
#[cfg(feature = "hyper-backend")]
pub(crate) mod hyper;
```

**Files:**
- Create: `src/backend/hyper/mod.rs` (module skeleton: `pub(crate) mod io;` for now), `src/backend/hyper/io.rs`

**Interfaces:**
- Consumes: `crate::service::Transport`.
- Produces (all `pub(crate)`, used by Tasks 7–8):
  - `struct IoShared<IO> { io: IO, buffered: Bytes, saw_bytes: Rc<Cell<bool>> }`
  - `struct HyperIo<IO>(pub Rc<RefCell<IoShared<IO>>>)` implementing `::hyper::rt::Read + ::hyper::rt::Write`. `HyperIo::new(io: IO, buffered: Bytes, saw_bytes: Rc<Cell<bool>>) -> (Self, Rc<RefCell<IoShared<IO>>>)` — the second handle lets the watchdog write a 408 after dropping hyper's future, and lets upgrade recovery reach the inner transport.
  - `struct SharedIo<IO>(pub Rc<RefCell<IoShared<IO>>>)` implementing tokio `AsyncRead + AsyncWrite` (hence `Transport`), for handing an upgraded transport back as `Box<dyn Transport>`.
  - `saw_bytes` is set to `true` on every successful nonzero read — the phase watchdog's idle→header signal.

- [ ] **Step 1: Write the failing tests**

At the bottom of `src/backend/hyper/io.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::pin::Pin;
    use std::rc::Rc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Drive a `hyper::rt::Read` manually and collect what it yields.
    async fn read_some<IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
        io: &mut HyperIo<IO>,
    ) -> Vec<u8> {
        // hyper::rt::ReadBufCursor cannot be constructed outside hyper, so
        // exercise the adapter through hyper itself in Task 8's tests; here,
        // test the pieces that are directly constructible.
        unreachable!("see buffered/write tests below")
    }

    #[tokio::test]
    async fn write_passes_through_and_flushes() {
        let (mut client, server) = tokio::io::duplex(4096);
        let (io, _shared) = HyperIo::new(server, bytes::Bytes::new(), Rc::new(Cell::new(false)));
        let mut io = io;
        std::future::poll_fn(|cx| ::hyper::rt::Write::poll_write(Pin::new(&mut io), cx, b"abc"))
            .await
            .unwrap();
        std::future::poll_fn(|cx| ::hyper::rt::Write::poll_flush(Pin::new(&mut io), cx))
            .await
            .unwrap();
        let mut buf = [0u8; 3];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"abc");
    }

    #[tokio::test]
    async fn shared_io_reads_and_writes_through_the_same_state() {
        let (mut client, server) = tokio::io::duplex(4096);
        let (_hyper_io, shared) =
            HyperIo::new(server, bytes::Bytes::from_static(b"pre"), Rc::new(Cell::new(false)));
        let mut sio = SharedIo(shared);
        // Prepended bytes come out first.
        let mut buf = [0u8; 3];
        sio.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pre");
        // Then the live transport.
        client.write_all(b"xyz").await.unwrap();
        sio.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"xyz");
        // Writes reach the peer.
        sio.write_all(b"ok").await.unwrap();
        sio.flush().await.unwrap();
        let mut out = [0u8; 2];
        client.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"ok");
    }
}
```

Delete the `read_some` placeholder before committing — write only the two real tests. (`HyperIo`'s read path gets covered end-to-end in Task 8, where hyper drives it; `SharedIo` shares the same buffered-then-transport logic and is testable directly, which is what the second test does.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --features hyper-backend --lib backend::hyper::io`
Expected: FAIL to compile — types don't exist.

- [ ] **Step 3: Implement**

`src/backend/hyper/io.rs`:

```rust
//! Hand-rolled tokio <-> `hyper::rt` IO adapter.
//!
//! hyper-util's `TokioIo` would do this, but pulling a dependency in for ~50
//! lines is the wrong trade for this crate's dependency posture. The inner
//! state is `Rc`-shared so the deadline watchdog can write a 408 after
//! dropping hyper's serve future, and so upgrade recovery can hand the
//! transport back out as a `Box<dyn Transport>`.

use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub(crate) struct IoShared<IO> {
    pub(crate) io: IO,
    /// Bytes read before hyper took over (h2c sniff read-ahead). Served first.
    pub(crate) buffered: Bytes,
    /// Set on every nonzero read: the watchdog's idle -> header signal.
    pub(crate) saw_bytes: Rc<Cell<bool>>,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> IoShared<IO> {
    /// Fill `out` from the prepend buffer first, then the transport.
    fn poll_read_into(
        &mut self,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.buffered.is_empty() {
            let n = self.buffered.len().min(out.remaining());
            out.put_slice(&self.buffered.split_to(n));
            self.saw_bytes.set(true);
            return Poll::Ready(Ok(()));
        }
        let before = out.filled().len();
        let poll = Pin::new(&mut self.io).poll_read(cx, out);
        if matches!(poll, Poll::Ready(Ok(()))) && out.filled().len() > before {
            self.saw_bytes.set(true);
        }
        poll
    }
}

pub(crate) struct HyperIo<IO>(pub(crate) Rc<RefCell<IoShared<IO>>>);

impl<IO: AsyncRead + AsyncWrite + Unpin> HyperIo<IO> {
    pub(crate) fn new(
        io: IO,
        buffered: Bytes,
        saw_bytes: Rc<Cell<bool>>,
    ) -> (Self, Rc<RefCell<IoShared<IO>>>) {
        let shared = Rc::new(RefCell::new(IoShared {
            io,
            buffered,
            saw_bytes,
        }));
        (Self(shared.clone()), shared)
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> ::hyper::rt::Read for HyperIo<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ::hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        // `ReadBufCursor`'s uninit API is unsafe; this crate forbids unsafe.
        // Read into a small initialized scratch and copy via the safe
        // `put_slice`. One memcpy per read, same trade `conn.rs` documents for
        // its zeroed read buffer.
        let mut scratch = [0u8; 8 * 1024];
        let want = scratch.len().min(buf.remaining());
        let mut read_buf = ReadBuf::new(&mut scratch[..want]);
        match self.0.borrow_mut().poll_read_into(cx, &mut read_buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {
                buf.put_slice(read_buf.filled());
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> ::hyper::rt::Write for HyperIo<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_shutdown(cx)
    }
}

/// Tokio-flavored view over the same shared state, for upgrade handoff and the
/// watchdog's post-drop error write.
pub(crate) struct SharedIo<IO>(pub(crate) Rc<RefCell<IoShared<IO>>>);

impl<IO: AsyncRead + AsyncWrite + Unpin> AsyncRead for SharedIo<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.0.borrow_mut().poll_read_into(cx, buf)
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> AsyncWrite for SharedIo<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_shutdown(cx)
    }
}
```

`src/backend/hyper/mod.rs` for now is just:

```rust
//! Serving through hyper's `conn::http1`. See BACKENDS.md for divergences.

pub(crate) mod io;
```

- [ ] **Step 4: Run tests**

Run: `cargo test --features hyper-backend --lib backend::hyper && cargo test`
Expected: PASS (and the default build is untouched).

- [ ] **Step 5: Commit**

```bash
git add src/backend
git commit -m "feat: hand-rolled tokio<->hyper::rt IO adapter with prepend buffer"
```

---

### Task 4: Raw-frames request body (`Body::from_backend`)

The bespoke `Body` re-frames its input (`ChunkedDecoder` for chunked bodies). hyper's `Incoming` yields *already de-framed* data frames, so a hyper-backed body must stream frames until EOF without re-framing — while still reporting the original `BodyKind`, enforcing `max_body_bytes` cumulatively (the decoder normally does this for chunked → 413 parity), and surfacing trailers.

**Files:**
- Modify: `src/service.rs` (add to `BodyState`/`Body`)
- Test: `src/service.rs` tests module

**Interfaces:**
- Consumes: existing `BodyState`, `BodyIo`, `BodyError`.
- Produces:
  - `#[cfg(feature = "hyper-backend")] pub(crate) fn Body::from_backend(kind: BodyKind, io: Rc<RefCell<dyn BodyIo>>, needs_continue: bool, max_body_bytes: u64, fully_read: Rc<Cell<bool>>, trailers_slot: Rc<RefCell<Option<HeaderVec>>>) -> Body`
  - Behavior: `poll_chunk` pulls `take_buffered(usize::MAX)` then `poll_fill`; an empty `poll_fill` return is clean EOF → `finish()`, take `trailers_slot` contents (or empty `HeaderVec`) into `state.trailers`, return `None`. Cumulative bytes over `max_body_bytes` → `fail()` + `BodyError::TooLarge` (413, matching `ChunkedError::BodyTooLarge`). `needs_continue` handling identical to `Body::new` (calls `poll_send_continue` before first read — a no-op for hyper, see Task 5).
  - Implementation shape: add a `raw: bool` (cfg-gated) field to `BodyState` plus `consumed: u64` and `trailers_slot: Option<Rc<RefCell<Option<HeaderVec>>>>`; branch at the top of `poll_chunk`'s loop when `raw` is set. `kind` is stored untouched so `body.kind()` reports what the wire actually carried.

- [ ] **Step 1: Write the failing tests**

In `src/service.rs` `tests` module (reusing the existing `MockIo`):

```rust
#[cfg(feature = "hyper-backend")]
mod backend_body {
    use super::*;

    fn raw_body(buffered: &'static [u8], reads: Vec<&'static [u8]>, cap: u64) -> Body {
        Body::from_backend(
            BodyKind::Chunked,
            MockIo::with_buffered(buffered, reads),
            false,
            cap,
            Rc::new(Cell::new(false)),
            Rc::new(RefCell::new(None)),
        )
    }

    /// Raw mode must NOT run the chunked decoder: the input is already
    /// de-framed data, and decoding it again would corrupt or reject it.
    #[tokio::test]
    async fn streams_frames_until_eof_without_reframing() {
        // "5\r\n..." would be chunk framing; here it is literal payload.
        let mut b = raw_body(b"hel", vec![b"lo world", b""], 1024);
        assert_eq!(b.kind(), BodyKind::Chunked, "kind reports the wire framing");
        assert_eq!(&b.collect(1024).await.unwrap()[..], b"hello world");
        assert!(b.was_fully_read());
        assert!(b.trailers().is_some(), "empty trailers after clean EOF");
    }

    #[tokio::test]
    async fn enforces_the_cumulative_body_cap() {
        let mut b = raw_body(b"hello", vec![b" world", b""], 8);
        let err = b.collect(1024).await.unwrap_err();
        assert!(matches!(err, BodyError::TooLarge), "got {err:?}");
        assert_eq!(err.status(), 413);
        assert!(!b.was_fully_read());
    }

    #[tokio::test]
    async fn surfaces_trailers_from_the_slot() {
        let slot = Rc::new(RefCell::new(None));
        let io = MockIo::with_buffered(b"hi", vec![b""]);
        let mut b = Body::from_backend(
            BodyKind::Chunked,
            io,
            false,
            1024,
            Rc::new(Cell::new(false)),
            slot.clone(),
        );
        let mut t = HeaderVec::new();
        t.push((HeaderId::Etag, Bytes::from_static(b"x")));
        *slot.borrow_mut() = Some(t);
        b.collect(1024).await.unwrap();
        let t = b.trailers().expect("trailers after end");
        assert_eq!(crate::header::get_str(t, &HeaderId::Etag), Some("x"));
    }

    /// `poll_send_continue` still runs before the first read, so the lazy
    /// contract holds wherever the transport can honor it.
    #[tokio::test]
    async fn sends_continue_before_first_read() {
        let io = MockIo::with_buffered(b"hi", vec![b""]);
        let mut b = Body::from_backend(
            BodyKind::Length(2),
            io.clone(),
            true,
            1024,
            Rc::new(Cell::new(false)),
            Rc::new(RefCell::new(None)),
        );
        b.collect(1024).await.unwrap();
        assert_eq!(io.borrow().continues, 1);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --features hyper-backend --lib service::tests::backend_body`
Expected: FAIL to compile — `from_backend` doesn't exist.

- [ ] **Step 3: Implement**

In `BodyState`, add (cfg-gated with `#[cfg(feature = "hyper-backend")]` on each field, defaulted in the existing constructors — `raw: false`, `consumed: 0`, `cap: u64::MAX`, `trailers_slot: None`):

```rust
    /// Raw-frames mode: the transport already de-framed the body (hyper), so
    /// yield bytes until EOF instead of applying length/chunk accounting.
    #[cfg(feature = "hyper-backend")]
    raw: bool,
    #[cfg(feature = "hyper-backend")]
    consumed: u64,
    #[cfg(feature = "hyper-backend")]
    cap: u64,
    #[cfg(feature = "hyper-backend")]
    trailers_slot: Option<Rc<RefCell<Option<HeaderVec>>>>,
```

Constructor:

```rust
    /// A body whose transport already de-framed it (the hyper backend).
    ///
    /// `kind` is reported, not enforced: framing was hyper's job. The cap is
    /// enforced cumulatively so chunked bodies keep 413 parity with the
    /// bespoke decoder.
    #[cfg(feature = "hyper-backend")]
    pub(crate) fn from_backend(
        kind: BodyKind,
        io: Rc<RefCell<dyn BodyIo>>,
        needs_continue: bool,
        max_body_bytes: u64,
        fully_read: Rc<Cell<bool>>,
        trailers_slot: Rc<RefCell<Option<HeaderVec>>>,
    ) -> Self {
        let done = matches!(kind, BodyKind::None);
        fully_read.set(done);
        Self {
            state: BodyState {
                kind,
                buffered: Bytes::new(),
                remaining: 0,
                decoder: None,
                trailers: if done { Some(HeaderVec::new()) } else { None },
                io: Some(io),
                continue_sent: !needs_continue,
                needs_continue,
                done,
                fully_read,
                raw: true,
                consumed: 0,
                cap: max_body_bytes,
                trailers_slot: Some(trailers_slot),
            },
        }
    }
```

In `poll_chunk`, after the 100-continue block and before the `loop`:

```rust
        #[cfg(feature = "hyper-backend")]
        if s.raw {
            return Self::poll_raw(s, cx);
        }
```

And the raw poll (associated fn on `Body`):

```rust
    #[cfg(feature = "hyper-backend")]
    fn poll_raw(
        s: &mut BodyState,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, BodyError>>> {
        loop {
            if s.buffered.is_empty()
                && let Some(io) = &s.io
            {
                let got = io.borrow_mut().take_buffered(usize::MAX);
                if !got.is_empty() {
                    s.buffered = got;
                }
            }
            if !s.buffered.is_empty() {
                let chunk = std::mem::take(&mut s.buffered);
                s.consumed += chunk.len() as u64;
                if s.consumed > s.cap {
                    s.fail();
                    return Poll::Ready(Some(Err(BodyError::TooLarge)));
                }
                return Poll::Ready(Some(Ok(chunk)));
            }
            match Self::fill(s, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    s.fail();
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Ok(false)) => {
                    // Clean EOF: hyper delivered the whole body.
                    s.finish();
                    s.trailers = Some(
                        s.trailers_slot
                            .as_ref()
                            .and_then(|slot| slot.borrow_mut().take())
                            .unwrap_or_default(),
                    );
                    return Poll::Ready(None);
                }
                Poll::Ready(Ok(true)) => continue,
            }
        }
    }
```

Update `Body::empty`, `Body::new`, `Body::from_bytes` to initialize the new fields (`raw: false, consumed: 0, cap: u64::MAX, trailers_slot: None`, each behind `#[cfg(feature = "hyper-backend")]`).

- [ ] **Step 4: Run tests both ways**

Run: `cargo test --features hyper-backend --lib service && cargo test --lib service`
Expected: PASS — new tests green under the feature, existing body tests green in both builds.

- [ ] **Step 5: Commit**

```bash
git add src/service.rs
git commit -m "feat: raw-frames Body constructor for de-framed backend transports"
```

---

### Task 5: Request body adapter (`HyperBodyIo`)

**Files:**
- Create: `src/backend/hyper/body.rs` (this task: the inbound half)
- Modify: `src/backend/hyper/mod.rs` (add `pub(crate) mod body;`)

**Interfaces:**
- Consumes: `crate::service::BodyIo`, `crate::header::{self, HeaderId, HeaderVec}`, `::hyper::body::{Body as HttpBody, Frame}`.
- Produces: `pub(crate) struct HyperBodyIo<B> { body: B, stash: Bytes, trailers_slot: Rc<RefCell<Option<HeaderVec>>>, err: Option<io::ErrorKind-ish> }` implementing `BodyIo`, generic over `B: ::hyper::body::Body<Data = Bytes> + Unpin` where `B::Error: Into<Box<dyn std::error::Error + Send + Sync>>`. Production instantiation is `B = ::hyper::body::Incoming`; the generic exists because `Incoming` cannot be constructed in tests.
  - `poll_fill`: poll `Pin::new(&mut self.body).poll_frame(cx)`. Data frame → return its `Bytes`. Trailers frame → convert each header (`HeaderId::from_bytes` else `header::intern`), reject any `id.forbidden_in_trailers()` with `io::Error::new(io::ErrorKind::InvalidData, "forbidden trailer field")` (surfaces as `BodyError::Io` → status 400, matching `ChunkedError::ForbiddenTrailer`'s 400), else store in `trailers_slot` and keep polling. `None` → EOF (`Bytes::new()`). Body error → `io::Error::other(e)`.
  - `take_buffered(max)` / `push_back`: operate on the local `stash` (hyper owns real buffering; the stash only holds a frame the `Body` split or pushed back).
  - `poll_send_continue`: `Poll::Ready(Ok(()))` — hyper sends `100 Continue` itself, eagerly, when it parses `Expect`. **This is the recorded divergence** (BACKENDS.md, Task 9).

- [ ] **Step 1: Write the failing tests**

In `src/backend/hyper/body.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::HeaderId;
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A scripted `http_body::Body` standing in for `Incoming`.
    struct Scripted(VecDeque<::hyper::body::Frame<Bytes>>);

    impl ::hyper::body::Body for Scripted {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<::hyper::body::Frame<Bytes>, Self::Error>>> {
            Poll::Ready(self.0.pop_front().map(Ok))
        }
    }

    fn body_io(frames: Vec<::hyper::body::Frame<Bytes>>) -> HyperBodyIo<Scripted> {
        HyperBodyIo::new(
            Scripted(frames.into()),
            std::rc::Rc::new(std::cell::RefCell::new(None)),
        )
    }

    async fn fill(io: &mut HyperBodyIo<Scripted>) -> std::io::Result<Bytes> {
        std::future::poll_fn(|cx| io.poll_fill(cx)).await
    }

    #[tokio::test]
    async fn yields_data_frames_then_empty_at_eof() {
        let mut io = body_io(vec![
            ::hyper::body::Frame::data(Bytes::from_static(b"hel")),
            ::hyper::body::Frame::data(Bytes::from_static(b"lo")),
        ]);
        assert_eq!(&fill(&mut io).await.unwrap()[..], b"hel");
        assert_eq!(&fill(&mut io).await.unwrap()[..], b"lo");
        assert!(fill(&mut io).await.unwrap().is_empty(), "EOF is empty");
    }

    #[tokio::test]
    async fn captures_trailers_into_the_slot() {
        let mut trailers = ::hyper::http::HeaderMap::new();
        trailers.insert(
            ::hyper::http::header::ETAG,
            ::hyper::http::HeaderValue::from_static("x"),
        );
        let slot = std::rc::Rc::new(std::cell::RefCell::new(None));
        let mut io = HyperBodyIo::new(
            Scripted(
                vec![
                    ::hyper::body::Frame::data(Bytes::from_static(b"hi")),
                    ::hyper::body::Frame::trailers(trailers),
                ]
                .into(),
            ),
            slot.clone(),
        );
        assert_eq!(&fill(&mut io).await.unwrap()[..], b"hi");
        assert!(fill(&mut io).await.unwrap().is_empty());
        let t = slot.borrow_mut().take().expect("trailers captured");
        assert_eq!(crate::header::get_str(&t, &HeaderId::Etag), Some("x"));
    }

    #[tokio::test]
    async fn rejects_forbidden_trailers() {
        let mut trailers = ::hyper::http::HeaderMap::new();
        trailers.insert(
            ::hyper::http::header::CONTENT_LENGTH,
            ::hyper::http::HeaderValue::from_static("5"),
        );
        let mut io = body_io(vec![::hyper::body::Frame::trailers(trailers)]);
        let err = fill(&mut io).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn stash_round_trips_through_take_and_push_back() {
        let mut io = body_io(vec![]);
        io.push_back(Bytes::from_static(b"abc"));
        assert_eq!(&io.take_buffered(2)[..], b"ab");
        assert_eq!(&io.take_buffered(usize::MAX)[..], b"c");
        assert!(io.take_buffered(usize::MAX).is_empty());
    }

    #[tokio::test]
    async fn send_continue_is_a_noop() {
        let mut io = body_io(vec![]);
        let r = std::future::poll_fn(|cx| io.poll_send_continue(cx)).await;
        assert!(r.is_ok());
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --features hyper-backend --lib backend::hyper::body`
Expected: FAIL to compile.

- [ ] **Step 3: Implement**

```rust
//! `BodyIo` over a hyper request body.
//!
//! hyper de-frames the body (content-length accounting, chunk decoding, its
//! own buffering); this adapter just relays data frames, captures trailers,
//! and applies the trailer-field policy the bespoke `ChunkedDecoder` applies.

use crate::header::{self, HeaderId, HeaderVec};
use crate::service::BodyIo;
use bytes::Bytes;
use std::cell::RefCell;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

pub(crate) struct HyperBodyIo<B> {
    body: B,
    /// Bytes the `Body` took but did not consume, or pushed back.
    stash: Bytes,
    trailers_slot: Rc<RefCell<Option<HeaderVec>>>,
}

impl<B> HyperBodyIo<B> {
    pub(crate) fn new(body: B, trailers_slot: Rc<RefCell<Option<HeaderVec>>>) -> Self {
        Self {
            body,
            stash: Bytes::new(),
            trailers_slot,
        }
    }
}

impl<B> BodyIo for HyperBodyIo<B>
where
    B: ::hyper::body::Body<Data = Bytes> + Unpin,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Bytes>> {
        loop {
            match Pin::new(&mut self.body).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(Bytes::new())),
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(io::Error::other(e.into())));
                }
                Poll::Ready(Some(Ok(frame))) => {
                    let frame = match frame.into_data() {
                        Ok(data) => return Poll::Ready(Ok(data)),
                        Err(frame) => frame,
                    };
                    if let Ok(map) = frame.into_trailers() {
                        let mut vec = HeaderVec::new();
                        for (name, value) in map.iter() {
                            let id = HeaderId::from_bytes(name.as_str().as_bytes())
                                .unwrap_or_else(|| header::intern(name.as_str()));
                            // Same policy as `ChunkedDecoder`: a trailer that
                            // could change message semantics is rejected, and
                            // the rejection closes the connection (status 400
                            // via BodyError::Io).
                            if id.forbidden_in_trailers() {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "forbidden trailer field",
                                )));
                            }
                            vec.push((id, Bytes::copy_from_slice(value.as_bytes())));
                        }
                        *self.trailers_slot.borrow_mut() = Some(vec);
                    }
                    // Unknown frame types are skipped; keep polling.
                }
            }
        }
    }

    fn poll_send_continue(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // hyper sends the interim response itself, eagerly, when it parses
        // `Expect: 100-continue`. Known divergence from the bespoke stack's
        // lazy send — recorded in BACKENDS.md.
        Poll::Ready(Ok(()))
    }

    fn take_buffered(&mut self, max: usize) -> Bytes {
        let n = self.stash.len().min(max);
        self.stash.split_to(n)
    }

    fn push_back(&mut self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        if self.stash.is_empty() {
            self.stash = bytes;
        } else {
            let mut joined = Vec::with_capacity(bytes.len() + self.stash.len());
            joined.extend_from_slice(&bytes);
            joined.extend_from_slice(&self.stash);
            self.stash = Bytes::from(joined);
        }
    }
}
```

(If `header::intern` takes `&str` and lowercases — check its doc; hyper's `HeaderName::as_str()` is already lowercase, so either way is fine.)

- [ ] **Step 4: Run tests**

Run: `cargo test --features hyper-backend --lib backend::hyper::body`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/backend/hyper
git commit -m "feat: BodyIo adapter over hyper request bodies with trailer parity"
```

---

### Task 6: Response body adapter (`HyperOutBody`)

**Files:**
- Modify: `src/backend/hyper/body.rs` (add the outbound half)

**Interfaces:**
- Consumes: `crate::service::ResponseBody`.
- Produces: `pub(crate) struct HyperOutBody(ResponseBody)` with `pub(crate) fn new(body: ResponseBody) -> Self`, implementing `::hyper::body::Body<Data = Bytes, Error = crate::service::BodyError>`:
  - `Empty` → `is_end_stream() == true`, exact `SizeHint` of 0 → hyper emits `content-length: 0` framing exactly like the native writer's `OutBody::None`/empty-fixed path.
  - `Full(b)` → one data frame then end; exact `SizeHint` of `b.len()` → hyper picks `Content-Length`, same as `OutBody::Fixed`.
  - `Stream(s)` → poll frames through; default (unbounded) `SizeHint` → hyper picks chunked, same as `OutBody::Chunked`. A stream item `Err(e)` is returned as the body error, which makes hyper abort the connection without a terminating chunk — the native loop's mid-stream-failure behavior (`Disposition::Close`, no last-chunk).
  - No trailers ever (matches native).

- [ ] **Step 1: Write the failing tests**

Append to `src/backend/hyper/body.rs` tests:

```rust
    use crate::service::{futures_stream, ResponseBody};

    fn poll_out(
        b: &mut HyperOutBody,
    ) -> Poll<Option<Result<::hyper::body::Frame<Bytes>, crate::service::BodyError>>> {
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(&waker);
        Pin::new(b).poll_frame(&mut cx)
    }

    #[test]
    fn empty_body_ends_immediately_with_exact_zero_hint() {
        let mut b = HyperOutBody::new(ResponseBody::Empty);
        assert!(::hyper::body::Body::is_end_stream(&b));
        assert_eq!(::hyper::body::Body::size_hint(&b).exact(), Some(0));
        assert!(matches!(poll_out(&mut b), Poll::Ready(None)));
    }

    #[test]
    fn full_body_yields_one_frame_with_exact_hint() {
        let mut b = HyperOutBody::new(ResponseBody::Full(Bytes::from_static(b"hello")));
        assert_eq!(::hyper::body::Body::size_hint(&b).exact(), Some(5));
        let Poll::Ready(Some(Ok(frame))) = poll_out(&mut b) else {
            panic!("expected a data frame");
        };
        assert_eq!(&frame.into_data().unwrap()[..], b"hello");
        assert!(matches!(poll_out(&mut b), Poll::Ready(None)));
    }

    #[test]
    fn stream_body_has_no_exact_hint_so_hyper_chunks_it() {
        struct Two(u8);
        impl futures_stream::Stream for Two {
            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Bytes, crate::service::BodyError>>> {
                self.0 += 1;
                match self.0 {
                    1 => Poll::Ready(Some(Ok(Bytes::from_static(b"a")))),
                    2 => Poll::Ready(Some(Ok(Bytes::from_static(b"b")))),
                    _ => Poll::Ready(None),
                }
            }
        }
        let mut b = HyperOutBody::new(ResponseBody::Stream(Box::pin(Two(0))));
        assert_eq!(::hyper::body::Body::size_hint(&b).exact(), None);
        let Poll::Ready(Some(Ok(f))) = poll_out(&mut b) else { panic!() };
        assert_eq!(&f.into_data().unwrap()[..], b"a");
        let Poll::Ready(Some(Ok(f))) = poll_out(&mut b) else { panic!() };
        assert_eq!(&f.into_data().unwrap()[..], b"b");
        assert!(matches!(poll_out(&mut b), Poll::Ready(None)));
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --features hyper-backend --lib backend::hyper::body`
Expected: FAIL to compile — `HyperOutBody` doesn't exist.

- [ ] **Step 3: Implement**

```rust
/// A response body in hyper's dialect.
///
/// Framing choice is delegated to hyper via `SizeHint`: exact hints yield
/// `Content-Length`, the absence of one yields chunked — the same decision
/// table as the native writer's `OutBody`.
pub(crate) struct HyperOutBody {
    body: crate::service::ResponseBody,
    done: bool,
}

impl HyperOutBody {
    pub(crate) fn new(body: crate::service::ResponseBody) -> Self {
        let done = matches!(body, crate::service::ResponseBody::Empty);
        Self { body, done }
    }
}

impl ::hyper::body::Body for HyperOutBody {
    type Data = Bytes;
    type Error = crate::service::BodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<::hyper::body::Frame<Bytes>, Self::Error>>> {
        if self.done {
            return Poll::Ready(None);
        }
        match &mut self.body {
            crate::service::ResponseBody::Empty => {
                self.done = true;
                Poll::Ready(None)
            }
            crate::service::ResponseBody::Full(b) => {
                let data = std::mem::take(b);
                self.done = true;
                if data.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(::hyper::body::Frame::data(data))))
                }
            }
            crate::service::ResponseBody::Stream(s) => match s.as_mut().poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    self.done = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    Poll::Ready(Some(Ok(::hyper::body::Frame::data(chunk))))
                }
                Poll::Ready(Some(Err(e))) => {
                    // Mid-stream failure: erroring the body makes hyper drop
                    // the connection without a terminating chunk, which is the
                    // native loop's Disposition::Close for the same case.
                    self.done = true;
                    Poll::Ready(Some(Err(e)))
                }
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }

    fn size_hint(&self) -> ::hyper::body::SizeHint {
        match &self.body {
            crate::service::ResponseBody::Empty => ::hyper::body::SizeHint::with_exact(0),
            crate::service::ResponseBody::Full(b) => {
                ::hyper::body::SizeHint::with_exact(b.len() as u64)
            }
            crate::service::ResponseBody::Stream(_) => ::hyper::body::SizeHint::default(),
        }
    }
}
```

(Note: after the first `Full` frame is taken, `size_hint` sees an empty `Bytes` — hyper reads the hint before polling, so this is fine; if a debug assertion in hyper disagrees, keep the original length in a field.)

- [ ] **Step 4: Run tests**

Run: `cargo test --features hyper-backend --lib backend::hyper::body`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/backend/hyper/body.rs
git commit -m "feat: hyper response body adapter delegating framing via SizeHint"
```

---

### Task 7: Service bridge — head conversion, parity checks, response conversion

**Files:**
- Create: `src/backend/hyper/bridge.rs`
- Modify: `src/backend/hyper/mod.rs` (add `pub(crate) mod bridge;`)

**Interfaces:**
- Consumes: Task 4's `Body::from_backend`, Task 5's `HyperBodyIo`, Task 6's `HyperOutBody`, `crate::{Head, Method, Version, Limits}`, `crate::header::{HeaderId}`, `crate::bytestr::ByteStr`, `crate::conn::ConnConfig`.
- Produces:
  - `pub(crate) fn convert_head(parts: &::hyper::http::request::Parts, limits: &Limits) -> Result<Head, u16>` — pure, unit-testable. Err is the rejection status.
  - `pub(crate) struct Bridge<S> { pub service: Rc<S>, pub cfg: Rc<ConnConfig>, pub upgrade_slot: Rc<RefCell<Option<::hyper::upgrade::OnUpgrade>>>, pub sent_101: Rc<Cell<bool>>, pub phase: Rc<PhaseClock> }` implementing `::hyper::service::Service<::hyper::http::Request<::hyper::body::Incoming>>` with `Response = ::hyper::http::Response<HyperOutBody>`, `Error = std::convert::Infallible`, `Future = Pin<Box<dyn Future<Output = ...>>>` (no `Send`). (`PhaseClock` arrives in Task 8; until then take `phase: Rc<Cell<Phase>>`-free — define `Bridge` WITHOUT the phase field in this task and add it in Task 8.)
  - Head conversion rules:
    - Method: `Method::from_bytes(parts.method.as_str().as_bytes())` else `Method::Other(ByteStr::from_utf8(Bytes::copy_from_slice(...)).expect("method tokens are ASCII"))`.
    - Target: `parts.uri` — if `uri.path_and_query()` is `Some` and the uri has no scheme, use `path_and_query().as_str()`; otherwise (absolute-form, asterisk-form) use the full `uri.to_string()`. Copy into `ByteStr`.
    - Version: `::hyper::http::Version::HTTP_11 => Version::Http11`, `HTTP_10 => Version::Http10`, anything else → `Err(505)`.
    - Headers: wire order via `parts.headers.iter()`; name → `HeaderId::from_bytes` else `header::intern`; value → `Bytes::copy_from_slice`.
    - Parity checks hyper does not enforce: header count > `limits.max_headers` → `Err(431)`; cumulative bytes (target len + Σ(name len + value len + 4) + 26) > `limits.max_head_bytes` → `Err(431)` (the constant approximates request-line overhead; exactness is not required — hyper's own `max_buf_size` catches the wire-level cap, this check keeps the *count-independent* cumulative cap enforced); declared `Content-Length` > `limits.max_body_bytes` → `Err(413)`.
  - Per-request flow in `call(&self, mut req)`:
    1. `let on_upgrade = ::hyper::upgrade::on(&mut req);` — always taken; only stored later if the response is an accepted 101.
    2. Compute `wants_upgrade` (Upgrade header present + Connection token `upgrade`, using the converted `Head`'s `connection_has_token`) and `expects_continue`, and `is_head = parts.method == ::hyper::http::Method::HEAD` (hyper suppresses HEAD bodies itself; keep the converted `Head` faithful).
    3. `convert_head` — on `Err(status)`, return `Response::status_only(status)` converted, with `Connection: close` semantics: also set `sent_reject` so the backend closes (hyper closes on its own for most of these; to force it, add `connection: close` to the response headers via `.header(::hyper::http::header::CONNECTION, "close")`).
    4. Build the `Body`: `BodyKind` from the converted head via `crate::framing::decide(&head, &self.cfg.limits)` — reusing the bespoke framing decision table keeps rejection parity for anything hyper let through (on `Err(e)` reject with `e.status()` + close). Then `Body::from_backend(kind, Rc::new(RefCell::new(HyperBodyIo::new(req_body, trailers_slot.clone()))), expects_continue, cfg.limits.max_body_bytes, fully_read, trailers_slot)`.
    5. Await `self.service.call(Request { head, body })`.
    6. Convert the `Response`: status via `::hyper::http::StatusCode::from_u16` (invalid → 500); headers appended with `HeaderName::from_bytes(id.as_str().as_bytes())` / `HeaderValue::from_maybe_shared(value)` (skip values that fail validation — the native writer would have written them raw, but hyper refuses; note for the audit); add `cfg.server_name` as `server` if the handler didn't set one (parity with `write_response`); strip the body when `matches!(status, 204 | 304) || (100..200).contains(&status)` (parity: the native writer drops such bodies rather than desyncing).
    7. Upgrade: if `status == 101 && wants_upgrade`, store `on_upgrade` in `self.upgrade_slot` and set `self.sent_101`. If `status == 101 && !wants_upgrade`, downgrade parity: native does NOT upgrade but still writes the 101 head and then treats the connection per keep-alive — replicate by sending the 101 as-is without storing the upgrade (hyper will handle the rest; audit in Task 9 verifies observable equivalence).
    8. Keep-alive parity for unread bodies: native closes when the handler didn't fully read the body. After the handler returns, check `fully_read.get()`; if false, add `connection: close` to the response headers. (hyper honors an explicit close header and closes after writing.)

- [ ] **Step 1: Write the failing tests for `convert_head`**

In `src/backend/hyper/bridge.rs`:

```rust
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
        let p = parts(::hyper::http::Request::builder().uri("http://a/x").header("host", "a"));
        let head = convert_head(&p, &Limits::default()).unwrap();
        assert_eq!(head.target.as_str(), "http://a/x");
    }

    #[test]
    fn too_many_headers_rejects_431() {
        let mut b = ::hyper::http::Request::builder().uri("/").header("host", "a");
        for i in 0..Limits::default().max_headers {
            b = b.header(format!("x-h-{i}"), "v");
        }
        assert_eq!(convert_head(&parts(b), &Limits::default()), Err(431));
    }

    #[test]
    fn cumulative_header_bytes_reject_431() {
        let limits = Limits { max_head_bytes: 64, ..Default::default() };
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
        let limits = Limits { max_body_bytes: 4, ..Default::default() };
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
        let p = parts(::hyper::http::Request::builder().method("PURGE").uri("/").header("host", "a"));
        let head = convert_head(&p, &Limits::default()).unwrap();
        assert!(matches!(head.method, Method::Other(ref t) if t.as_str() == "PURGE"));
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --features hyper-backend --lib backend::hyper::bridge`
Expected: FAIL to compile.

- [ ] **Step 3: Implement `convert_head`**

```rust
//! The hyper service bridge: converts heads and responses between dialects
//! and enforces the parity checks hyper does not.

use crate::bytestr::ByteStr;
use crate::header::{self, HeaderId, HeaderVec};
use crate::{Head, Limits, Method, Version};
use bytes::Bytes;

/// Approximate request-line overhead ("METHOD  HTTP/1.1\r\n" scaffolding) for
/// the cumulative head-bytes cap. The wire-exact cap is enforced by hyper's
/// `max_buf_size`; this check exists so a head that is *cumulatively* huge
/// without any single oversized piece is still rejected at 431 parity.
const HEAD_OVERHEAD: usize = 26;

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

    let target = ByteStr::from_utf8(Bytes::from(target_str.into_bytes()))
        .expect("http::Uri is valid UTF-8");

    Ok(Head {
        method,
        target,
        version,
        headers,
    })
}
```

- [ ] **Step 4: Run the head tests**

Run: `cargo test --features hyper-backend --lib backend::hyper::bridge`
Expected: PASS.

- [ ] **Step 5: Implement the `Bridge` service**

Add to `bridge.rs` (no unit test of its own — `Incoming` cannot be constructed outside hyper, so `Bridge` is covered by Task 8's end-to-end tests, which run within two commits of this one):

```rust
use crate::conn::ConnConfig;
use crate::service::{Body, H1Service, Request};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use super::body::{HyperBodyIo, HyperOutBody};

pub(crate) struct Bridge<S> {
    pub(crate) service: Rc<S>,
    pub(crate) cfg: Rc<ConnConfig>,
    /// Filled only when a 101 on a request that asked for an upgrade goes out.
    pub(crate) upgrade_slot: Rc<RefCell<Option<::hyper::upgrade::OnUpgrade>>>,
    pub(crate) sent_101: Rc<Cell<bool>>,
}

type BridgeResponse = ::hyper::http::Response<HyperOutBody>;

impl<S: H1Service + 'static> ::hyper::service::Service<::hyper::http::Request<::hyper::body::Incoming>>
    for Bridge<S>
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

    let status =
        ::hyper::http::StatusCode::from_u16(status).unwrap_or(::hyper::http::StatusCode::INTERNAL_SERVER_ERROR);

    // Parity with the native writer: bodies on 204/304/1xx are dropped, not
    // written, so a handler mistake degrades identically under both backends.
    let body_forbidden =
        matches!(status.as_u16(), 204 | 304) || status.is_informational();
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
    if !has_server && let Some(name) = &cfg.server_name
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
```

(`headers.into_iter()` — `HeaderVec` is a `SmallVec` of `(HeaderId, Bytes)`, so this iterates owned pairs. Adjust import list as needed: `use crate::header::HeaderId;`.)

- [ ] **Step 6: Verify it compiles**

Run: `cargo check --features hyper-backend && cargo test --features hyper-backend --lib`
Expected: compiles; existing tests still green.

- [ ] **Step 7: Commit**

```bash
git add src/backend/hyper
git commit -m "feat: hyper service bridge with head conversion and parity checks"
```

---

### Task 8: The hyper `Backend` impl — builder config, deadline watchdog, upgrade recovery

**Files:**
- Modify: `src/backend/hyper/mod.rs` (the `Backend` impl and `PhaseClock`), `src/backend/mod.rs` (the cfg alias)

**Interfaces:**
- Consumes: everything from Tasks 3–7; `crate::deadline::ConnDeadline`; `crate::write::{self, DateCache, ResponseHead, OutBody}`; `crate::header::HeaderVec`.
- Produces:
  - `pub(crate) struct HyperBackend;` implementing `Backend`.
  - `pub(crate) struct PhaseClock { phase: Cell<Phase>, generation: Cell<u64>, notify: tokio::sync::Notify }` with `pub(crate) fn set(&self, p: Phase)` (store + bump generation + `notify_waiters`) and `#[derive(Clone, Copy, PartialEq)] pub(crate) enum Phase { Idle, Head, Handler }`.
  - In `src/backend/mod.rs`:

```rust
#[cfg(feature = "hyper-backend")]
pub(crate) type ActiveBackend = hyper::HyperBackend;
```

**Deadline parity design** (the load-bearing part — native semantics, produced from outside hyper):

- *Idle* (`idle_timeout`): from connection start / end of a response until the first byte of the next request. Signal: `saw_bytes` flag in `IoShared` (Task 3) — the watchdog polls it each wakeup; when set, clear it and move `Idle → Head`. On expiry: drop the serve future, close silently (native returns `Ok(None)` with no response).
- *Head* (`header_timeout`): first byte until the bridge's `call` runs. Signal: `Bridge::call` sets `Phase::Handler` (add a `phase: Rc<PhaseClock>` field to `Bridge`, set at the top of `call`, and set back to `Phase::Idle` just before returning the converted response — response writing is covered by hyper plus the overall write behavior; see divergence note below). On expiry: drop the serve future, write a native-formatted `408` + close via the retained `SharedIo` handle and `write::write_head` (same bytes as `Connection::write_error`).
- *Handler/body* (`body_timeout`): while the handler runs. Native races the handler against `body_timeout` and answers 408. Same action as Head expiry: drop, write 408, close.
- *Write* (`write_timeout`): native re-arms per flush. From outside hyper the per-flush boundary is invisible; the watchdog treats `Phase::Idle`-with-response-in-flight identically to idle. **Approximation:** after the bridge returns a response, the phase goes `Idle`; a peer that stops reading mid-response is cut off by `idle_timeout` instead of `write_timeout` (75s vs 30s default). Record in BACKENDS.md; the conformance suite's write-stall test (if the audit finds one failing) gets handled in Task 9. If a test *requires* write_timeout parity, refine: keep phase `Write` from bridge-return until the next `saw_bytes` or serve-future completion, using `write_timeout`.

Watchdog loop (runs `select!`-ed against the serve future):

```rust
async fn watchdog(clock: Rc<PhaseClock>, saw_bytes: Rc<Cell<bool>>, limits: &Limits, deadline: &mut ConnDeadline) -> Phase {
    loop {
        let gen = clock.generation.get();
        let phase = clock.phase.get();
        deadline.arm(match phase {
            Phase::Idle => limits.idle_timeout,
            Phase::Head => limits.header_timeout,
            Phase::Handler => limits.body_timeout,
        });
        tokio::select! {
            biased;
            _ = clock.notify.notified() => continue,
            () = deadline.expired() => {
                // A read may have arrived without a phase change (Idle -> Head
                // is signalled by the IO adapter, not the bridge).
                if phase == Phase::Idle && saw_bytes.replace(false) {
                    clock.set(Phase::Head);
                    continue;
                }
                if clock.generation.get() != gen { continue; }
                return phase; // expired in this phase
            }
        }
    }
}
```

Wait — `saw_bytes` transitions must be prompt, not deadline-delayed: if the idle deadline is longer than the header deadline this is safe (idle ≥ header is not guaranteed by `Limits`!). Correct approach: have the IO adapter's `poll_read_into` call `clock.set(Phase::Head)` directly when phase is `Idle` and bytes arrive. So: `IoShared.saw_bytes` becomes `IoShared.on_first_byte: Rc<PhaseClock>` — in Task 3 keep the `Rc<Cell<bool>>` signature simple, then in this task change it to hold the `PhaseClock` and set `Idle → Head` inline (one comparison per read; only fires on the transition). Update Task 3's tests mechanically (construct a `PhaseClock` instead of a `Cell<bool>`).

`HyperBackend::serve` shape:

```rust
impl Backend for HyperBackend {
    async fn serve<IO, S>(
        io: IO,
        service: Rc<S>,
        cfg: Rc<ConnConfig>,
        date: Rc<RefCell<DateCache>>,
        buffered: Bytes,
    ) -> io::Result<Option<Upgraded>>
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

        let conn = ::hyper::server::conn::http1::Builder::new()
            // Timing policy is ours: the watchdog owns every deadline.
            .header_read_timeout(None)
            // Wire-level head cap; the cumulative cap lives in convert_head.
            .max_buf_size(cfg.limits.max_head_bytes)
            // Native semantics: EOF on read closes; responses flush per
            // response, not batched across pipelined requests.
            .half_close(false)
            .pipeline_flush(false)
            .serve_connection(hyper_io, bridge)
            .with_upgrades();
        tokio::pin!(conn);

        let mut deadline = ConnDeadline::new(cfg.tick);
        let result = tokio::select! {
            biased;
            expired = watchdog(&clock, &cfg.limits, &mut deadline) => Err(expired),
            r = &mut conn => Ok(r),
        };

        match result {
            // Idle expiry: silent close, no response owed (native parity).
            Err(Phase::Idle) => Ok(None),
            // Header/body expiry: native writes a bare 408 and closes.
            Err(Phase::Head) | Err(Phase::Handler) => {
                drop(conn); // release hyper's borrow of the shared IO
                write_error_close(&shared, &date, 408).await;
                Ok(None)
            }
            Ok(Ok(())) => {
                if sent_101.get()
                    && let Some(on_upgrade) = upgrade_slot.borrow_mut().take()
                {
                    drop(conn);
                    return Ok(recover_upgrade::<IO>(on_upgrade).await);
                }
                Ok(None)
            }
            // hyper handled and reported protocol errors itself (it writes its
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
```

Support functions in `mod.rs`:

```rust
/// Write a native-formatted bare error response after hyper's future is gone.
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
            &ResponseHead { status, headers: HeaderVec::new() },
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

/// Await hyper's upgrade handshake and repackage as our `Upgraded`.
async fn recover_upgrade<IO>(on_upgrade: ::hyper::upgrade::OnUpgrade) -> Option<Upgraded>
where
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let upgraded = on_upgrade.await.ok()?;
    let parts = upgraded.downcast::<HyperIo<IO>>().ok()?;
    Some(Upgraded {
        io: Box::new(SharedIo(parts.io.0)),
        // hyper's read-ahead satisfies the `buffered` contract: bytes the peer
        // sent past the 101 head, which must not be dropped.
        buffered: parts.read_buf,
    })
}

/// Walk the error source chain for an `io::Error` to preserve error-kind
/// parity with the native loop where possible.
fn find_io_error(e: &::hyper::Error) -> Option<io::Error> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(s) = source {
        if let Some(io_err) = s.downcast_ref::<io::Error>() {
            return Some(io::Error::new(io_err.kind(), io_err.to_string()));
        }
        source = s.source();
    }
    None
}
```

(`watchdog` signature in real code: `async fn watchdog(clock: &PhaseClock, limits: &Limits, deadline: &mut ConnDeadline) -> Phase` — the `saw_bytes` handling moved into the IO adapter per the note above, so the loop is exactly: snapshot generation+phase, arm, select notify vs expiry, return phase on un-bumped expiry.)

- [ ] **Step 1: Write the failing end-to-end tests**

In `src/backend/hyper/mod.rs`:

```rust
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
        assert!(out.to_ascii_lowercase().contains("content-length: 2"), "{out}");
        assert!(!out.to_ascii_lowercase().contains("transfer-encoding"), "{out}");
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
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --features hyper-backend --lib backend::hyper -- --nocapture`
Expected: FAIL to compile (`HyperBackend`, `PhaseClock` missing).

- [ ] **Step 3: Implement**

Write `PhaseClock`, `HyperBackend`, `watchdog`, `write_error_close`, `recover_upgrade`, `find_io_error` as designed above; retrofit Task 3's `IoShared` to hold `Rc<PhaseClock>` (rename `saw_bytes` → `clock`; on nonzero read: `if clock.phase.get() == Phase::Idle { clock.set(Phase::Head); }`), and add the `phase: Rc<PhaseClock>` field to `Bridge` (set `Phase::Handler` at the top of `call`'s async block, `Phase::Idle` right before returning the response). Flip the alias in `src/backend/mod.rs`:

```rust
#[cfg(feature = "hyper-backend")]
pub(crate) type ActiveBackend = hyper::HyperBackend;
```

Iterate until Step 1's tests pass. Known likely adjustments (do whichever the compiler/tests demand — they are implementation details, not design changes): pinning of the serve future vs `drop(conn)` (use an `Option`-wrapped future or a scoped block so the future is dropped before writing the 408); `watchdog` borrowing `clock` vs cloning the `Rc`; `Body`/`HeaderVec` import paths.

- [ ] **Step 4: Run the whole library both ways**

Run: `cargo test --features hyper-backend --lib && cargo test --lib`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/backend
git commit -m "feat: hyper backend impl with phase watchdog for native deadline parity"
```

---

### Task 9: Conformance run, rejection-path audit, `BACKENDS.md`

**Files:**
- Create: `BACKENDS.md`
- Modify: `tests/rfc9112.rs`, `tests/dispatch.rs`, `tests/tls_dispatch.rs` — **only** where the audit justifies a `#[cfg_attr(feature = "hyper-backend", ignore = "...")]`.

**Interfaces:**
- Consumes: the complete hyper backend.
- Produces: green conformance suite under `--features hyper-backend` and `--features hyper-backend,tls`; a divergence table where every ignore has a row.

- [ ] **Step 1: Run the full suite under the hyper backend**

Run: `cargo test --features hyper-backend && cargo test --features hyper-backend,tls`
Expected: mostly green; collect every failure.

- [ ] **Step 2: Audit each failure one by one**

Decision rule from the spec, applied per test:
- **Adapter bug** (timing tests, body caps, keep-alive semantics, upgrade handoff — anything Tasks 3–8 claim to enforce): fix the adapter, not the test.
- **Same observable class, different detail** (e.g. native says `400 Bad Request` with empty body, hyper's 400 has a different reason phrase or a small body; both close): loosen nothing silently — if the test already asserts only status + close, it passes; if it asserts exact bytes hyper cannot produce, prefer relaxing the assertion to the observable class *if the class is what the test is really about*, with a comment.
- **Genuine mismatch that cannot be shimmed** (hyper rejects before the adapter sees the request — malformed request lines, bad chunk framing, oversized heads producing hyper's status choice instead of ours; hyper's eager 100-continue): `#[cfg_attr(feature = "hyper-backend", ignore = "see BACKENDS.md: <row anchor>")]` **plus** a BACKENDS.md row.

Expected audit hot spots (verify each empirically, do not assume):
- `bare_cr_400_close` / `bare_lf_400_close` / `obs_fold_400_close` / `whitespace_before_colon_400_close` / `bad_request_line_400_close` — hyper parses these itself; check its status/close behavior matches (likely 400 + close → shared test stays).
- `oversized_head_431_close` — hyper's `max_buf_size` overflow produces hyper's error (likely 431 in hyper 1.x; verify).
- `unsupported_transfer_coding_501_close`, `chunked_not_final_400_close`, `transfer_encoding_on_http_10_400_close`, content-length conflict tests — hyper may reject before `framing::decide` runs; check the status class.
- `expect_100_continue_interim_then_final` — hyper's eager send should still satisfy interim-then-final ordering.
- `forbidden_trailer_rejected` — flows through `HyperBodyIo`'s policy; should pass.
- `body_timeout_408_close`, `header_timeout_408_close`, `idle_timeout_closes_without_response` — watchdog territory; failures here are adapter bugs by definition.
- `tests/dispatch.rs` h2c and `tests/tls_dispatch.rs` — dispatch happens before the backend; should pass unchanged. A failure here means the `buffered` hand-off (Task 3) is broken — fix, never ignore.

- [ ] **Step 3: Write `BACKENDS.md`**

```markdown
# Backend divergences

The `hyper-backend` feature swaps the per-connection serving path from the
bespoke protocol stack to hyper's `conn::http1` behind the same public API.
Parity is strict: anything the adapter can enforce (limits, deadlines, body
caps, upgrades, keep-alive rules) is enforced identically, and the conformance
suite runs unchanged against both backends. This table is the exhaustive list
of what could not be shimmed. Every
`#[cfg_attr(feature = "hyper-backend", ignore)]` in the test suite must
reference a row here; a row without a strong justification is a bug.

| Behavior | Native | hyper backend | Why it cannot be shimmed |
|---|---|---|---|
| `Expect: 100-continue` interim response | Sent lazily on the handler's first body read; a handler that rejects without reading sends none | Sent eagerly by hyper when it parses `Expect` | hyper owns the transport during head processing; there is no pre-write hook to defer the interim response |
| Write-stall cutoff mid-response | `write_timeout` re-armed per flush | Covered by the connection-level watchdog at `idle_timeout` granularity | hyper exposes no per-flush boundary to re-arm against |
```

Extend the table with one row per audit finding from Step 2. Delete the write-stall row if Task 8's refinement (a `Write` phase using `write_timeout`) was implemented and the relevant test passes.

- [ ] **Step 4: Run everything, all four configurations**

Run:
```bash
cargo test
cargo test --features tls
cargo test --features hyper-backend
cargo test --features hyper-backend,tls
```
Expected: all green (ignored tests report as ignored, each with a BACKENDS.md pointer in its ignore string).

- [ ] **Step 5: Commit**

```bash
git add BACKENDS.md tests/
git commit -m "test: conformance suite green under hyper backend; document divergences"
```

---

### Task 10: CI matrix

**Files:**
- Create: `.github/workflows/ci.yml` (no CI exists in this repo yet)

**Interfaces:**
- Produces: explicit per-combo jobs. The combos are **explicitly listed, not `--all-features`**, so a feature rename cannot silently drop them from CI.

- [ ] **Step 1: Write the workflow**

```yaml
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:

env:
  CARGO_TERM_COLOR: always

jobs:
  test:
    name: test (${{ matrix.name }})
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        include:
          # Explicit rows, not --all-features: a combo that stops being listed
          # here stops being tested, visibly.
          - name: default
            flags: ""
          - name: tls
            flags: "--features tls"
          - name: hyper-backend
            flags: "--features hyper-backend"
          - name: hyper-backend-tls
            flags: "--features hyper-backend,tls"
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
        with:
          key: ${{ matrix.name }}
      - run: cargo test ${{ matrix.flags }}

  lint:
    name: fmt + clippy
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --check
      - run: cargo clippy --features hyper-backend,tls -- -D warnings
      - run: cargo clippy -- -D warnings

  bench-compiles:
    name: benches compile (both backends)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo bench --bench e2e -- --test
      - run: cargo bench --bench e2e --features hyper-backend -- --test
```

Note: this repo uses `edition.workspace = true` etc.; if it builds standalone without a workspace root, CI will surface that immediately — fix by whatever the repo's existing standalone-build story is (the repo builds locally today, so CI should match; do not change Cargo.toml for CI's sake).

- [ ] **Step 2: Validate locally**

Run: `cargo fmt --check && cargo clippy --features hyper-backend,tls -- -D warnings && cargo bench --bench e2e --features hyper-backend -- --test`
Expected: all clean. Fix any clippy findings in the new code now.

- [ ] **Step 3: Commit**

```bash
git add .github
git commit -m "ci: explicit feature-combo matrix including hyper-backend rows"
```

---

### Task 11: A/B harness verification and final sweep

**Files:**
- Modify: none expected (verification task; touch only what the checks flag)

- [ ] **Step 1: Confirm the A/B harness measures what it claims**

Run: `cargo bench --bench e2e -- --test && cargo bench --bench e2e --features hyper-backend -- --test`
Expected: both run. Sanity-check the hyper path is actually exercised: temporarily `panic!("hyper")` inside `HyperBackend::serve`, re-run the second command, confirm it panics, revert. (This guards against the alias silently resolving to native.)

- [ ] **Step 2: Confirm `parse`/`write` benches and differential fuzz still target the bespoke modules**

Run: `cargo bench --bench parse -- --test && cargo bench --bench write -- --test`
Expected: unaffected. `fuzz/` is its own workspace and untouched by this feature.

- [ ] **Step 3: Full final verification**

Run:
```bash
cargo fmt --check
cargo test
cargo test --features tls
cargo test --features hyper-backend
cargo test --features hyper-backend,tls
cargo clippy --features hyper-backend,tls -- -D warnings
cargo doc --no-deps --features hyper-backend
```
Expected: all green. `missing_docs` is a warn-level lint on this crate — the new public item (`serve_connection`) must be documented (it is, per Task 2).

- [ ] **Step 4: Update CHANGELOG.md**

Add under an Unreleased heading (match the file's existing format):

```markdown
- `hyper-backend` cargo feature: serve connections through hyper's
  `conn::http1` behind the same public API. Compile-time swap; strict parity;
  divergences documented in `BACKENDS.md`. Adds `serve_connection`, the
  backend-selected per-connection entry point.
```

- [ ] **Step 5: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog entry for the hyper-backend feature"
```

---

## Self-Review (performed while writing)

- **Spec coverage:** compile-time swap behind same types (T2, T8 alias); connection-layer-only swap (seam at `serve_h1`, accept loop/TLS/h2c untouched); strict parity (T4 caps, T5 trailers, T7 checks, T8 watchdog, T9 audit); internal Backend trait (T2); native impl unchanged behind trait (T2); hyper dep without hyper-util, hand-rolled IO (T1, T3); service bridge with header caps (T7); `HyperBodyIo` with no-op `poll_send_continue` + divergence recorded (T5, T9); body-out with framing-choice parity tests (T6, T8 test); upgrades via `hyper::upgrade` + `Parts.read_buf` (T8); `max_head_bytes → max_buf_size`, `header_read_timeout` disabled, `half_close`/`pipeline_flush` (T8); error mapping (T8 `find_io_error`); conformance under both combos in CI, explicit rows (T10); rejection-path audit with ignore+row rule (T9); timing via ConnDeadline around the serve future (T8 watchdog); benches as A/B harness (T2 Step 5, T11); BACKENDS.md with 100-continue row (T9). All five deliverables have tasks.
- **Known deliberate deviations from the spec's illustrative sketch:** the trait returns `io::Result<Option<Upgraded>>` (matching `Connection::serve`, which is what `serve_h1` actually consumes) rather than the sketch's `Result<Disposition, ServeError>`; a public `serve_connection` was added because the e2e bench is an external target that cannot see a `pub(crate)` trait — without it the "A/B via `cargo bench --features hyper-backend`" deliverable is unimplementable.
- **Type consistency:** `Body::from_backend(kind, io, needs_continue, max_body_bytes, fully_read, trailers_slot)` — signature identical in T4 (definition) and T7 (call). `HyperBodyIo::new(body, trailers_slot)` consistent T5/T7. `HyperIo::new(io, buffered, clock)` — T3 defines with `Rc<Cell<bool>>`, T8 retrofits to `Rc<PhaseClock>` and says to update T3's tests; executors doing tasks out of order: T8's version wins.
- **Placeholder scan:** T3 Step 1 contains an explicitly-deleted placeholder helper with instructions to remove it; no TBDs remain. T8 Step 3 names the expected compiler-driven adjustments rather than hiding them.
