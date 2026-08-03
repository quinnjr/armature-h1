# armature-h1 hyper backend

**Date:** 2026-08-03
**Branch:** `feature/hyper-backend`
**Status:** Approved design

## Goal

A `hyper-backend` cargo feature that serves connections through hyper's
`conn::http1` instead of the bespoke protocol stack, behind the same public
API. It is a full-fidelity backend serving three purposes at once: a
production escape hatch for downstream users who hit a bug or gap in the
bespoke stack, an A/B benchmarking harness that isolates the protocol
implementation, and a correctness cross-check extending the existing
differential fuzzing.

## Decisions

| Question | Decision |
|---|---|
| Selection mechanism | **Compile-time swap.** Enabling `hyper-backend` replaces the serving path behind the same types. Non-additive by design: whoever enables the feature decides for the whole binary. |
| Swap depth | **Connection layer only.** The thread-per-core accept loop, core affinity, socket2 tuning, TLS dispatch, h2c preface sniffing, and graceful shutdown stay ours. Only per-connection serving swaps. |
| Divergence policy | **Strict parity.** Anything the adapter can enforce (limits, deadlines, body caps, upgrades) is enforced identically. The conformance suite runs unchanged against both backends. |
| Structure | **Internal `Backend` trait** (approach B): an explicit seam with two impls, feature-selected. |

## Architecture

A private `backend` module defines the seam:

```rust
pub(crate) trait Backend {
    async fn serve<S: H1Service>(
        transport: impl Transport,
        service: Rc<S>,
        config: &ConnConfig,
        deadline: ConnDeadline,
    ) -> Result<Disposition, ServeError>;
}
```

- `backend::native` — the existing `Connection` loop moved behind the trait
  unchanged. Compiled when `hyper-backend` is off.
- `backend::hyper` — drives
  `hyper::server::conn::http1::Builder::serve_connection_with_upgrades`.
  Compiled when `hyper-backend` is on.

A cfg-gated alias `pub(crate) type ActiveBackend = ...` selects one;
`server.rs` calls `ActiveBackend::serve(...)` and is otherwise untouched.

hyper 1.x http1 serves `!Send` services, so the thread-per-core model and the
`Rc`/`RefCell` handler contract survive.

The bespoke protocol modules (`parse`, `framing`, `chunked`, `write`, `conn`)
stay compiled and exported under both features — the feature changes which
backend the server *uses*, not the API — so downstream code and armature-core
compile identically either way.

**Dependencies:** `hyper = { version = "1", features = ["server", "http1"],
optional = true }`. No hyper-util: the `tokio::io` ↔ `hyper::rt` IO adapter
(~50 lines) is hand-rolled to keep the dependency tree tight.

## Components (`backend::hyper`)

### 1. Service bridge

A `hyper::service::Service<http::Request<Incoming>>` impl wrapping
`Rc<S: H1Service>`. Per request:

1. Convert the head: method via `Method` from the hyper method's bytes,
   target as `ByteStr`, headers copied into our `HeaderVec` with `HeaderId`
   interning.
2. Apply parity checks hyper does not enforce — header-count cap and
   cumulative header-bytes cap from `Limits` — rejecting with the same status
   the bespoke framing layer uses.
3. Build our `Request` with a hyper-backed `Body`, await the handler, convert
   our `Response` back.

### 2. Body-in: `HyperBodyIo`

Our `Body` abstracts its transport behind the `BodyIo` trait; `HyperBodyIo`
implements it over `Incoming`:

- `poll_fill` polls `Incoming` frames and yields data `Bytes`.
- `take_buffered` / `push_back` operate on a small local stash (hyper owns
  real buffering).
- `poll_send_continue` is a no-op: hyper sends `100 Continue` itself when it
  parses `Expect`. **Known divergence:** the bespoke stack sends the interim
  response lazily on first body read; hyper sends it eagerly. Recorded in
  BACKENDS.md.

Body caps (`Body::collect`) live in our `Body` and hold unchanged.

### 3. Body-out

`ResponseBody` (buffered or streamed) wrapped in an `http_body::Body` impl
yielding data frames; no trailers, same as native. Content-Length vs chunked
selection is delegated to hyper; tests assert hyper picks the same framing our
`write` module would.

### 4. Upgrades and config mapping

- Upgrades: `hyper::upgrade::on`, then wrap the upgraded IO into our
  `Upgraded { io, buffered }` — hyper's `Parts` supplies the read-ahead bytes
  that satisfy the `buffered` contract.
- `Limits::max_head_bytes` → `Builder::max_buf_size`.
- Keep-alive and header-read timing driven by our `ConnDeadline` wrapped
  around the serve future; hyper's `header_read_timeout` is disabled so timing
  policy stays ours.
- `half_close` / `pipeline_flush` set to match native semantics.

### Error mapping

`serve_connection` errors map into the same `Disposition` / error accounting
the native loop reports, so server-level logging and shutdown behave
identically.

## Testing and CI

- **Conformance:** the existing integration suite runs unchanged under
  `--features hyper-backend`. CI adds explicit jobs for the combos
  `hyper-backend` and `hyper-backend,tls` (explicitly listed, not
  `--all-features`, so they cannot silently stop running).
- **Rejection-path audit:** tests asserting rejection behavior hyper handles
  before the adapter sees the request (malformed request lines, bad chunk
  framing, oversized heads) are audited one-by-one. Same observable class →
  test stays shared. Genuine mismatch →
  `#[cfg_attr(feature = "hyper-backend", ignore = "...")]` **plus** a row in
  `BACKENDS.md` stating native behavior, hyper behavior, and why it cannot be
  shimmed. An empty ignore list is the goal; every entry needs justification.
- **Timing tests:** use `tokio::time::pause`; `ConnDeadline` wraps the hyper
  serve future, so failures there are adapter bugs, not divergences.
- **Benches:** `cargo bench` vs `cargo bench --features hyper-backend` is the
  A/B harness via the existing `e2e` bench. `parse`/`write` benches always
  measure the bespoke modules.

## Deliverables

1. `backend` module with `native` and `hyper` impls, feature-gated.
2. `hyper-backend` feature in Cargo.toml with optional `hyper` dep and
   hand-rolled IO adapter.
3. `BACKENDS.md` divergence table (100-continue laziness entry at minimum).
4. CI matrix rows for `hyper-backend` and `hyper-backend,tls`.
5. Conformance suite green under both backends, ignores justified.

## Out of scope

- Runtime backend selection.
- hyper http2 — h2c fallback still routes to `H2Fallback` exactly as today.
- Trailer support.
- Any changes to armature-core.
