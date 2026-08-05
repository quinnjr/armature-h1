# Changelog — `armature-h1`

All notable changes to this crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-08-04

### Added

- `Request::peer`: the address of the socket the request arrived on, stamped
  onto every request `Server` serves. Previously the accept loop discarded it,
  so a consumer had no trustworthy client identifier at all — every address in
  a header is set by the caller, so rate limiting, deduplication and audit
  logging keyed on one are keyed on a value the client chooses.
- `UpgradeConsumer`, `CloseUpgrade`, and `Server::serve_with`: a pluggable
  destination for a connection a handler upgrades with status 101, matching
  the existing `H2Fallback` hook for HTTP/2. Without it `Server` closed every
  upgraded connection, so a WebSocket handshake could be completed and then
  silently dropped; driving `Connection` directly was the only way to get the
  socket. `serve` and `serve_with_fallback` still close, via `CloseUpgrade`.
- `Connection::with_peer`, for a caller driving `Connection` itself.
- `Upgraded::peer`, carrying the same address to an upgrade consumer. A
  consumer owns the connection for the rest of its life and has no `Request`
  left to read the address off, so without this the address a WebSocket
  session is attributed to would again be one the client chose.

### Changed

- **Breaking**: `Request` has a new public field, `peer`, so a struct-literal
  construction outside this crate needs it. `Request` is normally received
  from the crate rather than built.
- **Breaking**: `serve_connection` takes a sixth argument, `peer:
  Option<SocketAddr>`. Pass `None` for a transport with no address to report;
  it is not a "trusted source" signal, it means unknown.
- **Breaking**: `H2Fallback::handle` takes a fourth argument, `peer:
  Option<SocketAddr>`. A fallback serves whole connections rather than
  requests, so it never sees a `Request` and had no way at all to learn the
  client address — leaving every request it served attributable only to
  caller-chosen headers, which is the same hole `Request::peer` closes on the
  HTTP/1 path.
- An upgrade handoff is now forfeited when the request body was not read to
  its end, not only when the body outlives the response. Bytes of an unread
  body are still on the wire, and `Upgraded::buffered` is documented as the
  peer's *first post-upgrade frames* — handing body bytes over under that
  contract is the smuggling shape aimed at the upgrade consumer instead of at
  the parser. A handler upgrading a request that carried a body must now drain
  it; previously such a connection was handed off. Native backend only; see
  `BACKENDS.md`.

### Fixed

- A shutdown signalled before `serve` began was silently discarded.
  `ServerHandle::shutdown` used `watch::Sender::send`, which fails *and leaves
  the value unchanged* when no receiver exists — and the window between `bind`
  and `serve` is exactly when none does. `send_replace` stores the flag
  regardless, so the workers observe it the moment they subscribe.
- A shutdown landing partway through the worker-spawn loop stopped only the
  workers that had already subscribed, while the rest went on accepting.
  `serve` then blocked in `join` forever with `is_shutting_down()` reporting
  true. Each worker now re-reads the flag at the top of every accept
  iteration rather than trusting `changed()` alone, and the subscription is
  taken once before the spawn loop so no worker can start after the signal
  without seeing it.

## [0.2.0] - 2026-08-04

### Added

- `hyper-backend` cargo feature: serve connections through hyper's
  `conn::http1` behind the same public API. Compile-time swap; strict parity;
  divergences documented in `BACKENDS.md`. Adds `serve_connection`, the
  backend-selected per-connection entry point.
- `Head::new`, which caches the target's `?` split so `path()`/`query()` no
  longer re-scan on every call. **Breaking:** `Head` gained a private field,
  so literal construction outside the crate is no longer possible — construct
  through `Head::new` (or `parse_head`, as before).
- `backend_differential` fuzz target: drives the same bytes through the native
  `Connection` loop and the hyper backend in one process and cross-checks the
  parity claims BACKENDS.md leaves intact (matching 200-response payloads;
  hyper never serving more requests than the stricter native parser accepts on
  CRLF-framed input).

### Changed

- Set `autobenches = false` so the `parse`, `write` and `e2e` bench targets are
  governed solely by their `[[bench]]` entries, matching the convention the rest
  of the framework's benchmark-owning crates now follow. The comment above those
  entries had claimed this was already the case; it was not.
- Streamed responses on the default (native) backend now coalesce before
  flushing: chunks that become ready within the same poll are written together
  as one flush, while an idle stream still flushes immediately rather than
  waiting for more data. This reduces syscalls on a handler that produces
  several small chunks back-to-back without changing what is observed on the
  wire.
- `Connection::serve` returns `Ok(None)` instead of panicking when a handler
  retains the request `Body` past a `101` response: the upgrade is forfeited
  and the connection is closed cleanly rather than the worker aborting.
- **`Head`'s `target` field is sealed behind accessors.** `Head::target` is no
  longer a public field; use `Head::target()` (and the existing `path()`/
  `query()`) instead. **Breaking**, alongside the `Head::new`-private-field
  change above — both land in the same 0.x minor per this family's policy that
  each 0.x minor is the breaking unit.

### Fixed

- `Config::limits()` silently clamps `max_headers` above
  `Limits::MAX_HEADERS_CEILING` rather than only warning: a caller that reads
  the field back after setting it can no longer observe a value the parser's
  fixed scratch array could never have served in the first place. The
  clamp-with-`tracing::warn!` behavior is unchanged; only the fact that the
  public field's read-back now always matches what will actually be enforced
  is new.
- Duplicate framing-relevant header fields (`Content-Length`,
  `Transfer-Encoding`) supplied more than once by a handler are now dropped
  down to the single value the writer actually frames with, instead of being
  written verbatim alongside it.
- A `304` response's `Content-Length`, when a handler supplies one, is now
  always written in decimal — a non-decimal value the handler happened to
  hand in was previously passed through unchanged.
- The `hyper-backend` bridge now matches native on `204` and `Content-Length`
  framing parity: a `204` drops any handler-attached body and framing field
  identically on both backends, closing a divergence between them.
- **Chunked line-length limits were bypassable by packetization.** The bound on
  a chunk-size line and on a trailer line was enforced only while the decoder
  was still waiting for the terminating LF. A peer that delivered an over-long
  line in a single segment arrived with the LF already present, so the length
  was never checked and the line was accepted whole — meaning the limit
  constrained only senders that dribbled bytes, which is the opposite of the
  threat. Both limits now apply however the input is split. The trailer path
  additionally reported the wrong error depending on arrival: `MissingCrlf`
  when dribbled, `TrailerTooLarge` when delivered at once; it is now
  `TrailerTooLarge` either way.

- **Two remaining response-desync vectors closed.** A 204, 304 or 1xx response
  now drops any body a handler attached: the writer already suppresses their
  framing field, so writing body bytes anyway left them unaccounted for and a
  keep-alive peer read them as the next response. And a handler
  `Content-Length` that disagrees with the body it framed is dropped in favour
  of the true length, rather than trusted verbatim — response splitting reached
  through arithmetic instead of through a CRLF.

- **Request smuggling on the fixed-length body path.** `take_buffered` is
  bounded by what the body is owed, but `fill` is not — it hands over the whole
  socket read. When a `Content-Length` body straddled two reads and the second
  carried the front of a pipelined request, those bytes were dropped while the
  body still reported itself fully read, so keep-alive survived and the peer's
  response queue shifted by one. The chunked decoder already returned its
  residue; the fixed-length arm now does too.

- Every deadline in `Limits` is now enforced against something that polls it.
  `body_timeout` was armed and never awaited, so it had no effect at all — and
  its test passed by reaching the idle timeout instead. `write_timeout` now
  covers every flush of a streamed body rather than only the final write.
- Handler-supplied header and trailer values containing CR, LF or NUL are
  rejected, and `HeaderId::Other` names are re-validated as tokens. Response
  splitting is request smuggling run backwards, and the framing checks now
  consult what was actually emitted so a dropped `Content-Length` cannot leave a
  response undelimited.
- `Transfer-Encoding` on an HTTP/1.0 request is rejected (the TE-downgrade
  smuggling vector), and `#` is rejected in the request target — RFC 9110 §7.1
  puts a fragment outside it, and no RFC 9112 §3.2 form admits one.
- `Response::upgrade`, `BufPool` and `Limits::max_pipeline_depth` are removed.
  Each was public API that did nothing: the callback was only ever dropped
  unrun, the pool had no call sites while the README documented its
  observability, and the pipeline cap was never read.

### Added — `0.1.0` (new crate)

A zero-allocation HTTP/1.1 server layer. The steady-state request path — parse,
dispatch, write — performs no heap allocations at all, which
`tests/alloc_regression.rs` asserts against a budget of zero per request.

Types:

- `Method` — well-known methods are unit variants, so dispatch is a discriminant
  comparison; an unrecognized token is carried as `Method::Other(ByteStr)`.
  Includes `QUERY` (draft-ietf-httpbis-safe-method-w-body). `From<&str>`,
  `From<String>`, `PartialEq<str>`, `PartialEq<&str>` and `Display` let it stand
  in for the `String` it replaces at a call site.
- `ByteStr` — an immutable UTF-8 string backed by `Bytes`, so a request target or
  header value can be a refcounted slice of the connection's read buffer rather
  than a fresh `String`. Derefs to `str`; `Hash` delegates to `str` so the
  `Borrow<str>` impl is sound for map lookups.
- `HeaderId` — well-known header names as an enum, with `header::intern` mapping
  a name to one (lowercasing an unrecognized name so lookups stay
  case-insensitive), and `HeaderVec` keeping 16 headers inline.
- `Version`, `Limits`, `ConnConfig`, `Connection`, `Request`, `Response`,
  `DateCache`.

Framing follows RFC 9112 §6: `framing::decide` resolves `Transfer-Encoding` and
`Content-Length` together and rejects the combinations that enable request
smuggling, including `Transfer-Encoding` on an HTTP/1.0 request (the TE-downgrade
vector). Request targets are validated against RFC 3986's character set and
RFC 9112 §3.2's four target forms; a `#` fragment is rejected, since RFC 9110
§7.1 puts it outside the request target.

The reverse direction is covered too: a handler-supplied header or trailer whose
value contains CR, LF, or NUL — or whose `HeaderId::Other` name is not an RFC
9110 token — is dropped rather than written, and the writer frames the response
as if it had never been supplied. Response splitting is request smuggling run
backwards.

Every deadline in `Limits` is enforced against something that polls it: the
header and idle deadlines against the head read, the body deadline against the
handler call itself, and the write deadline against *every* flush of a streamed
response rather than only the last.

Protocol upgrades leave through `Connection::serve`'s `Ok(Some(Upgraded))`.
`Server` has no upgrade-consumer hook and closes such connections; a service that
upgrades must drive `Connection` itself.

`Connection` is `!Send` by design — it holds `Rc`s — and is driven on a
thread-per-core runtime with `SO_REUSEPORT`.

The crate keeps `#![forbid(unsafe_code)]`; the counting allocator the regression
test needs lives in the test target, not the library.

Fuzzing: three `cargo-fuzz` targets (`parse_head`, `chunked`,
`framing_differential`). The differential target compares framing decisions
against hyper and panics only when both implementations accept a message but
disagree on its body length — which is what a smuggling primitive looks like.

## [0.1.1] - 2026-08-04

### Fixed

- Requirements on sibling armature crates name a minor instead of `0`. Under
  Cargo's 0.x rules `version = "0"` matches any release ever made, and edition
  2024 selects the MSRV-aware resolver, so a consumer declaring an older
  `rust-version` was handed the oldest version satisfying it — resolving
  `armature-core = "0"` on Rust 1.89 produced `armature-core 0.2.3` while an
  explicit `armature-core = "0.8"` elsewhere in the same graph pulled 0.8.2.
  Two copies of core, and a build failing on symbols the older one lacks. Each
  0.x minor in this family is a breaking change, so the requirement now names
  one. No API change.
