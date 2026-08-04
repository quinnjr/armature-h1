# Backend divergences

The `hyper-backend` feature swaps the per-connection serving path from the
bespoke protocol stack to hyper's `conn::http1` behind the same public API.
Parity is strict: anything the adapter can enforce (limits, deadlines, body
caps, upgrades, keep-alive rules) is enforced identically, and the conformance
suite runs unchanged against both backends. This table is the exhaustive list
of what could not be shimmed. Every
`#[cfg_attr(feature = "hyper-backend", ignore)]` in the test suite must
reference a row here; a row without a strong justification is a bug.

**Verified against hyper 1.10–1.11 and http 1.4.** `Cargo.toml` floors
`hyper` at `"1.10"`; this re-review re-checked every citation below against
both `hyper` 1.10.1 and 1.11.0. Every "why it cannot be shimmed" below cites
the behaviour of a specific upstream version, and several cite specific source
files. A minor-version bump of either crate can add the hook a row says does
not exist, or move the code a row points at: recheck the citations — and
re-run the ignored conformance tests unignored — whenever either dependency's
minor version moves. The `Test` workflow's `ignored-divergence probes` step
(`.github/workflows/test.yml`, running on the `hyper-backend` and
`hyper-backend-tls` matrix rows) re-runs `cargo test --test rfc9112 --
--ignored` on every push and pull request as an informational,
continue-on-error check: a pass there means hyper gained a hook one of these
rows assumed did not exist, which is a prompt to shrink this table, not a CI
failure.

The shape of every unshimmable case is the same: hyper owns the transport from
the first byte of a head until it hands a parsed `http::Request` to the service
bridge. Anything it decides in that window — how permissively it parses, which
status it picks for a head it rejects, what it drops off a target — it also
*acts on*, writing its own response and ending the connection, before any
adapter code runs. There is no pre-parse hook and no error-response hook; the
only alternative would be to re-parse the byte stream in the IO adapter ahead
of hyper, which is the bespoke stack again.

## Parsing and rejection

| Behavior | Native | hyper backend | Why it cannot be shimmed |
|---|---|---|---|
| Bare LF as a head line terminator (`GET / HTTP/1.1\nHost: a\r\n\r\n`) | `400` + close — or, when the LF-terminated head is the last thing on the stream, a silent close: the native scanner only recognises CRLFCRLF, so such a head is still *incomplete* at EOF and no response is owed. Leniency that differs from a peer's is the request-smuggling vector (RFC 9112 section 2.2 permits accepting it; this crate declines) | Accepted, request served normally (`200`) | httparse treats a bare LF as a line terminator and hyper 1.10 exposes no server-side `ParserConfig` knob for it. The head bytes are consumed inside hyper's parser; by the time the bridge has a `Request` the terminator bytes are gone. |
| Fragment in the request target (`GET /a#frag`) | `400` + close: the fragment is not part of the request target (RFC 9110 section 7.1) and routing on it means routing on bytes an upstream hop would have stripped | Fragment silently truncated, request served on `/a` (`200`) | `http::uri::PathAndQuery::from_shared` truncates at the `#` before hyper builds the `Request` (`http-1.4.2/src/uri/path.rs`). The bridge only ever sees the stripped target, so the condition is not observable to it. |
| Status for an unsupported HTTP version (`HTTP/1.2`) | `505 HTTP Version Not Supported` + close | `400 Bad Request` + close | hyper maps `Parse::Version` to `StatusCode::BAD_REQUEST` in `Server::on_error` (`hyper-1.10.1/src/proto/h1/role.rs`) and writes that response itself. The service is never called, and `on_error`'s status table is not configurable. |
| Status for an unsupported transfer coding (`Transfer-Encoding: gzip`) | `501 Not Implemented` + close, from `framing::decide` | `400 Bad Request` + close | hyper rejects `is_te && !is_te_chunked` during head parsing with `Parse::transfer_encoding_invalid()`, which `on_error` maps to `400`. `framing::decide` never runs, because the bridge is never called. |
| Status for an unparseable h2c preface without detection (`PRI * HTTP/2.0\r\n...`, h2c prior-knowledge detection off) | `505 HTTP Version Not Supported` + close | Connection closed with zero response bytes | hyper's h1 parser abandons the connection on this input before any error-response hook the adapter could use; there is nothing to convert into a status because hyper never produces one. |

The rejection *class* matches for the bare-LF, HTTP-version, and
transfer-coding rows: the request is refused and the connection closes, and
only the status differs, because it is hyper's to choose. The fragment row
diverges further — the request is *served*, not refused. The h2c row diverges
the other way: no response is written at all, because hyper never gets far
enough to have one to write.

## Limits

| Behavior | Native | hyper backend | Why it cannot be shimmed |
|---|---|---|---|
| `max_head_bytes` below 8 KiB | Enforced on the wire: the read stops and `431` goes out as soon as the cap is crossed | Enforced by the cumulative check in `bridge::convert_head`, after the whole head is buffered | `http1::Builder::max_buf_size` panics below an 8 KiB floor, so a smaller configured cap cannot be pushed down to hyper's read buffer. A head between the configured cap and 8 KiB is still rejected with `431`, just after being read rather than during. |
| `max_head_bytes` cumulative count, at or above 8 KiB | Byte-exact count of the wire | The bridge counts the request line exactly (method, SP, target, SP, `HTTP/1.x`, CRLF) plus, per header, `name: value\r\n`, plus the final CRLF | httparse trims optional whitespace (OWS) around field values before the bridge ever sees them, so bytes of OWS that were on the wire are not in the reconstructed count. The residual divergence from native's wire count is bounded by that trimmed OWS, itself bounded by hyper's `max_buf_size`. |

## Timing

The native loop's four deadlines (`idle`, `header`, `body`, `write`) have no
equivalent in hyper's connection future — `header_read_timeout` is the only one
it offers and it covers a different window — so they are produced from outside
by a phase clock the IO adapter and the bridge advance, plus a watchdog that
races hyper's future. That reconstruction is exact for `idle`, `header` and
`body`; the rows below are where it is not.

| Behavior | Native | hyper backend | Why it cannot be shimmed |
|---|---|---|---|
| `write_timeout` granularity | Re-armed per flush, i.e. once per body chunk | Re-armed on every write that reaches the transport | hyper exposes no per-flush boundary to re-arm against; the IO adapter sees writes, not flush boundaries. Finer-grained, so a progressing stream never expires under either. |
| A pipelined head arriving while a response is still being written | Arms `header_timeout` from the head's first byte, so a stalled second head gets a `408` | `note_read` only re-phases from `Phase::Idle` (see its doc), so the partial head sits buffered without starting `header_timeout`. Once the response finishes and the connection returns to `Phase::Idle`, the already-buffered bytes produce no further read to re-arm anything, so the wait is bounded by `idle_timeout` with a silent close instead | Re-phasing `Write -> Head` would put `header_timeout` over the remainder of a perfectly healthy response stream and kill it. This is the safe direction — no unbounded wait, just a silent close instead of a `408` — not the native reaction. A head that arrives *after* a response completes is unaffected: the connection is back in `Idle`, so `Idle -> Head` fires and `header_timeout` applies exactly as native. Pinned by `a_partial_head_arriving_mid_write_is_bounded_by_the_idle_deadline` in `src/backend/hyper/mod.rs`. |
| `write_timeout` expiry, and any expiry after bytes of the *final* response have gone out | Writes nothing, closes | Writes nothing, closes | Parity, listed because it is a deliberate suppression rather than an accident: splicing a bare `408` into a half-written response would corrupt framing the peer is already parsing. The suppression is scoped to bytes written in the write phase, so hyper's eager interim `100 Continue` (written in the handler phase) does not trigger it — an interim response is defined to be followed by a final one, and native writes the `408` after one too. |

A note on the `HEAD` keep-alive wait, which an earlier revision listed here as
unshimmable: hyper forces `Encoder::length(0)` for a `HEAD` response and never
polls the response body, so the body's own end-of-stream — half of the only
externally observable "response finished" signal — cannot fire. The bridge
knows the method before `head` moves into the `Request` and signals end-of-stream
on hyper's behalf for `HEAD`, so the connection returns to the idle phase and
the wait is bounded by `idle_timeout` exactly as native. The shim is exact
rather than a guess because hyper's zero-length encoder for `HEAD` is a
guarantee, not a heuristic. Any *other* case where hyper finishes a message
without draining its body would need the same treatment.

## Responses

| Behavior | Native | hyper backend | Why it cannot be shimmed |
|---|---|---|---|
| `Expect: 100-continue` interim response | Sent lazily on the handler's first body read; a handler that rejects without reading sends none | Sent eagerly by hyper when it parses `Expect` | hyper owns the transport during head processing; there is no pre-write hook to defer the interim response. |
| A handler status code outside `http`'s accepted range (`< 100` or `> 999`) | The raw `u16` is written into the status line verbatim; the writer never inspects it | `bridge::convert_response` falls back to `500 Internal Server Error` | hyper's response type is `http::Response`, whose status is a `StatusCode`; there is no way to hand hyper an arbitrary `u16`. `StatusCode::from_u16` is the only constructor, and it rejects anything outside `100..=999`. A three-digit status in that range round-trips unchanged, so this is only reachable from a handler that constructs an out-of-range status — which is a handler bug under either backend, degraded differently. |
| A handler `Connection` field on a response that must close (unread request body) | The handler's value is written verbatim; the loop closes anyway, because reuse is decided from `body_consumed`, not from the field | The handler's value is dropped and `Connection: close` is written | hyper decides reuse *from the response's `Connection` field*. Honouring a handler `keep-alive` there would reuse a connection the native loop closes — hyper drains a small unread body and serves the next pipelined request. Overriding the field is the only lever the bridge has over hyper's accounting; the close behaviour matches, only the emitted field differs. |
| A 2xx response to `CONNECT` | An ordinary response: `content-length: 0` framing is written, the connection continues under the normal keep-alive rules, and a pipelined request behind it is served | hyper hijacks it as a tunnel-establishment upgrade: no framing field is written, the connection is not reused, and — since the bridge's upgrade slot only fills on a `101` — the tunnel transport hyper hands back is dropped | hyper's role/encoder for `CONNECT` decides the tunnel semantics from the status class before the response ever reaches the bridge; there is no hook to observe or override that decision, only its aftermath. |
| A handler that retains the request `Body` past a `101` response | The upgrade is forfeited and the connection closes: the transport is shared through an `Rc`, so `into_parts` finds an outstanding handle and returns `None` rather than handing out a socket a body still reads from | The transport is handed back and the upgrade proceeds normally | hyper's request body is `Incoming`, a channel endpoint rather than a borrow of the socket, so a retained body holds nothing `into_parts` could detect. There is no live-handle check to fail. Neither backend panics or hangs; native is the stricter of the two, and the divergence is only reachable from a handler that keeps its `Body` alive across the response. |
| A handler header or trailer value containing a control byte other than CR, LF, or NUL (e.g. `0x01`) | `write::valid_field_value` permits it and writes it verbatim; only CR, LF, and NUL are rejected | The field is silently dropped | `hyper::http::HeaderValue::from_maybe_shared` rejects *all* control bytes, not just CR/LF/NUL, and the bridge drops any field that fails to construct (see the `Server`/`Connection` "only a field that survived validation" note in the bridge source). There is no lower-level constructor that accepts the wider set native permits. |
