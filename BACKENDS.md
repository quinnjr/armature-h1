# Backend divergences

The `hyper-backend` feature swaps the per-connection serving path from the
bespoke protocol stack to hyper's `conn::http1` behind the same public API.
Parity is strict: anything the adapter can enforce (limits, deadlines, body
caps, upgrades, keep-alive rules) is enforced identically, and the conformance
suite runs unchanged against both backends. This table is the exhaustive list
of what could not be shimmed. Every
`#[cfg_attr(feature = "hyper-backend", ignore)]` in the test suite must
reference a row here; a row without a strong justification is a bug.

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
| Bare LF as a head line terminator (`GET / HTTP/1.1\nHost: a\r\n\r\n`) | `400` + close: leniency that differs from a peer's is the request-smuggling vector (RFC 9112 section 2.2 permits accepting it; this crate declines) | Accepted, request served normally (`200`) | httparse treats a bare LF as a line terminator and hyper 1.10 exposes no server-side `ParserConfig` knob for it. The head bytes are consumed inside hyper's parser; by the time the bridge has a `Request` the terminator bytes are gone. |
| Fragment in the request target (`GET /a#frag`) | `400` + close: the fragment is not part of the request target (RFC 9110 section 7.1) and routing on it means routing on bytes an upstream hop would have stripped | Fragment silently truncated, request served on `/a` (`200`) | `http::uri::PathAndQuery::from_shared` truncates at the `#` before hyper builds the `Request` (`http-1.4.2/src/uri/path.rs`). The bridge only ever sees the stripped target, so the condition is not observable to it. |
| Status for an unsupported HTTP version (`HTTP/1.2`) | `505 HTTP Version Not Supported` + close | `400 Bad Request` + close | hyper maps `Parse::Version` to `StatusCode::BAD_REQUEST` in `Server::on_error` (`hyper-1.10.1/src/proto/h1/role.rs`) and writes that response itself. The service is never called, and `on_error`'s status table is not configurable. |
| Status for an unsupported transfer coding (`Transfer-Encoding: gzip`) | `501 Not Implemented` + close, from `framing::decide` | `400 Bad Request` + close | hyper rejects `is_te && !is_te_chunked` during head parsing with `Parse::transfer_encoding_invalid()`, which `on_error` maps to `400`. `framing::decide` never runs, because the bridge is never called. |

The rejection *class* matches in every case above except the fragment: the
request is refused and the connection closes. Only the status differs, and only
because it is hyper's to choose.

## Limits

| Behavior | Native | hyper backend | Why it cannot be shimmed |
|---|---|---|---|
| `max_head_bytes` below 8 KiB | Enforced on the wire: the read stops and `431` goes out as soon as the cap is crossed | Enforced only by the cumulative check in `bridge::convert_head`, after the whole head is buffered | `http1::Builder::max_buf_size` panics below an 8 KiB floor, so a smaller configured cap cannot be pushed down to hyper's read buffer. A head between the configured cap and 8 KiB is still rejected with `431`, just after being read rather than during. |

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
| Keep-alive wait after a `HEAD` response | Bounded by `idle_timeout` (75s default) | Bounded by `write_timeout` (30s default): the connection never leaves the write phase | hyper forces `Encoder::length(0)` for `HEAD` and never polls the response body, so the body's end-of-stream — half of the only externally observable "response finished" signal — never fires. The wait is still bounded, is *shorter* than native's, and the close is silent under both. Same shape for any future case where hyper finishes a message without draining its body. |
| A pipelined head arriving while a response is still being written | Arms `header_timeout` from the head's first byte | Leaves the phase at `Write`, so that head is bounded by `write_timeout` | Re-phasing `Write -> Head` would put `header_timeout` over the remainder of a perfectly healthy response stream and kill it. A head that arrives *after* a response completes is unaffected: the connection is back in `Idle`, so `Idle -> Head` fires and `header_timeout` applies exactly as native. |
| `write_timeout` expiry, and any expiry after bytes have gone out for the current response | Writes nothing, closes | Writes nothing, closes | Parity, listed because it is a deliberate suppression rather than an accident: splicing a bare `408` into a half-written body would corrupt framing the peer is already parsing. |

## Responses

| Behavior | Native | hyper backend | Why it cannot be shimmed |
|---|---|---|---|
| `Expect: 100-continue` interim response | Sent lazily on the handler's first body read; a handler that rejects without reading sends none | Sent eagerly by hyper when it parses `Expect` | hyper owns the transport during head processing; there is no pre-write hook to defer the interim response. |
| A handler `Connection` field on a response that must close (unread request body) | The handler's value is written verbatim; the loop closes anyway, because reuse is decided from `body_consumed`, not from the field | The handler's value is dropped and `Connection: close` is written | hyper decides reuse *from the response's `Connection` field*. Honouring a handler `keep-alive` there would reuse a connection the native loop closes — hyper drains a small unread body and serves the next pipelined request. Overriding the field is the only lever the bridge has over hyper's accounting; the close behaviour matches, only the emitted field differs. |
