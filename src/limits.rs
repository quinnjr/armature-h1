//! Resource limits and deadlines.
//!
//! These are the slowloris and resource-exhaustion defenses. Every field has a
//! finite default; there is deliberately no "unlimited" setting for the head,
//! because an unbounded head read is a trivial memory-exhaustion vector.
//!
//! There is no pipeline-depth cap, and none is needed: the connection loop stops
//! reading the moment one complete head is buffered and does not read again
//! until that request's response is written. Nothing is ever parsed ahead, so
//! pipelining costs only whatever the peer managed to write into the socket
//! buffer — which `max_head_bytes` and the read chunk size already bound.

use std::time::Duration;

/// The largest `max_headers` the parser's fixed scratch array can serve.
///
/// `parse::parse_head` allocates its `httparse::Header` array on the stack at
/// this size, so [`Limits::max_headers`] must never exceed it.
pub const MAX_HEADERS_CEILING: usize = 128;

/// Per-connection resource limits and deadlines.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Maximum bytes in the request line plus header section. Exceeding this
    /// yields 431 and closes.
    pub max_head_bytes: usize,
    /// Maximum header field count. Exceeding this yields 431 and closes.
    ///
    /// Values above [`MAX_HEADERS_CEILING`] are clamped to it, with a
    /// `tracing::warn!`, by [`Limits::clamp_max_headers`] — called from both
    /// [`Config::limits`](crate::server::Config::limits) and the worker setup
    /// in `server::worker_loop`, since `Config`'s fields are public and so
    /// `cfg.limits` can also be set directly, bypassing the former — the
    /// parser's scratch array cannot serve more than that regardless of what
    /// is configured here.
    pub max_headers: usize,
    /// Maximum request body bytes. Exceeding this yields 413 and closes.
    pub max_body_bytes: u64,
    /// Deadline for the complete head to arrive once the first byte does.
    pub header_timeout: Duration,
    /// Deadline for the complete body to arrive once the head is parsed.
    ///
    /// It bounds the body, not the handler: once the body has been read to
    /// its end, or when the request has none, the handler may run as long as
    /// it needs. A body still incomplete at the deadline — the peer stalled,
    /// or the handler is not reading it — gets a `408` and a close.
    pub body_timeout: Duration,
    /// Deadline for a handler to produce its response, counted from when it is
    /// called; `None` (the default) leaves handlers unbounded.
    ///
    /// Separate from [`body_timeout`](Self::body_timeout), which bounds the
    /// peer: this bounds the server's own work, so a handler waiting on a
    /// downstream that never answers cannot hold its connection — and its
    /// `max_connections` slot — forever. Expiry answers `503 Service
    /// Unavailable` and closes. Off by default because long-polling and
    /// streaming-setup handlers legitimately run for as long as they like.
    pub handler_timeout: Option<Duration>,
    /// Deadline for the next request to begin on an idle keep-alive connection.
    pub idle_timeout: Duration,
    /// Deadline for a response write to complete.
    pub write_timeout: Duration,
    /// Maximum open connections per worker. Once a worker holds this many it
    /// stops accepting until one closes, leaving new connections in the
    /// kernel's listen backlog instead of spending memory and file
    /// descriptors on sockets it cannot serve. With `n` workers the server as
    /// a whole holds at most `n * max_connections`. `0` is treated as `1`.
    pub max_connections: usize,
}

impl Limits {
    /// Clamp `max_headers` to [`MAX_HEADERS_CEILING`], with a `tracing::warn!`
    /// if clamping was necessary.
    ///
    /// The parser's fixed `httparse::Header` scratch array cannot serve more
    /// than [`MAX_HEADERS_CEILING`] headers regardless of what is configured,
    /// so every path that accepts a caller-supplied `Limits` must call this
    /// before the value is used to size anything.
    pub(crate) fn clamp_max_headers(&mut self) {
        if self.max_headers > MAX_HEADERS_CEILING {
            tracing::warn!(
                configured = self.max_headers,
                ceiling = MAX_HEADERS_CEILING,
                "Limits::max_headers exceeds MAX_HEADERS_CEILING; clamping"
            );
            self.max_headers = MAX_HEADERS_CEILING;
        }
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_head_bytes: 16 * 1024,
            max_headers: 96,
            max_body_bytes: 2 * 1024 * 1024,
            header_timeout: Duration::from_secs(10),
            body_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(75),
            write_timeout: Duration::from_secs(30),
            max_connections: DEFAULT_MAX_CONNECTIONS_PER_WORKER,
            handler_timeout: None,
        }
    }
}

/// Default for [`Limits::max_connections`].
pub const DEFAULT_MAX_CONNECTIONS_PER_WORKER: usize = 10_000;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_documented_values() {
        let l = Limits::default();
        assert_eq!(l.max_head_bytes, 16 * 1024);
        assert_eq!(l.max_headers, 96);
        assert_eq!(l.max_body_bytes, 2 * 1024 * 1024);
        assert_eq!(l.header_timeout, Duration::from_secs(10));
        assert_eq!(l.body_timeout, Duration::from_secs(30));
        assert_eq!(l.idle_timeout, Duration::from_secs(75));
        assert_eq!(l.write_timeout, Duration::from_secs(30));
        assert_eq!(l.max_connections, DEFAULT_MAX_CONNECTIONS_PER_WORKER);
    }

    /// `max_headers` must not exceed the fixed httparse scratch array in
    /// parse.rs, or heads that fit the limit would fail to parse.
    #[test]
    fn max_headers_fits_the_parser_scratch_array() {
        assert!(Limits::default().max_headers <= MAX_HEADERS_CEILING);
    }

    #[test]
    fn builder_overrides_apply() {
        let l = Limits {
            max_body_bytes: 64,
            ..Default::default()
        };
        assert_eq!(l.max_body_bytes, 64);
        assert_eq!(l.max_headers, 96, "unrelated fields keep their defaults");
    }
}
