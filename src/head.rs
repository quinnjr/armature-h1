//! The parsed request head.

use crate::header::{self, HeaderId, HeaderVec};
use crate::{ByteStr, Method, Version};
use bytes::Bytes;

/// A parsed request line and header section.
///
/// Every [`Bytes`] within shares the connection's read buffer allocation, so
/// constructing a `Head` copies nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    /// The request method.
    pub method: Method,
    /// The request target, exactly as received.
    ///
    /// Private because [`path`](Head::path) and [`query`](Head::query) serve a
    /// `?` index cached alongside it: an assignment that did not recompute the
    /// index would leave the two slicing at a stale offset, which panics
    /// (out of range, or off a char boundary) or silently misroutes. Read it
    /// with [`target`](Head::target) and replace it with
    /// [`set_target`](Head::set_target), which recomputes the split.
    target: ByteStr,
    /// The protocol version.
    pub version: Version,
    /// The header fields, in wire order.
    pub headers: HeaderVec,
    /// Byte index of the first `?` in `target`, computed once per target.
    ///
    /// `path()` and `query()` are called on hot paths — routing, logging — and
    /// re-scanning the target for `?` on each call is pure repeated work.
    ///
    /// This is a pure function of `target`, so the derived `PartialEq`/`Eq`
    /// stay consistent with comparing the observable fields alone: two `Head`s
    /// with equal targets necessarily have equal `query_at`.
    query_at: Option<usize>,
}

/// Byte index of the first `?` in `target`.
///
/// Scans the raw bytes rather than the `&str`: `?` is ASCII, so a byte position
/// is always a char boundary, and this avoids a UTF-8 revalidation here on top
/// of the one `as_str` already performs.
#[inline]
fn find_query(target: &ByteStr) -> Option<usize> {
    target.as_bytes().iter().position(|&b| b == b'?')
}

impl Head {
    /// Assembles a `Head` from its parts, caching the target's query split.
    ///
    /// This is the only way to build a `Head` outside the crate: the cached
    /// `?` index is private, so literal struct construction is not available.
    pub fn new(method: Method, target: ByteStr, version: Version, headers: HeaderVec) -> Head {
        let query_at = find_query(&target);
        Head {
            method,
            target,
            version,
            headers,
            query_at,
        }
    }

    /// The request target, exactly as received.
    #[inline]
    pub fn target(&self) -> &ByteStr {
        &self.target
    }

    /// Replaces the request target, recomputing the cached query split.
    ///
    /// This is the only way to rewrite the target — middleware that mutates it
    /// must go through here so [`path`](Self::path) and [`query`](Self::query)
    /// keep slicing at an offset that is actually inside the new target.
    #[inline]
    pub fn set_target(&mut self, target: ByteStr) {
        self.query_at = find_query(&target);
        self.target = target;
    }

    /// The first value for `id`.
    #[inline]
    pub fn get(&self, id: &HeaderId) -> Option<&Bytes> {
        header::get(&self.headers, id)
    }

    /// The first value for `id` as a string, or `None` if absent or not UTF-8.
    #[inline]
    pub fn get_str(&self, id: &HeaderId) -> Option<&str> {
        header::get_str(&self.headers, id)
    }

    /// Every value for `id`, in wire order.
    #[inline]
    pub fn all<'a>(&'a self, id: &'a HeaderId) -> impl Iterator<Item = &'a Bytes> + 'a {
        header::all(&self.headers, id)
    }

    /// How many times `id` appears.
    #[inline]
    pub fn count(&self, id: &HeaderId) -> usize {
        header::count(&self.headers, id)
    }

    /// The target up to, but excluding, the first `?`.
    ///
    /// The `?` position is cached, so this is a slice of an already-known
    /// range. The one remaining `O(len)` cost is the UTF-8 revalidation inside
    /// [`ByteStr::as_str`], which stays because the crate forbids `unsafe` and
    /// so cannot skip the check.
    #[inline]
    pub fn path(&self) -> &str {
        let t = self.target.as_str();
        match self.query_at {
            Some(i) => &t[..i],
            None => t,
        }
    }

    /// The target after the first `?`, or `None` when there is no `?`.
    ///
    /// A trailing `?` yields `Some("")`, which is distinct from `None`. This is
    /// deliberate: it lets a caller distinguish "no query" from "empty query"
    /// without re-examining the target.
    ///
    /// As with [`path`](Self::path), only the `?` scan is cached away; the
    /// UTF-8 revalidation in [`ByteStr::as_str`] remains.
    #[inline]
    pub fn query(&self) -> Option<&str> {
        let t = self.target.as_str();
        self.query_at.map(|i| &t[i + 1..])
    }

    /// Whether `Connection` carries `token`, compared case-insensitively
    /// against each comma-separated element.
    pub fn connection_has_token(&self, token: &str) -> bool {
        self.all(&HeaderId::Connection).any(|v| {
            std::str::from_utf8(v)
                .map(|s| s.split(',').any(|t| t.trim().eq_ignore_ascii_case(token)))
                .unwrap_or(false)
        })
    }

    /// Whether the connection should persist after this request.
    ///
    /// HTTP/1.1 persists unless `Connection: close`; HTTP/1.0 closes unless
    /// `Connection: keep-alive`.
    #[inline]
    pub fn is_keep_alive(&self) -> bool {
        match self.version {
            Version::Http11 => !self.connection_has_token("close"),
            Version::Http10 => self.connection_has_token("keep-alive"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::HeaderVec;
    use crate::{Limits, parse_head};

    /// A `GET` head for `target`, with no header fields.
    fn get(target: &'static str) -> Head {
        Head::new(
            Method::Get,
            ByteStr::from_static(target),
            Version::Http11,
            HeaderVec::new(),
        )
    }

    #[test]
    fn splits_target_with_query() {
        let h = get("/search?q=rust&n=2");
        assert_eq!(h.path(), "/search");
        assert_eq!(h.query(), Some("q=rust&n=2"));
    }

    #[test]
    fn trailing_question_mark_is_an_empty_query() {
        // `Some("")` and `None` must stay distinguishable: a bare `?` is an
        // empty query, not the absence of one.
        let h = get("/items?");
        assert_eq!(h.path(), "/items");
        assert_eq!(h.query(), Some(""));
    }

    #[test]
    fn no_question_mark_is_all_path() {
        let h = get("/items/42");
        assert_eq!(h.path(), "/items/42");
        assert_eq!(h.query(), None);
    }

    #[test]
    fn asterisk_form_is_all_path() {
        let h = get("*");
        assert_eq!(h.path(), "*");
        assert_eq!(h.query(), None);
    }

    #[test]
    fn only_the_first_question_mark_splits() {
        let h = get("/a?b?c");
        assert_eq!(h.path(), "/a");
        assert_eq!(h.query(), Some("b?c"));
    }

    #[test]
    fn empty_target_is_handled() {
        let h = get("");
        assert_eq!(h.path(), "");
        assert_eq!(h.query(), None);
    }

    #[test]
    fn new_matches_a_parsed_head() {
        // The cached index is a pure function of the target, so a hand-built
        // `Head` must compare equal to a parsed one with the same parts —
        // otherwise the derived `PartialEq` would be observing private state
        // that the public fields do not explain.
        let raw = Bytes::from_static(b"GET /search?q=rust HTTP/1.1\r\nhost: a\r\n\r\n");
        let (parsed, _) = parse_head(&raw, &Limits::default())
            .expect("must parse")
            .expect("must be complete");

        let mut headers = HeaderVec::new();
        headers.push((HeaderId::Host, Bytes::from_static(b"a")));
        let built = Head::new(
            Method::Get,
            ByteStr::from_static("/search?q=rust"),
            Version::Http11,
            headers,
        );

        assert_eq!(parsed, built);
        assert_eq!(parsed.path(), built.path());
        assert_eq!(parsed.query(), built.query());
    }

    #[test]
    fn target_accessor_returns_the_raw_target() {
        let h = get("/search?q=rust");
        assert_eq!(h.target().as_str(), "/search?q=rust");
    }

    /// Before `target` was sealed, a middleware assignment left `query_at`
    /// pointing past the end of the new target and `path()` panicked. Rewriting
    /// long→short is exactly that shape.
    #[test]
    fn set_target_shrinking_does_not_leave_a_dangling_index() {
        let mut h = get("/a/very/long/prefix?q=1");
        h.set_target(ByteStr::from_static("/b"));
        assert_eq!(h.target().as_str(), "/b");
        assert_eq!(h.path(), "/b");
        assert_eq!(h.query(), None);
    }

    #[test]
    fn set_target_growing_recomputes_the_split() {
        let mut h = get("/b");
        h.set_target(ByteStr::from_static("/a/very/long/prefix?q=1&r=2"));
        assert_eq!(h.path(), "/a/very/long/prefix");
        assert_eq!(h.query(), Some("q=1&r=2"));
    }

    #[test]
    fn set_target_covers_query_presence_transitions() {
        // query → no query
        let mut h = get("/a?x=1");
        h.set_target(ByteStr::from_static("/longer/path"));
        assert_eq!(h.path(), "/longer/path");
        assert_eq!(h.query(), None);

        // no query → query
        h.set_target(ByteStr::from_static("/c?y=2"));
        assert_eq!(h.path(), "/c");
        assert_eq!(h.query(), Some("y=2"));

        // query → query, at a different offset
        h.set_target(ByteStr::from_static("/dd?z=3"));
        assert_eq!(h.path(), "/dd");
        assert_eq!(h.query(), Some("z=3"));

        // to a trailing `?`, which stays distinguishable from no query
        h.set_target(ByteStr::from_static("/e?"));
        assert_eq!(h.path(), "/e");
        assert_eq!(h.query(), Some(""));

        // and to the empty target
        h.set_target(ByteStr::from_static(""));
        assert_eq!(h.path(), "");
        assert_eq!(h.query(), None);
    }

    /// The stale index could also land mid-character rather than out of range,
    /// which slices a multi-byte scalar and panics on the char boundary.
    #[test]
    fn set_target_across_multibyte_content_stays_on_a_boundary() {
        let mut h = get("/aaaaaaaa?q=1");
        h.set_target(ByteStr::from_static("/\u{1f600}\u{1f600}"));
        assert_eq!(h.path(), "/\u{1f600}\u{1f600}");
        assert_eq!(h.query(), None);
    }

    /// `query_at` is a pure function of the target, so a rewritten `Head` must
    /// compare equal to one built with the same target from the start.
    #[test]
    fn set_target_leaves_a_head_equal_to_a_freshly_built_one() {
        let mut h = get("/old/target?a=1");
        h.set_target(ByteStr::from_static("/new?b=2"));
        assert_eq!(h, get("/new?b=2"));
    }
}
