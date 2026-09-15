//! Response serialization.
//!
//! Everything here writes into a caller-supplied [`BytesMut`] — the connection's
//! write buffer — so a response is assembled in place and leaves in one
//! `writev` rather than being built up through intermediate allocations.

use crate::Version;
use crate::header::{HeaderId, HeaderVec};
use bytes::{BufMut, Bytes, BytesMut};
use std::time::SystemTime;

/// The length of an IMF-fixdate: `Sun, 06 Nov 1994 08:49:37 GMT`.
const DATE_LEN: usize = 29;

/// A once-per-second cache of the `Date` field value.
///
/// Formatting a date is comparatively expensive and the result only changes on
/// the second, so it is reformatted at most once per second per worker. `now` is
/// a parameter rather than read internally so the cache is testable without a
/// clock.
#[derive(Debug)]
pub struct DateCache {
    secs: u64,
    buf: [u8; DATE_LEN],
    valid: bool,
}

impl Default for DateCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DateCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self {
            secs: 0,
            buf: [0; DATE_LEN],
            valid: false,
        }
    }

    /// The IMF-fixdate for `now`, reformatting only when the second changed.
    pub fn get(&mut self, now: SystemTime) -> &[u8] {
        let secs = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if !self.valid || secs != self.secs {
            let formatted = httpdate::fmt_http_date(now);
            debug_assert_eq!(formatted.len(), DATE_LEN);
            let bytes = formatted.as_bytes();
            let n = bytes.len().min(DATE_LEN);
            self.buf[..n].copy_from_slice(&bytes[..n]);
            self.secs = secs;
            self.valid = true;
        }
        &self.buf
    }
}

/// How a response body is framed on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutBody {
    /// No body.
    None,
    /// A body of known length.
    Fixed(Bytes),
    /// A chunked body.
    Chunked,
}

/// The status and headers of a response.
#[derive(Clone, Debug, Default)]
pub struct ResponseHead {
    /// The status code.
    pub status: u16,
    /// Headers supplied by the handler.
    pub headers: HeaderVec,
}

/// The reason phrase for `status`, or `""` if unregistered.
///
/// An empty phrase is valid on the wire (RFC 9112 section 4), so an unknown code
/// needs no invented text.
pub fn reason_phrase(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        101 => "Switching Protocols",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        408 => "Request Timeout",
        409 => "Conflict",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Content Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        416 => "Range Not Satisfiable",
        417 => "Expectation Failed",
        421 => "Misdirected Request",
        422 => "Unprocessable Content",
        426 => "Upgrade Required",
        428 => "Precondition Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        _ => "",
    }
}

/// Append `v` as decimal digits.
///
/// Avoids `format!`, which would allocate a `String` per call on a path that
/// runs at least once per response.
pub fn write_u64(out: &mut BytesMut, v: u64) {
    // u64::MAX is 20 digits.
    let mut scratch = [0u8; 20];
    let mut i = scratch.len();
    let mut n = v;
    loop {
        i -= 1;
        scratch[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.put_slice(&scratch[i..]);
}

/// Whether a status code forbids any body framing field.
///
/// 204 and 304 must carry neither `Content-Length` nor `Transfer-Encoding`
/// (RFC 9110 sections 15.3.5 and 15.4.5).
#[inline]
pub(crate) fn forbids_body_framing(status: u16) -> bool {
    matches!(status, 204 | 304) || (100..200).contains(&status)
}

/// Whether `Content-Length` on this status describes a representation rather
/// than framing the body being written.
///
/// 304 is the only such status: RFC 9110 section 15.4.5 permits a
/// `Content-Length` giving the length the representation *would* have had, as
/// on a HEAD. Because the writer emits no framing of its own for a status that
/// [`forbids_body_framing`], nothing on the wire can disagree with it, so the
/// value passes through as long as it is well-formed. 204 and 1xx get no such
/// exemption — RFC 9110 section 8.6 forbids the field outright there.
///
/// This is the single expression of that rule; every caller that needs to know
/// whether a length is descriptive must ask here rather than testing `304`
/// again, so the two sides cannot drift apart on which statuses qualify.
#[inline]
pub(crate) fn content_length_is_descriptive(status: u16) -> bool {
    status == 304
}

/// The `Connection` token to state for this version and persistence decision,
/// or `None` when the default already says it.
///
/// Persistence is the HTTP/1.1 default, so a `Connection: keep-alive` there is
/// pure noise on every response of a persistent connection. HTTP/1.0 defaults
/// the other way and has to say so. Either version must state `close`, because
/// that one is never the default and a peer that misses it holds a connection
/// open against a server that has already decided to shut it.
///
/// This is the single definition of that mapping. It is a function rather than
/// inline `put_slice` calls so that a second writer — the `hyper-backend`
/// bridge, which sets the field on hyper's response rather than serializing it
/// — produces the same answer by calling it instead of transcribing the table
/// and asserting parity in a comment that nothing checks.
#[inline]
pub(crate) fn connection_field(version: Version, keep_alive: bool) -> Option<&'static str> {
    match (version, keep_alive) {
        (Version::Http11, true) => None,
        (Version::Http10, true) => Some("keep-alive"),
        (_, false) => Some("close"),
    }
}

/// Whether a field value can be written without splitting the response.
///
/// CR, LF, or NUL in a value terminates the field early and lets whatever
/// follows be read as further header fields or as a body. That is response
/// splitting — the mirror image of the request smuggling this crate is built to
/// reject — and a handler that reflects request data into a header (a computed
/// `Location`, an echoed correlation id) is one bad input away from it. The
/// check lives here, at the last point before the bytes reach the wire, rather
/// than at each of the call sites that could produce one.
#[inline]
fn valid_field_value(value: &[u8]) -> bool {
    !value.iter().any(|b| matches!(b, b'\r' | b'\n' | 0))
}

/// Whether a field name is a token (RFC 9110 section 5.6.2).
///
/// Well-known [`HeaderId`]s render to fixed tokens by construction.
/// [`HeaderId::Other`] carries whatever the parser or the handler put in it, and
/// a name containing a colon, a space, or CRLF splits the response exactly as a
/// value does.
#[inline]
fn valid_field_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

/// Whether this field may be emitted as-is.
#[inline]
fn writable(id: &HeaderId, value: &[u8]) -> bool {
    let name_ok = match id {
        HeaderId::Other(name) => valid_field_name(name.as_str()),
        _ => true,
    };
    name_ok && valid_field_value(value)
}

/// Parse a header value as a decimal length, rejecting anything else.
///
/// Deliberately stricter than `str::parse`: no sign, no whitespace, no empty
/// value. A value this cannot read is treated as disagreeing with the body,
/// which is the safe direction — the writer then emits the true length.
#[inline]
pub(crate) fn parse_len(v: &Bytes) -> Option<u64> {
    if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(v).ok()?.parse().ok()
}

/// Whether a `Transfer-Encoding` value actually frames the body as chunked.
///
/// Only the final coding delimits the message (RFC 9112 section 6.1), so
/// `gzip, chunked` frames and a bare `gzip` does not. Codings are compared
/// case-insensitively with the optional whitespace around the commas trimmed,
/// the same reading `framing::decide` applies on the request side; the two must
/// not disagree about what "chunked" means.
#[inline]
fn te_frames_chunked(v: &Bytes) -> bool {
    let Ok(s) = std::str::from_utf8(v) else {
        return false;
    };
    s.split(',')
        .map(str::trim)
        .rfind(|c| !c.is_empty())
        .is_some_and(|c| c.eq_ignore_ascii_case("chunked"))
}

/// Serialize a status line and header section into `out`.
///
/// Emits `Date`, `Connection`, and the body framing field itself, but never
/// duplicates one the handler already supplied. A duplicate `Content-Length` on
/// a response is the mirror image of the request ambiguity this crate rejects,
/// so it must not be possible to produce one by accident.
pub fn write_head(
    out: &mut BytesMut,
    version: Version,
    resp: &ResponseHead,
    body: &OutBody,
    date: &[u8],
    keep_alive: bool,
) {
    out.put_slice(version.as_bytes());
    out.put_u8(b' ');
    write_u64(out, resp.status as u64);
    let phrase = reason_phrase(resp.status);
    if !phrase.is_empty() {
        out.put_u8(b' ');
        out.put_slice(phrase.as_bytes());
    }
    out.put_slice(b"\r\n");

    // Everything the decisions below need comes from one walk of the field
    // list: whether `Date` and `Connection` were supplied, the first framing
    // value of each kind and how many writable instances of it there are, and
    // per-field writability for the emit loop. Asking `emitted` five times and
    // then re-running `writable` per field walked a 96-entry list up to six
    // times over for what a single pass already has.
    //
    // Each of those decisions must agree with what is actually *written*, not
    // with what the handler supplied: a `Content-Length` dropped for containing
    // CRLF would otherwise suppress the framing field too and leave the response
    // undelimited — trading one splitting bug for another. So only writable
    // fields count here, and the first writable one wins, exactly as the wire
    // sees it.
    //
    // The counts matter because the emit loop below writes *every* writable
    // instance, not just the first. Judging only the first would let
    // `.header(TE, "chunked").header(TE, "gzip")` past the staleness check on a
    // chunked body and then emit both, whose combined final coding is `gzip` —
    // an undelimited response with chunk frames in it. See the count check
    // below.
    let mut field_writable: smallvec::SmallVec<[bool; 16]> =
        smallvec::SmallVec::with_capacity(resp.headers.len());
    let mut has_date = false;
    let mut has_connection = false;
    let mut handler_len: Option<&Bytes> = None;
    let mut handler_te: Option<&Bytes> = None;
    let mut len_count = 0usize;
    let mut te_count = 0usize;
    for (id, value) in resp.headers.iter() {
        let ok = writable(id, value);
        field_writable.push(ok);
        if !ok {
            continue;
        }
        match id {
            HeaderId::Date => has_date = true,
            HeaderId::Connection => has_connection = true,
            HeaderId::ContentLength => {
                len_count += 1;
                if handler_len.is_none() {
                    handler_len = Some(value);
                }
            }
            HeaderId::TransferEncoding => {
                te_count += 1;
                if handler_te.is_none() {
                    handler_te = Some(value);
                }
            }
            _ => {}
        }
    }

    // A handler `Content-Length` that disagrees with the body it framed is
    // dropped, so the writer below reclaims framing and emits the true length.
    // Trusting it would put more (or fewer) bytes on the wire than the field
    // accounts for, and a keep-alive peer reads the difference as the start of
    // the next response — response splitting reached through arithmetic rather
    // than through a CRLF.
    //
    // A duplicated framing field is by definition one the writer cannot defer
    // to: two writable `Content-Length`s are the response-side twin of the
    // request ambiguity this crate rejects outright, whether their values
    // conflict or merely repeat. Both go down the drop-and-reclaim path, so the
    // writer emits exactly one true framing field.
    //
    // On a descriptive status the field is not framing, so the only rule that
    // applies is well-formedness: RFC 9110 section 15.4.5 permits a *decimal*
    // length describing the representation that would have been sent, not a
    // malformed one. Shipping `content-length: abc` because the status is 304
    // hands the peer a field it must then guess about.
    let stale_content_length = handler_len.is_some_and(|v| {
        len_count > 1
            || if content_length_is_descriptive(resp.status) {
                parse_len(v).is_none()
            } else {
                match body {
                    OutBody::Fixed(b) => parse_len(v) != Some(b.len() as u64),
                    OutBody::None => parse_len(v) != Some(0),
                    // A chunked body carries its own framing; a length cannot agree.
                    OutBody::Chunked => true,
                }
            }
    });

    // A handler `Transfer-Encoding` only counts as handler framing when it
    // actually frames *this* body as chunked. Anything else is a field the
    // writer must not defer to: `chunked` over a `Fixed` body would put raw
    // unframed bytes behind a chunked advertisement (an undelimited response —
    // the desync this crate exists to make impossible), `chunked` over no body
    // leaves the peer waiting for chunks that never come, and a non-chunked
    // coding over a chunked body advertises the wrong one. Dropping it takes the
    // same path as a stale length: the writer emits true framing below. More
    // than one writable instance is stale for the same reason as a duplicated
    // length — the emit loop writes them all, and it is their *combined* final
    // coding that delimits the message, not the first one checked.
    let stale_transfer_encoding = handler_te
        .is_some_and(|v| te_count > 1 || *body != OutBody::Chunked || !te_frames_chunked(v));

    // Handler-supplied headers first, so the checks below can see them.
    for (i, (id, value)) in resp.headers.iter().enumerate() {
        if stale_content_length && id == &HeaderId::ContentLength {
            tracing::warn!("dropping response content-length that disagrees with the body length");
            continue;
        }
        if stale_transfer_encoding && id == &HeaderId::TransferEncoding {
            tracing::warn!("dropping response transfer-encoding that does not frame the body");
            continue;
        }
        if !field_writable[i] {
            // The status line is already in `out`, so there is no error left to
            // return; dropping the field is the only outcome that does not put
            // attacker-chosen bytes on the wire.
            tracing::warn!(
                field = id.as_str(),
                "dropping response header with an invalid field name or value"
            );
            continue;
        }
        out.put_slice(id.as_str().as_bytes());
        out.put_slice(b": ");
        out.put_slice(value);
        out.put_slice(b"\r\n");
    }

    if !has_date {
        out.put_slice(b"date: ");
        out.put_slice(date);
        out.put_slice(b"\r\n");
    }

    // Body framing, unless the handler already framed it or the status forbids
    // framing entirely.
    let handler_framed = (!stale_content_length && handler_len.is_some())
        || (!stale_transfer_encoding && handler_te.is_some());
    if !handler_framed && !forbids_body_framing(resp.status) {
        match body {
            OutBody::Fixed(b) => {
                out.put_slice(b"content-length: ");
                write_u64(out, b.len() as u64);
                out.put_slice(b"\r\n");
            }
            OutBody::Chunked => {
                out.put_slice(b"transfer-encoding: chunked\r\n");
            }
            OutBody::None => {
                out.put_slice(b"content-length: 0\r\n");
            }
        }
    }

    if !has_connection && let Some(token) = connection_field(version, keep_alive) {
        out.put_slice(b"connection: ");
        out.put_slice(token.as_bytes());
        out.put_slice(b"\r\n");
    }

    out.put_slice(b"\r\n");
}

/// Append one chunk of a chunked body.
pub fn write_chunk(out: &mut BytesMut, data: &[u8]) {
    write_hex(out, data.len() as u64);
    out.put_slice(b"\r\n");
    out.put_slice(data);
    out.put_slice(b"\r\n");
}

/// Append the terminating zero-length chunk and trailer section.
///
/// Trailers are held to the same field-name and field-value rules as headers: a
/// CRLF in a trailer value ends the trailer section early, and the bytes after
/// it become the start of whatever the peer reads next.
pub fn write_last_chunk(out: &mut BytesMut, trailers: &HeaderVec) {
    out.put_slice(b"0\r\n");
    for (id, value) in trailers.iter() {
        if !writable(id, value) {
            tracing::warn!(
                field = id.as_str(),
                "dropping trailer with an invalid field name or value"
            );
            continue;
        }
        out.put_slice(id.as_str().as_bytes());
        out.put_slice(b": ");
        out.put_slice(value);
        out.put_slice(b"\r\n");
    }
    out.put_slice(b"\r\n");
}

/// Append `v` as lowercase hex digits.
fn write_hex(out: &mut BytesMut, v: u64) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut scratch = [0u8; 16];
    let mut i = scratch.len();
    let mut n = v;
    loop {
        i -= 1;
        scratch[i] = HEX[(n % 16) as usize];
        n /= 16;
        if n == 0 {
            break;
        }
    }
    out.put_slice(&scratch[i..]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const EPOCH_DATE: &[u8] = b"Thu, 01 Jan 1970 00:00:00 GMT";

    fn head_of(status: u16, headers: &[(HeaderId, &'static str)]) -> ResponseHead {
        ResponseHead {
            status,
            headers: headers
                .iter()
                .map(|(id, v)| (id.clone(), Bytes::from_static(v.as_bytes())))
                .collect(),
        }
    }

    fn render(version: Version, resp: &ResponseHead, body: &OutBody, keep_alive: bool) -> String {
        let mut out = BytesMut::new();
        write_head(&mut out, version, resp, body, EPOCH_DATE, keep_alive);
        String::from_utf8(out.to_vec()).unwrap()
    }

    #[test]
    fn writes_a_minimal_200() {
        let got = render(
            Version::Http11,
            &head_of(200, &[]),
            &OutBody::Fixed(Bytes::from_static(b"hello")),
            true,
        );
        assert_eq!(
            got,
            "HTTP/1.1 200 OK\r\n\
             date: Thu, 01 Jan 1970 00:00:00 GMT\r\n\
             content-length: 5\r\n\
             \r\n"
        );
    }

    #[test]
    fn write_u64_matches_to_string() {
        for v in [0u64, 1, 9, 10, 99, 100, 12345, u64::MAX] {
            let mut out = BytesMut::new();
            write_u64(&mut out, v);
            assert_eq!(String::from_utf8(out.to_vec()).unwrap(), v.to_string());
        }
    }

    #[test]
    fn reason_phrases_are_correct() {
        assert_eq!(reason_phrase(200), "OK");
        assert_eq!(reason_phrase(201), "Created");
        assert_eq!(reason_phrase(204), "No Content");
        assert_eq!(reason_phrase(301), "Moved Permanently");
        assert_eq!(reason_phrase(304), "Not Modified");
        assert_eq!(reason_phrase(400), "Bad Request");
        assert_eq!(reason_phrase(404), "Not Found");
        assert_eq!(reason_phrase(408), "Request Timeout");
        assert_eq!(reason_phrase(413), "Content Too Large");
        assert_eq!(reason_phrase(431), "Request Header Fields Too Large");
        assert_eq!(reason_phrase(500), "Internal Server Error");
        assert_eq!(reason_phrase(501), "Not Implemented");
        assert_eq!(reason_phrase(505), "HTTP Version Not Supported");
        assert_eq!(reason_phrase(599), "", "unregistered codes invent nothing");
    }

    #[test]
    fn fixed_body_emits_content_length() {
        let got = render(
            Version::Http11,
            &head_of(200, &[]),
            &OutBody::Fixed(Bytes::from_static(b"hello")),
            true,
        );
        assert!(got.contains("content-length: 5\r\n"));
        assert!(!got.contains("transfer-encoding"));
    }

    #[test]
    fn chunked_body_emits_transfer_encoding() {
        let got = render(Version::Http11, &head_of(200, &[]), &OutBody::Chunked, true);
        assert!(got.contains("transfer-encoding: chunked\r\n"));
        assert!(!got.contains("content-length"));
    }

    #[test]
    fn empty_body_emits_zero_length() {
        let got = render(Version::Http11, &head_of(200, &[]), &OutBody::None, true);
        assert!(got.contains("content-length: 0\r\n"));
    }

    /// RFC 9110 15.3.5 and 15.4.5: neither status may carry body framing.
    #[test]
    fn no_framing_fields_on_204_or_304() {
        for status in [204u16, 304] {
            let got = render(Version::Http11, &head_of(status, &[]), &OutBody::None, true);
            assert!(
                !got.contains("content-length"),
                "{status} must not frame a body: {got}"
            );
            assert!(
                !got.contains("transfer-encoding"),
                "{status} must not frame a body: {got}"
            );
        }
    }

    #[test]
    fn does_not_duplicate_handler_supplied_date() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::Date, "Mon, 01 Jan 2001 00:00:00 GMT")]),
            &OutBody::None,
            true,
        );
        assert_eq!(got.matches("date: ").count(), 1);
        assert!(got.contains("Mon, 01 Jan 2001"));
    }

    /// A duplicate Content-Length on a response is the mirror image of the
    /// request ambiguity this crate rejects, so it must be impossible to emit.
    #[test]
    fn does_not_duplicate_handler_supplied_content_length() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::ContentLength, "5")]),
            &OutBody::Fixed(Bytes::from_static(b"hello")),
            true,
        );
        assert_eq!(got.matches("content-length").count(), 1);
    }

    #[test]
    fn does_not_add_content_length_when_handler_set_transfer_encoding() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::TransferEncoding, "chunked")]),
            &OutBody::Chunked,
            true,
        );
        assert_eq!(got.matches("transfer-encoding").count(), 1);
        assert!(!got.contains("content-length"));
    }

    /// A handler `Transfer-Encoding: chunked` over a fixed body would put raw,
    /// unframed bytes behind a chunked advertisement: the peer reads the body as
    /// a chunk-size line and the response is undelimited from there on.
    #[test]
    fn a_transfer_encoding_over_a_fixed_body_is_dropped() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::TransferEncoding, "chunked")]),
            &OutBody::Fixed(Bytes::from_static(b"hello")),
            true,
        );
        assert!(!got.contains("transfer-encoding"), "{got}");
        assert_eq!(got.matches("content-length").count(), 1, "{got}");
        assert!(got.contains("content-length: 5\r\n"), "{got}");
    }

    /// The same field over no body at all leaves the peer waiting for chunks
    /// that never arrive.
    #[test]
    fn a_transfer_encoding_over_no_body_is_dropped() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::TransferEncoding, "chunked")]),
            &OutBody::None,
            true,
        );
        assert!(!got.contains("transfer-encoding"), "{got}");
        assert!(got.contains("content-length: 0\r\n"), "{got}");
    }

    /// Only the final coding frames the message, so `gzip` alone advertises a
    /// coding the writer is not applying and does not delimit anything.
    #[test]
    fn a_non_chunked_transfer_encoding_over_a_chunked_body_is_replaced() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::TransferEncoding, "gzip")]),
            &OutBody::Chunked,
            true,
        );
        assert!(!got.contains("gzip"), "{got}");
        assert_eq!(got.matches("transfer-encoding").count(), 1, "{got}");
        assert!(got.contains("transfer-encoding: chunked\r\n"), "{got}");
    }

    /// `gzip, chunked` does frame the message — chunked is final — so the
    /// handler stays in charge of it.
    #[test]
    fn a_transfer_encoding_ending_in_chunked_is_preserved() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::TransferEncoding, "gzip, chunked")]),
            &OutBody::Chunked,
            true,
        );
        assert_eq!(got.matches("transfer-encoding").count(), 1, "{got}");
        assert!(
            got.contains("transfer-encoding: gzip, chunked\r\n"),
            "{got}"
        );
        assert!(!got.contains("content-length"), "{got}");
    }

    /// RFC 9110 section 15.4.5: a 304 may carry the `Content-Length` the full
    /// representation would have had. The writer frames nothing on a 304, so
    /// there is nothing for it to disagree with. RFC 9110 section 8.6 forbids
    /// the field on a 204, which keeps that one on the staleness path.
    #[test]
    fn a_handler_content_length_survives_on_304_but_not_204() {
        let got = render(
            Version::Http11,
            &head_of(304, &[(HeaderId::ContentLength, "1234")]),
            &OutBody::None,
            true,
        );
        assert!(got.contains("content-length: 1234\r\n"), "{got}");
        assert_eq!(got.matches("content-length").count(), 1, "{got}");

        let got = render(
            Version::Http11,
            &head_of(204, &[(HeaderId::ContentLength, "1234")]),
            &OutBody::None,
            true,
        );
        assert!(!got.contains("content-length"), "{got}");
    }

    /// The 304 exemption is about the field *describing* a representation, not
    /// about skipping well-formedness: RFC 9110 section 15.4.5 permits a decimal
    /// length, and `abc` is not one.
    #[test]
    fn a_malformed_content_length_is_dropped_on_304() {
        let got = render(
            Version::Http11,
            &head_of(304, &[(HeaderId::ContentLength, "abc")]),
            &OutBody::None,
            true,
        );
        assert!(!got.contains("content-length"), "{got}");

        // A decimal one still survives, so the exemption itself is intact.
        let got = render(
            Version::Http11,
            &head_of(304, &[(HeaderId::ContentLength, "9")]),
            &OutBody::None,
            true,
        );
        assert!(got.contains("content-length: 9\r\n"), "{got}");
    }

    /// The emit loop writes every writable instance, so judging only the first
    /// would let two `Transfer-Encoding`s through whose *combined* final coding
    /// is `gzip` — an undelimited response carrying chunk frames.
    #[test]
    fn duplicate_transfer_encoding_is_stale_and_reclaimed() {
        let got = render(
            Version::Http11,
            &head_of(
                200,
                &[
                    (HeaderId::TransferEncoding, "chunked"),
                    (HeaderId::TransferEncoding, "gzip"),
                ],
            ),
            &OutBody::Chunked,
            true,
        );
        assert_eq!(got.matches("transfer-encoding").count(), 1, "{got}");
        assert!(got.contains("transfer-encoding: chunked\r\n"), "{got}");
        assert!(!got.contains("gzip"), "{got}");
    }

    /// Even two instances that would each pass on their own are stale: the pair
    /// is the response-side twin of the request framing ambiguity this crate
    /// rejects, so one true field replaces both.
    #[test]
    fn duplicate_transfer_encoding_is_stale_even_when_identical() {
        let got = render(
            Version::Http11,
            &head_of(
                200,
                &[
                    (HeaderId::TransferEncoding, "chunked"),
                    (HeaderId::TransferEncoding, "chunked"),
                ],
            ),
            &OutBody::Chunked,
            true,
        );
        assert_eq!(got.matches("transfer-encoding").count(), 1, "{got}");
        assert!(got.contains("transfer-encoding: chunked\r\n"), "{got}");
    }

    #[test]
    fn duplicate_content_length_is_stale_and_reclaimed() {
        // Conflicting: the classic splitting shape.
        let got = render(
            Version::Http11,
            &head_of(
                200,
                &[
                    (HeaderId::ContentLength, "5"),
                    (HeaderId::ContentLength, "99"),
                ],
            ),
            &OutBody::Fixed(Bytes::from_static(b"hello")),
            true,
        );
        assert_eq!(got.matches("content-length").count(), 1, "{got}");
        assert!(got.contains("content-length: 5\r\n"), "{got}");
        assert!(!got.contains("99"), "{got}");

        // Identical, and both agreeing with the body: still one field on the
        // wire, because the emit loop would otherwise write the pair.
        let got = render(
            Version::Http11,
            &head_of(
                200,
                &[
                    (HeaderId::ContentLength, "5"),
                    (HeaderId::ContentLength, "5"),
                ],
            ),
            &OutBody::Fixed(Bytes::from_static(b"hello")),
            true,
        );
        assert_eq!(got.matches("content-length").count(), 1, "{got}");
        assert!(got.contains("content-length: 5\r\n"), "{got}");
    }

    /// A duplicated descriptive length is dropped too, and because the status
    /// forbids framing the writer emits nothing in its place.
    #[test]
    fn duplicate_content_length_is_stale_on_304() {
        let got = render(
            Version::Http11,
            &head_of(
                304,
                &[
                    (HeaderId::ContentLength, "1234"),
                    (HeaderId::ContentLength, "1234"),
                ],
            ),
            &OutBody::None,
            true,
        );
        assert!(!got.contains("content-length"), "{got}");
    }

    /// Only *writable* instances count: a second one carrying CRLF is dropped
    /// by the splitting check anyway, so it must not make the well-formed first
    /// one stale as well.
    #[test]
    fn an_unwritable_duplicate_does_not_make_the_first_stale() {
        let got = render(
            Version::Http11,
            &head_of(
                200,
                &[
                    (HeaderId::ContentLength, "5"),
                    (HeaderId::ContentLength, "5\r\nx: y"),
                ],
            ),
            &OutBody::Fixed(Bytes::from_static(b"hello")),
            true,
        );
        assert_eq!(got.matches("content-length").count(), 1, "{got}");
        assert!(got.contains("content-length: 5\r\n"), "{got}");
        assert!(!got.contains("x: y"), "{got}");
    }

    /// All three arms of the mapping, pinned here so the bridge — which now
    /// calls this rather than transcribing it — cannot drift from the writer
    /// without a test failing.
    #[test]
    fn connection_field_covers_every_arm() {
        assert_eq!(connection_field(Version::Http11, true), None);
        assert_eq!(connection_field(Version::Http10, true), Some("keep-alive"));
        assert_eq!(connection_field(Version::Http11, false), Some("close"));
        assert_eq!(connection_field(Version::Http10, false), Some("close"));
    }

    #[test]
    fn content_length_is_descriptive_only_on_304() {
        assert!(content_length_is_descriptive(304));
        for status in [100u16, 200, 204, 400, 500] {
            assert!(!content_length_is_descriptive(status), "{status}");
        }
    }

    #[test]
    fn emits_connection_close_when_not_keep_alive() {
        let got = render(Version::Http11, &head_of(200, &[]), &OutBody::None, false);
        assert!(got.contains("connection: close\r\n"));
    }

    #[test]
    fn omits_connection_header_when_keep_alive_on_http11() {
        let got = render(Version::Http11, &head_of(200, &[]), &OutBody::None, true);
        assert!(
            !got.contains("connection:"),
            "persistence is the HTTP/1.1 default; stating it is noise"
        );
    }

    #[test]
    fn emits_connection_keep_alive_on_http10() {
        let got = render(Version::Http10, &head_of(200, &[]), &OutBody::None, true);
        assert!(got.contains("connection: keep-alive\r\n"));
        assert!(got.starts_with("HTTP/1.0 200 OK\r\n"));
    }

    #[test]
    fn does_not_duplicate_handler_supplied_connection() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::Connection, "close")]),
            &OutBody::None,
            false,
        );
        assert_eq!(got.matches("connection").count(), 1);
    }

    /// The mirror image of request smuggling: a CRLF in a handler-supplied value
    /// ends the header section early, and everything after it is read as more
    /// header fields or as a body.
    #[test]
    fn drops_header_values_that_would_split_the_response() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::Location, "/a\r\nX-Injected: 1")]),
            &OutBody::None,
            true,
        );
        assert!(!got.contains("X-Injected"), "{got}");
        assert!(
            !got.contains("location"),
            "the whole field goes, not just the tail: {got}"
        );
    }

    #[test]
    fn drops_values_containing_bare_cr_lf_or_nul() {
        for bad in ["a\rb", "a\nb", "a\0b"] {
            let got = render(
                Version::Http11,
                &head_of(200, &[(HeaderId::Etag, bad)]),
                &OutBody::None,
                true,
            );
            assert!(!got.contains("etag"), "{bad:?} must be dropped: {got}");
        }
    }

    /// `HeaderId::Other` carries whatever a handler put in it; a name with a
    /// space or a colon splits the response just as a value does.
    #[test]
    fn drops_custom_field_names_that_are_not_tokens() {
        let mut headers = HeaderVec::new();
        for name in ["x bad", "x:bad", "x\r\nbad", ""] {
            headers.push((
                HeaderId::Other(crate::ByteStr::from(name)),
                Bytes::from_static(b"1"),
            ));
        }
        headers.push((
            HeaderId::Other(crate::ByteStr::from_static("x-good")),
            Bytes::from_static(b"1"),
        ));
        let got = render(
            Version::Http11,
            &ResponseHead {
                status: 200,
                headers,
            },
            &OutBody::None,
            true,
        );
        assert!(!got.contains("bad"), "{got}");
        assert!(got.contains("x-good: 1\r\n"), "{got}");
    }

    /// Dropping a field must not leave the response undelimited. The framing
    /// decision follows what was written, so a rejected `Content-Length` puts the
    /// writer back in charge of framing rather than suppressing it.
    #[test]
    fn a_dropped_content_length_does_not_suppress_framing() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::ContentLength, "5\r\nX-Injected: 1")]),
            &OutBody::Fixed(Bytes::from_static(b"hello")),
            true,
        );
        assert!(!got.contains("X-Injected"), "{got}");
        assert_eq!(got.matches("content-length").count(), 1, "{got}");
        assert!(got.contains("content-length: 5\r\n"), "{got}");
    }

    #[test]
    fn write_last_chunk_drops_an_injecting_trailer() {
        let mut trailers = HeaderVec::new();
        trailers.push((HeaderId::Etag, Bytes::from_static(b"x\r\nX-Injected: 1")));
        let mut out = BytesMut::new();
        write_last_chunk(&mut out, &trailers);
        assert_eq!(&out[..], b"0\r\n\r\n");
    }

    #[test]
    fn date_cache_reformats_only_on_second_change() {
        let mut c = DateCache::new();
        let t0 = SystemTime::UNIX_EPOCH + Duration::from_millis(1_500);
        let first = c.get(t0).to_vec();
        let same_second = c.get(t0 + Duration::from_millis(400)).to_vec();
        assert_eq!(first, same_second);
        let next_second = c.get(t0 + Duration::from_secs(1)).to_vec();
        assert_ne!(first, next_second);
    }

    #[test]
    fn date_format_is_imf_fixdate() {
        let mut c = DateCache::new();
        let d = c.get(SystemTime::UNIX_EPOCH);
        assert_eq!(d.len(), DATE_LEN);
        assert_eq!(d, EPOCH_DATE);
    }

    #[test]
    fn write_chunk_frames_correctly() {
        let mut out = BytesMut::new();
        write_chunk(&mut out, b"hello");
        assert_eq!(&out[..], b"5\r\nhello\r\n");

        let mut out = BytesMut::new();
        write_chunk(&mut out, &[0u8; 31]);
        assert!(out.starts_with(b"1f\r\n"), "sizes are lowercase hex");
    }

    #[test]
    fn write_last_chunk_without_trailers() {
        let mut out = BytesMut::new();
        write_last_chunk(&mut out, &HeaderVec::new());
        assert_eq!(&out[..], b"0\r\n\r\n");
    }

    #[test]
    fn write_last_chunk_with_trailers() {
        let mut trailers = HeaderVec::new();
        trailers.push((HeaderId::Etag, Bytes::from_static(b"x")));
        let mut out = BytesMut::new();
        write_last_chunk(&mut out, &trailers);
        assert_eq!(&out[..], b"0\r\netag: x\r\n\r\n");
    }

    /// The round trip that matters: what we write, our own parser must accept.
    #[test]
    fn written_head_parses_as_a_valid_message() {
        let got = render(
            Version::Http11,
            &head_of(200, &[(HeaderId::ContentType, "text/plain")]),
            &OutBody::Fixed(Bytes::from_static(b"hello")),
            true,
        );
        // Strict CRLF throughout, and exactly one blank-line terminator.
        assert!(crate::parse::prescan(got.as_bytes()).is_ok());
        assert_eq!(crate::parse::find_head_end(got.as_bytes()), Some(got.len()));
    }

    /// A handler `Content-Length` that disagrees with the body it framed must
    /// not reach the wire: the peer would read the difference as the start of
    /// the next response.
    #[test]
    fn a_wrong_handler_content_length_is_replaced_with_the_true_one() {
        let mut headers = HeaderVec::new();
        headers.push((HeaderId::ContentLength, Bytes::from_static(b"5")));
        let head = ResponseHead {
            status: 200,
            headers,
        };
        let mut out = BytesMut::new();
        let body = OutBody::Fixed(Bytes::from_static(b"hello world"));
        write_head(&mut out, Version::Http11, &head, &body, b"D", true);

        let text = String::from_utf8_lossy(&out).to_lowercase();
        assert_eq!(text.matches("content-length:").count(), 1, "{text}");
        assert!(text.contains("content-length: 11"), "{text}");
        assert!(!text.contains("content-length: 5"), "{text}");
    }

    /// An agreeing one is left alone, so a handler stays in charge of its own
    /// framing whenever it is telling the truth.
    #[test]
    fn a_correct_handler_content_length_is_preserved() {
        let mut headers = HeaderVec::new();
        headers.push((HeaderId::ContentLength, Bytes::from_static(b"11")));
        let head = ResponseHead {
            status: 200,
            headers,
        };
        let mut out = BytesMut::new();
        let body = OutBody::Fixed(Bytes::from_static(b"hello world"));
        write_head(&mut out, Version::Http11, &head, &body, b"D", true);

        let text = String::from_utf8_lossy(&out).to_lowercase();
        assert_eq!(text.matches("content-length:").count(), 1, "{text}");
        assert!(text.contains("content-length: 11"), "{text}");
    }
}
