//! Message body framing: the decision of how long the request body is.
//!
//! This is the most security-critical code in the crate. Request smuggling is,
//! in essence, two HTTP implementations disagreeing about where one message ends
//! and the next begins — so this module resolves nothing ambiguously. Every
//! unclear case is an error, and every error closes the connection.
//!
//! The function is deliberately pure and synchronous so it can be exhaustively
//! unit-tested and differentially fuzzed against another implementation.

use crate::header::HeaderId;
use crate::{Head, Limits, Method, Version};

/// How to read the request body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyKind {
    /// No body. A request without `Content-Length` or `Transfer-Encoding` has no
    /// body — unlike a response, whose length may run to end-of-stream.
    None,
    /// A body of exactly this many bytes.
    Length(u64),
    /// A chunked body, terminated by a zero-length chunk.
    Chunked,
}

/// An unresolvable or unacceptable framing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FramingError {
    /// Both `Content-Length` and `Transfer-Encoding` were present.
    #[error("both Content-Length and Transfer-Encoding present")]
    LengthAndTransferEncoding,
    /// `Content-Length` appeared more than once with differing values.
    #[error("conflicting Content-Length values")]
    DuplicateContentLength,
    /// `Content-Length` was not a bare decimal integer.
    #[error("malformed Content-Length")]
    InvalidContentLength,
    /// `chunked` was present but was not the final transfer coding.
    #[error("chunked is not the final transfer coding")]
    ChunkedNotFinal,
    /// A transfer coding other than `chunked` was requested.
    #[error("unsupported transfer coding")]
    UnsupportedTransferEncoding,
    /// `Transfer-Encoding` appeared on an HTTP/1.0 request.
    #[error("Transfer-Encoding on an HTTP/1.0 request")]
    TransferEncodingOnHttp10,
    /// HTTP/1.1 requires exactly one `Host`.
    #[error("missing Host")]
    MissingHost,
    /// More than one `Host` is ambiguous at any version.
    #[error("multiple Host fields")]
    MultipleHost,
    /// `Host` was not a `uri-host [ ":" port ]` (RFC 9112 section 3.2). An empty
    /// `Host` is valid: it is how a client says the target has no authority.
    #[error("invalid Host")]
    InvalidHost,
    /// An absolute-form target's authority was empty, carried userinfo, or was
    /// not a `uri-host [ ":" port ]`; or a CONNECT target was not exactly
    /// `uri-host ":" port`.
    #[error("invalid authority in the request target")]
    InvalidAuthority,
    /// An absolute-form target named a scheme other than `http` or `https`.
    #[error("unsupported scheme in the request target")]
    UnsupportedScheme,
    /// `Host` named a different authority than an absolute-form or CONNECT
    /// target.
    #[error("Host disagrees with the request target's authority")]
    HostMismatch,
    /// The declared body exceeded `Limits::max_body_bytes`.
    #[error("declared body too large")]
    BodyTooLarge,
}

impl FramingError {
    /// The status code to answer with before closing the connection.
    #[inline]
    pub fn status(&self) -> u16 {
        match self {
            FramingError::BodyTooLarge => 413,
            FramingError::UnsupportedTransferEncoding => 501,
            _ => 400,
        }
    }
}

/// Validate `Host` and, for an absolute-form target, the target's authority.
///
/// RFC 9112 section 3.2.2 has the target's authority win over `Host`, which
/// leaves two names for one request. Anything that reads `Host` instead of
/// [`Head::authority`] — a virtual-host router, an allowlist, a log line, the
/// next hop — would act on the losing one, so a disagreement is refused here
/// rather than resolved silently, the same answer this module gives every
/// other pair of fields that could be read two ways.
fn check_authority(head: &Head) -> Result<(), FramingError> {
    // An empty `Host` is what RFC 9112 section 3.2 requires of a client whose
    // target URI has no authority, so it is well-formed — it just names
    // nothing, and there is nothing for it to disagree with.
    let host = match head.get(&HeaderId::Host) {
        Some(v) if !v.is_empty() => Some(parse_authority(v).ok_or(FramingError::InvalidHost)?),
        _ => None,
    };
    // CONNECT's authority-form target (RFC 9112 section 3.2.3) is the
    // request's authority just as an absolute-form one is, and is held to the
    // same rules: a real `uri-host ":" port`, no userinfo, and no `Host`
    // naming somewhere else.
    if head.method == Method::Connect {
        let (target_host, target_port) = parse_authority(head.target().as_bytes())
            .filter(|(_, port)| port.is_some())
            .ok_or(FramingError::InvalidAuthority)?;
        if let Some((host, port)) = host {
            let same = host.eq_ignore_ascii_case(target_host)
                && port.is_none_or(|p| Some(p) == target_port);
            if !same {
                return Err(FramingError::HostMismatch);
            }
        }
        return Ok(());
    }
    let Some(scheme) = head.absolute_scheme() else {
        return Ok(());
    };
    let default_port = if scheme.eq_ignore_ascii_case("http") {
        80
    } else if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        return Err(FramingError::UnsupportedScheme);
    };
    let authority = head.authority().unwrap_or_default();
    let (target_host, target_port) =
        parse_authority(authority.as_bytes()).ok_or(FramingError::InvalidAuthority)?;
    if let Some((host, port)) = host {
        let same = host.eq_ignore_ascii_case(target_host)
            && port.unwrap_or(default_port) == target_port.unwrap_or(default_port);
        if !same {
            return Err(FramingError::HostMismatch);
        }
    }
    Ok(())
}

/// Split an authority into its host and port, or `None` unless it is exactly
/// `uri-host [ ":" port ]` (RFC 3986 section 3.2).
///
/// No userinfo (`@` is not a host character, so it fails here): RFC 9110
/// section 4.2.4 deprecates it in `http` URIs, and `http://trusted@evil/` is
/// the classic way to make one reader see a different host than another. An
/// empty port (`host:`) is allowed and means the default.
fn parse_authority(a: &[u8]) -> Option<(&[u8], Option<u16>)> {
    let (host, rest) = if a.first() == Some(&b'[') {
        let close = a.iter().position(|&b| b == b']')?;
        let inner = &a[1..close];
        if inner.is_empty()
            || !inner
                .iter()
                .all(|b| b.is_ascii_hexdigit() || matches!(b, b':' | b'.'))
        {
            return None;
        }
        (&a[..=close], &a[close + 1..])
    } else {
        let end = a.iter().position(|&b| b == b':').unwrap_or(a.len());
        let host = &a[..end];
        if host.is_empty() || !valid_reg_name(host) {
            return None;
        }
        (host, &a[end..])
    };
    let port = match rest {
        [] | [b':'] => None,
        [b':', digits @ ..] => {
            if digits.len() > 5 || !digits.iter().all(u8::is_ascii_digit) {
                return None;
            }
            let n = digits
                .iter()
                .fold(0u32, |n, d| n * 10 + u32::from(d - b'0'));
            Some(u16::try_from(n).ok()?)
        }
        _ => return None,
    };
    Some((host, port))
}

/// `reg-name` (RFC 3986 section 3.2.2), which also covers an IPv4 address:
/// unreserved characters, sub-delims, and well-formed percent-encodings.
fn valid_reg_name(host: &[u8]) -> bool {
    let mut i = 0;
    while i < host.len() {
        match host[i] {
            b'%' => {
                if !host
                    .get(i + 1..i + 3)
                    .is_some_and(|h| h.iter().all(u8::is_ascii_hexdigit))
                {
                    return false;
                }
                i += 3;
                continue;
            }
            b if b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'-' | b'.'
                        | b'_'
                        | b'~'
                        | b'!'
                        | b'$'
                        | b'&'
                        | b'\''
                        | b'('
                        | b')'
                        | b'*'
                        | b'+'
                        | b','
                        | b';'
                        | b'='
                ) => {}
            _ => return false,
        }
        i += 1;
    }
    true
}

/// Decide how to frame the request body.
///
/// Rule order is fixed and load-bearing:
///
/// 1. `Host` validity, and its agreement with an absolute-form target
/// 2. `Content-Length`-with-`Transfer-Encoding` conflict
/// 3. `Transfer-Encoding` analysis
/// 4. `Content-Length` analysis
/// 5. Declared-size limit
///
/// Step 2 precedes 3 and 4 so that a request carrying a *valid* value of each is
/// rejected outright rather than silently resolved in favor of one. That silent
/// resolution — and disagreement between peers about which one wins — is the
/// smuggling vector.
pub fn decide(head: &Head, limits: &Limits) -> Result<BodyKind, FramingError> {
    // Everything the rules below need about *presence* comes from one walk of
    // the field list. Asking `count()` three times and then iterating again per
    // field walked a 96-entry list up to five times over, for information a
    // single pass already has.
    let mut host_count = 0usize;
    let mut has_len = false;
    let mut has_te = false;
    for (id, _) in head.headers.iter() {
        match id {
            HeaderId::Host => host_count += 1,
            HeaderId::ContentLength => has_len = true,
            HeaderId::TransferEncoding => has_te = true,
            _ => {}
        }
    }

    // 1. Host. More than one is ambiguous at any version; HTTP/1.1 additionally
    //    requires at least one (RFC 9112 section 3.2).
    match host_count {
        0 if head.version == Version::Http11 => return Err(FramingError::MissingHost),
        n if n > 1 => return Err(FramingError::MultipleHost),
        _ => {}
    }
    check_authority(head)?;

    // 2. Conflict. Checked before either field is analyzed.
    if has_len && has_te {
        return Err(FramingError::LengthAndTransferEncoding);
    }

    // 3. Transfer-Encoding. Codings accumulate across repeated fields in wire
    //    order, exactly as if they had been sent as one comma list.
    if has_te {
        // RFC 9112 section 6.1: a server must not reuse a connection after
        // receiving Transfer-Encoding in an HTTP/1.0 request. An HTTP/1.0 sender
        // has no business emitting one at all, and a hop that ignores it and
        // reads the body as unframed while we read it as chunked is the
        // TE-downgrade smuggling vector. Rejecting closes the connection, which
        // covers the "must not reuse" requirement as a side effect.
        if head.version == Version::Http10 {
            return Err(FramingError::TransferEncodingOnHttp10);
        }

        // Work on bytes, never `str`: `str::trim` strips *Unicode* whitespace,
        // so `chunked\xC2\xA0` (NBSP) would become `chunked` here while a front
        // proxy that only strips SP/HTAB sees an unknown coding. Any non-ASCII
        // byte in a framing field is rejected outright.
        let mut codings: smallvec::SmallVec<[&[u8]; 4]> = smallvec::SmallVec::new();
        for value in head.all(&HeaderId::TransferEncoding) {
            if !value.is_ascii() {
                return Err(FramingError::ChunkedNotFinal);
            }
            for coding in value.split(|&b| b == b',') {
                let coding = trim_ows(coding);
                if !coding.is_empty() {
                    codings.push(coding);
                }
            }
        }

        let chunked_count = codings
            .iter()
            .filter(|c| c.eq_ignore_ascii_case(b"chunked"))
            .count();

        return match chunked_count {
            // A single chunked coding, and it must be last.
            1 if codings
                .last()
                .is_some_and(|c| c.eq_ignore_ascii_case(b"chunked")) =>
            {
                if codings.len() == 1 {
                    Ok(BodyKind::Chunked)
                } else {
                    // e.g. `gzip, chunked`: chunked frames the message, but an
                    // inner coding we cannot decode remains. 501 rather than
                    // silently handing the handler compressed bytes.
                    Err(FramingError::UnsupportedTransferEncoding)
                }
            }
            // Present but not final, or present more than once: the message
            // length is undetermined.
            n if n >= 1 => Err(FramingError::ChunkedNotFinal),
            // No chunked coding at all: some coding we do not implement.
            _ => Err(FramingError::UnsupportedTransferEncoding),
        };
    }

    // 4. Content-Length. Every value across every field, and every element of
    //    every comma list, must parse identically (RFC 9112 section 6.3).
    if has_len {
        let mut agreed: Option<u64> = None;
        for value in head.all(&HeaderId::ContentLength) {
            // Bytes, not `str`, for the same reason as Transfer-Encoding above:
            // only SP/HTAB is optional whitespace, and `parse_content_length`
            // rejects every non-digit byte, non-ASCII included.
            for element in value.split(|&b| b == b',') {
                let n = parse_content_length(trim_ows(element))?;
                match agreed {
                    None => agreed = Some(n),
                    Some(prev) if prev == n => {}
                    Some(_) => return Err(FramingError::DuplicateContentLength),
                }
            }
        }
        let len = agreed.ok_or(FramingError::InvalidContentLength)?;

        // 5. Declared-size limit, before a single body byte is read.
        if len > limits.max_body_bytes {
            return Err(FramingError::BodyTooLarge);
        }
        return Ok(BodyKind::Length(len));
    }

    // Neither field: a request has no body.
    Ok(BodyKind::None)
}

/// Parse one `Content-Length` element as a bare decimal integer.
///
/// Rejects the empty string, signs, whitespace, non-digits, and overflow.
/// `str::parse::<u64>` would accept a leading `+`, which RFC 9112 does not, so
/// digits are checked explicitly.
#[inline]
fn parse_content_length(bytes: &[u8]) -> Result<u64, FramingError> {
    if bytes.is_empty() {
        return Err(FramingError::InvalidContentLength);
    }
    bytes.iter().try_fold(0u64, |acc, &b| {
        if !b.is_ascii_digit() {
            return Err(FramingError::InvalidContentLength);
        }
        acc.checked_mul(10)
            .and_then(|n| n.checked_add(u64::from(b - b'0')))
            .ok_or(FramingError::InvalidContentLength)
    })
}

/// Strip RFC 9110 optional whitespace (`OWS = *( SP / HTAB )`) from both ends.
///
/// Deliberately *not* `str::trim` or `<[u8]>::trim_ascii`: the former strips
/// Unicode whitespace (NBSP, NEL, ideographic space) and the latter also strips
/// CR, LF and FF. Framing must agree byte-for-byte with every other hop, so only
/// the two bytes the grammar allows are removed.
#[inline]
pub(crate) fn trim_ows(mut s: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = s {
        s = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = s {
        s = rest;
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Limits, parse_head};
    use bytes::Bytes;

    /// Build a head from a raw request, asserting it parses.
    fn head(raw: &'static [u8]) -> crate::Head {
        parse_head(&Bytes::from_static(raw), &Limits::default())
            .expect("must parse")
            .expect("must be complete")
            .0
    }

    fn decide_raw(raw: &'static [u8]) -> Result<BodyKind, FramingError> {
        decide(&head(raw), &Limits::default())
    }

    // ---- Host and absolute-form authority ----

    #[test]
    fn absolute_form_with_a_matching_host_is_accepted() {
        for raw in [
            &b"GET http://a.example/x HTTP/1.1\r\nHost: a.example\r\n\r\n"[..],
            b"GET http://a.example/x HTTP/1.1\r\nHost: A.EXAMPLE:80\r\n\r\n",
            b"GET https://a.example:443/ HTTP/1.1\r\nHost: a.example\r\n\r\n",
            b"GET HTTP://[::1]:8080/ HTTP/1.1\r\nHost: [::1]:8080\r\n\r\n",
            b"GET http://a.example/ HTTP/1.0\r\n\r\n",
        ] {
            assert_eq!(
                decide(&head(raw), &Limits::default()),
                Ok(BodyKind::None),
                "{raw:?}"
            );
        }
    }

    #[test]
    fn host_disagreeing_with_the_target_authority_is_rejected() {
        for raw in [
            &b"GET http://a.example/ HTTP/1.1\r\nHost: b.example\r\n\r\n"[..],
            b"GET http://a.example/ HTTP/1.1\r\nHost: a.example:8080\r\n\r\n",
            b"GET https://a.example/ HTTP/1.1\r\nHost: a.example:80\r\n\r\n",
        ] {
            assert_eq!(
                decide(&head(raw), &Limits::default()),
                Err(FramingError::HostMismatch),
                "{raw:?}"
            );
        }
    }

    #[test]
    fn target_userinfo_or_a_bad_authority_is_rejected() {
        for raw in [
            &b"GET http://trusted@evil.example/ HTTP/1.1\r\nHost: evil.example\r\n\r\n"[..],
            b"GET http:///x HTTP/1.1\r\nHost: a\r\n\r\n",
            b"GET http://a.example:99999/ HTTP/1.1\r\nHost: a.example\r\n\r\n",
            b"GET http://a%zz/ HTTP/1.1\r\nHost: a\r\n\r\n",
        ] {
            assert_eq!(
                decide(&head(raw), &Limits::default()),
                Err(FramingError::InvalidAuthority),
                "{raw:?}"
            );
        }
    }

    /// CONNECT's target is its authority, held to the same rules.
    #[test]
    fn connect_targets_are_validated_and_reconciled_with_host() {
        let ok = |raw: &'static [u8]| decide(&head(raw), &Limits::default());
        assert_eq!(
            ok(b"CONNECT a.example:443 HTTP/1.1\r\nHost: a.example:443\r\n\r\n"),
            Ok(BodyKind::None)
        );
        assert_eq!(
            ok(b"CONNECT a.example:443 HTTP/1.1\r\nHost: A.example\r\n\r\n"),
            Ok(BodyKind::None)
        );
        assert_eq!(
            ok(b"CONNECT trusted@evil.example:443 HTTP/1.1\r\nHost: good.example\r\n\r\n"),
            Err(FramingError::InvalidAuthority)
        );
        assert_eq!(
            ok(b"CONNECT a.example:443 HTTP/1.1\r\nHost: good.example\r\n\r\n"),
            Err(FramingError::HostMismatch)
        );
        assert_eq!(
            ok(b"CONNECT a.example:443 HTTP/1.1\r\nHost: a.example:8443\r\n\r\n"),
            Err(FramingError::HostMismatch)
        );
    }

    #[test]
    fn a_scheme_other_than_http_is_rejected() {
        assert_eq!(
            decide_raw(b"GET ftp://a.example/ HTTP/1.1\r\nHost: a.example\r\n\r\n"),
            Err(FramingError::UnsupportedScheme)
        );
    }

    #[test]
    fn a_malformed_host_is_rejected() {
        for raw in [
            &b"GET / HTTP/1.1\r\nHost: user@a\r\n\r\n"[..],
            b"GET / HTTP/1.1\r\nHost: a/b\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a:port\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: [zz]\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: a b\r\n\r\n",
        ] {
            assert_eq!(
                decide(&head(raw), &Limits::default()),
                Err(FramingError::InvalidHost),
                "{raw:?}"
            );
        }
        assert_eq!(
            decide_raw(b"GET / HTTP/1.1\r\nHost: a:\r\n\r\n"),
            Ok(BodyKind::None)
        );
    }

    /// RFC 9112 section 3.2: an empty `Host` is how a client says the target
    /// URI has no authority. It is accepted, and names nothing to compare.
    #[test]
    fn an_empty_host_is_accepted() {
        for raw in [
            &b"OPTIONS * HTTP/1.1\r\nHost:\r\n\r\n"[..],
            b"GET / HTTP/1.1\r\nHost: \r\n\r\n",
        ] {
            assert_eq!(
                decide(&head(raw), &Limits::default()),
                Ok(BodyKind::None),
                "{raw:?}"
            );
        }
    }

    // ---- positive cases ----

    #[test]
    fn no_framing_headers_means_no_body() {
        assert_eq!(
            decide_raw(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n"),
            Ok(BodyKind::None)
        );
    }

    #[test]
    fn content_length_gives_a_fixed_body() {
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\n"),
            Ok(BodyKind::Length(5))
        );
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n"),
            Ok(BodyKind::Length(0))
        );
    }

    #[test]
    fn transfer_encoding_chunked_gives_a_chunked_body() {
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"),
            Ok(BodyKind::Chunked)
        );
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: CHUNKED\r\n\r\n"),
            Ok(BodyKind::Chunked),
            "transfer codings are case-insensitive"
        );
    }

    /// RFC 9112 section 6.3: duplicate Content-Length fields with identical
    /// values may be treated as one.
    #[test]
    fn identical_duplicate_content_length_is_accepted() {
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n"
            ),
            Ok(BodyKind::Length(5))
        );
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5, 5\r\n\r\n"),
            Ok(BodyKind::Length(5)),
            "a comma list of identical values is the same case"
        );
    }

    #[test]
    fn http_10_needs_no_host() {
        assert_eq!(decide_raw(b"GET / HTTP/1.0\r\n\r\n"), Ok(BodyKind::None));
    }

    // ---- the rejection table from the spec ----

    /// RFC 9112 section 6.1: both present is unresolvable ambiguity.
    #[test]
    fn rejects_content_length_with_transfer_encoding() {
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n"
            ),
            Err(FramingError::LengthAndTransferEncoding)
        );
        // Order on the wire must not change the outcome.
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n"
            ),
            Err(FramingError::LengthAndTransferEncoding)
        );
    }

    #[test]
    fn rejects_conflicting_content_length() {
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n"
            ),
            Err(FramingError::DuplicateContentLength)
        );
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5, 6\r\n\r\n"),
            Err(FramingError::DuplicateContentLength)
        );
    }

    #[test]
    fn rejects_malformed_content_length() {
        for raw in [
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: abc\r\n\r\n"[..],
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: \r\n\r\n"[..],
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: +5\r\n\r\n"[..],
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: -5\r\n\r\n"[..],
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5x\r\n\r\n"[..],
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 0x5\r\n\r\n"[..],
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5 5\r\n\r\n"[..],
            // u64 overflow must not wrap.
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 99999999999999999999999\r\n\r\n"[..],
        ] {
            let h = parse_head(&Bytes::copy_from_slice(raw), &Limits::default())
                .unwrap()
                .unwrap()
                .0;
            assert_eq!(
                decide(&h, &Limits::default()),
                Err(FramingError::InvalidContentLength),
                "should have rejected: {}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    /// RFC 9112 section 6.1: chunked must be the final coding, or the message
    /// length is undetermined.
    #[test]
    fn rejects_chunked_not_final() {
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked, gzip\r\n\r\n"),
            Err(FramingError::ChunkedNotFinal)
        );
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: gzip\r\n\r\n"
            ),
            Err(FramingError::ChunkedNotFinal),
            "codings accumulate across repeated fields"
        );
    }

    /// Two chunked codings would mean two framing layers; reject rather than
    /// pick one.
    #[test]
    fn rejects_repeated_chunked() {
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked, chunked\r\n\r\n"
            ),
            Err(FramingError::ChunkedNotFinal)
        );
    }

    #[test]
    fn rejects_unsupported_transfer_coding() {
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip\r\n\r\n"),
            Err(FramingError::UnsupportedTransferEncoding)
        );
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: identity\r\n\r\n"),
            Err(FramingError::UnsupportedTransferEncoding)
        );
    }

    #[test]
    fn rejects_missing_or_multiple_host_on_http_11() {
        assert_eq!(
            decide_raw(b"GET / HTTP/1.1\r\n\r\n"),
            Err(FramingError::MissingHost)
        );
        assert_eq!(
            decide_raw(b"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n"),
            Err(FramingError::MultipleHost)
        );
    }

    /// Multiple Host fields are ambiguous regardless of version.
    #[test]
    fn rejects_multiple_host_on_http_10_too() {
        assert_eq!(
            decide_raw(b"GET / HTTP/1.0\r\nHost: a\r\nHost: b\r\n\r\n"),
            Err(FramingError::MultipleHost)
        );
    }

    /// RFC 9112 section 6.1. An HTTP/1.0 sender cannot legitimately use chunked,
    /// so a `Transfer-Encoding` here is a downgrade attempt: a hop that reads the
    /// body as unframed while we read it as chunked disagrees about where the
    /// message ends.
    #[test]
    fn rejects_transfer_encoding_on_http_10() {
        assert_eq!(
            decide_raw(b"POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n"),
            Err(FramingError::TransferEncodingOnHttp10)
        );
        assert_eq!(
            decide_raw(b"POST / HTTP/1.0\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"),
            Err(FramingError::TransferEncodingOnHttp10)
        );
        // An unsupported coding on HTTP/1.0 is rejected for the version, before
        // the coding is even looked at.
        assert_eq!(
            decide_raw(b"POST / HTTP/1.0\r\nTransfer-Encoding: gzip\r\n\r\n"),
            Err(FramingError::TransferEncodingOnHttp10)
        );
    }

    /// The conflict rule still wins: a version violation must not mask the
    /// ambiguity that both framing fields together create.
    #[test]
    fn conflict_precedes_the_http_10_transfer_encoding_rule() {
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.0\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n"
            ),
            Err(FramingError::LengthAndTransferEncoding)
        );
    }

    #[test]
    fn rejects_oversized_declared_body() {
        let limits = Limits {
            max_body_bytes: 4,
            ..Default::default()
        };
        let h = head(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\n");
        assert_eq!(decide(&h, &limits), Err(FramingError::BodyTooLarge));
    }

    // ---- ordering: the conflict check must win ----

    /// A request carrying both a *valid* Content-Length and a *valid*
    /// Transfer-Encoding must be rejected as a conflict, never resolved in favor
    /// of one. This ordering is the smuggling defense.
    #[test]
    fn conflict_check_precedes_individual_analysis() {
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n"
            ),
            Err(FramingError::LengthAndTransferEncoding),
            "must not report ChunkedNotFinal or a Length body"
        );
    }

    /// An invalid Content-Length alongside a Transfer-Encoding is still first
    /// and foremost a conflict.
    #[test]
    fn conflict_reported_even_when_content_length_is_garbage() {
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: abc\r\nTransfer-Encoding: chunked\r\n\r\n"
            ),
            Err(FramingError::LengthAndTransferEncoding)
        );
    }

    /// H16: `str::trim` strips Unicode whitespace, so NBSP (`C2 A0`), NEL
    /// (`C2 85`) and IDEOGRAPHIC SPACE (`E3 80 80`) padding used to vanish and
    /// `chunked<NBSP>` framed the body as chunked while a proxy that trims only
    /// SP/HTAB saw an unknown coding. Every such value must now be rejected.
    #[test]
    fn rejects_unicode_whitespace_around_transfer_encoding() {
        for raw in [
            &b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\xC2\xA0\r\n\r\n"[..],
            b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\xC2\x85\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: \xE3\x80\x80chunked\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip,\xC2\xA0chunked\r\n\r\n",
        ] {
            let head = parse_head(&Bytes::copy_from_slice(raw), &Limits::default())
                .expect("httparse admits obs-text")
                .expect("complete")
                .0;
            let got = decide(&head, &Limits::default());
            assert!(
                got.is_err(),
                "{:?} -> {got:?}",
                String::from_utf8_lossy(raw)
            );
            assert_ne!(got, Ok(BodyKind::Chunked));
        }
    }

    #[test]
    fn rejects_unicode_whitespace_around_content_length() {
        for raw in [
            &b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\xC2\xA0\r\n\r\n"[..],
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\xC2\x85\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: \xE3\x80\x805\r\n\r\n",
            b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5,\xC2\xA05\r\n\r\n",
        ] {
            let head = parse_head(&Bytes::copy_from_slice(raw), &Limits::default())
                .expect("httparse admits obs-text")
                .expect("complete")
                .0;
            assert_eq!(
                decide(&head, &Limits::default()),
                Err(FramingError::InvalidContentLength),
                "{:?}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    /// Only SP and HTAB are optional whitespace around list elements.
    #[test]
    fn ows_around_list_elements_is_still_accepted() {
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5 ,\t5\r\n\r\n"),
            Ok(BodyKind::Length(5))
        );
        assert_eq!(
            decide_raw(b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: ,\tchunked \r\n\r\n"),
            Ok(BodyKind::Chunked)
        );
        assert_eq!(trim_ows(b" \t a \t"), b"a");
        assert_eq!(trim_ows(b"\x0ca\x0c"), b"\x0ca\x0c", "FF is not OWS");
        assert_eq!(trim_ows(b"   "), b"");
    }

    #[test]
    fn content_length_overflow_is_rejected() {
        assert_eq!(
            decide_raw(
                b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 18446744073709551616\r\n\r\n"
            ),
            Err(FramingError::InvalidContentLength)
        );
    }

    #[test]
    fn status_codes_map_correctly() {
        assert_eq!(FramingError::LengthAndTransferEncoding.status(), 400);
        assert_eq!(FramingError::DuplicateContentLength.status(), 400);
        assert_eq!(FramingError::InvalidContentLength.status(), 400);
        assert_eq!(FramingError::ChunkedNotFinal.status(), 400);
        assert_eq!(FramingError::MissingHost.status(), 400);
        assert_eq!(FramingError::MultipleHost.status(), 400);
        assert_eq!(FramingError::UnsupportedTransferEncoding.status(), 501);
        assert_eq!(FramingError::TransferEncodingOnHttp10.status(), 400);
        assert_eq!(FramingError::BodyTooLarge.status(), 413);
    }
}
