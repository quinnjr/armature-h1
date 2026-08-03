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

// No production caller exists yet (wired in Task 7); keep the type from
// tripping dead-code lints under `--all-targets` in the meantime.
#[allow(dead_code)]
pub(crate) struct HyperBodyIo<B> {
    body: B,
    /// Bytes the `Body` took but did not consume, or pushed back.
    stash: Bytes,
    trailers_slot: Rc<RefCell<Option<HeaderVec>>>,
}

#[allow(dead_code)] // wired in a later task
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
