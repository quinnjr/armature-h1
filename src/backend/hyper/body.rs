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
                        Ok(data) => {
                            if data.is_empty() {
                                // An empty data frame is not EOF in hyper's
                                // framing, but `BodyIo::poll_fill`'s contract
                                // treats an empty return as EOF. Skip it so a
                                // zero-length frame doesn't truncate the body.
                                continue;
                            }
                            return Poll::Ready(Ok(data));
                        }
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

/// A response body in hyper's dialect.
///
/// Framing choice is delegated to hyper via `SizeHint`: exact hints yield
/// `Content-Length`, the absence of one yields chunked — the same decision
/// table as the native writer's `OutBody`.
pub(crate) struct HyperOutBody {
    body: crate::service::ResponseBody,
    done: bool,
    /// Told when the body has no more frames, which is half of the clock's
    /// "this response is finished" signal (the other half is a drained write
    /// buffer). Without it a keep-alive connection could not tell an idle wait
    /// from a stalled write.
    clock: Rc<super::PhaseClock>,
}

impl HyperOutBody {
    pub(crate) fn new(body: crate::service::ResponseBody, clock: Rc<super::PhaseClock>) -> Self {
        // A zero-length body is finished before it starts. That has to include
        // an *empty* `Full`, not just `Empty`: hyper reads the size hint, sees
        // `Content-Length: 0`, and closes the message out without ever polling
        // the body — so `poll_frame` is not a signal that would ever arrive.
        let done = match &body {
            crate::service::ResponseBody::Empty => true,
            crate::service::ResponseBody::Full(b) => b.is_empty(),
            crate::service::ResponseBody::Stream(_) => false,
        };
        if done {
            clock.note_body_end();
        }
        Self { body, done, clock }
    }

    /// Mark the body finished, once.
    fn finish(&mut self) {
        self.done = true;
        self.clock.note_body_end();
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
                self.finish();
                Poll::Ready(None)
            }
            crate::service::ResponseBody::Full(b) => {
                let data = std::mem::take(b);
                self.finish();
                if data.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(::hyper::body::Frame::data(data))))
                }
            }
            crate::service::ResponseBody::Stream(s) => match s.as_mut().poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    self.finish();
                    Poll::Ready(None)
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    Poll::Ready(Some(Ok(::hyper::body::Frame::data(chunk))))
                }
                Poll::Ready(Some(Err(e))) => {
                    // Mid-stream failure: erroring the body makes hyper drop
                    // the connection without a terminating chunk, which is the
                    // native loop's Disposition::Close for the same case.
                    self.finish();
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

#[cfg(test)]
mod tests {
    use super::super::{Phase, PhaseClock};
    use super::*;
    use crate::header::HeaderId;
    use bytes::Bytes;
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    fn out_clock() -> Rc<PhaseClock> {
        Rc::new(PhaseClock::new(Phase::Write))
    }

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
    async fn skips_empty_data_frames_instead_of_treating_them_as_eof() {
        let mut io = body_io(vec![
            ::hyper::body::Frame::data(Bytes::new()),
            ::hyper::body::Frame::data(Bytes::from_static(b"rest")),
        ]);
        assert_eq!(&fill(&mut io).await.unwrap()[..], b"rest");
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

    use crate::service::{ResponseBody, futures_stream};

    fn poll_out(
        b: &mut HyperOutBody,
    ) -> Poll<Option<Result<::hyper::body::Frame<Bytes>, crate::service::BodyError>>> {
        use ::hyper::body::Body as _;
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        Pin::new(b).poll_frame(&mut cx)
    }

    #[test]
    fn empty_body_ends_immediately_with_exact_zero_hint() {
        let mut b = HyperOutBody::new(ResponseBody::Empty, out_clock());
        assert!(::hyper::body::Body::is_end_stream(&b));
        assert_eq!(::hyper::body::Body::size_hint(&b).exact(), Some(0));
        assert!(matches!(poll_out(&mut b), Poll::Ready(None)));
    }

    #[test]
    fn full_body_yields_one_frame_with_exact_hint() {
        let mut b = HyperOutBody::new(
            ResponseBody::Full(Bytes::from_static(b"hello")),
            out_clock(),
        );
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
        let mut b = HyperOutBody::new(ResponseBody::Stream(Box::pin(Two(0))), out_clock());
        assert_eq!(::hyper::body::Body::size_hint(&b).exact(), None);
        let Poll::Ready(Some(Ok(f))) = poll_out(&mut b) else {
            panic!()
        };
        assert_eq!(&f.into_data().unwrap()[..], b"a");
        let Poll::Ready(Some(Ok(f))) = poll_out(&mut b) else {
            panic!()
        };
        assert_eq!(&f.into_data().unwrap()[..], b"b");
        assert!(matches!(poll_out(&mut b), Poll::Ready(None)));
    }
}
