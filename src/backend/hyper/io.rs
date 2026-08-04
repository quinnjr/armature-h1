//! Hand-rolled tokio <-> `hyper::rt` IO adapter.
//!
//! hyper-util's `TokioIo` would do this, but pulling a dependency in for ~50
//! lines is the wrong trade for this crate's dependency posture. The inner
//! state is `Rc`-shared so the deadline watchdog can write a 408 after
//! dropping hyper's serve future, and so upgrade recovery can hand the
//! transport back out as a `Box<dyn Transport>`.

use super::{Phase, PhaseClock};
use bytes::Bytes;
use std::cell::RefCell;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub(crate) struct IoShared<IO> {
    pub(crate) io: IO,
    /// Bytes read before hyper took over (h2c sniff read-ahead). Served first.
    pub(crate) buffered: Bytes,
    /// The watchdog's phase clock, driven from both directions of the wire.
    ///
    /// A nonzero read while the connection is idle is the first byte of a
    /// request head — the native loop's idle -> header transition; signalling it
    /// here rather than polling a flag keeps it prompt even when
    /// `idle_timeout < header_timeout`. A nonzero *write* is response progress,
    /// which is what re-arms `write_timeout` the way the native writer re-arms
    /// per flush.
    pub(crate) clock: Rc<PhaseClock>,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> IoShared<IO> {
    /// Fill `out` from the prepend buffer first, then the transport.
    fn poll_read_into(
        &mut self,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.buffered.is_empty() {
            let n = self.buffered.len().min(out.remaining());
            // A zero-remaining `ReadBuf` moves nothing, so it is not the
            // "first byte of a request" the phase transition is about; the
            // transport read below applies the same `> before` rule.
            if n > 0 {
                out.put_slice(&self.buffered.split_to(n));
                self.note_read();
            }
            return Poll::Ready(Ok(()));
        }
        let before = out.filled().len();
        let poll = Pin::new(&mut self.io).poll_read(cx, out);
        if matches!(poll, Poll::Ready(Ok(()))) && out.filled().len() > before {
            self.note_read();
        }
        poll
    }

    /// Move `Idle -> Head` on the first byte of a new request.
    ///
    /// Deliberately *only* from `Idle`. Bytes arriving in `Write` are a
    /// pipelined head landing while a response is still going out; re-phasing
    /// to `Head` there would put `header_timeout` over the remainder of a
    /// perfectly healthy stream and kill it. See the module docs.
    fn note_read(&self) {
        if self.clock.phase() == Phase::Idle {
            self.clock.set(Phase::Head);
        }
    }

    /// Report bytes reaching the transport, which re-arms a write deadline.
    fn note_write(&self, n: usize) {
        if n > 0 {
            self.clock.note_write();
        }
    }

    /// Write through to the transport, reporting progress to the clock.
    fn poll_write_from(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.io).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            self.note_write(*n);
        }
        poll
    }

    /// Flush through to the transport, reporting completion to the clock.
    ///
    /// A successful flush means hyper owes the wire nothing more *right now*;
    /// combined with an exhausted response body that is the end of the
    /// response, and the clock uses it to return to `Idle`.
    fn poll_flush_from(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let poll = Pin::new(&mut self.io).poll_flush(cx);
        if matches!(poll, Poll::Ready(Ok(()))) {
            self.clock.note_flush();
        }
        poll
    }
}

pub(crate) struct HyperIo<IO>(pub(crate) Rc<RefCell<IoShared<IO>>>);

impl<IO: AsyncRead + AsyncWrite + Unpin> HyperIo<IO> {
    pub(crate) fn new(
        io: IO,
        buffered: Bytes,
        clock: Rc<PhaseClock>,
    ) -> (Self, Rc<RefCell<IoShared<IO>>>) {
        let shared = Rc::new(RefCell::new(IoShared {
            io,
            buffered,
            clock,
        }));
        (Self(shared.clone()), shared)
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> ::hyper::rt::Read for HyperIo<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ::hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        // `ReadBufCursor`'s uninit API is unsafe; this crate forbids unsafe.
        // Read into a small initialized scratch and copy via the safe
        // `put_slice`. One memcpy per read, same trade `conn.rs` documents for
        // its zeroed read buffer.
        let mut scratch = [0u8; 8 * 1024];
        let want = scratch.len().min(buf.remaining());
        let mut read_buf = ReadBuf::new(&mut scratch[..want]);
        match self.0.borrow_mut().poll_read_into(cx, &mut read_buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {
                buf.put_slice(read_buf.filled());
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> ::hyper::rt::Write for HyperIo<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.borrow_mut().poll_write_from(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.borrow_mut().poll_flush_from(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_shutdown(cx)
    }
}

/// Tokio-flavored view over the same shared state, for upgrade handoff and the
/// watchdog's post-drop error write.
pub(crate) struct SharedIo<IO>(pub(crate) Rc<RefCell<IoShared<IO>>>);

impl<IO: AsyncRead + AsyncWrite + Unpin> AsyncRead for SharedIo<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.0.borrow_mut().poll_read_into(cx, buf)
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> AsyncWrite for SharedIo<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.borrow_mut().poll_write_from(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.borrow_mut().poll_flush_from(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::rc::Rc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn clock() -> Rc<PhaseClock> {
        Rc::new(PhaseClock::new(Phase::Idle))
    }

    #[tokio::test]
    async fn write_passes_through_and_flushes() {
        let (mut client, server) = tokio::io::duplex(4096);
        let (io, _shared) = HyperIo::new(server, bytes::Bytes::new(), clock());
        let mut io = io;
        std::future::poll_fn(|cx| ::hyper::rt::Write::poll_write(Pin::new(&mut io), cx, b"abc"))
            .await
            .unwrap();
        std::future::poll_fn(|cx| ::hyper::rt::Write::poll_flush(Pin::new(&mut io), cx))
            .await
            .unwrap();
        let mut buf = [0u8; 3];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"abc");
    }

    #[tokio::test]
    async fn shared_io_reads_and_writes_through_the_same_state() {
        let (mut client, server) = tokio::io::duplex(4096);
        let (_hyper_io, shared) = HyperIo::new(server, bytes::Bytes::from_static(b"pre"), clock());
        let mut sio = SharedIo(shared);
        // Prepended bytes come out first.
        let mut buf = [0u8; 3];
        sio.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pre");
        // Then the live transport.
        client.write_all(b"xyz").await.unwrap();
        sio.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"xyz");
        // Writes reach the peer.
        sio.write_all(b"ok").await.unwrap();
        sio.flush().await.unwrap();
        let mut out = [0u8; 2];
        client.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"ok");
    }

    /// The first byte of a request is the idle -> header transition; the IO
    /// adapter must report it so the watchdog switches deadlines promptly.
    #[tokio::test]
    async fn first_byte_moves_the_clock_from_idle_to_head() {
        let (mut client, server) = tokio::io::duplex(4096);
        let c = clock();
        let (_hyper_io, shared) = HyperIo::new(server, bytes::Bytes::new(), c.clone());
        let mut sio = SharedIo(shared);
        assert_eq!(c.phase(), Phase::Idle);
        client.write_all(b"G").await.unwrap();
        let mut buf = [0u8; 1];
        sio.read_exact(&mut buf).await.unwrap();
        assert_eq!(c.phase(), Phase::Head);
    }

    /// A pipelined head arriving mid-response must not re-phase to `Head`:
    /// that would put `header_timeout` over the rest of a healthy stream.
    #[tokio::test]
    async fn reads_during_a_write_do_not_re_phase() {
        let (mut client, server) = tokio::io::duplex(4096);
        let c = clock();
        let (_hyper_io, shared) = HyperIo::new(server, bytes::Bytes::new(), c.clone());
        let mut sio = SharedIo(shared);
        c.set(Phase::Write);
        client.write_all(b"G").await.unwrap();
        let mut buf = [0u8; 1];
        sio.read_exact(&mut buf).await.unwrap();
        assert_eq!(c.phase(), Phase::Write);
    }

    /// Write progress is what re-arms `write_timeout`, so it has to bump the
    /// generation the watchdog snapshots — and mark the response as started.
    #[tokio::test]
    async fn writes_report_progress_to_the_clock() {
        let (mut client, server) = tokio::io::duplex(4096);
        let c = clock();
        let (_hyper_io, shared) = HyperIo::new(server, bytes::Bytes::new(), c.clone());
        let mut sio = SharedIo(shared);
        c.set(Phase::Write);
        assert!(!c.wrote_bytes());
        let before = c.generation.get();
        sio.write_all(b"ok").await.unwrap();
        assert!(c.wrote_bytes(), "a nonzero write starts the response");
        assert!(c.generation.get() > before, "write progress re-arms");
        let mut out = [0u8; 2];
        client.read_exact(&mut out).await.unwrap();
        // Starting a new handler resets the per-response write flag.
        c.set(Phase::Handler);
        assert!(!c.wrote_bytes());
    }
}
