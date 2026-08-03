//! Hand-rolled tokio <-> `hyper::rt` IO adapter.
//!
//! hyper-util's `TokioIo` would do this, but pulling a dependency in for ~50
//! lines is the wrong trade for this crate's dependency posture. The inner
//! state is `Rc`-shared so the deadline watchdog can write a 408 after
//! dropping hyper's serve future, and so upgrade recovery can hand the
//! transport back out as a `Box<dyn Transport>`.

use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[allow(dead_code)]
pub(crate) struct IoShared<IO> {
    pub(crate) io: IO,
    /// Bytes read before hyper took over (h2c sniff read-ahead). Served first.
    pub(crate) buffered: Bytes,
    /// Set on every nonzero read: the watchdog's idle -> header signal.
    pub(crate) saw_bytes: Rc<Cell<bool>>,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> IoShared<IO> {
    /// Fill `out` from the prepend buffer first, then the transport.
    #[allow(dead_code)]
    fn poll_read_into(
        &mut self,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.buffered.is_empty() {
            let n = self.buffered.len().min(out.remaining());
            out.put_slice(&self.buffered.split_to(n));
            self.saw_bytes.set(true);
            return Poll::Ready(Ok(()));
        }
        let before = out.filled().len();
        let poll = Pin::new(&mut self.io).poll_read(cx, out);
        if matches!(poll, Poll::Ready(Ok(()))) && out.filled().len() > before {
            self.saw_bytes.set(true);
        }
        poll
    }
}

#[allow(dead_code)]
pub(crate) struct HyperIo<IO>(pub(crate) Rc<RefCell<IoShared<IO>>>);

impl<IO: AsyncRead + AsyncWrite + Unpin> HyperIo<IO> {
    #[allow(dead_code)]
    pub(crate) fn new(
        io: IO,
        buffered: Bytes,
        saw_bytes: Rc<Cell<bool>>,
    ) -> (Self, Rc<RefCell<IoShared<IO>>>) {
        let shared = Rc::new(RefCell::new(IoShared {
            io,
            buffered,
            saw_bytes,
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
        Pin::new(&mut self.0.borrow_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_shutdown(cx)
    }
}

/// Tokio-flavored view over the same shared state, for upgrade handoff and the
/// watchdog's post-drop error write.
#[allow(dead_code)]
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
        Pin::new(&mut self.0.borrow_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.borrow_mut().io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::pin::Pin;
    use std::rc::Rc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn write_passes_through_and_flushes() {
        let (mut client, server) = tokio::io::duplex(4096);
        let (io, _shared) = HyperIo::new(server, bytes::Bytes::new(), Rc::new(Cell::new(false)));
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
        let (_hyper_io, shared) = HyperIo::new(
            server,
            bytes::Bytes::from_static(b"pre"),
            Rc::new(Cell::new(false)),
        );
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
}
