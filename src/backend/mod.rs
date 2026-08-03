//! The per-connection serving seam.
//!
//! Two implementations exist: [`native`] (the bespoke `Connection` loop) and,
//! behind the `hyper-backend` feature, `hyper` (hyper's `conn::http1`).
//! `ActiveBackend` selects one at compile time. The feature changes which
//! backend the server *uses*, not the API: the bespoke protocol modules stay
//! compiled and exported either way.

pub(crate) mod native;

use crate::conn::ConnConfig;
use crate::service::{H1Service, Upgraded};
use crate::write::DateCache;
use bytes::Bytes;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::rc::Rc;
use tokio::io::{AsyncRead, AsyncWrite};

/// One way of serving an HTTP/1 connection to completion.
pub(crate) trait Backend {
    /// Serve `io` until close, error, or upgrade.
    ///
    /// `buffered` holds bytes protocol dispatch already read (h2c sniffing);
    /// they are part of the first request and must be consumed before `io`.
    fn serve<IO, S>(
        io: IO,
        service: Rc<S>,
        cfg: Rc<ConnConfig>,
        date: Rc<RefCell<DateCache>>,
        buffered: Bytes,
    ) -> impl Future<Output = io::Result<Option<Upgraded>>>
    where
        IO: AsyncRead + AsyncWrite + Unpin + 'static,
        S: H1Service + 'static;
}

#[cfg(not(feature = "hyper-backend"))]
pub(crate) type ActiveBackend = native::NativeBackend;

/// Serve one connection through whichever backend this build selected.
///
/// This is the same entry the server's accept loop uses; it exists publicly so
/// out-of-crate harnesses — the `e2e` bench in particular — measure the
/// selected backend rather than always the native `Connection`.
pub async fn serve_connection<IO, S>(
    io: IO,
    service: Rc<S>,
    cfg: Rc<ConnConfig>,
    date: Rc<RefCell<DateCache>>,
    buffered: Bytes,
) -> io::Result<Option<Upgraded>>
where
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
    S: H1Service + 'static,
{
    ActiveBackend::serve(io, service, cfg, date, buffered).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::DateCache;
    use crate::{ConnConfig, Request, Response};
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn hello(_req: Request) -> Response {
        Response::text("hi")
    }

    #[tokio::test]
    async fn serve_connection_serves_through_the_active_backend() {
        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        let task = local.spawn_local(serve_connection(
            server,
            Rc::new(hello),
            Rc::new(ConnConfig::default()),
            Rc::new(RefCell::new(DateCache::new())),
            bytes::Bytes::new(),
        ));
        let out = local
            .run_until(async move {
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    client.read_to_end(&mut out),
                )
                .await;
                let _ = task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(out.ends_with("hi"), "{out}");
    }

    /// Dispatch read-ahead bytes (h2c sniffing) must reach the backend intact.
    #[tokio::test]
    async fn serve_connection_honors_pre_buffered_bytes() {
        let (mut client, server) = tokio::io::duplex(4096);
        let local = tokio::task::LocalSet::new();
        // First 4 bytes of the request arrive via `buffered`, the rest on the wire.
        let task = local.spawn_local(serve_connection(
            server,
            Rc::new(hello),
            Rc::new(ConnConfig::default()),
            Rc::new(RefCell::new(DateCache::new())),
            bytes::Bytes::from_static(b"GET "),
        ));
        let out = local
            .run_until(async move {
                client
                    .write_all(b"/ HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let mut out = Vec::new();
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    client.read_to_end(&mut out),
                )
                .await;
                let _ = task.await;
                String::from_utf8_lossy(&out).into_owned()
            })
            .await;
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
    }
}
