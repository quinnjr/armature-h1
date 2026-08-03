//! The bespoke `Connection` loop, behind the backend seam.
//!
//! Always compiled: `Connection` is public API regardless of feature, and the
//! differential story depends on the bespoke stack existing under both builds.

use super::Backend;
use crate::conn::{ConnConfig, Connection};
use crate::service::{H1Service, Upgraded};
use crate::write::DateCache;
use bytes::Bytes;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use tokio::io::{AsyncRead, AsyncWrite};

/// Shares one service across every connection on a worker.
pub(crate) struct RcService<S>(pub(crate) Rc<S>);

impl<S: H1Service> H1Service for RcService<S> {
    type Future = S::Future;

    #[inline]
    fn call(&self, req: crate::Request) -> Self::Future {
        self.0.call(req)
    }
}

pub(crate) struct NativeBackend;

impl Backend for NativeBackend {
    async fn serve<IO, S>(
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
        Connection::with_buffered(io, RcService(service), cfg, date, buffered)
            .serve()
            .await
    }
}
