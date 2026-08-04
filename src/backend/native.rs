//! The bespoke `Connection` loop, behind the backend seam.
//!
//! Compiled unconditionally, including under `hyper-backend` where the
//! active-backend alias selects hyper and nothing in this file is constructed.
//! That is deliberate: keeping the seam's native side type-checked in every
//! feature combination is what stops a change to `Backend`, `H1Service` or
//! `Upgraded` from compiling green on one feature row and breaking the other.
//! (The differential fuzz target reaches the native stack through
//! `Connection::with_buffered` directly, not through `NativeBackend`, so it is
//! not what keeps these items alive.) Hence the `dead_code` allowances below.

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
#[cfg_attr(feature = "hyper-backend", allow(dead_code))]
pub(crate) struct RcService<S>(pub(crate) Rc<S>);

impl<S: H1Service> H1Service for RcService<S> {
    type Future = S::Future;

    #[inline]
    fn call(&self, req: crate::Request) -> Self::Future {
        self.0.call(req)
    }
}

#[cfg_attr(feature = "hyper-backend", allow(dead_code))]
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
