use std::fmt;
use std::sync::Arc;

use futures_core::future::BoxFuture;
use futures_util::future;
use otterbrix::Database as ObDatabase;
use parking_lot::Mutex;
use sqlx_core::connection::{Connection, LogSettings};
use sqlx_core::error::Error;
use sqlx_core::transaction::Transaction;

use crate::database::Otterbrix;
use crate::options::OtterbrixConnectOptions;

pub struct OtterbrixConnection {
    pub(crate) inner: Arc<Mutex<ObDatabase>>,
    pub(crate) log_settings: LogSettings,
}

impl fmt::Debug for OtterbrixConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtterbrixConnection")
            .finish_non_exhaustive()
    }
}

impl Connection for OtterbrixConnection {
    type Database = Otterbrix;
    type Options = OtterbrixConnectOptions;

    fn close(self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(future::ready(Ok(())))
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(future::ready(Ok(())))
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(future::ready(Ok(())))
    }

    fn begin(&mut self) -> BoxFuture<'_, Result<Transaction<'_, Otterbrix>, Error>> {
        Box::pin(async move {
            Err(Error::protocol(
                "transactions are not supported by the Otterbrix driver",
            ))
        })
    }

    fn shrink_buffers(&mut self) {}

    fn flush(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(future::ready(Ok(())))
    }

    fn should_flush(&self) -> bool {
        false
    }
}
