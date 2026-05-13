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

/// Live SQLx connection to an embedded Otterbrix engine.
///
/// `OtterbrixConnection` is the [`Connection`] implementation of the
/// [`Otterbrix`](crate::Otterbrix) database. Construct it via SQLx's
/// standard entry points — [`Connection::connect`] (with an
/// `otterbrix:///path` URL) or
/// [`Connection::connect_with`] (with an [`OtterbrixConnectOptions`]).
///
/// # Concurrency
///
/// Internally the connection holds an `Arc<parking_lot::Mutex<otterbrix::Database>>`,
/// so cloning it would share the same engine instance. SQLx itself does not
/// require `Clone` for connections, and this type does not implement it.
/// Every call into the engine runs on a Tokio blocking thread via
/// [`tokio::task::spawn_blocking`], guarded by the Rust-side mutex.
///
/// # Lifecycle
///
/// `close` and `close_hard` are no-ops — the engine is shut down when the
/// connection is dropped. `ping` always succeeds. `begin` returns an error
/// (see [`OtterbrixTransactionManager`](crate::OtterbrixTransactionManager)).
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
                "transactions are not supported by otterbrix",
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
