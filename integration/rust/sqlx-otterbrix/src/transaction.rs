use std::borrow::Cow;

use futures_core::future::BoxFuture;
use futures_util::future;
use sqlx_core::database::Database;
use sqlx_core::error::Error;
use sqlx_core::transaction::TransactionManager;

use crate::connection::OtterbrixConnection;
use crate::database::Otterbrix;

/// SQLx [`TransactionManager`] for the [`Otterbrix`](crate::Otterbrix) database.
///
/// **Otterbrix has no transactional API.** All three lifecycle methods —
/// `begin`, `commit`, `rollback` — return
/// [`Error::Protocol`](sqlx_core::error::Error::Protocol) with the message
/// `"transactions are not supported by otterbrix"` (or `"invalid
/// transaction state for otterbrix"` for `commit`/`rollback`).
/// `start_rollback` is a no-op and `get_transaction_depth` always returns `0`.
///
/// As a result, calling [`Connection::begin`](sqlx_core::connection::Connection::begin)
/// on an [`OtterbrixConnection`](crate::OtterbrixConnection) is a hard
/// error rather than a silent no-op. Code that depends on transactional
/// rollback semantics will not run unmodified against this driver.
#[derive(Debug)]
pub struct OtterbrixTransactionManager;

impl TransactionManager for OtterbrixTransactionManager {
    type Database = Otterbrix;

    fn begin<'conn>(
        _conn: &'conn mut <Self::Database as Database>::Connection,
        _statement: Option<Cow<'static, str>>,
    ) -> BoxFuture<'conn, Result<(), Error>> {
        Box::pin(future::ready(Err(Error::protocol(
            "transactions are not supported by otterbrix",
        ))))
    }

    fn commit(_conn: &mut OtterbrixConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(future::ready(Err(Error::protocol(
            "invalid transaction state for otterbrix",
        ))))
    }

    fn rollback(_conn: &mut OtterbrixConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(future::ready(Err(Error::protocol(
            "invalid transaction state for otterbrix",
        ))))
    }

    fn start_rollback(_conn: &mut OtterbrixConnection) {}

    fn get_transaction_depth(_conn: &OtterbrixConnection) -> usize {
        0
    }
}
