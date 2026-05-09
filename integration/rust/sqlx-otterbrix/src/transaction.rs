use std::borrow::Cow;

use futures_core::future::BoxFuture;
use futures_util::future;
use sqlx_core::database::Database;
use sqlx_core::error::Error;
use sqlx_core::transaction::TransactionManager;

use crate::connection::OtterbrixConnection;
use crate::database::Otterbrix;

#[derive(Debug)]
pub struct OtterbrixTransactionManager;

impl TransactionManager for OtterbrixTransactionManager {
    type Database = Otterbrix;

    fn begin<'conn>(
        _conn: &'conn mut <Self::Database as Database>::Connection,
        _statement: Option<Cow<'static, str>>,
    ) -> BoxFuture<'conn, Result<(), Error>> {
        Box::pin(future::ready(Err(Error::protocol(
            "transactions are not supported by the Otterbrix driver",
        ))))
    }

    fn commit(_conn: &mut OtterbrixConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(future::ready(Err(Error::protocol(
            "invalid transaction state for Otterbrix",
        ))))
    }

    fn rollback(_conn: &mut OtterbrixConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(future::ready(Err(Error::protocol(
            "invalid transaction state for Otterbrix",
        ))))
    }

    fn start_rollback(_conn: &mut OtterbrixConnection) {}

    fn get_transaction_depth(_conn: &OtterbrixConnection) -> usize {
        0
    }
}
