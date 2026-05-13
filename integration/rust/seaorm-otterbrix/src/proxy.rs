use std::sync::Arc;

use async_trait::async_trait;
use otterbrix::{Cursor, Database};
use parking_lot::Mutex;
use sea_orm::{DbErr, ProxyDatabaseTrait, ProxyExecResult, ProxyRow, RuntimeErr, Statement};

use crate::convert::{
    cursor_to_proxy_rows, map_otterbrix_error, rewrite_placeholders, split_statement,
    statement_params,
};

#[derive(Clone)]
pub struct OtterbrixProxy {
    db: Arc<Mutex<Database>>,
}

impl OtterbrixProxy {
    pub fn new(db: Database) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
        }
    }

    pub fn from_arc(db: Arc<Mutex<Database>>) -> Self {
        Self { db }
    }
}

impl std::fmt::Debug for OtterbrixProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtterbrixProxy").finish_non_exhaustive()
    }
}

fn map_join_error(err: tokio::task::JoinError) -> DbErr {
    DbErr::Conn(RuntimeErr::Internal(format!(
        "blocking otterbrix task aborted: {err}"
    )))
}

const TX_UNSUPPORTED_LOG: &str = "transactions are not supported by otterbrix (proxy no-op)";

async fn run_blocking_with_cursor<F, R>(
    db: &Arc<Mutex<Database>>,
    statement: Statement,
    finish: F,
) -> Result<R, DbErr>
where
    F: for<'db> FnOnce(Cursor<'db>) -> R + Send + 'static,
    R: Send + 'static,
{
    let db = Arc::clone(db);
    tokio::task::spawn_blocking(move || -> Result<R, DbErr> {
        let (sql, values) = split_statement(statement);
        let sql = rewrite_placeholders(&sql);
        let guard = db.lock();
        let cursor = match values.as_ref() {
            Some(v) => {
                let params = statement_params(v)?;
                guard.execute_with_params(&sql, &params)
            }
            None => guard.execute(&sql),
        }
        .map_err(map_otterbrix_error)?;
        Ok(finish(cursor))
    })
    .await
    .map_err(map_join_error)?
}

#[async_trait]
impl ProxyDatabaseTrait for OtterbrixProxy {
    async fn query(&self, statement: Statement) -> Result<Vec<ProxyRow>, DbErr> {
        run_blocking_with_cursor(&self.db, statement, |cursor| cursor_to_proxy_rows(&cursor)).await
    }

    async fn execute(&self, statement: Statement) -> Result<ProxyExecResult, DbErr> {
        run_blocking_with_cursor(&self.db, statement, |cursor| {
            let affected = cursor.size().max(0) as u64;
            ProxyExecResult {
                last_insert_id: 0,
                rows_affected: affected,
            }
        })
        .await
    }

    async fn begin(&self) {
        log::warn!(target: "seaorm_otterbrix", "{}", TX_UNSUPPORTED_LOG);
    }

    async fn commit(&self) {
        log::warn!(target: "seaorm_otterbrix", "{}", TX_UNSUPPORTED_LOG);
    }

    async fn rollback(&self) {
        log::warn!(target: "seaorm_otterbrix", "{}", TX_UNSUPPORTED_LOG);
    }

    fn start_rollback(&self) {}
}
