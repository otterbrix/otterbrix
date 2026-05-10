use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use otterbrix::Database;
use sea_orm::{DbErr, ProxyDatabaseTrait, ProxyExecResult, ProxyRow, RuntimeErr, Statement};

use crate::convert::{
    cursor_to_proxy_rows, map_otterbrix_error, rewrite_placeholders, split_statement,
    statement_params,
};

/// Otterbrix' wrapper does not advertise `Sync`, so we serialize FFI access
/// through a `Mutex`. The lock is taken inside `spawn_blocking`, so it is held
/// only on the dedicated blocking thread and never across an `.await` point.
#[derive(Clone, Debug)]
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

    pub fn database(&self) -> &Mutex<Database> {
        &self.db
    }
}

fn map_join_error(err: tokio::task::JoinError) -> DbErr {
    DbErr::Conn(RuntimeErr::Internal(format!(
        "blocking otterbrix task aborted: {err}"
    )))
}

fn map_poison<T>(err: std::sync::PoisonError<T>) -> DbErr {
    DbErr::Conn(RuntimeErr::Internal(format!(
        "otterbrix database mutex poisoned: {err}"
    )))
}

const TX_UNSUPPORTED_LOG: &str =
    "User transactions are not supported on Otterbrix (proxy no-op).";

#[async_trait]
impl ProxyDatabaseTrait for OtterbrixProxy {
    async fn query(&self, statement: Statement) -> Result<Vec<ProxyRow>, DbErr> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> Result<Vec<ProxyRow>, DbErr> {
            let (sql, values) = split_statement(statement);
            let sql = rewrite_placeholders(&sql);
            let guard = db.lock().map_err(map_poison)?;
            let cursor = match values.as_ref() {
                Some(v) => {
                    let params = statement_params(v)?;
                    guard.execute_with_params(&sql, &params)
                }
                None => guard.execute(&sql),
            }
            .map_err(map_otterbrix_error)?;
            Ok(cursor_to_proxy_rows(&cursor))
        })
        .await
        .map_err(map_join_error)?
    }

    async fn execute(&self, statement: Statement) -> Result<ProxyExecResult, DbErr> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> Result<ProxyExecResult, DbErr> {
            let (sql, values) = split_statement(statement);
            let sql = rewrite_placeholders(&sql);
            let guard = db.lock().map_err(map_poison)?;
            let cursor = match values.as_ref() {
                Some(v) => {
                    let params = statement_params(v)?;
                    guard.execute_with_params(&sql, &params)
                }
                None => guard.execute(&sql),
            }
            .map_err(map_otterbrix_error)?;
            let affected = cursor.size().max(0) as u64;
            Ok(ProxyExecResult {
                last_insert_id: 0,
                rows_affected: affected,
            })
        })
        .await
        .map_err(map_join_error)?
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
