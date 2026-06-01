//! SeaORM proxy backend implementation.

use std::sync::Arc;

use async_trait::async_trait;
use otterbrix::{Cursor, Database};
use parking_lot::Mutex;
use sea_orm::{DbErr, ProxyDatabaseTrait, ProxyExecResult, ProxyRow, RuntimeErr, Statement};

use crate::convert::{
    cursor_to_proxy_rows, map_otterbrix_error, rewrite_placeholders, split_statement,
    statement_params,
};

/// SeaORM proxy backend backed by an [`otterbrix::Database`].
///
/// `OtterbrixProxy` implements [`ProxyDatabaseTrait`], which lets
/// `sea-orm`'s `Database::connect_proxy` route every query through the
/// Otterbrix engine. Internally each call is forwarded to
/// [`otterbrix::Database::execute`] or
/// [`otterbrix::Database::execute_with_params`] on a blocking thread (via
/// [`tokio::task::spawn_blocking`]).
///
/// # Concurrency
///
/// `OtterbrixProxy` is `Clone + Send + Sync`. Internally it owns
/// [`Arc<parking_lot::Mutex<Database>>`][parking_lot::Mutex] — multiple
/// clones share the same database, and access is serialised through the
/// Rust-side mutex (the C engine itself also serialises calls through its
/// own per-instance mutex, so this is a defence in depth).
///
/// # Limitations
///
/// - **Transactions are not supported.** Otterbrix exposes no transactional
///   API, so `begin` / `commit` / `rollback` log a warning and return without
///   error (proxy no-op). `start_rollback` is also a no-op. This matches
///   `sea-orm`'s `ProxyDatabaseTrait` contract for backends without
///   transactions.
/// - **`last_insert_id` is always `0`.** Otterbrix does not return generated
///   identifiers; only `rows_affected` is populated.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use otterbrix::{Config, Database};
/// use sea_orm::{DatabaseConnection, DbBackend, ProxyDatabaseTrait};
/// use seaorm_otterbrix::OtterbrixProxy;
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Database::open(Config::new("./data"))?;
/// db.create_database("app")?;
/// db.create_collection("app", "t")?;
///
/// let proxy: Arc<Box<dyn ProxyDatabaseTrait>> = Arc::new(Box::new(OtterbrixProxy::new(db)));
/// let conn: DatabaseConnection =
///     sea_orm::Database::connect_proxy(DbBackend::Sqlite, proxy).await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct OtterbrixProxy {
    db: Arc<Mutex<Database>>,
}

impl OtterbrixProxy {
    /// Wraps an owned [`Database`] into a fresh proxy.
    ///
    /// The proxy takes ownership of `db` and stores it inside an `Arc<Mutex<_>>`.
    /// Use [`OtterbrixProxy::from_arc`] instead if you already share a database
    /// with other components.
    pub fn new(db: Database) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
        }
    }

    /// Builds a proxy from an externally managed `Arc<Mutex<Database>>`.
    ///
    /// Useful when the same database needs to be reachable both directly
    /// (for DDL / setup) and through SeaORM (for the application layer).
    /// All clones of the resulting proxy share the same lock.
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
