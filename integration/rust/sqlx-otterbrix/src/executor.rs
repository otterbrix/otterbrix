//! [`Executor`] implementation for [`OtterbrixConnection`].
//!
//! Each query goes through [`tokio::task::spawn_blocking`] so that the
//! synchronous Otterbrix C facade does not block the async runtime; the
//! resulting cursor is materialised eagerly into a vector of
//! [`OtterbrixRow`]s.
//!
//! Two methods are intentional stubs because the engine does not expose the
//! corresponding capability:
//!
//! - [`Executor::prepare_with`] returns an empty
//!   [`OtterbrixStatement`] with a placeholder count derived from the SQL
//!   text — column metadata is not available without executing the statement.
//! - [`Executor::describe`] returns
//!   [`Error::Protocol`](sqlx_core::error::Error::Protocol) — the engine has
//!   no `DESCRIBE`-style API, so SQLx offline macros are unsupported.

use std::sync::Arc;

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::future;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use otterbrix::Database as ObDatabase;
use parking_lot::Mutex;
use sqlx_core::describe::Describe;
use sqlx_core::error::Error;
use sqlx_core::executor::{Execute, Executor};
use sqlx_core::logger::QueryLogger;
use sqlx_core::Either;

use crate::arguments::OtterbrixArguments;
use crate::convert::{self, materialize_cursor, rewrite_placeholders};
use crate::database::Otterbrix;
use crate::query_result::OtterbrixQueryResult;
use crate::r#type::OtterbrixTypeInfo;
use crate::row::OtterbrixRow;
use crate::statement::OtterbrixStatement;
use crate::OtterbrixConnection;

/// Synchronous body of a query: rewrites placeholders, takes the
/// per-database mutex, dispatches `execute` / `execute_with_params`, and
/// materialises the cursor. Always called from `spawn_blocking`.
fn run_sql(
    db: &Arc<Mutex<ObDatabase>>,
    sql: &str,
    maybe_args: Option<OtterbrixArguments<'_>>,
) -> Result<(Vec<OtterbrixRow>, u64), Error> {
    let rewritten = rewrite_placeholders(sql);
    let guard = db.lock();
    let cursor = match maybe_args {
        None => guard.execute(&rewritten),
        Some(ref args) => {
            let params = convert::arguments_to_params(&args.values)?;
            guard.execute_with_params(&rewritten, &params)
        }
    }
    .map_err(convert::map_otterbrix_error)?;
    materialize_cursor(&cursor)
}

impl<'c> Executor<'c> for &'c mut OtterbrixConnection {
    type Database = Otterbrix;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<OtterbrixQueryResult, OtterbrixRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Otterbrix>,
    {
        let sql = query.sql().to_owned();
        let maybe_args = match query.take_arguments().map_err(Error::Encode) {
            Ok(v) => v,
            Err(e) => return stream::once(future::ready(Err(e))).boxed(),
        };
        let maybe_args = maybe_args.map(OtterbrixArguments::into_static);
        let db = self.inner.clone();
        let log_settings = self.log_settings.clone();

        stream::once(async move {
            let mut logger = QueryLogger::new(&sql, log_settings);
            let sql_for_blocking = sql.clone();
            let join =
                tokio::task::spawn_blocking(move || run_sql(&db, &sql_for_blocking, maybe_args))
                    .await;
            let items: Vec<Result<Either<OtterbrixQueryResult, OtterbrixRow>, Error>> = match join {
                Err(e) => vec![Err(Error::protocol(format!("task join: {e}")))],
                Ok(Err(e)) => vec![Err(e)],
                Ok(Ok((rows, rows_affected))) => {
                    logger.increase_rows_affected(rows_affected);
                    for _ in &rows {
                        logger.increment_rows_returned();
                    }
                    let mut v = vec![Ok(Either::Left(OtterbrixQueryResult::from_execution(
                        rows_affected,
                    )))];
                    v.extend(rows.into_iter().map(|r| Ok(Either::Right(r))));
                    v
                }
            };
            drop(logger);
            stream::iter(items)
        })
        .flatten()
        .boxed()
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<OtterbrixRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        Box::pin(async move {
            let mut stream = self.fetch_many(query);
            while let Some(step) = stream.try_next().await? {
                if let Either::Right(row) = step {
                    return Ok(Some(row));
                }
            }
            Ok(None)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        _parameters: &[OtterbrixTypeInfo],
    ) -> BoxFuture<'e, Result<OtterbrixStatement<'q>, Error>>
    where
        'c: 'e,
    {
        use sqlx_core::HashMap;
        use std::borrow::Cow;
        use std::sync::Arc;

        let parameters = convert::count_placeholders(sql);
        let columns = Arc::new(Vec::new());
        let column_names = Arc::new(HashMap::default());
        Box::pin(future::ready(Ok(OtterbrixStatement {
            sql: Cow::Borrowed(sql),
            parameters,
            columns,
            column_names,
        })))
    }

    fn describe<'e, 'q: 'e>(
        self,
        _sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        Box::pin(future::ready(Err(Error::protocol(
            "DESCRIBE / offline macros are not supported by otterbrix",
        ))))
    }
}
