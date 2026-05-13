#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Native [SQLx](https://docs.rs/sqlx) driver for the
//! [Otterbrix](https://docs.rs/otterbrix) engine.
//!
//! `sqlx-otterbrix` implements every trait that `sqlx-core` requires from a
//! database backend — [`Database`](sqlx_core::database::Database),
//! [`Connection`](sqlx_core::connection::Connection),
//! [`ConnectOptions`](sqlx_core::connection::ConnectOptions),
//! [`Executor`](sqlx_core::executor::Executor),
//! [`Row`](sqlx_core::row::Row),
//! [`Column`](sqlx_core::column::Column),
//! [`Statement`](sqlx_core::statement::Statement),
//! [`TypeInfo`](sqlx_core::type_info::TypeInfo),
//! [`Value`](sqlx_core::value::Value),
//! [`ValueRef`](sqlx_core::value::ValueRef),
//! [`Arguments`](sqlx_core::arguments::Arguments),
//! [`DatabaseError`](sqlx_core::error::DatabaseError) — and provides
//! [`Type`](sqlx_core::types::Type) / [`Encode`](sqlx_core::encode::Encode) /
//! [`Decode`](sqlx_core::decode::Decode) instances for the standard scalar
//! Rust types. With those in place, the regular `sqlx::query`,
//! `sqlx::query_as`, `query.bind(...)`, `row.try_get(...)` API works as for
//! any other SQLx backend.
//!
//! # Quick start
//!
//! ```no_run
//! use sqlx_core::connection::Connection;
//! use sqlx_core::row::Row;
//! use sqlx_otterbrix::{Otterbrix, OtterbrixConnectOptions, OtterbrixConnection};
//!
//! # async fn run() -> Result<(), sqlx_core::error::Error> {
//! let mut conn =
//!     OtterbrixConnection::connect_with(&OtterbrixConnectOptions::new("./data")).await?;
//!
//! sqlx_core::query::query::<Otterbrix>("CREATE DATABASE app;")
//!     .execute(&mut conn).await?;
//! sqlx_core::query::query::<Otterbrix>(
//!     "CREATE TABLE app.t (id bigint, name string);",
//! )
//! .execute(&mut conn).await?;
//!
//! sqlx_core::query::query::<Otterbrix>(
//!     "INSERT INTO app.t (id, name) VALUES (?, ?);",
//! )
//! .bind(1_i64)
//! .bind("alice")
//! .execute(&mut conn).await?;
//!
//! let row = sqlx_core::query::query::<Otterbrix>(
//!     "SELECT name FROM app.t WHERE id = ?",
//! )
//! .bind(1_i64)
//! .fetch_one(&mut conn).await?;
//!
//! let name: String = row.try_get("name")?;
//! assert_eq!(name, "alice");
//! # Ok(()) }
//! ```
//!
//! In a downstream crate you would normally import the umbrella `sqlx`
//! crate and use `sqlx::query::<Otterbrix>(...)` instead of
//! `sqlx_core::query::query::<Otterbrix>(...)`. The `sqlx_core` form is
//! used here only because this crate depends on `sqlx-core` directly,
//! without `sqlx` itself.
//!
//! # URL scheme
//!
//! The driver registers the `otterbrix://` URL scheme. The path component is
//! interpreted as the filesystem base directory passed to
//! [`otterbrix::Config::new`]:
//!
//! ```text
//! otterbrix:///tmp/data        // absolute path
//! otterbrix:./relative/path    // relative path
//! ```
//!
//! See the [`ConnectOptions`](sqlx_core::connection::ConnectOptions) and
//! [`FromStr`](std::str::FromStr) implementations on
//! [`OtterbrixConnectOptions`] for the exact parsing rules.
//!
//! # Placeholder syntax
//!
//! SQLx's standard `?` positional placeholders are accepted and rewritten
//! internally to the `$1`, `$2`, ... form expected by the engine; native
//! `$N` placeholders pass through untouched. The rewrite respects single-
//! and double-quoted string regions.
//!
//! # Concurrency
//!
//! [`OtterbrixConnection`] holds the underlying [`otterbrix::Database`]
//! behind an `Arc<parking_lot::Mutex<_>>` and dispatches every query
//! through [`tokio::task::spawn_blocking`], so the synchronous Otterbrix
//! C facade never blocks the async runtime. The connection is [`Send`]
//! (it can be moved between tasks and managed by a SQLx pool) but not
//! [`Sync`] — only one query may be in flight per connection at a time.
//!
//! # Limitations
//!
//! - **Transactions are not supported.** `begin` / `commit` / `rollback`
//!   return [`Error::Protocol`](sqlx_core::error::Error::Protocol) with the
//!   message `"transactions are not supported by otterbrix"` (or
//!   `"invalid transaction state for otterbrix"` for `commit` / `rollback`).
//! - **`prepare_with` is a stub.** It returns an empty
//!   [`OtterbrixStatement`] with only the placeholder count — no column
//!   metadata, no parameter type list. Sufficient for runtime queries but
//!   not for static analysis.
//! - **`describe` is unsupported.** SQLx offline macros (`sqlx::query!` &
//!   friends) cannot be used with this driver, since the engine exposes no
//!   `DESCRIBE`-style API.
//! - **`OtterbrixQueryResult::last_insert_rowid` is always `0`.** The
//!   engine does not surface generated identifiers; only `rows_affected` is
//!   populated.
//! - **Engine error category surfacing is coarse-grained.** All engine
//!   errors come through with [`ErrorKind::Other`](sqlx_core::error::ErrorKind::Other);
//!   downcast [`OtterbrixDbError`] if you need the raw integer code.

mod arguments;
mod column;
mod connection;
mod convert;
mod database;
mod error;
mod executor;
mod options;
mod query_result;
mod row;
mod statement;
mod transaction;
mod r#type;
mod types;
mod value;

pub use arguments::{OtterbrixArgumentBuffer, OtterbrixArgumentValue, OtterbrixArguments};
pub use column::OtterbrixColumn;
pub use connection::OtterbrixConnection;
pub use database::Otterbrix;
pub use error::OtterbrixDbError;
pub use options::OtterbrixConnectOptions;
pub use query_result::OtterbrixQueryResult;
pub use r#type::OtterbrixTypeInfo;
pub use row::OtterbrixRow;
pub use statement::OtterbrixStatement;
pub use transaction::OtterbrixTransactionManager;
pub use value::{OtterbrixValue, OtterbrixValueRef};
