#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! SeaORM proxy backend for the [Otterbrix](https://docs.rs/otterbrix) engine.
//!
//! `seaorm-otterbrix` plugs into SeaORM through
//! [`sea_orm::ProxyDatabaseTrait`]: every
//! query that an application sends through `sea-orm`'s `DatabaseConnection`
//! is forwarded to an embedded [`otterbrix::Database`] instance. The crate
//! is intentionally small — it only does the type-and-error mapping needed
//! to make Otterbrix look like any other SeaORM-compatible database.
//!
//! The single public type is [`OtterbrixProxy`]; all conversion helpers are
//! `pub(crate)`. The one helper that is exposed is
//! [`positional_proxy_column_key`], which lets callers retrieve cells by
//! position when a result set has duplicate column names.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use otterbrix::{Config, Database};
//! use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, ProxyDatabaseTrait, Statement};
//! use seaorm_otterbrix::OtterbrixProxy;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Database::open(Config::new("./data"))?;
//! db.create_database("app")?;
//! db.create_collection("app", "t")?;
//!
//! let proxy: Arc<Box<dyn ProxyDatabaseTrait>> = Arc::new(Box::new(OtterbrixProxy::new(db)));
//! let conn: DatabaseConnection =
//!     sea_orm::Database::connect_proxy(DbBackend::Sqlite, proxy).await?;
//!
//! conn.execute(Statement::from_string(
//!     DbBackend::Postgres,
//!     "INSERT INTO app.t (id, name) VALUES (1, 'alice');".to_string(),
//! )).await?;
//! # Ok(()) }
//! ```
//!
//! # Concurrency
//!
//! [`OtterbrixProxy`] is `Clone + Send + Sync` and stores the underlying
//! [`otterbrix::Database`] inside an `Arc<Mutex<_>>`. Cloning the proxy
//! shares the same database handle; concurrent queries are serialised by the
//! Rust-side mutex (and again by the engine's internal mutex on the C++
//! side). All Otterbrix calls happen on a Tokio blocking thread via
//! [`tokio::task::spawn_blocking`].
//!
//! # Error mapping
//!
//! Errors produced by the engine are translated into [`sea_orm::DbErr`] as
//! follows:
//!
//! - [`otterbrix::Error::Query`] → [`DbErr::Exec`](sea_orm::DbErr::Exec);
//! - [`otterbrix::Error::NullPointer`] and
//!   [`otterbrix::Error::InvalidPath`] → [`DbErr::Conn`](sea_orm::DbErr::Conn);
//! - [`otterbrix::Error::TypeMismatch`] → [`DbErr::Type`](sea_orm::DbErr::Type).
//!
//! The original `Display` text of the underlying [`otterbrix::Error`] is
//! preserved verbatim, so the `otterbrix core ...` / `otterbrix ...` prefixes
//! still indicate whether the error came from the C++ engine or from the
//! Rust wrapper layer.
//!
//! # Limitations
//!
//! - **Transactions are not supported.** `begin`, `commit` and `rollback` log
//!   a warning to the `seaorm_otterbrix` target and return without touching
//!   the engine. Code that expects transactional rollback semantics will
//!   silently observe partial writes; check your usage if this matters.
//! - **`last_insert_id` is always `0`.** The engine does not surface
//!   generated identifiers; only `rows_affected` is populated.

mod convert;
mod proxy;

pub use convert::positional_proxy_column_key;
pub use proxy::OtterbrixProxy;
