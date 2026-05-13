#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Safe, idiomatic Rust bindings to the Otterbrix in-process database engine.
//!
//! `otterbrix` is a thin wrapper around the C ABI exposed by the upstream
//! Otterbrix project. It hides every `unsafe` FFI call behind a small set of
//! owned types ([`Database`], [`Cursor`], [`Value`]) that map cleanly to
//! Rust's resource-management model:
//!
//! - resources are released by [`Drop`], no manual `free` calls are required;
//! - errors are returned as [`Result<T, Error>`](Result), not signalled through
//!   sentinel values or out-parameters;
//! - lifetimes are used to prevent use-after-free at compile time
//!   ([`Cursor`] borrows from its [`Database`]);
//! - the public surface is small enough to learn in one sitting.
//!
//! For applications that prefer a SQL toolkit or an ORM, two adapters are
//! built on top of this crate:
//!
//! - [`sqlx-otterbrix`](https://crates.io/crates/sqlx-otterbrix) — a native
//!   `sqlx` driver;
//! - [`seaorm-otterbrix`](https://crates.io/crates/seaorm-otterbrix) — a
//!   `SeaORM` `ProxyDatabaseTrait` adapter.
//!
//! For very low-level use (custom wrappers, alternate language bindings) see
//! [`otterbrix-sys`](https://crates.io/crates/otterbrix-sys).
//!
//! # Quick start
//!
//! ```no_run
//! use otterbrix::{Config, Database};
//!
//! let cfg = Config::new("./data");
//! let db = Database::open(cfg).expect("open database");
//!
//! db.create_database("app").unwrap();
//! db.create_collection("app", "t").unwrap();
//!
//! db.execute("INSERT INTO app.t (id, name) VALUES (1, 'alice');").unwrap();
//!
//! let cursor = db.execute("SELECT id, name FROM app.t;").unwrap();
//! for row in cursor.rows() {
//!     let id: i64 = row.get_by_name("id").get().unwrap();
//!     let name: String = row.get_by_name("name").get().unwrap();
//!     println!("{id}: {name}");
//! }
//! ```
//!
//! # Concurrency model
//!
//! The Otterbrix C facade is a synchronous, blocking API: every call into the
//! engine is serialised through an instance-level mutex on the C++ side.
//! Reflecting that, this crate provides:
//!
//! - [`Database`] is [`Send`] but **not** [`Sync`] — a single instance can be
//!   moved between threads, but cannot be shared concurrently;
//! - [`Cursor`] is [`Send`] and tied to its parent [`Database`] by lifetime;
//! - to share a single [`Database`] across threads, wrap it in either:
//!   - `Arc<Mutex<Database>>` for synchronous code, using [`std::sync::Arc`]
//!     and [`std::sync::Mutex`];
//!   - `Arc<Mutex<Database>>` for asynchronous code, using
//!     [`std::sync::Arc`] and [`tokio::sync::Mutex`][tokio-mutex] (whose
//!     guard is `Send` across `await` points, unlike the standard library's).
//!
//! [tokio-mutex]: https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html
//!
//! For a deeper discussion of multithreading semantics and the tested
//! guarantees, see the project documentation.
//!
//! # Error handling
//!
//! All fallible operations return [`Result<T, Error>`](Result). Variants of
//! [`Error`] are categorised by origin so callers can distinguish C++ engine
//! errors from wrapper-side validation errors:
//!
//! - [`Error::Query`] / [`Error::NullPointer`] — produced by the engine; the
//!   `Display` text starts with `otterbrix core ...`;
//! - [`Error::InvalidPath`] / [`Error::TypeMismatch`] — produced by the
//!   wrapper; the `Display` text starts with `otterbrix ...`.
//!
//! # Build requirements
//!
//! This crate transitively links against `libotterbrix.so` from the upstream
//! Otterbrix C++ build. By default,
//! [`otterbrix-sys`](https://crates.io/crates/otterbrix-sys) resolves both
//! the header and the shared object under `<repo>/build/integration/c`, and
//! `build.rs` embeds an `rpath` so the linker can locate the library at run
//! time without `LD_LIBRARY_PATH`. Set `OTTERBRIX_LIB_DIR` and/or
//! `OTTERBRIX_INCLUDE_DIR` to override the default search path.

mod config;
mod cursor;
mod database;
mod error;
mod utils;
mod value;

pub use config::{Config, ConfigBuilder};
pub use cursor::{
    Cursor, LogicalType, Row, Rows, LOGICAL_TYPE_BIGINT, LOGICAL_TYPE_BOOLEAN, LOGICAL_TYPE_DOUBLE,
    LOGICAL_TYPE_FLOAT, LOGICAL_TYPE_INTEGER, LOGICAL_TYPE_NA, LOGICAL_TYPE_SMALLINT,
    LOGICAL_TYPE_STRING_LITERAL, LOGICAL_TYPE_TINYINT, LOGICAL_TYPE_UBIGINT, LOGICAL_TYPE_UINTEGER,
    LOGICAL_TYPE_USMALLINT, LOGICAL_TYPE_UTINYINT,
};
pub use database::{Database, SqlParam, SqlParamValue};
pub use error::{Error, Result};
pub use value::{FromValue, Value};
