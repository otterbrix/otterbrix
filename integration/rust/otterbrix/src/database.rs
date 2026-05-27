use crate::config::Config;
use crate::cursor::Cursor;
use crate::error::{Error, Result};
use crate::utils::{make_sv, string_from_c};
use std::fmt;
use std::marker::PhantomData;
use std::path::Path;

/// A bound parameter for a parameterised SQL statement.
///
/// Each `SqlParam` carries a 1-based positional index (matching the `$1`,
/// `$2`, ... placeholders accepted by the engine) and the value to bind. Use
/// [`Database::execute_with_params`] to execute a statement with parameters.
#[derive(Debug, Clone)]
pub struct SqlParam<'a> {
    /// 1-based position of the placeholder in the SQL text.
    pub index: i32,
    /// Value to bind at this position.
    pub value: SqlParamValue<'a>,
}

/// The set of value kinds that can be bound as a [`SqlParam`].
///
/// String values borrow from the caller (`&'a str`); all other variants are
/// owned. The engine performs the necessary type coercions when binding the
/// value to its placeholder; for instance, a [`SqlParamValue::Int64`] bound
/// against a `BIGINT` column requires no conversion.
#[derive(Debug, Clone, Copy)]
pub enum SqlParamValue<'a> {
    /// SQL `NULL`.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Unsigned 64-bit integer.
    UInt64(u64),
    /// 64-bit floating-point.
    Double(f64),
    /// UTF-8 string slice borrowed from the caller.
    Str(&'a str),
}

fn raw_sql_params(params: &[SqlParam<'_>]) -> Vec<otterbrix_sys::sql_param_t> {
    let empty = make_sv("");
    params
        .iter()
        .map(|p| {
            let (kind, bool_value, int64_value, uint64_value, double_value, string_value) =
                match &p.value {
                    SqlParamValue::Null => (
                        otterbrix_sys::sql_param_kind_t_SQL_PARAM_NULL,
                        0u8,
                        0i64,
                        0u64,
                        0.0f64,
                        empty,
                    ),
                    SqlParamValue::Bool(b) => (
                        otterbrix_sys::sql_param_kind_t_SQL_PARAM_BOOL,
                        u8::from(*b),
                        0,
                        0,
                        0.0,
                        empty,
                    ),
                    SqlParamValue::Int64(n) => (
                        otterbrix_sys::sql_param_kind_t_SQL_PARAM_INT64,
                        0,
                        *n,
                        0,
                        0.0,
                        empty,
                    ),
                    SqlParamValue::UInt64(n) => (
                        otterbrix_sys::sql_param_kind_t_SQL_PARAM_UINT64,
                        0,
                        0,
                        *n,
                        0.0,
                        empty,
                    ),
                    SqlParamValue::Double(x) => (
                        otterbrix_sys::sql_param_kind_t_SQL_PARAM_DOUBLE,
                        0,
                        0,
                        0,
                        *x,
                        empty,
                    ),
                    SqlParamValue::Str(s) => (
                        otterbrix_sys::sql_param_kind_t_SQL_PARAM_STRING,
                        0,
                        0,
                        0,
                        0.0,
                        make_sv(s),
                    ),
                };
            otterbrix_sys::sql_param_t {
                index: p.index,
                kind,
                bool_value,
                int64_value,
                uint64_value,
                double_value,
                string_value,
            }
        })
        .collect()
}

/// An open Otterbrix database.
///
/// `Database` is the central type of this crate: it owns a handle to a
/// running engine instance and exposes methods for executing SQL statements
/// and managing schema objects. Instances are constructed with
/// [`Database::open`] and released automatically on drop.
///
/// # Concurrency
///
/// `Database` is [`Send`] but not [`Sync`]. The underlying C facade
/// serialises every call through an internal mutex, so a single instance can
/// be moved freely between threads, but it cannot be shared concurrently
/// without external synchronisation. To share a single instance across
/// threads, wrap it in `Arc<Mutex<Database>>` — using [`std::sync::Arc`] and
/// either [`std::sync::Mutex`] for synchronous code or
/// [`tokio::sync::Mutex`][tokio-mutex] for asynchronous code.
///
/// [tokio-mutex]: https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html
///
/// # Lifetimes
///
/// All [`Cursor`] values returned by methods on `Database` borrow from the
/// `Database` (lifetime `'_`). This guarantees, at compile time, that no
/// cursor can outlive the database that produced it — preventing the
/// use-after-free bugs that would otherwise occur when the database's memory
/// arena is freed.
pub struct Database {
    ptr: otterbrix_sys::otterbrix_ptr,
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database").field("ptr", &self.ptr).finish()
    }
}

fn cursor_or_error<'db>(ptr: otterbrix_sys::cursor_ptr) -> Result<Cursor<'db>> {
    if ptr.is_null() {
        return Err(Error::NullPointer);
    }
    if unsafe { otterbrix_sys::cursor_is_error(ptr) } {
        let err = unsafe { otterbrix_sys::cursor_get_error(ptr) };
        let message = unsafe { string_from_c(err.message) };
        let message = if message.is_empty() {
            format!("error code {}", err.code)
        } else {
            message
        };
        unsafe { otterbrix_sys::release_cursor(ptr) };
        return Err(Error::Query {
            code: err.code,
            message,
        });
    }
    Ok(Cursor {
        ptr,
        _db: PhantomData,
    })
}

fn path_to_str(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| Error::InvalidPath(path.display().to_string()))
}

impl Database {
    /// Opens a database from `config`.
    ///
    /// All four storage paths in `config` must be valid UTF-8; non-UTF-8 paths
    /// produce [`Error::InvalidPath`]. The engine will create any missing
    /// directories under those paths.
    ///
    /// # Errors
    ///
    /// - [`Error::InvalidPath`] — at least one path in `config` is not valid UTF-8.
    /// - [`Error::NullPointer`] — the engine failed to allocate the instance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use otterbrix::{Config, Database};
    /// let cfg = Config::new("./data");
    /// let db = Database::open(cfg).expect("open database");
    /// db.create_database("app").unwrap();
    /// ```
    pub fn open(config: Config) -> Result<Self> {
        let log_path = path_to_str(&config.log_path)?;
        let wal_path = path_to_str(&config.wal_path)?;
        let disk_path = path_to_str(&config.disk_path)?;
        let main_path = path_to_str(&config.main_path)?;

        let cfg = otterbrix_sys::config_t {
            level: config.level,
            log_path: make_sv(log_path),
            wal_path: make_sv(wal_path),
            disk_path: make_sv(disk_path),
            main_path: make_sv(main_path),
            wal_on: config.wal_on,
            disk_on: config.disk_on,
            sync_to_disk: config.sync_to_disk,
        };

        let ptr = unsafe { otterbrix_sys::otterbrix_create(cfg) };
        if ptr.is_null() {
            return Err(Error::NullPointer);
        }
        Ok(Database { ptr })
    }

    /// Executes a SQL statement without parameters.
    ///
    /// Returns a [`Cursor`] for both DML and DQL statements. For DDL (e.g.
    /// `CREATE DATABASE`) and DML (e.g. `INSERT`) the cursor is empty but
    /// signals success; for `SELECT` the cursor exposes rows and columns.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Query`] if the engine reports a query error and
    /// [`Error::NullPointer`] if the engine returns a null cursor pointer
    /// (an internal failure).
    pub fn execute(&self, sql: &str) -> Result<Cursor<'_>> {
        let ptr = unsafe { otterbrix_sys::execute_sql(self.ptr, make_sv(sql)) };
        cursor_or_error(ptr)
    }

    /// Executes a parameterised SQL statement.
    ///
    /// Placeholders in `sql` use the `$N` syntax (1-based). Each [`SqlParam`]
    /// in `params` declares the position and value of one binding; the order
    /// of entries in the slice does not need to match placeholder order.
    ///
    /// # Errors
    ///
    /// Same as [`Database::execute`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use otterbrix::{Config, Database, SqlParam, SqlParamValue};
    /// # let db = Database::open(Config::new("./data")).unwrap();
    /// # db.create_database("app").unwrap();
    /// # db.create_collection("app", "t").unwrap();
    /// let params = [
    ///     SqlParam { index: 1, value: SqlParamValue::Int64(7) },
    ///     SqlParam { index: 2, value: SqlParamValue::Str("ok") },
    /// ];
    /// db.execute_with_params(
    ///     "INSERT INTO app.t (id, name) VALUES ($1, $2);",
    ///     &params,
    /// ).unwrap();
    /// ```
    pub fn execute_with_params(&self, sql: &str, params: &[SqlParam<'_>]) -> Result<Cursor<'_>> {
        let raw = raw_sql_params(params);
        let ptr = unsafe {
            otterbrix_sys::execute_sql_params(self.ptr, make_sv(sql), raw.as_ptr(), raw.len())
        };
        cursor_or_error(ptr)
    }

    /// Creates a new logical database.
    ///
    /// Uses the engine's dedicated `create_database` entry point — preferable
    /// to the equivalent `CREATE DATABASE` SQL statement, since it avoids SQL
    /// parsing and never has to be escaped against injection. Returns an
    /// empty cursor on success.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Query`] if the database already exists or the name is
    /// invalid.
    pub fn create_database(&self, name: &str) -> Result<Cursor<'_>> {
        let ptr = unsafe { otterbrix_sys::create_database(self.ptr, make_sv(name)) };
        cursor_or_error(ptr)
    }

    /// Creates a new collection inside an existing database.
    ///
    /// Uses the engine's dedicated `create_collection` entry point —
    /// preferable to the equivalent `CREATE TABLE` SQL statement, since it
    /// avoids SQL parsing and never has to be escaped against injection.
    /// Returns an empty cursor on success.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Query`] if `database` does not exist, the collection
    /// already exists, or the name is invalid.
    pub fn create_collection(&self, database: &str, collection: &str) -> Result<Cursor<'_>> {
        let ptr = unsafe {
            otterbrix_sys::create_collection(self.ptr, make_sv(database), make_sv(collection))
        };
        cursor_or_error(ptr)
    }

    /// Drops a logical database (`DROP DATABASE`).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Query`] if `name` does not refer to an existing database.
    pub fn drop_database(&self, name: &str) -> Result<Cursor<'_>> {
        let ptr = unsafe { otterbrix_sys::drop_database(self.ptr, make_sv(name)) };
        cursor_or_error(ptr)
    }

    /// Drops a collection from a database (`DROP COLLECTION`).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Query`] if either `database` or `collection` does not exist.
    pub fn drop_collection(&self, database: &str, collection: &str) -> Result<Cursor<'_>> {
        let ptr = unsafe {
            otterbrix_sys::drop_collection(self.ptr, make_sv(database), make_sv(collection))
        };
        cursor_or_error(ptr)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        unsafe { otterbrix_sys::otterbrix_destroy(self.ptr) };
    }
}

// SAFETY: Database wraps an `otterbrix_t*` whose every method is serialised
// internally by a per-instance `std::mutex`. Moving the pointer to another
// thread is therefore safe; sharing it concurrently is not, which is why we
// do **not** implement Sync.
unsafe impl Send for Database {}
