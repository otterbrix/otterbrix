use crate::config::Config;
use crate::cursor::Cursor;
use crate::error::{Error, Result};
use crate::utils::{make_sv, string_from_c};
use std::fmt;
use std::marker::PhantomData;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct SqlParam<'a> {
    pub index: i32,
    pub value: SqlParamValue<'a>,
}

#[derive(Debug, Clone, Copy)]
pub enum SqlParamValue<'a> {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Double(f64),
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

    pub fn execute(&self, sql: &str) -> Result<Cursor<'_>> {
        let ptr = unsafe { otterbrix_sys::execute_sql(self.ptr, make_sv(sql)) };
        cursor_or_error(ptr)
    }

    pub fn execute_with_params(&self, sql: &str, params: &[SqlParam<'_>]) -> Result<Cursor<'_>> {
        let raw = raw_sql_params(params);
        let ptr = unsafe {
            otterbrix_sys::execute_sql_params(self.ptr, make_sv(sql), raw.as_ptr(), raw.len())
        };
        cursor_or_error(ptr)
    }

    pub fn create_database(&self, name: &str) -> Result<Cursor<'_>> {
        let ptr = unsafe { otterbrix_sys::create_database(self.ptr, make_sv(name)) };
        cursor_or_error(ptr)
    }

    pub fn create_collection(&self, database: &str, collection: &str) -> Result<Cursor<'_>> {
        let ptr = unsafe {
            otterbrix_sys::create_collection(self.ptr, make_sv(database), make_sv(collection))
        };
        cursor_or_error(ptr)
    }

    pub fn drop_database(&self, name: &str) -> Result<Cursor<'_>> {
        let ptr = unsafe { otterbrix_sys::drop_database(self.ptr, make_sv(name)) };
        cursor_or_error(ptr)
    }

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

unsafe impl Send for Database {}
