use crate::config::Config;
use crate::cursor::Cursor;
use crate::error::{Error, Result};
use crate::utils::{make_sv, string_from_c};
use std::fmt;
use std::path::Path;

pub struct Database {
    ptr: otterbrix_sys::otterbrix_ptr,
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("ptr", &self.ptr)
            .finish()
    }
}

fn cursor_or_error(ptr: otterbrix_sys::cursor_ptr) -> Result<Cursor> {
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
    Ok(Cursor { ptr })
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

    pub fn execute(&self, sql: &str) -> Result<Cursor> {
        let ptr = unsafe { otterbrix_sys::execute_sql(self.ptr, make_sv(sql)) };
        cursor_or_error(ptr)
    }

    pub fn create_database(&self, name: &str) -> Result<Cursor> {
        let ptr = unsafe { otterbrix_sys::create_database(self.ptr, make_sv(name)) };
        cursor_or_error(ptr)
    }

    pub fn create_collection(&self, database: &str, collection: &str) -> Result<Cursor> {
        let ptr = unsafe {
            otterbrix_sys::create_collection(self.ptr, make_sv(database), make_sv(collection))
        };
        cursor_or_error(ptr)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        unsafe { otterbrix_sys::otterbrix_destroy(self.ptr) };
    }
}
