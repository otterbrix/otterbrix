use crate::utils::{make_sv, string_from_c};
use crate::value::Value;
use std::fmt;
use std::marker::PhantomData;

pub type LogicalType = i32;

pub const LOGICAL_TYPE_NA: LogicalType = 0;
pub const LOGICAL_TYPE_BOOLEAN: LogicalType = 10;
pub const LOGICAL_TYPE_TINYINT: LogicalType = 11;
pub const LOGICAL_TYPE_SMALLINT: LogicalType = 12;
pub const LOGICAL_TYPE_INTEGER: LogicalType = 13;
pub const LOGICAL_TYPE_BIGINT: LogicalType = 14;
pub const LOGICAL_TYPE_FLOAT: LogicalType = 23;
pub const LOGICAL_TYPE_DOUBLE: LogicalType = 24;
pub const LOGICAL_TYPE_UTINYINT: LogicalType = 27;
pub const LOGICAL_TYPE_USMALLINT: LogicalType = 28;
pub const LOGICAL_TYPE_UINTEGER: LogicalType = 29;
pub const LOGICAL_TYPE_UBIGINT: LogicalType = 30;
pub const LOGICAL_TYPE_STRING_LITERAL: LogicalType = 35;

pub struct Cursor<'db> {
    pub(crate) ptr: otterbrix_sys::cursor_ptr,
    pub(crate) _db: PhantomData<&'db ()>,
}

impl<'db> Cursor<'db> {
    pub fn size(&self) -> i32 {
        unsafe { otterbrix_sys::cursor_size(self.ptr) }
    }

    pub fn column_count(&self) -> i32 {
        unsafe { otterbrix_sys::cursor_column_count(self.ptr) }
    }

    pub fn column_name(&self, index: i32) -> Option<String> {
        let ptr = unsafe { otterbrix_sys::cursor_column_name(self.ptr, index) };
        if ptr.is_null() {
            return None;
        }
        Some(unsafe { string_from_c(ptr) })
    }

    pub fn column_logical_type(&self, index: i32) -> Option<LogicalType> {
        let v = unsafe { otterbrix_sys::cursor_column_logical_type(self.ptr, index) };
        if v < 0 {
            None
        } else {
            Some(v)
        }
    }

    pub fn has_next(&self) -> bool {
        unsafe { otterbrix_sys::cursor_has_next(self.ptr) }
    }

    pub fn get_value(&self, row: i32, column: i32) -> Value {
        let ptr = unsafe { otterbrix_sys::cursor_get_value(self.ptr, row, column) };
        Value::from_raw(ptr)
    }

    pub fn get_value_by_name(&self, row: i32, column: &str) -> Value {
        let ptr =
            unsafe { otterbrix_sys::cursor_get_value_by_name(self.ptr, row, make_sv(column)) };
        Value::from_raw(ptr)
    }

    pub fn rows(&self) -> Rows<'_, 'db> {
        Rows {
            cursor: self,
            index: 0,
            total: self.size(),
        }
    }
}

impl fmt::Debug for Cursor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cursor")
            .field("size", &self.size())
            .field("column_count", &self.column_count())
            .finish()
    }
}

impl Drop for Cursor<'_> {
    fn drop(&mut self) {
        unsafe { otterbrix_sys::release_cursor(self.ptr) };
    }
}

unsafe impl Send for Cursor<'_> {}

pub struct Row<'a, 'db> {
    cursor: &'a Cursor<'db>,
    index: i32,
}

impl<'a, 'db> Row<'a, 'db> {
    pub fn index(&self) -> i32 {
        self.index
    }

    pub fn get(&self, column: i32) -> Value {
        self.cursor.get_value(self.index, column)
    }

    pub fn get_by_name(&self, column: &str) -> Value {
        self.cursor.get_value_by_name(self.index, column)
    }
}

impl fmt::Debug for Row<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row").field("index", &self.index).finish()
    }
}

pub struct Rows<'a, 'db> {
    cursor: &'a Cursor<'db>,
    index: i32,
    total: i32,
}

impl<'a, 'db> Iterator for Rows<'a, 'db> {
    type Item = Row<'a, 'db>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.total {
            return None;
        }
        let row = Row {
            cursor: self.cursor,
            index: self.index,
        };
        self.index += 1;
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.total - self.index) as usize;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for Rows<'_, '_> {}
