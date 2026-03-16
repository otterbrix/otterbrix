use crate::utils::{make_sv, string_from_c};
use crate::value::Value;
use std::fmt;

pub struct Cursor {
    pub(crate) ptr: otterbrix_sys::cursor_ptr,
}

impl Cursor {
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

    pub fn rows(&self) -> Rows<'_> {
        Rows {
            cursor: self,
            index: 0,
            total: self.size(),
        }
    }
}

impl fmt::Debug for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cursor")
            .field("size", &self.size())
            .field("column_count", &self.column_count())
            .finish()
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        unsafe { otterbrix_sys::release_cursor(self.ptr) };
    }
}

pub struct Row<'a> {
    cursor: &'a Cursor,
    index: i32,
}

impl<'a> Row<'a> {
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

impl fmt::Debug for Row<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row").field("index", &self.index).finish()
    }
}

pub struct Rows<'a> {
    cursor: &'a Cursor,
    index: i32,
    total: i32,
}

impl<'a> Iterator for Rows<'a> {
    type Item = Row<'a>;

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

impl ExactSizeIterator for Rows<'_> {}
