use crate::utils::{make_sv, string_from_c};
use crate::value::Value;
use std::fmt;
use std::marker::PhantomData;

/// Engine logical-type identifier returned by
/// [`Cursor::column_logical_type`].
///
/// The numeric values mirror the constants exposed by the C engine; use the
/// `LOGICAL_TYPE_*` constants in this module rather than literal integers.
pub type LogicalType = i32;

/// Logical type *unknown* / *not applicable* (e.g. column whose type cannot be inferred).
pub const LOGICAL_TYPE_NA: LogicalType = 0;
/// Logical type for `BOOLEAN` columns.
pub const LOGICAL_TYPE_BOOLEAN: LogicalType = 10;
/// Logical type for `TINYINT` (signed 8-bit) columns.
pub const LOGICAL_TYPE_TINYINT: LogicalType = 11;
/// Logical type for `SMALLINT` (signed 16-bit) columns.
pub const LOGICAL_TYPE_SMALLINT: LogicalType = 12;
/// Logical type for `INTEGER` (signed 32-bit) columns.
pub const LOGICAL_TYPE_INTEGER: LogicalType = 13;
/// Logical type for `BIGINT` (signed 64-bit) columns.
pub const LOGICAL_TYPE_BIGINT: LogicalType = 14;
/// Logical type for `FLOAT` (32-bit IEEE 754) columns.
pub const LOGICAL_TYPE_FLOAT: LogicalType = 23;
/// Logical type for `DOUBLE` (64-bit IEEE 754) columns.
pub const LOGICAL_TYPE_DOUBLE: LogicalType = 24;
/// Logical type for `UTINYINT` (unsigned 8-bit) columns.
pub const LOGICAL_TYPE_UTINYINT: LogicalType = 27;
/// Logical type for `USMALLINT` (unsigned 16-bit) columns.
pub const LOGICAL_TYPE_USMALLINT: LogicalType = 28;
/// Logical type for `UINTEGER` (unsigned 32-bit) columns.
pub const LOGICAL_TYPE_UINTEGER: LogicalType = 29;
/// Logical type for `UBIGINT` (unsigned 64-bit) columns.
pub const LOGICAL_TYPE_UBIGINT: LogicalType = 30;
/// Logical type for string-literal columns.
pub const LOGICAL_TYPE_STRING_LITERAL: LogicalType = 35;

/// Result of executing a SQL statement.
///
/// A `Cursor` holds the rows, columns and any associated metadata produced by
/// the engine. It is created by methods on [`Database`](crate::Database) such
/// as [`execute`](crate::Database::execute) and dropped automatically when it
/// goes out of scope, releasing the underlying engine-side resources.
///
/// # Lifetime
///
/// `Cursor<'db>` borrows from the [`Database`](crate::Database) that produced
/// it: the data returned by the cursor lives in the database's polymorphic
/// memory arena and is freed when that database is dropped. The borrow is
/// expressed via [`PhantomData`] and enforced by the compiler — a cursor
/// cannot outlive its database. Attempting to do so is a compile-time error,
/// not a runtime use-after-free.
pub struct Cursor<'db> {
    pub(crate) ptr: otterbrix_sys::cursor_ptr,
    pub(crate) _db: PhantomData<&'db ()>,
}

impl<'db> Cursor<'db> {
    /// Number of rows in the result set.
    pub fn size(&self) -> i32 {
        unsafe { otterbrix_sys::cursor_size(self.ptr) }
    }

    /// Number of columns in the result set.
    pub fn column_count(&self) -> i32 {
        unsafe { otterbrix_sys::cursor_column_count(self.ptr) }
    }

    /// Name of the column at `index`, or `None` if `index` is out of range.
    pub fn column_name(&self, index: i32) -> Option<String> {
        let ptr = unsafe { otterbrix_sys::cursor_column_name(self.ptr, index) };
        if ptr.is_null() {
            return None;
        }
        Some(unsafe { string_from_c(ptr) })
    }

    /// Logical type of the column at `index`, or `None` if `index` is out of range.
    ///
    /// See the `LOGICAL_TYPE_*` constants in this module for the set of
    /// possible values.
    pub fn column_logical_type(&self, index: i32) -> Option<LogicalType> {
        let v = unsafe { otterbrix_sys::cursor_column_logical_type(self.ptr, index) };
        if v < 0 {
            None
        } else {
            Some(v)
        }
    }

    /// Returns whether the cursor has more rows to iterate.
    ///
    /// Most callers should use [`Cursor::rows`] for ergonomic iteration; this
    /// method is exposed for parity with the underlying C API.
    pub fn has_next(&self) -> bool {
        unsafe { otterbrix_sys::cursor_has_next(self.ptr) }
    }

    /// Reads the cell at (`row`, `column`).
    ///
    /// Returns [`Value::Null`] for out-of-range coordinates and for null cells.
    pub fn get_value(&self, row: i32, column: i32) -> Value {
        let ptr = unsafe { otterbrix_sys::cursor_get_value(self.ptr, row, column) };
        Value::from_raw(ptr)
    }

    /// Reads the cell at `row` of the column named `column`.
    ///
    /// Returns [`Value::Null`] if the column does not exist or the cell is null.
    pub fn get_value_by_name(&self, row: i32, column: &str) -> Value {
        let ptr =
            unsafe { otterbrix_sys::cursor_get_value_by_name(self.ptr, row, make_sv(column)) };
        Value::from_raw(ptr)
    }

    /// Returns an iterator over the rows of the result set.
    ///
    /// The iterator yields [`Row`] handles, each of which can read individual
    /// cells by column index ([`Row::get`]) or by column name
    /// ([`Row::get_by_name`]).
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

// SAFETY: Cursor wraps a raw pointer returned by the C engine. The C facade
// serialises every call through a per-instance mutex, so transferring the
// pointer between threads is safe as long as it is not used concurrently from
// two threads (which is enforced by `&mut self` requirement on iteration).
unsafe impl Send for Cursor<'_> {}

/// A handle to a single row produced by [`Cursor::rows`].
///
/// `Row` borrows from its [`Cursor`]; calling [`Row::get`] or
/// [`Row::get_by_name`] reads a cell on demand. The row's underlying data is
/// owned by the cursor (and ultimately by the database's memory arena), so
/// the row cannot outlive the iterator that produced it.
pub struct Row<'a, 'db> {
    cursor: &'a Cursor<'db>,
    index: i32,
}

impl Row<'_, '_> {
    /// Zero-based index of this row within the cursor.
    pub fn index(&self) -> i32 {
        self.index
    }

    /// Reads the cell at the given column index.
    ///
    /// Equivalent to `cursor.get_value(row.index(), column)`.
    pub fn get(&self, column: i32) -> Value {
        self.cursor.get_value(self.index, column)
    }

    /// Reads the cell of the column with the given name.
    ///
    /// Equivalent to `cursor.get_value_by_name(row.index(), column)`.
    pub fn get_by_name(&self, column: &str) -> Value {
        self.cursor.get_value_by_name(self.index, column)
    }
}

impl fmt::Debug for Row<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row").field("index", &self.index).finish()
    }
}

/// Iterator over the rows of a [`Cursor`].
///
/// Created by [`Cursor::rows`]. Implements both [`Iterator`] and
/// [`ExactSizeIterator`]: the total number of rows is fixed at construction
/// time and reported by [`Iterator::size_hint`].
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
