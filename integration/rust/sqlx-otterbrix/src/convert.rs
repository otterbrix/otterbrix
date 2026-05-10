use otterbrix::{
    Cursor, LogicalType, SqlParam, SqlParamValue, Value as ObValue, LOGICAL_TYPE_BIGINT,
    LOGICAL_TYPE_BOOLEAN, LOGICAL_TYPE_DOUBLE, LOGICAL_TYPE_FLOAT, LOGICAL_TYPE_INTEGER,
    LOGICAL_TYPE_NA, LOGICAL_TYPE_SMALLINT, LOGICAL_TYPE_STRING_LITERAL, LOGICAL_TYPE_TINYINT,
    LOGICAL_TYPE_UBIGINT, LOGICAL_TYPE_UINTEGER, LOGICAL_TYPE_USMALLINT, LOGICAL_TYPE_UTINYINT,
};

use crate::arguments::OtterbrixArgumentValue;
use crate::column::OtterbrixColumn;
use crate::error::OtterbrixDbError;
use crate::r#type::OtterbrixTypeInfo;
use crate::row::OtterbrixRow;
use crate::value::OtterbrixValue;

use sqlx_core::error::Error;
use sqlx_core::ext::ustr::UStr;
use sqlx_core::HashMap;

pub(crate) fn map_otterbrix_error(err: otterbrix::Error) -> Error {
    match err {
        otterbrix::Error::Query { code, message } => {
            Error::Database(Box::new(OtterbrixDbError { code, message }))
        }
        otterbrix::Error::NullPointer => {
            Error::Protocol("otterbrix returned a null pointer from the native API".to_owned())
        }
        otterbrix::Error::InvalidPath(path) => {
            Error::Configuration(format!("otterbrix invalid path: {path}").into())
        }
        otterbrix::Error::TypeMismatch { expected, got } => Error::Protocol(format!(
            "otterbrix type mismatch: expected {expected}, got {got}"
        )),
    }
}

/// Maps `?` placeholders to `$1`, `$2`, … (skips `?` inside string literals), matching
/// [`seaorm-otterbrix`].
pub fn rewrite_placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut in_single = false;
    let mut in_double = false;
    let mut idx: u32 = 1;
    for c in sql.chars() {
        match c {
            '\'' if !in_double => {
                in_single = !in_single;
                out.push(c);
            }
            '"' if !in_single => {
                in_double = !in_double;
                out.push(c);
            }
            '?' if !in_single && !in_double => {
                out.push('$');
                out.push_str(&idx.to_string());
                idx += 1;
            }
            _ => out.push(c),
        }
    }
    out
}

pub(crate) fn count_placeholders(sql: &str) -> usize {
    let mut in_single = false;
    let mut in_double = false;
    let mut n = 0usize;
    for c in sql.chars() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '?' if !in_single && !in_double => n += 1,
            _ => {}
        }
    }
    n
}

pub(crate) fn arguments_to_params<'a>(
    args: &'a [OtterbrixArgumentValue<'a>],
) -> Result<Vec<SqlParam<'a>>, Error> {
    args.iter()
        .enumerate()
        .map(|(i, v)| {
            let value = match v {
                OtterbrixArgumentValue::Null => SqlParamValue::Null,
                OtterbrixArgumentValue::Bool(b) => SqlParamValue::Bool(*b),
                OtterbrixArgumentValue::Int64(n) => SqlParamValue::Int64(*n),
                OtterbrixArgumentValue::UInt64(n) => SqlParamValue::UInt64(*n),
                OtterbrixArgumentValue::Double(x) => SqlParamValue::Double(*x),
                OtterbrixArgumentValue::Str(s) => SqlParamValue::Str(s.as_ref()),
            };
            Ok(SqlParam {
                index: (i as i32) + 1,
                value,
            })
        })
        .collect()
}

fn logical_to_type_info(lt: Option<LogicalType>) -> OtterbrixTypeInfo {
    match lt {
        None | Some(LOGICAL_TYPE_NA) => OtterbrixTypeInfo::text(),
        Some(LOGICAL_TYPE_BOOLEAN) => OtterbrixTypeInfo::bool(),
        Some(LOGICAL_TYPE_TINYINT)
        | Some(LOGICAL_TYPE_SMALLINT)
        | Some(LOGICAL_TYPE_INTEGER)
        | Some(LOGICAL_TYPE_BIGINT) => OtterbrixTypeInfo::integer(),
        Some(LOGICAL_TYPE_UTINYINT)
        | Some(LOGICAL_TYPE_USMALLINT)
        | Some(LOGICAL_TYPE_UINTEGER)
        | Some(LOGICAL_TYPE_UBIGINT) => OtterbrixTypeInfo::unsigned(),
        Some(LOGICAL_TYPE_FLOAT) | Some(LOGICAL_TYPE_DOUBLE) => OtterbrixTypeInfo::float(),
        Some(LOGICAL_TYPE_STRING_LITERAL) => OtterbrixTypeInfo::text(),
        Some(_) => OtterbrixTypeInfo::text(),
    }
}

pub(crate) fn materialize_cursor(cursor: &Cursor) -> Result<(Vec<OtterbrixRow>, u64), Error> {
    let column_count = cursor.column_count().max(0) as usize;
    let row_count = cursor.size().max(0) as usize;

    let mut name_hits: HashMap<String, usize> = HashMap::default();
    for i in 0..column_count {
        let base = cursor
            .column_name(i as i32)
            .unwrap_or_else(|| format!("col_{i}"));
        *name_hits.entry(base).or_insert(0) += 1;
    }
    let positional_keys = name_hits.values().any(|&c| c > 1);

    let mut columns: Vec<OtterbrixColumn> = Vec::with_capacity(column_count);
    let mut column_names: HashMap<UStr, usize> = HashMap::default();

    for i in 0..column_count {
        let logical = cursor.column_logical_type(i as i32);
        let type_info = logical_to_type_info(logical);
        let name = if positional_keys {
            format!("{i:08}")
        } else {
            cursor
                .column_name(i as i32)
                .unwrap_or_else(|| format!("col_{i}"))
        };
        let u = UStr::new(&name);
        column_names.insert(u.clone(), i);
        columns.push(OtterbrixColumn {
            name: u,
            ordinal: i,
            type_info,
        });
    }

    let columns = std::sync::Arc::new(columns);
    let column_names = std::sync::Arc::new(column_names);

    let mut rows = Vec::with_capacity(row_count);
    for r in 0..cursor.size().max(0) {
        let mut values = Vec::with_capacity(column_count);
        for c in 0..column_count {
            let cell = cursor.get_value(r, c as i32);
            let col_logical = cursor.column_logical_type(c as i32);
            values.push(cell_to_value(cell, col_logical));
        }
        rows.push(OtterbrixRow {
            values: values.into_boxed_slice(),
            columns: std::sync::Arc::clone(&columns),
            column_names: std::sync::Arc::clone(&column_names),
        });
    }

    let rows_affected = estimate_rows_affected(cursor, column_count, row_count);
    Ok((rows, rows_affected))
}

fn cell_to_value(cell: ObValue, col_logical: Option<LogicalType>) -> OtterbrixValue {
    let type_info = if matches!(cell, ObValue::Null) {
        OtterbrixTypeInfo::null()
    } else {
        logical_to_type_info(col_logical)
    };
    OtterbrixValue {
        raw: cell,
        type_info,
    }
}

fn estimate_rows_affected(cursor: &Cursor, column_count: usize, row_count: usize) -> u64 {
    if column_count > 0 {
        row_count as u64
    } else {
        (cursor.size().max(0) as u64).max(1)
    }
}
