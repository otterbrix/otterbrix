//! Conversion glue between SQLx and Otterbrix.
//!
//! All items in this module are `pub(crate)` and considered implementation
//! details; the public surface of the crate goes through the SQLx trait
//! implementations defined in sibling modules.

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

/// Maps an [`otterbrix::Error`] to the corresponding [`sqlx_core::error::Error`].
///
/// Engine query errors are wrapped in [`OtterbrixDbError`] and surface as
/// `Error::Database`; structural failures (`NullPointer`, `TypeMismatch`)
/// become `Error::Protocol`; invalid paths become `Error::Configuration`.
/// The original `Display` text is preserved in every variant.
pub(crate) fn map_otterbrix_error(err: otterbrix::Error) -> Error {
    let msg = err.to_string();
    match err {
        otterbrix::Error::Query { code, message } => {
            Error::Database(Box::new(OtterbrixDbError { code, message }))
        }
        otterbrix::Error::NullPointer => Error::Protocol(msg),
        otterbrix::Error::InvalidPath(_) => Error::Configuration(msg.into()),
        otterbrix::Error::TypeMismatch { .. } => Error::Protocol(msg),
    }
}

/// Rewrites SQLx-style `?` placeholders to engine-style `$1`, `$2`, ...
///
/// Skips `?` inside single-quoted string literals and double-quoted
/// identifiers, and respects the standard SQL doubled-quote escape (`''`).
/// Existing `$N` placeholders pass through untouched. Mirrors the
/// implementation in `seaorm-otterbrix` and shares the same behaviour.
pub(crate) fn rewrite_placeholders(sql: &str) -> String {
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

/// Counts `?` placeholders in `sql` using the same skipping rules as
/// [`rewrite_placeholders`]. Used to populate the placeholder count of
/// [`OtterbrixStatement`](crate::OtterbrixStatement) without performing
/// the rewrite.
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

/// Converts a slice of [`OtterbrixArgumentValue`]s into Otterbrix
/// [`SqlParam`]s with 1-based indices.
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

/// Picks the matching [`OtterbrixTypeInfo`] for an Otterbrix logical type.
/// Falls back to `text` for unknown / `NA` / absent types.
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

/// Walks an Otterbrix [`Cursor`] and produces a vector of
/// [`OtterbrixRow`]s plus the row-count (used as `rows_affected` for DML).
///
/// If the result set has duplicate column names, the function falls back to
/// positional `"00000000"`-style keys for every column of that result;
/// otherwise real column names are used. Column ordinals always reflect
/// declaration order.
pub(crate) fn materialize_cursor(cursor: &Cursor<'_>) -> Result<(Vec<OtterbrixRow>, u64), Error> {
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
    for r in 0..row_count as i32 {
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

    let rows_affected = cursor.size().max(0) as u64;
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

#[cfg(test)]
mod placeholder_tests {
    use super::{count_placeholders, rewrite_placeholders};

    #[test]
    fn rewrites_simple_sequence() {
        assert_eq!(
            rewrite_placeholders("SELECT * FROM t WHERE a = ? AND b = ?"),
            "SELECT * FROM t WHERE a = $1 AND b = $2"
        );
    }

    #[test]
    fn ignores_question_marks_inside_single_quotes() {
        assert_eq!(
            rewrite_placeholders("SELECT * FROM t WHERE a = '?' AND b = ?"),
            "SELECT * FROM t WHERE a = '?' AND b = $1"
        );
    }

    #[test]
    fn ignores_question_marks_inside_double_quotes() {
        assert_eq!(
            rewrite_placeholders(r#"SELECT "?" FROM t WHERE a = ?"#),
            r#"SELECT "?" FROM t WHERE a = $1"#
        );
    }

    #[test]
    fn handles_doubled_single_quote_inside_string() {
        assert_eq!(
            rewrite_placeholders("SELECT * FROM t WHERE a = 'O''Brien' AND b = ?"),
            "SELECT * FROM t WHERE a = 'O''Brien' AND b = $1"
        );
    }

    #[test]
    fn leaves_existing_dollar_placeholders_untouched() {
        assert_eq!(
            rewrite_placeholders("SELECT * FROM t WHERE a = $1 AND b = $2"),
            "SELECT * FROM t WHERE a = $1 AND b = $2"
        );
    }

    #[test]
    fn count_matches_rewrite() {
        let sql = "INSERT INTO t (a, b, c) VALUES (?, ?, ?)";
        assert_eq!(count_placeholders(sql), 3);
    }

    #[test]
    fn count_skips_placeholders_inside_strings() {
        let sql = "SELECT '?' FROM t WHERE a = ? AND b = \"?\" AND c = ?";
        assert_eq!(count_placeholders(sql), 2);
    }

    #[test]
    fn count_zero_for_no_placeholders() {
        assert_eq!(count_placeholders("SELECT 1"), 0);
    }
}

#[cfg(test)]
mod error_mapping_tests {
    use super::map_otterbrix_error;
    use otterbrix::Error as ObError;
    use sqlx_core::error::Error;

    #[test]
    fn query_error_becomes_database_error_with_code_and_message() {
        let err = map_otterbrix_error(ObError::Query {
            code: 42,
            message: "boom".to_owned(),
        });
        match err {
            Error::Database(db_err) => {
                assert_eq!(db_err.message(), "boom");
                assert_eq!(db_err.code().as_deref(), Some("42"));
            }
            other => panic!("expected Error::Database, got {other:?}"),
        }
    }

    #[test]
    fn null_pointer_becomes_protocol_error() {
        let err = map_otterbrix_error(ObError::NullPointer);
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn invalid_path_becomes_configuration_error() {
        let err = map_otterbrix_error(ObError::InvalidPath("/x/y".into()));
        assert!(matches!(err, Error::Configuration(_)), "got {err:?}");
    }

    #[test]
    fn type_mismatch_becomes_protocol_error() {
        let err = map_otterbrix_error(ObError::TypeMismatch {
            expected: "Int",
            got: "String",
        });
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }
}

#[cfg(test)]
mod arguments_to_params_tests {
    use super::arguments_to_params;
    use crate::arguments::OtterbrixArgumentValue;
    use otterbrix::SqlParamValue;

    #[test]
    fn empty_list_yields_empty_vec() {
        let params = arguments_to_params(&[]).expect("ok");
        assert!(params.is_empty());
    }

    #[test]
    fn indices_start_from_one_and_are_sequential() {
        let args = vec![
            OtterbrixArgumentValue::Int64(10),
            OtterbrixArgumentValue::Int64(20),
            OtterbrixArgumentValue::Int64(30),
        ];
        let params = arguments_to_params(&args).expect("ok");
        let indices: Vec<i32> = params.iter().map(|p| p.index).collect();
        assert_eq!(indices, vec![1, 2, 3]);
    }

    #[test]
    fn each_kind_maps_to_matching_sql_param_value() {
        let args = vec![
            OtterbrixArgumentValue::Null,
            OtterbrixArgumentValue::Bool(true),
            OtterbrixArgumentValue::Int64(-7),
            OtterbrixArgumentValue::UInt64(42),
            OtterbrixArgumentValue::Double(1.5),
            OtterbrixArgumentValue::Str(std::borrow::Cow::Borrowed("hi")),
        ];
        let params = arguments_to_params(&args).expect("ok");
        assert!(matches!(params[0].value, SqlParamValue::Null));
        assert!(matches!(params[1].value, SqlParamValue::Bool(true)));
        assert!(matches!(params[2].value, SqlParamValue::Int64(-7)));
        assert!(matches!(params[3].value, SqlParamValue::UInt64(42)));
        match params[4].value {
            SqlParamValue::Double(v) => assert!((v - 1.5).abs() < 1e-9),
            ref other => panic!("expected Double, got {other:?}"),
        }
        match &params[5].value {
            SqlParamValue::Str(s) => assert_eq!(*s, "hi"),
            other => panic!("expected Str, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod logical_to_type_info_tests {
    use super::logical_to_type_info;
    use crate::r#type::OtterbrixTypeInfo;
    use otterbrix::{
        LOGICAL_TYPE_BIGINT, LOGICAL_TYPE_BOOLEAN, LOGICAL_TYPE_DOUBLE, LOGICAL_TYPE_FLOAT,
        LOGICAL_TYPE_INTEGER, LOGICAL_TYPE_NA, LOGICAL_TYPE_SMALLINT, LOGICAL_TYPE_STRING_LITERAL,
        LOGICAL_TYPE_TINYINT, LOGICAL_TYPE_UBIGINT, LOGICAL_TYPE_UINTEGER, LOGICAL_TYPE_USMALLINT,
        LOGICAL_TYPE_UTINYINT,
    };

    #[test]
    fn maps_each_logical_to_expected_kind() {
        let cases: &[(_, OtterbrixTypeInfo)] = &[
            (LOGICAL_TYPE_BOOLEAN, OtterbrixTypeInfo::bool()),
            (LOGICAL_TYPE_TINYINT, OtterbrixTypeInfo::integer()),
            (LOGICAL_TYPE_SMALLINT, OtterbrixTypeInfo::integer()),
            (LOGICAL_TYPE_INTEGER, OtterbrixTypeInfo::integer()),
            (LOGICAL_TYPE_BIGINT, OtterbrixTypeInfo::integer()),
            (LOGICAL_TYPE_UTINYINT, OtterbrixTypeInfo::unsigned()),
            (LOGICAL_TYPE_USMALLINT, OtterbrixTypeInfo::unsigned()),
            (LOGICAL_TYPE_UINTEGER, OtterbrixTypeInfo::unsigned()),
            (LOGICAL_TYPE_UBIGINT, OtterbrixTypeInfo::unsigned()),
            (LOGICAL_TYPE_FLOAT, OtterbrixTypeInfo::float()),
            (LOGICAL_TYPE_DOUBLE, OtterbrixTypeInfo::float()),
            (LOGICAL_TYPE_STRING_LITERAL, OtterbrixTypeInfo::text()),
        ];
        for (lt, expected) in cases {
            let got = logical_to_type_info(Some(*lt));
            assert_eq!(&got, expected, "wrong kind for {lt}");
        }
    }

    #[test]
    fn na_falls_back_to_text() {
        let got = logical_to_type_info(Some(LOGICAL_TYPE_NA));
        assert_eq!(got, OtterbrixTypeInfo::text());
    }

    #[test]
    fn none_falls_back_to_text() {
        let got = logical_to_type_info(None);
        assert_eq!(got, OtterbrixTypeInfo::text());
    }

    #[test]
    fn unknown_logical_falls_back_to_text() {
        let got = logical_to_type_info(Some(9999));
        assert_eq!(got, OtterbrixTypeInfo::text());
    }
}
