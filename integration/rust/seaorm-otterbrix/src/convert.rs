//! Type conversion glue between SeaORM and Otterbrix.
//!
//! All functions here are `pub(crate)` and considered implementation details
//! of [`OtterbrixProxy`](crate::OtterbrixProxy). The single exception is
//! [`positional_proxy_column_key`], which is part of the public API because
//! callers may need it to extract values from a [`ProxyRow`] when the result
//! set has duplicate column names.

use std::collections::{BTreeMap, HashMap};

use otterbrix::{
    Cursor, Error as ObError, LogicalType, SqlParam, SqlParamValue, Value as ObValue,
    LOGICAL_TYPE_BIGINT, LOGICAL_TYPE_BOOLEAN, LOGICAL_TYPE_DOUBLE, LOGICAL_TYPE_FLOAT,
    LOGICAL_TYPE_INTEGER, LOGICAL_TYPE_SMALLINT, LOGICAL_TYPE_STRING_LITERAL, LOGICAL_TYPE_TINYINT,
    LOGICAL_TYPE_UBIGINT, LOGICAL_TYPE_UINTEGER, LOGICAL_TYPE_USMALLINT, LOGICAL_TYPE_UTINYINT,
};
use sea_orm::{sea_query::Value as SeaValue, DbErr, ProxyRow, RuntimeErr, Statement, Values};

/// Width (in characters) of zero-padded positional column keys used by
/// [`positional_proxy_column_key`]. Must be wide enough to keep keys
/// lexicographically ordered for any plausible column count.
const POSITIONAL_KEY_WIDTH: usize = 8;

/// Maps an [`otterbrix::Error`] to the corresponding SeaORM [`DbErr`] variant.
///
/// Engine-level errors (`Query`, `NullPointer`) become `DbErr::Exec` /
/// `DbErr::Conn`, type-mismatch errors become `DbErr::Type`, invalid paths
/// become `DbErr::Conn`. The `Display` text of the original error is
/// preserved verbatim inside the wrapping variant.
pub(crate) fn map_otterbrix_error(err: ObError) -> DbErr {
    let msg = err.to_string();
    match err {
        ObError::Query { .. } => DbErr::Exec(RuntimeErr::Internal(msg)),
        ObError::NullPointer => DbErr::Conn(RuntimeErr::Internal(msg)),
        ObError::InvalidPath(_) => DbErr::Conn(RuntimeErr::Internal(msg)),
        ObError::TypeMismatch { .. } => DbErr::Type(msg),
    }
}

/// Converts a single SeaORM [`SeaValue`] into an Otterbrix [`SqlParam`].
///
/// Numeric subtypes are widened to `Int64`/`UInt64`/`Double`. Returns
/// `DbErr::Type` for SeaORM variants that have no representation in
/// Otterbrix (e.g. `Bytes`, `Json`).
pub(crate) fn sea_value_to_param(index: i32, value: &SeaValue) -> Result<SqlParam<'_>, DbErr> {
    let v = match value {
        SeaValue::Bool(Some(b)) => SqlParamValue::Bool(*b),

        SeaValue::TinyInt(Some(v)) => SqlParamValue::Int64(*v as i64),
        SeaValue::SmallInt(Some(v)) => SqlParamValue::Int64(*v as i64),
        SeaValue::Int(Some(v)) => SqlParamValue::Int64(*v as i64),
        SeaValue::BigInt(Some(v)) => SqlParamValue::Int64(*v),

        SeaValue::TinyUnsigned(Some(v)) => SqlParamValue::UInt64(*v as u64),
        SeaValue::SmallUnsigned(Some(v)) => SqlParamValue::UInt64(*v as u64),
        SeaValue::Unsigned(Some(v)) => SqlParamValue::UInt64(*v as u64),
        SeaValue::BigUnsigned(Some(v)) => SqlParamValue::UInt64(*v),

        SeaValue::Float(Some(v)) => SqlParamValue::Double(*v as f64),
        SeaValue::Double(Some(v)) => SqlParamValue::Double(*v),

        SeaValue::String(Some(s)) => SqlParamValue::Str(s.as_str()),

        SeaValue::Bool(None) => SqlParamValue::Null,
        SeaValue::TinyInt(None)
        | SeaValue::SmallInt(None)
        | SeaValue::Int(None)
        | SeaValue::BigInt(None) => SqlParamValue::Null,
        SeaValue::TinyUnsigned(None)
        | SeaValue::SmallUnsigned(None)
        | SeaValue::Unsigned(None)
        | SeaValue::BigUnsigned(None) => SqlParamValue::Null,
        SeaValue::Float(None) | SeaValue::Double(None) => SqlParamValue::Null,
        SeaValue::String(None) => SqlParamValue::Null,

        other => {
            return Err(DbErr::Type(format!(
                "otterbrix cannot encode sea_orm parameter type: {other:?}"
            )))
        }
    };

    Ok(SqlParam { index, value: v })
}

/// Converts SeaORM positional [`Values`] into a vector of Otterbrix
/// [`SqlParam`]s, assigning 1-based indices.
pub(crate) fn statement_params(values: &Values) -> Result<Vec<SqlParam<'_>>, DbErr> {
    values
        .0
        .iter()
        .enumerate()
        .map(|(i, v)| sea_value_to_param((i as i32) + 1, v))
        .collect()
}

/// Converts an Otterbrix [`ObValue`] into a SeaORM [`SeaValue`].
///
/// For `ObValue::Null` the matching null variant is selected based on the
/// column's logical type (see [`null_for_type`]); for non-null values the
/// most general SeaORM subtype is used (`BigInt`/`BigUnsigned`/`Double`).
pub(crate) fn cursor_value_to_sea(value: ObValue, column_type: Option<LogicalType>) -> SeaValue {
    match value {
        ObValue::Null => null_for_type(column_type),
        ObValue::Bool(b) => SeaValue::Bool(Some(b)),
        ObValue::Int(v) => SeaValue::BigInt(Some(v)),
        ObValue::UInt(v) => SeaValue::BigUnsigned(Some(v)),
        ObValue::Double(v) => SeaValue::Double(Some(v)),
        ObValue::String(s) => SeaValue::String(Some(Box::new(s))),
    }
}

/// Picks the matching `SeaValue::*(None)` variant for a given Otterbrix
/// logical column type. Falls back to `SeaValue::String(None)` for unknown
/// or absent types.
fn null_for_type(column_type: Option<LogicalType>) -> SeaValue {
    match column_type {
        Some(LOGICAL_TYPE_BOOLEAN) => SeaValue::Bool(None),
        Some(LOGICAL_TYPE_TINYINT) => SeaValue::TinyInt(None),
        Some(LOGICAL_TYPE_SMALLINT) => SeaValue::SmallInt(None),
        Some(LOGICAL_TYPE_INTEGER) => SeaValue::Int(None),
        Some(LOGICAL_TYPE_BIGINT) => SeaValue::BigInt(None),
        Some(LOGICAL_TYPE_UTINYINT) => SeaValue::TinyUnsigned(None),
        Some(LOGICAL_TYPE_USMALLINT) => SeaValue::SmallUnsigned(None),
        Some(LOGICAL_TYPE_UINTEGER) => SeaValue::Unsigned(None),
        Some(LOGICAL_TYPE_UBIGINT) => SeaValue::BigUnsigned(None),
        Some(LOGICAL_TYPE_FLOAT) => SeaValue::Float(None),
        Some(LOGICAL_TYPE_DOUBLE) => SeaValue::Double(None),
        Some(LOGICAL_TYPE_STRING_LITERAL) => SeaValue::String(None),
        _ => SeaValue::String(None),
    }
}

/// Builds the positional key string used by [`OtterbrixProxy`](crate::OtterbrixProxy)
/// when a result set has duplicate column names.
///
/// SeaORM's [`ProxyRow`] indexes columns by name, but a `JOIN` between two
/// tables that both contain an `id` column produces a row whose two cells
/// would map to the same key and overwrite each other. To avoid that, the
/// proxy detects duplicates at result-set construction time and switches to
/// **positional keys** for *every* column of that result: the i-th column is
/// stored under the key `positional_proxy_column_key(i)` instead of its real
/// name. The keys are zero-padded so that lexicographic ordering matches the
/// numeric column order.
///
/// Use this function to read individual cells out of such a `ProxyRow`, e.g.
/// `row.try_get("", &positional_proxy_column_key(0))`.
///
/// The exact key format (`"00000000"`, `"00000001"`, ...) is part of the
/// public contract, so cleaning it up later would be a breaking change.
#[must_use]
pub fn positional_proxy_column_key(index: usize) -> String {
    format!("{index:0width$}", width = POSITIONAL_KEY_WIDTH)
}

/// Walks a [`Cursor`] and produces a vector of SeaORM [`ProxyRow`]s.
///
/// If two or more columns of the result set share the same name, the
/// function falls back to **positional keys** (see
/// [`positional_proxy_column_key`]) for every column of that result; otherwise
/// real column names are used.
pub(crate) fn cursor_to_proxy_rows(cursor: &Cursor<'_>) -> Vec<ProxyRow> {
    let column_count = cursor.column_count();
    let row_count = cursor.size();

    let mut name_hits: HashMap<String, usize> = HashMap::new();
    for i in 0..column_count {
        let base = cursor.column_name(i).unwrap_or_else(|| format!("col_{i}"));
        *name_hits.entry(base).or_insert(0) += 1;
    }
    let positional_keys = name_hits.values().any(|&c| c > 1);

    let columns: Vec<(String, Option<LogicalType>)> = (0..column_count)
        .map(|i| {
            let key = if positional_keys {
                positional_proxy_column_key(i as usize)
            } else {
                cursor.column_name(i).unwrap_or_else(|| format!("col_{i}"))
            };
            let logical = cursor.column_logical_type(i);
            (key, logical)
        })
        .collect();

    let mut rows = Vec::with_capacity(row_count.max(0) as usize);
    for r in 0..row_count {
        let mut map: BTreeMap<String, SeaValue> = BTreeMap::new();
        for (c, (name, logical)) in columns.iter().enumerate() {
            let value = cursor.get_value(r, c as i32);
            map.insert(name.clone(), cursor_value_to_sea(value, *logical));
        }
        rows.push(ProxyRow::new(map));
    }
    rows
}

/// Splits a SeaORM [`Statement`] into its raw SQL text and optional bound values.
pub(crate) fn split_statement(statement: Statement) -> (String, Option<Values>) {
    (statement.sql, statement.values)
}

/// Rewrites SQL placeholder syntax from `?` (used by SeaORM's MySQL/SQLite
/// backends) to `$N` (accepted by Otterbrix).
///
/// The rewrite respects single-quoted string literals, double-quoted
/// identifiers and the standard SQL doubled-quote escape (`''`). `?`
/// inside such regions is preserved verbatim. Existing `$N` placeholders
/// are left untouched.
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

#[cfg(test)]
mod placeholder_tests {
    use super::rewrite_placeholders;

    #[test]
    fn rewrites_simple_sequence() {
        assert_eq!(
            rewrite_placeholders("SELECT * FROM t WHERE a = ? AND b = ?"),
            "SELECT * FROM t WHERE a = $1 AND b = $2"
        );
    }

    #[test]
    fn skips_question_inside_single_quoted_string() {
        assert_eq!(
            rewrite_placeholders("INSERT INTO t (s) VALUES ('what?')"),
            "INSERT INTO t (s) VALUES ('what?')"
        );
    }

    #[test]
    fn skips_question_inside_double_quoted_identifier() {
        assert_eq!(
            rewrite_placeholders("SELECT \"col?\" FROM t WHERE a = ?"),
            "SELECT \"col?\" FROM t WHERE a = $1"
        );
    }

    #[test]
    fn handles_doubled_single_quote_escape() {
        assert_eq!(
            rewrite_placeholders("INSERT INTO t (s) VALUES ('it''s ?'), (?)"),
            "INSERT INTO t (s) VALUES ('it''s ?'), ($1)"
        );
    }

    #[test]
    fn leaves_dollar_placeholders_untouched() {
        assert_eq!(
            rewrite_placeholders("SELECT * FROM t LIMIT $1 OFFSET $2"),
            "SELECT * FROM t LIMIT $1 OFFSET $2"
        );
    }
}

#[cfg(test)]
mod error_mapping_tests {
    use super::map_otterbrix_error;
    use otterbrix::Error as ObError;
    use sea_orm::DbErr;

    #[test]
    fn query_error_becomes_exec_with_code_and_message() {
        let mapped = map_otterbrix_error(ObError::Query {
            code: 7,
            message: "syntax".into(),
        });
        match mapped {
            DbErr::Exec(inner) => {
                let s = format!("{inner:?}");
                assert!(s.contains('7'), "expected error code in {s:?}");
                assert!(s.contains("syntax"), "expected message in {s:?}");
            }
            other => panic!("expected DbErr::Exec, got {other:?}"),
        }
    }

    #[test]
    fn null_pointer_becomes_conn_error() {
        match map_otterbrix_error(ObError::NullPointer) {
            DbErr::Conn(inner) => {
                let s = format!("{inner:?}");
                assert!(s.contains("null pointer"), "got {s:?}");
            }
            other => panic!("expected DbErr::Conn, got {other:?}"),
        }
    }

    #[test]
    fn invalid_path_becomes_conn_error() {
        match map_otterbrix_error(ObError::InvalidPath("/no/such/path".into())) {
            DbErr::Conn(inner) => {
                let s = format!("{inner:?}");
                assert!(s.contains("invalid path"), "got {s:?}");
                assert!(s.contains("/no/such/path"), "got {s:?}");
            }
            other => panic!("expected DbErr::Conn, got {other:?}"),
        }
    }

    #[test]
    fn type_mismatch_becomes_type_error() {
        match map_otterbrix_error(ObError::TypeMismatch {
            expected: "i64",
            got: "string",
        }) {
            DbErr::Type(s) => {
                assert!(s.contains("i64"), "got {s:?}");
                assert!(s.contains("string"), "got {s:?}");
            }
            other => panic!("expected DbErr::Type, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod null_for_type_tests {
    use super::null_for_type;
    use otterbrix::{
        LogicalType, LOGICAL_TYPE_BIGINT, LOGICAL_TYPE_BOOLEAN, LOGICAL_TYPE_DOUBLE,
        LOGICAL_TYPE_FLOAT, LOGICAL_TYPE_INTEGER, LOGICAL_TYPE_SMALLINT,
        LOGICAL_TYPE_STRING_LITERAL, LOGICAL_TYPE_TINYINT, LOGICAL_TYPE_UBIGINT,
        LOGICAL_TYPE_UINTEGER, LOGICAL_TYPE_USMALLINT, LOGICAL_TYPE_UTINYINT,
    };
    use sea_orm::sea_query::Value as SeaValue;

    #[test]
    fn maps_known_logical_types_to_matching_null_variants() {
        type NullCase = (LogicalType, fn(&SeaValue) -> bool);
        let cases: &[NullCase] = &[
            (LOGICAL_TYPE_BOOLEAN, |v| matches!(v, SeaValue::Bool(None))),
            (LOGICAL_TYPE_TINYINT, |v| {
                matches!(v, SeaValue::TinyInt(None))
            }),
            (LOGICAL_TYPE_SMALLINT, |v| {
                matches!(v, SeaValue::SmallInt(None))
            }),
            (LOGICAL_TYPE_INTEGER, |v| matches!(v, SeaValue::Int(None))),
            (LOGICAL_TYPE_BIGINT, |v| matches!(v, SeaValue::BigInt(None))),
            (LOGICAL_TYPE_UTINYINT, |v| {
                matches!(v, SeaValue::TinyUnsigned(None))
            }),
            (LOGICAL_TYPE_USMALLINT, |v| {
                matches!(v, SeaValue::SmallUnsigned(None))
            }),
            (LOGICAL_TYPE_UINTEGER, |v| {
                matches!(v, SeaValue::Unsigned(None))
            }),
            (LOGICAL_TYPE_UBIGINT, |v| {
                matches!(v, SeaValue::BigUnsigned(None))
            }),
            (LOGICAL_TYPE_FLOAT, |v| matches!(v, SeaValue::Float(None))),
            (LOGICAL_TYPE_DOUBLE, |v| matches!(v, SeaValue::Double(None))),
            (LOGICAL_TYPE_STRING_LITERAL, |v| {
                matches!(v, SeaValue::String(None))
            }),
        ];
        for (lt, predicate) in cases {
            let v = null_for_type(Some(*lt));
            assert!(predicate(&v), "wrong NULL variant for {lt}: {v:?}");
        }
    }

    #[test]
    fn unknown_logical_type_falls_back_to_string_null() {
        assert!(matches!(null_for_type(Some(9999)), SeaValue::String(None)));
    }

    #[test]
    fn missing_logical_type_falls_back_to_string_null() {
        assert!(matches!(null_for_type(None), SeaValue::String(None)));
    }
}

#[cfg(test)]
mod sea_value_to_param_tests {
    use super::sea_value_to_param;
    use otterbrix::SqlParamValue;
    use sea_orm::{sea_query::Value as SeaValue, DbErr};

    fn check<F>(value: SeaValue, f: F)
    where
        F: FnOnce(&SqlParamValue<'_>),
    {
        let param = sea_value_to_param(1, &value).expect("param conversion succeeds");
        f(&param.value);
    }

    #[test]
    fn null_variants_collapse_to_sql_null() {
        let nulls = [
            SeaValue::Bool(None),
            SeaValue::TinyInt(None),
            SeaValue::SmallInt(None),
            SeaValue::Int(None),
            SeaValue::BigInt(None),
            SeaValue::TinyUnsigned(None),
            SeaValue::SmallUnsigned(None),
            SeaValue::Unsigned(None),
            SeaValue::BigUnsigned(None),
            SeaValue::Float(None),
            SeaValue::Double(None),
            SeaValue::String(None),
        ];
        for v in nulls {
            let label = format!("{v:?}");
            check(v, |p| {
                assert!(
                    matches!(p, SqlParamValue::Null),
                    "expected SqlParamValue::Null for {label}"
                )
            });
        }
    }

    #[test]
    fn signed_integers_widen_to_int64() {
        check(SeaValue::TinyInt(Some(-5)), |p| {
            assert!(matches!(p, SqlParamValue::Int64(-5)))
        });
        check(SeaValue::SmallInt(Some(-1234)), |p| {
            assert!(matches!(p, SqlParamValue::Int64(-1234)))
        });
        check(SeaValue::Int(Some(-99)), |p| {
            assert!(matches!(p, SqlParamValue::Int64(-99)))
        });
        check(SeaValue::BigInt(Some(i64::MIN)), |p| {
            assert!(matches!(p, SqlParamValue::Int64(v) if *v == i64::MIN))
        });
    }

    #[test]
    fn unsigned_integers_widen_to_uint64() {
        check(SeaValue::TinyUnsigned(Some(7)), |p| {
            assert!(matches!(p, SqlParamValue::UInt64(7)))
        });
        check(SeaValue::BigUnsigned(Some(u64::MAX)), |p| {
            assert!(matches!(p, SqlParamValue::UInt64(v) if *v == u64::MAX))
        });
    }

    #[test]
    fn float_widens_to_double() {
        check(SeaValue::Float(Some(1.5)), |p| match p {
            SqlParamValue::Double(v) => assert!((v - 1.5).abs() < 1e-6),
            other => panic!("expected Double, got {other:?}"),
        });
    }

    #[test]
    fn unsupported_type_returns_type_error() {
        let v = SeaValue::Bytes(Some(Box::new(vec![1, 2, 3])));
        match sea_value_to_param(1, &v) {
            Err(DbErr::Type(_)) => {}
            other => panic!("expected DbErr::Type, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod positional_key_tests {
    use super::{positional_proxy_column_key, POSITIONAL_KEY_WIDTH};

    #[test]
    fn key_width_matches_constant() {
        assert_eq!(positional_proxy_column_key(0).len(), POSITIONAL_KEY_WIDTH);
        assert_eq!(positional_proxy_column_key(42).len(), POSITIONAL_KEY_WIDTH);
    }

    #[test]
    fn keys_sort_in_index_order() {
        let mut keys: Vec<_> = (0..50).map(positional_proxy_column_key).collect();
        let original = keys.clone();
        keys.sort();
        assert_eq!(keys, original, "lexicographic sort must match index order");
    }
}
