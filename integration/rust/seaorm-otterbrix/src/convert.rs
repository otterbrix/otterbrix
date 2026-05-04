use std::collections::{BTreeMap, HashMap};

use otterbrix::{
    Cursor, Error as ObError, LogicalType, SqlParam, SqlParamValue, Value as ObValue,
    LOGICAL_TYPE_BIGINT, LOGICAL_TYPE_BOOLEAN, LOGICAL_TYPE_DOUBLE, LOGICAL_TYPE_FLOAT,
    LOGICAL_TYPE_INTEGER, LOGICAL_TYPE_SMALLINT, LOGICAL_TYPE_STRING_LITERAL, LOGICAL_TYPE_TINYINT,
    LOGICAL_TYPE_UBIGINT, LOGICAL_TYPE_UINTEGER, LOGICAL_TYPE_USMALLINT, LOGICAL_TYPE_UTINYINT,
};
use sea_orm::{sea_query::Value as SeaValue, DbErr, ProxyRow, RuntimeErr, Statement, Values};

pub fn map_otterbrix_error(err: ObError) -> DbErr {
    match err {
        ObError::Query { code, message } => DbErr::Exec(RuntimeErr::Internal(format!(
            "otterbrix[{code}]: {message}"
        ))),
        ObError::NullPointer => DbErr::Conn(RuntimeErr::Internal(
            "otterbrix returned null pointer".into(),
        )),
        ObError::InvalidPath(path) => DbErr::Conn(RuntimeErr::Internal(format!(
            "otterbrix invalid path: {path}"
        ))),
        ObError::TypeMismatch { expected, got } => DbErr::Type(format!(
            "otterbrix type mismatch: expected {expected}, got {got}"
        )),
    }
}

pub fn sea_value_to_param<'a>(index: i32, value: &'a SeaValue) -> Result<SqlParam<'a>, DbErr> {
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

        SeaValue::Bool(None)
        | SeaValue::TinyInt(None)
        | SeaValue::SmallInt(None)
        | SeaValue::Int(None)
        | SeaValue::BigInt(None)
        | SeaValue::TinyUnsigned(None)
        | SeaValue::SmallUnsigned(None)
        | SeaValue::Unsigned(None)
        | SeaValue::BigUnsigned(None)
        | SeaValue::Float(None)
        | SeaValue::Double(None)
        | SeaValue::String(None) => {
            return Err(DbErr::Type(
                "NULL parameters are not supported by the otterbrix backend".into(),
            ))
        }

        other => {
            return Err(DbErr::Type(format!(
                "unsupported sea_orm parameter type: {other:?}"
            )))
        }
    };

    Ok(SqlParam { index, value: v })
}

pub fn statement_params<'a>(values: &'a Values) -> Result<Vec<SqlParam<'a>>, DbErr> {
    values
        .0
        .iter()
        .enumerate()
        .map(|(i, v)| sea_value_to_param((i as i32) + 1, v))
        .collect()
}

pub fn cursor_value_to_sea(value: ObValue, column_type: Option<LogicalType>) -> SeaValue {
    match value {
        ObValue::Null => null_for_type(column_type),
        ObValue::Bool(b) => SeaValue::Bool(Some(b)),
        ObValue::Int(v) => SeaValue::BigInt(Some(v)),
        ObValue::UInt(v) => SeaValue::BigUnsigned(Some(v)),
        ObValue::Double(v) => SeaValue::Double(Some(v)),
        ObValue::String(s) => SeaValue::String(Some(Box::new(s))),
    }
}

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

/// Stable map key for column `index` when [`cursor_to_proxy_rows`] uses positional labels (duplicate column names in one result).
#[must_use]
pub fn positional_proxy_column_key(index: usize) -> String {
    format!("{index:08}")
}

pub fn cursor_to_proxy_rows(cursor: &Cursor) -> Vec<ProxyRow> {
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
                format!("{i:08}")
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

pub fn split_statement(statement: Statement) -> (String, Option<Values>) {
    (statement.sql, statement.values)
}

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
