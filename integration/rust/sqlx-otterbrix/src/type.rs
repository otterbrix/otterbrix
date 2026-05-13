use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use sqlx_core::error::BoxDynError;
use sqlx_core::type_info::TypeInfo;

/// Internal representation of [`OtterbrixTypeInfo`].
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum OtterbrixKind {
    Null,
    Bool,
    Integer,
    Unsigned,
    Float,
    Text,
}

/// Logical SQL type reported by Otterbrix for encode/decode compatibility checks.
///
/// `OtterbrixTypeInfo` is the [`TypeInfo`] implementation of the
/// [`Otterbrix`](crate::Otterbrix) database. Each variant maps onto a family
/// of engine logical types — for example `Integer` covers `tinyint` through
/// `bigint`, `Unsigned` covers `utinyint` through `ubigint`, and `Float`
/// covers both `float` and `double`. Numeric variants are mutually
/// [`type_compatible`](TypeInfo::type_compatible) so that lossless widening
/// (`i32` → `BIGINT`, `u32` → `UBIGINT`, `i32` → `DOUBLE`) is accepted.
///
/// Construct via [`FromStr`]:
///
/// ```
/// # use std::str::FromStr;
/// use sqlx_otterbrix::OtterbrixTypeInfo;
/// assert_eq!(
///     OtterbrixTypeInfo::from_str("bigint").unwrap().to_string(),
///     "BIGINT"
/// );
/// ```
#[derive(Debug, Clone)]
pub struct OtterbrixTypeInfo(pub(crate) OtterbrixKind);

impl OtterbrixTypeInfo {
    /// Constructs the type info for SQL `NULL`.
    pub(crate) fn null() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Null)
    }

    /// Constructs the type info for `BOOLEAN`.
    pub(crate) fn bool() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Bool)
    }

    /// Constructs the type info for any signed integer column.
    pub(crate) fn integer() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Integer)
    }

    /// Constructs the type info for any unsigned integer column.
    pub(crate) fn unsigned() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Unsigned)
    }

    /// Constructs the type info for `FLOAT` or `DOUBLE`.
    pub(crate) fn float() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Float)
    }

    /// Constructs the type info for `STRING` / `TEXT` columns.
    pub(crate) fn text() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Text)
    }
}

impl Display for OtterbrixTypeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

impl TypeInfo for OtterbrixTypeInfo {
    fn is_null(&self) -> bool {
        matches!(self.0, OtterbrixKind::Null)
    }

    fn name(&self) -> &str {
        match self.0 {
            OtterbrixKind::Null => "NULL",
            OtterbrixKind::Bool => "BOOLEAN",
            OtterbrixKind::Integer => "BIGINT",
            OtterbrixKind::Unsigned => "UBIGINT",
            OtterbrixKind::Float => "DOUBLE",
            OtterbrixKind::Text => "STRING",
        }
    }

    fn type_compatible(&self, other: &Self) -> bool {
        matches!(
            (self.0, other.0),
            (OtterbrixKind::Integer, OtterbrixKind::Unsigned)
                | (OtterbrixKind::Unsigned, OtterbrixKind::Integer)
                | (OtterbrixKind::Float, OtterbrixKind::Integer)
                | (OtterbrixKind::Integer, OtterbrixKind::Float)
                | (OtterbrixKind::Float, OtterbrixKind::Unsigned)
                | (OtterbrixKind::Unsigned, OtterbrixKind::Float)
        ) || self.0 == other.0
    }
}

impl PartialEq for OtterbrixTypeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for OtterbrixTypeInfo {}

impl Hash for OtterbrixTypeInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl FromStr for OtterbrixTypeInfo {
    type Err = BoxDynError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        Ok(match s.as_str() {
            "boolean" | "bool" => OtterbrixTypeInfo::bool(),
            "bigint" | "integer" | "int" => OtterbrixTypeInfo::integer(),
            "ubigint" | "uinteger" => OtterbrixTypeInfo::unsigned(),
            "double" | "float" | "real" => OtterbrixTypeInfo::float(),
            "string" | "text" | "varchar" => OtterbrixTypeInfo::text(),
            "null" => OtterbrixTypeInfo::null(),
            _ => return Err(format!("unknown Otterbrix type name: {s}").into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_recognises_each_known_name() {
        let cases: &[(&str, OtterbrixTypeInfo)] = &[
            ("boolean", OtterbrixTypeInfo::bool()),
            ("Bool", OtterbrixTypeInfo::bool()),
            ("BIGINT", OtterbrixTypeInfo::integer()),
            ("integer", OtterbrixTypeInfo::integer()),
            ("int", OtterbrixTypeInfo::integer()),
            ("ubigint", OtterbrixTypeInfo::unsigned()),
            ("uinteger", OtterbrixTypeInfo::unsigned()),
            ("double", OtterbrixTypeInfo::float()),
            ("float", OtterbrixTypeInfo::float()),
            ("real", OtterbrixTypeInfo::float()),
            ("string", OtterbrixTypeInfo::text()),
            ("text", OtterbrixTypeInfo::text()),
            ("varchar", OtterbrixTypeInfo::text()),
            ("null", OtterbrixTypeInfo::null()),
        ];
        for (input, expected) in cases {
            let got: OtterbrixTypeInfo = input.parse().expect(input);
            assert_eq!(&got, expected, "mismatch for {input}");
        }
    }

    #[test]
    fn from_str_rejects_unknown_name() {
        let err = "blob".parse::<OtterbrixTypeInfo>().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("blob"), "msg = {msg}");
    }

    #[test]
    fn type_compatible_is_reflexive() {
        let infos = [
            OtterbrixTypeInfo::null(),
            OtterbrixTypeInfo::bool(),
            OtterbrixTypeInfo::integer(),
            OtterbrixTypeInfo::unsigned(),
            OtterbrixTypeInfo::float(),
            OtterbrixTypeInfo::text(),
        ];
        for ti in &infos {
            assert!(ti.type_compatible(ti));
        }
    }

    #[test]
    fn type_compatible_treats_numeric_kinds_interchangeably() {
        let int = OtterbrixTypeInfo::integer();
        let uint = OtterbrixTypeInfo::unsigned();
        let float = OtterbrixTypeInfo::float();
        for (a, b) in [(&int, &uint), (&uint, &float), (&float, &int)] {
            assert!(a.type_compatible(b), "expected {a} compat with {b}");
            assert!(b.type_compatible(a), "expected {b} compat with {a}");
        }
    }

    #[test]
    fn type_compatible_rejects_non_numeric_cross_kind() {
        assert!(!OtterbrixTypeInfo::text().type_compatible(&OtterbrixTypeInfo::integer()));
        assert!(!OtterbrixTypeInfo::bool().type_compatible(&OtterbrixTypeInfo::text()));
        assert!(!OtterbrixTypeInfo::null().type_compatible(&OtterbrixTypeInfo::integer()));
    }

    #[test]
    fn display_uses_name() {
        assert_eq!(OtterbrixTypeInfo::integer().to_string(), "BIGINT");
        assert_eq!(OtterbrixTypeInfo::unsigned().to_string(), "UBIGINT");
        assert_eq!(OtterbrixTypeInfo::float().to_string(), "DOUBLE");
        assert_eq!(OtterbrixTypeInfo::text().to_string(), "STRING");
        assert_eq!(OtterbrixTypeInfo::bool().to_string(), "BOOLEAN");
        assert_eq!(OtterbrixTypeInfo::null().to_string(), "NULL");
    }

    #[test]
    fn null_kind_reports_is_null() {
        assert!(OtterbrixTypeInfo::null().is_null());
        assert!(!OtterbrixTypeInfo::integer().is_null());
    }
}
