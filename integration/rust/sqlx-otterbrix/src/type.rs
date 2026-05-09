use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use sqlx_core::error::BoxDynError;
use sqlx_core::type_info::TypeInfo;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum OtterbrixKind {
    Null,
    Bool,
    Integer,
    Unsigned,
    Float,
    Text,
}

/// Logical SQL type tag reported by Otterbrix for encode/decode compatibility checks.
#[derive(Debug, Clone)]
pub struct OtterbrixTypeInfo(pub(crate) OtterbrixKind);

impl OtterbrixTypeInfo {
    pub(crate) fn null() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Null)
    }

    pub(crate) fn bool() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Bool)
    }

    pub(crate) fn integer() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Integer)
    }

    pub(crate) fn unsigned() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Unsigned)
    }

    pub(crate) fn float() -> Self {
        OtterbrixTypeInfo(OtterbrixKind::Float)
    }

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
