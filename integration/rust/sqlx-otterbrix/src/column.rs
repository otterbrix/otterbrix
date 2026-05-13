use sqlx_core::column::Column;
use sqlx_core::ext::ustr::UStr;

use crate::database::Otterbrix;
use crate::r#type::OtterbrixTypeInfo;

/// Single column of a result set.
///
/// `OtterbrixColumn` is the [`Column`](sqlx_core::column::Column)
/// implementation of the [`Otterbrix`](crate::Otterbrix) database. Each
/// column carries its name, zero-based ordinal and the inferred
/// [`OtterbrixTypeInfo`]; instances are produced by the driver while
/// materialising query results and exposed through
/// [`Row::columns`](sqlx_core::row::Row::columns).
#[derive(Debug, Clone)]
pub struct OtterbrixColumn {
    pub(crate) name: UStr,
    pub(crate) ordinal: usize,
    pub(crate) type_info: OtterbrixTypeInfo,
}

impl Column for OtterbrixColumn {
    type Database = Otterbrix;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &OtterbrixTypeInfo {
        &self.type_info
    }
}
