use sqlx_core::column::Column;
use sqlx_core::ext::ustr::UStr;

use crate::database::Otterbrix;
use crate::r#type::OtterbrixTypeInfo;

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
