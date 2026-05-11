use std::sync::Arc;

use sqlx_core::column::ColumnIndex;
use sqlx_core::error::Error;
use sqlx_core::row::Row;
use sqlx_core::HashMap;

use crate::column::OtterbrixColumn;
use crate::database::Otterbrix;
use crate::value::{OtterbrixValue, OtterbrixValueRef};

#[derive(Debug)]
pub struct OtterbrixRow {
    pub(crate) values: Box<[OtterbrixValue]>,
    pub(crate) columns: Arc<Vec<OtterbrixColumn>>,
    pub(crate) column_names: Arc<HashMap<sqlx_core::ext::ustr::UStr, usize>>,
}

impl Row for OtterbrixRow {
    type Database = Otterbrix;

    fn columns(&self) -> &[OtterbrixColumn] {
        &self.columns
    }

    fn try_get_raw<I>(&self, index: I) -> Result<OtterbrixValueRef<'_>, Error>
    where
        I: ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        Ok(OtterbrixValueRef::borrow(&self.values[index]))
    }
}

impl ColumnIndex<OtterbrixRow> for &'_ str {
    fn index(&self, row: &OtterbrixRow) -> Result<usize, Error> {
        row.column_names
            .get(*self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
            .copied()
    }
}

sqlx_core::impl_column_index_for_row!(OtterbrixRow);
