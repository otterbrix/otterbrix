use std::borrow::Cow;
use std::sync::Arc;

use either::Either;
use sqlx_core::column::ColumnIndex;
use sqlx_core::error::Error;
use sqlx_core::ext::ustr::UStr;
use sqlx_core::statement::Statement;
use sqlx_core::HashMap;

use crate::arguments::OtterbrixArguments;
use crate::column::OtterbrixColumn;
use crate::database::Otterbrix;
use crate::r#type::OtterbrixTypeInfo;

/// Prepared statement handle.
///
/// `OtterbrixStatement` is the [`Statement`](sqlx_core::statement::Statement)
/// implementation of the [`Otterbrix`](crate::Otterbrix) database. The
/// engine itself does not expose a separate prepare step — the driver
/// returns a lightweight handle that records the parameter count and is
/// re-executed by SQLx on each call.
///
/// Because the engine cannot describe results without executing the
/// statement, [`columns`](sqlx_core::statement::Statement::columns) and the
/// associated column names are always empty, and
/// [`parameters`](sqlx_core::statement::Statement::parameters) returns the
/// `?`-placeholder count rather than concrete type information.
#[derive(Debug, Clone)]
pub struct OtterbrixStatement<'q> {
    pub(crate) sql: Cow<'q, str>,
    pub(crate) parameters: usize,
    pub(crate) columns: Arc<Vec<OtterbrixColumn>>,
    pub(crate) column_names: Arc<HashMap<UStr, usize>>,
}

impl<'q> Statement<'q> for OtterbrixStatement<'q> {
    type Database = Otterbrix;

    fn to_owned(&self) -> OtterbrixStatement<'static> {
        OtterbrixStatement {
            sql: Cow::Owned(self.sql.clone().into_owned()),
            parameters: self.parameters,
            columns: Arc::clone(&self.columns),
            column_names: Arc::clone(&self.column_names),
        }
    }

    fn sql(&self) -> &str {
        &self.sql
    }

    fn parameters(&self) -> Option<Either<&[OtterbrixTypeInfo], usize>> {
        Some(Either::Right(self.parameters))
    }

    fn columns(&self) -> &[OtterbrixColumn] {
        &self.columns
    }

    sqlx_core::impl_statement_query!(OtterbrixArguments<'_>);
}

impl ColumnIndex<OtterbrixStatement<'_>> for &'_ str {
    fn index(&self, statement: &OtterbrixStatement<'_>) -> Result<usize, Error> {
        statement
            .column_names
            .get(*self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
            .copied()
    }
}

sqlx_core::impl_column_index_for_statement!(OtterbrixStatement);
