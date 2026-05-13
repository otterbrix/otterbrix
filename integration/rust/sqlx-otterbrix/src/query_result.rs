use std::iter::{Extend, IntoIterator};

/// Outcome of a non-query SQL statement.
///
/// `OtterbrixQueryResult` is the
/// [`Database::QueryResult`](sqlx_core::database::Database::QueryResult)
/// associated type for [`Otterbrix`](crate::Otterbrix). It carries the
/// number of rows affected by the statement; `last_insert_rowid` is always
/// `0` because the engine does not surface generated identifiers.
#[derive(Debug, Default)]
pub struct OtterbrixQueryResult {
    rows_affected: u64,
    last_insert_rowid: i64,
}

impl OtterbrixQueryResult {
    /// Number of rows affected by the statement.
    #[must_use]
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// Always `0`. The Otterbrix engine does not expose generated row IDs;
    /// the method is provided for API parity with other SQLx drivers.
    #[must_use]
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }

    pub(crate) fn from_execution(rows_affected: u64) -> Self {
        OtterbrixQueryResult {
            rows_affected,
            last_insert_rowid: 0,
        }
    }
}

impl Extend<OtterbrixQueryResult> for OtterbrixQueryResult {
    fn extend<T: IntoIterator<Item = OtterbrixQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.rows_affected += elem.rows_affected;
            self.last_insert_rowid = elem.last_insert_rowid;
        }
    }
}
