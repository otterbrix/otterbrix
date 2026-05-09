use std::iter::{Extend, IntoIterator};

#[derive(Debug, Default)]
pub struct OtterbrixQueryResult {
    rows_affected: u64,
    last_insert_rowid: i64,
}

impl OtterbrixQueryResult {
    #[must_use]
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

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
