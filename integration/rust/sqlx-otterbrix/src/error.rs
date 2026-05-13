use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use sqlx_core::error::{DatabaseError, ErrorKind};

#[derive(Debug)]
pub struct OtterbrixDbError {
    pub code: i32,
    pub message: String,
}

impl Display for OtterbrixDbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "otterbrix core query error (code {code}): {msg}",
            code = self.code,
            msg = self.message
        )
    }
}

impl StdError for OtterbrixDbError {}

impl DatabaseError for OtterbrixDbError {
    fn message(&self) -> &str {
        &self.message
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Owned(self.code.to_string()))
    }

    fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        ErrorKind::Other
    }
}
