use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

use sqlx_core::error::{DatabaseError, ErrorKind};

/// SQLx [`DatabaseError`] implementation for Otterbrix engine errors.
///
/// Wraps the integer error code and human-readable message produced by the
/// C++ engine (see [`otterbrix::Error::Query`]). Returned through
/// [`sqlx::Error::Database`](sqlx_core::error::Error::Database); use
/// `Box<dyn DatabaseError>::try_downcast_ref::<OtterbrixDbError>()` if you
/// need the raw `code`.
///
/// The `Display` text is `otterbrix core query error (code <N>): <msg>`,
/// matching the prefix convention used elsewhere in this driver.
#[derive(Debug)]
pub struct OtterbrixDbError {
    /// Engine-level numeric error code (always non-zero for a real error).
    pub code: i32,
    /// Human-readable message produced by the engine.
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
