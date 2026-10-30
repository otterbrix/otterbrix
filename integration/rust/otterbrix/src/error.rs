use std::fmt;

use crate::cursor::TransactionStatus;

/// Errors returned by the Otterbrix wrapper.
///
/// Error variants are categorised by **origin**:
///
/// - [`Error::Engine`] originates in the C++ engine
///   (the `otterbrix core ...` prefix in the [`Display`](fmt::Display) text);
/// - [`Error::InvalidPath`] and [`Error::TypeMismatch`] originate in this
///   Rust wrapper (the plain `otterbrix ...` prefix).
///
/// All variants implement [`std::error::Error`] and can be wrapped or boxed
/// like any other Rust error type.
#[derive(Debug, Clone)]
pub enum Error {
    /// The C++ engine refused to start or failed a statement.
    ///
    /// `code` is the engine's internal error code (non-zero), `message` is the
    /// human-readable reason produced by the engine, e.g. another engine already
    /// owning the same `main_path`. The message in [`Display`](fmt::Display) is
    /// `otterbrix core error (code <N>, transaction <status>, position <P>): <msg>`,
    /// each of `transaction` and `position` left out when it is `None`.
    Engine {
        /// Engine-specific error code. Always non-zero.
        code: i32,
        /// Human-readable message produced by the engine.
        message: String,
        /// Where the statement's session stands once the failed statement has
        /// run, as [`Cursor::transaction_status`](crate::Cursor::transaction_status)
        /// reports it; `None` when the engine refused to start.
        transaction_status: Option<TransactionStatus>,
        /// The 1-based character of the statement text the error points at,
        /// PostgreSQL's error position (a syntax error has one); `None` when the
        /// error points at no character.
        position: Option<u32>,
    },

    /// A filesystem path passed to the wrapper is not valid UTF-8.
    ///
    /// The C ABI of Otterbrix accepts `string_view`s, so we must convert paths
    /// to `&str`; this variant is returned when conversion fails. The message
    /// in [`Display`](fmt::Display) is `otterbrix invalid path: <path>`.
    InvalidPath(String),

    /// A value decoded from the engine had a different logical type than
    /// requested by the caller.
    ///
    /// Returned by [`Value::get`](crate::Value::get) and the [`FromValue`](crate::FromValue)
    /// implementations when the underlying value cannot be coerced into the
    /// requested Rust type. The message in [`Display`](fmt::Display) is
    /// `otterbrix type mismatch: expected <X>, got <Y>`.
    TypeMismatch {
        /// The Rust type the caller asked for, e.g. `"Int"` or `"String"`.
        expected: &'static str,
        /// The actual variant of [`Value`](crate::Value), e.g. `"Bool"` or `"Null"`.
        got: &'static str,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Engine {
                code,
                message,
                transaction_status,
                position,
            } => {
                write!(f, "otterbrix core error (code {code}")?;
                if let Some(status) = transaction_status {
                    write!(f, ", transaction {status}")?;
                }
                if let Some(position) = position {
                    write!(f, ", position {position}")?;
                }
                write!(f, "): {message}")
            }
            Error::InvalidPath(path) => write!(f, "otterbrix invalid path: {path}"),
            Error::TypeMismatch { expected, got } => {
                write!(f, "otterbrix type mismatch: expected {expected}, got {got}")
            }
        }
    }
}

impl std::error::Error for Error {}

/// A specialised [`Result`](std::result::Result) for Otterbrix wrapper operations.
pub type Result<T> = std::result::Result<T, Error>;
