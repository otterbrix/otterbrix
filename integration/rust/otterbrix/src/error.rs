use std::fmt;

/// Errors returned by the Otterbrix wrapper.
///
/// Error variants are categorised by **origin**:
///
/// - [`Error::NullPointer`] and [`Error::Query`] originate in the C++ engine
///   (the `otterbrix core ...` prefix in the [`Display`](fmt::Display) text);
/// - [`Error::InvalidPath`] and [`Error::TypeMismatch`] originate in this
///   Rust wrapper (the plain `otterbrix ...` prefix).
///
/// All variants implement [`std::error::Error`] and can be wrapped or boxed
/// like any other Rust error type.
#[derive(Debug, Clone)]
pub enum Error {
    /// The C++ engine returned a null pointer where a value was expected.
    ///
    /// Usually indicates an internal engine failure (allocation failure,
    /// invariant violation, or unexpected state). The message in
    /// [`Display`](fmt::Display) is `otterbrix core returned null pointer`.
    NullPointer,

    /// SQL execution failed inside the C++ engine.
    ///
    /// `code` is the engine's internal error code (non-zero), `message` is the
    /// human-readable message produced by the engine. The message in
    /// [`Display`](fmt::Display) is `otterbrix core query error (code <N>): <msg>`.
    Query {
        /// Engine-specific error code. Always non-zero.
        code: i32,
        /// Human-readable message produced by the engine.
        message: String,
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
            Error::NullPointer => write!(f, "otterbrix core returned null pointer"),
            Error::Query { code, message } => {
                write!(f, "otterbrix core query error (code {code}): {message}")
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
