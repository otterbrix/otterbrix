use std::fmt;

#[derive(Debug, Clone)]
pub enum Error {
    NullPointer,
    Query {
        code: i32,
        message: String,
    },
    InvalidPath(String),
    TypeMismatch {
        expected: &'static str,
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

pub type Result<T> = std::result::Result<T, Error>;
