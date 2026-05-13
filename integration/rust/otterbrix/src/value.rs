use crate::error::Error;
use crate::utils::string_from_c;
use std::fmt;

/// A scalar value returned by the engine.
///
/// Each cell of a result row is materialised into a `Value` by
/// [`Cursor::get_value`](crate::Cursor::get_value) or its by-name counterpart.
/// The variant indicates the engine's logical type after FFI conversion;
/// numeric subtypes (e.g. `tinyint`, `smallint`, `int`) are unified into
/// [`Value::Int`] / [`Value::UInt`].
///
/// Use [`Value::get`] with a [`FromValue`] target for type-checked extraction
/// into a Rust type, or the `as_*` accessors for direct, type-specific access
/// without conversion errors.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL `NULL`.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed integer; covers `tinyint`, `smallint`, `int`, `bigint` from the engine.
    Int(i64),
    /// Unsigned integer; covers `utinyint`, `usmallint`, `uinteger`, `ubigint`.
    UInt(u64),
    /// Floating-point; covers `float` and `double`.
    Double(f64),
    /// UTF-8 string. The engine returns owned bytes; this variant always owns its data.
    String(String),
}

impl Value {
    pub(crate) fn from_raw(ptr: otterbrix_sys::value_ptr) -> Self {
        if ptr.is_null() {
            return Value::Null;
        }

        let value = unsafe {
            if otterbrix_sys::value_is_null(ptr) {
                Value::Null
            } else if otterbrix_sys::value_is_bool(ptr) {
                Value::Bool(otterbrix_sys::value_get_bool(ptr))
            } else if otterbrix_sys::value_is_int(ptr) {
                Value::Int(otterbrix_sys::value_get_int(ptr))
            } else if otterbrix_sys::value_is_uint(ptr) {
                Value::UInt(otterbrix_sys::value_get_uint(ptr))
            } else if otterbrix_sys::value_is_double(ptr) {
                Value::Double(otterbrix_sys::value_get_double(ptr))
            } else if otterbrix_sys::value_is_string(ptr) {
                Value::String(string_from_c(otterbrix_sys::value_get_string(ptr)))
            } else {
                Value::Null
            }
        };

        unsafe { otterbrix_sys::release_value(ptr) };
        value
    }

    /// Returns `true` if the value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns `true` if the value is a [`Value::Bool`].
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Returns `true` if the value is a [`Value::Int`].
    pub fn is_int(&self) -> bool {
        matches!(self, Value::Int(_))
    }

    /// Returns `true` if the value is a [`Value::UInt`].
    pub fn is_uint(&self) -> bool {
        matches!(self, Value::UInt(_))
    }

    /// Returns `true` if the value is a [`Value::Double`].
    pub fn is_double(&self) -> bool {
        matches!(self, Value::Double(_))
    }

    /// Returns `true` if the value is a [`Value::String`].
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Returns `Some(b)` if the value is a [`Value::Bool`], `None` otherwise.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns `Some(n)` if the value is a [`Value::Int`], `None` otherwise.
    ///
    /// No widening from [`Value::UInt`]; if the engine returned an unsigned
    /// integer, this returns `None`.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns `Some(n)` if the value is a [`Value::UInt`], `None` otherwise.
    pub fn as_uint(&self) -> Option<u64> {
        match self {
            Value::UInt(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns `Some(x)` if the value is a [`Value::Double`], `None` otherwise.
    pub fn as_double(&self) -> Option<f64> {
        match self {
            Value::Double(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns `Some(&str)` if the value is a [`Value::String`], `None` otherwise.
    ///
    /// The returned slice borrows from `self`; clone or convert to [`String`]
    /// for an owned copy.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    /// Type-checked extraction into a Rust type that implements [`FromValue`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::TypeMismatch`] if the value's variant cannot be
    /// converted into `T` directly. No widening conversions are performed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use otterbrix::Value;
    /// let v = Value::Int(42);
    /// let n: i64 = v.get().unwrap();
    /// assert_eq!(n, 42);
    /// ```
    pub fn get<T: FromValue>(&self) -> Result<T, Error> {
        T::from_value(self)
    }

    /// Returns the variant name as a static string slice.
    ///
    /// Used by [`FromValue`] implementations to populate the `got` field of
    /// [`Error::TypeMismatch`].
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "Null",
            Value::Bool(_) => "Bool",
            Value::Int(_) => "Int",
            Value::UInt(_) => "UInt",
            Value::Double(_) => "Double",
            Value::String(_) => "String",
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Int(v) => write!(f, "{v}"),
            Value::UInt(v) => write!(f, "{v}"),
            Value::Double(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v}"),
        }
    }
}

/// Conversion from a borrowed [`Value`] to a Rust type.
///
/// Implementations are provided for [`bool`], [`i64`], [`u64`], [`f64`], and
/// [`String`]. Each implementation accepts the matching [`Value`] variant only
/// and rejects everything else with [`Error::TypeMismatch`] — no implicit
/// widening or narrowing is performed.
///
/// To extract a value, prefer [`Value::get`] over calling
/// [`FromValue::from_value`] directly.
pub trait FromValue: Sized {
    /// Decodes a borrowed [`Value`] into `Self`.
    fn from_value(value: &Value) -> Result<Self, Error>;
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self, Error> {
        value.as_bool().ok_or(Error::TypeMismatch {
            expected: "Bool",
            got: value.type_name(),
        })
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self, Error> {
        value.as_int().ok_or(Error::TypeMismatch {
            expected: "Int",
            got: value.type_name(),
        })
    }
}

impl FromValue for u64 {
    fn from_value(value: &Value) -> Result<Self, Error> {
        value.as_uint().ok_or(Error::TypeMismatch {
            expected: "UInt",
            got: value.type_name(),
        })
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self, Error> {
        value.as_double().ok_or(Error::TypeMismatch {
            expected: "Double",
            got: value.type_name(),
        })
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self, Error> {
        value.as_str().map(String::from).ok_or(Error::TypeMismatch {
            expected: "String",
            got: value.type_name(),
        })
    }
}
