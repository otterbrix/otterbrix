use crate::error::Error;
use crate::utils::string_from_c;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Double(f64),
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

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, Value::Int(_))
    }

    pub fn is_uint(&self) -> bool {
        matches!(self, Value::UInt(_))
    }

    pub fn is_double(&self) -> bool {
        matches!(self, Value::Double(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_uint(&self) -> Option<u64> {
        match self {
            Value::UInt(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            Value::Double(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn get<T: FromValue>(&self) -> Result<T, Error> {
        T::from_value(self)
    }

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

pub trait FromValue: Sized {
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
