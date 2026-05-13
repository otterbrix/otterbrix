use std::borrow::Cow;

use otterbrix::Value as ObValue;
use sqlx_core::value::{Value as SqlxValue, ValueRef};

use crate::database::Otterbrix;
use crate::r#type::OtterbrixTypeInfo;

/// Owned cell value produced by the Otterbrix engine.
///
/// `OtterbrixValue` is the [`Value`](sqlx_core::value::Value) implementation
/// of the [`Otterbrix`](crate::Otterbrix) database: it carries the raw
/// [`otterbrix::Value`] together with its inferred
/// [`OtterbrixTypeInfo`]. Decoding into a Rust type happens through the
/// standard SQLx [`Decode`](sqlx_core::decode::Decode) trait —
/// `row.try_get::<i64, _>("id")` and similar.
#[derive(Debug, Clone)]
pub struct OtterbrixValue {
    pub(crate) raw: ObValue,
    pub(crate) type_info: OtterbrixTypeInfo,
}

impl SqlxValue for OtterbrixValue {
    type Database = Otterbrix;

    fn as_ref(&self) -> OtterbrixValueRef<'_> {
        OtterbrixValueRef { inner: self }
    }

    fn type_info(&self) -> Cow<'_, OtterbrixTypeInfo> {
        Cow::Borrowed(&self.type_info)
    }

    fn is_null(&self) -> bool {
        matches!(self.raw, ObValue::Null)
    }
}

/// Borrowed reference to an [`OtterbrixValue`].
///
/// `OtterbrixValueRef` is the [`ValueRef`](sqlx_core::value::ValueRef)
/// implementation of the [`Otterbrix`](crate::Otterbrix) database. It
/// is the value type that [`Decode`](sqlx_core::decode::Decode) implementations
/// receive; users normally do not construct it directly.
#[derive(Clone)]
pub struct OtterbrixValueRef<'r> {
    inner: &'r OtterbrixValue,
}

impl<'r> OtterbrixValueRef<'r> {
    pub(crate) fn borrow(inner: &'r OtterbrixValue) -> Self {
        Self { inner }
    }

    pub(crate) fn as_ob(&self) -> &'r ObValue {
        &self.inner.raw
    }
}

impl<'r> ValueRef<'r> for OtterbrixValueRef<'r> {
    type Database = Otterbrix;

    fn to_owned(&self) -> OtterbrixValue {
        self.inner.clone()
    }

    fn type_info(&self) -> Cow<'_, OtterbrixTypeInfo> {
        Cow::Borrowed(&self.inner.type_info)
    }

    fn is_null(&self) -> bool {
        matches!(self.inner.raw, ObValue::Null)
    }
}
