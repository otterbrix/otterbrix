use std::borrow::Cow;

use otterbrix::Value as ObValue;
use sqlx_core::value::{Value as SqlxValue, ValueRef};

use crate::database::Otterbrix;
use crate::r#type::OtterbrixTypeInfo;

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
