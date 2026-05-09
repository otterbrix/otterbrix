use std::borrow::Cow;

use sqlx_core::arguments::{Arguments, IntoArguments};
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;

use crate::database::Otterbrix;

pub type OtterbrixArgumentBuffer<'q> = Vec<OtterbrixArgumentValue<'q>>;

#[derive(Debug, Clone)]
pub enum OtterbrixArgumentValue<'q> {
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Double(f64),
    Str(Cow<'q, str>),
}

impl<'q> OtterbrixArgumentValue<'q> {
    pub(crate) fn into_static(self) -> OtterbrixArgumentValue<'static> {
        match self {
            OtterbrixArgumentValue::Bool(b) => OtterbrixArgumentValue::Bool(b),
            OtterbrixArgumentValue::Int64(n) => OtterbrixArgumentValue::Int64(n),
            OtterbrixArgumentValue::UInt64(n) => OtterbrixArgumentValue::UInt64(n),
            OtterbrixArgumentValue::Double(x) => OtterbrixArgumentValue::Double(x),
            OtterbrixArgumentValue::Str(c) => {
                OtterbrixArgumentValue::Str(Cow::Owned(c.into_owned()))
            }
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct OtterbrixArguments<'q> {
    pub(crate) values: Vec<OtterbrixArgumentValue<'q>>,
}

impl<'q> OtterbrixArguments<'q> {
    pub(crate) fn into_static(self) -> OtterbrixArguments<'static> {
        OtterbrixArguments {
            values: self.values.into_iter().map(|v| v.into_static()).collect(),
        }
    }
}

impl<'q> Arguments<'q> for OtterbrixArguments<'q> {
    type Database = Otterbrix;

    fn reserve(&mut self, additional: usize, _size: usize) {
        self.values.reserve(additional);
    }

    fn add<T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: 'q + Encode<'q, Self::Database> + Type<Self::Database>,
    {
        let before = self.values.len();
        match value.encode(&mut self.values) {
            Ok(IsNull::Yes) => {
                self.values.truncate(before);
                Err("encoding NULL is not supported by the Otterbrix driver".into())
            }
            Ok(IsNull::No) => Ok(()),
            Err(e) => {
                self.values.truncate(before);
                Err(e)
            }
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

impl<'q> IntoArguments<'q, Otterbrix> for OtterbrixArguments<'q> {
    fn into_arguments(self) -> OtterbrixArguments<'q> {
        self
    }
}
