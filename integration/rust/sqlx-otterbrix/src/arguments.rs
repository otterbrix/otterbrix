use std::borrow::Cow;

use sqlx_core::arguments::{Arguments, IntoArguments};
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;

use crate::database::Otterbrix;

/// Encoder-side argument buffer used by [`Encode`] implementations.
///
/// This is the [`Database::ArgumentBuffer`](sqlx_core::database::Database::ArgumentBuffer)
/// associated type for [`Otterbrix`](crate::Otterbrix); a flat
/// [`Vec`] of [`OtterbrixArgumentValue`]s pushed in argument order.
pub type OtterbrixArgumentBuffer<'q> = Vec<OtterbrixArgumentValue<'q>>;

/// One bound argument value.
///
/// Numeric subtypes are widened to `Int64` / `UInt64` / `Double` before being
/// pushed into the buffer; strings are stored as [`Cow<'q, str>`] so that
/// borrowed `&str` arguments avoid an allocation while owned `String`
/// arguments are kept for the lifetime of the query.
#[derive(Debug, Clone)]
pub enum OtterbrixArgumentValue<'q> {
    /// SQL `NULL`.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer (covers `i8` … `i64` after widening).
    Int64(i64),
    /// Unsigned 64-bit integer (covers `u8` … `u64` after widening).
    UInt64(u64),
    /// 64-bit floating point (covers `f32` after widening and `f64`).
    Double(f64),
    /// UTF-8 string slice; owned or borrowed depending on the source.
    Str(Cow<'q, str>),
}

impl OtterbrixArgumentValue<'_> {
    pub(crate) fn into_static(self) -> OtterbrixArgumentValue<'static> {
        match self {
            OtterbrixArgumentValue::Null => OtterbrixArgumentValue::Null,
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

/// Collected positional arguments for a single SQL statement.
///
/// `OtterbrixArguments` is the [`Arguments`](sqlx_core::arguments::Arguments)
/// implementation of the [`Otterbrix`](crate::Otterbrix) database. It is
/// produced implicitly by `sqlx::query(...).bind(...)` and consumed by the
/// driver's [`Executor`](sqlx_core::executor::Executor) implementation.
#[derive(Default, Debug, Clone)]
pub struct OtterbrixArguments<'q> {
    pub(crate) values: Vec<OtterbrixArgumentValue<'q>>,
}

impl OtterbrixArguments<'_> {
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
                self.values.push(OtterbrixArgumentValue::Null);
                Ok(())
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
