use std::borrow::Cow;

use otterbrix::Value as ObValue;
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;

use crate::arguments::{OtterbrixArgumentBuffer, OtterbrixArgumentValue};
use crate::database::Otterbrix;
use crate::r#type::{OtterbrixKind, OtterbrixTypeInfo};
use crate::value::OtterbrixValueRef;

macro_rules! impl_int_encode {
    ($ty:ty, $map:expr) => {
        impl Type<Otterbrix> for $ty {
            fn type_info() -> OtterbrixTypeInfo {
                OtterbrixTypeInfo::integer()
            }

            fn compatible(ty: &OtterbrixTypeInfo) -> bool {
                matches!(
                    ty.0,
                    OtterbrixKind::Integer | OtterbrixKind::Unsigned | OtterbrixKind::Float
                )
            }
        }

        impl<'q> Encode<'q, Otterbrix> for $ty {
            fn encode_by_ref(
                &self,
                buf: &mut OtterbrixArgumentBuffer<'q>,
            ) -> Result<IsNull, BoxDynError> {
                buf.push(OtterbrixArgumentValue::Int64($map(*self)));
                Ok(IsNull::No)
            }
        }

        impl<'r> Decode<'r, Otterbrix> for $ty {
            fn decode(value: OtterbrixValueRef<'r>) -> Result<Self, BoxDynError> {
                decode_int_like(value)
            }
        }
    };
}

fn decode_int_like<T: TryFrom<i64> + TryFrom<u64>>(
    value: OtterbrixValueRef<'_>,
) -> Result<T, BoxDynError> {
    match value.as_ob() {
        ObValue::Int(i) => T::try_from(*i).map_err(|_| int_conv_err()),
        ObValue::UInt(u) => T::try_from(*u).map_err(|_| int_conv_err()),
        ObValue::Double(f) => {
            let i = *f as i64;
            T::try_from(i).map_err(|_| int_conv_err())
        }
        ObValue::Bool(b) => T::try_from(i64::from(*b)).map_err(|_| int_conv_err()),
        ObValue::Null => Err("unexpected NULL".into()),
        ObValue::String(_) => Err("unexpected string for integer decode".into()),
    }
}

fn int_conv_err() -> BoxDynError {
    "integer conversion out of range".into()
}

impl_int_encode!(i8, |v: i8| i64::from(v));
impl_int_encode!(i16, |v: i16| i64::from(v));
impl_int_encode!(i32, |v: i32| i64::from(v));
impl_int_encode!(i64, |v: i64| v);

macro_rules! impl_uint_encode {
    ($ty:ty, $map:expr) => {
        impl Type<Otterbrix> for $ty {
            fn type_info() -> OtterbrixTypeInfo {
                OtterbrixTypeInfo::unsigned()
            }

            fn compatible(ty: &OtterbrixTypeInfo) -> bool {
                matches!(
                    ty.0,
                    OtterbrixKind::Unsigned | OtterbrixKind::Integer | OtterbrixKind::Float
                )
            }
        }

        impl<'q> Encode<'q, Otterbrix> for $ty {
            fn encode_by_ref(
                &self,
                buf: &mut OtterbrixArgumentBuffer<'q>,
            ) -> Result<IsNull, BoxDynError> {
                buf.push(OtterbrixArgumentValue::UInt64($map(*self)));
                Ok(IsNull::No)
            }
        }

        impl<'r> Decode<'r, Otterbrix> for $ty {
            fn decode(value: OtterbrixValueRef<'r>) -> Result<Self, BoxDynError> {
                decode_uint_like(value)
            }
        }
    };
}

fn decode_uint_like<T: TryFrom<u64> + TryFrom<i64>>(
    value: OtterbrixValueRef<'_>,
) -> Result<T, BoxDynError> {
    match value.as_ob() {
        ObValue::UInt(u) => T::try_from(*u).map_err(|_| int_conv_err()),
        ObValue::Int(i) => {
            let u = u64::try_from(*i).map_err(|_| int_conv_err())?;
            T::try_from(u).map_err(|_| int_conv_err())
        }
        ObValue::Double(f) => {
            let u = *f as u64;
            T::try_from(u).map_err(|_| int_conv_err())
        }
        ObValue::Bool(b) => T::try_from(u64::from(*b)).map_err(|_| int_conv_err()),
        ObValue::Null => Err("unexpected NULL".into()),
        ObValue::String(_) => Err("unexpected string for unsigned decode".into()),
    }
}

impl_uint_encode!(u8, |v: u8| u64::from(v));
impl_uint_encode!(u16, |v: u16| u64::from(v));
impl_uint_encode!(u32, |v: u32| u64::from(v));
impl_uint_encode!(u64, |v: u64| v);

impl Type<Otterbrix> for bool {
    fn type_info() -> OtterbrixTypeInfo {
        OtterbrixTypeInfo::bool()
    }

    fn compatible(ty: &OtterbrixTypeInfo) -> bool {
        matches!(
            ty.0,
            OtterbrixKind::Bool | OtterbrixKind::Integer | OtterbrixKind::Unsigned
        )
    }
}

impl<'q> Encode<'q, Otterbrix> for bool {
    fn encode_by_ref(&self, buf: &mut OtterbrixArgumentBuffer<'q>) -> Result<IsNull, BoxDynError> {
        buf.push(OtterbrixArgumentValue::Bool(*self));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Otterbrix> for bool {
    fn decode(value: OtterbrixValueRef<'r>) -> Result<Self, BoxDynError> {
        match value.as_ob() {
            ObValue::Bool(b) => Ok(*b),
            ObValue::Int(i) => Ok(*i != 0),
            ObValue::UInt(u) => Ok(*u != 0),
            ObValue::Null => Err("unexpected NULL".into()),
            _ => Err("cannot decode bool".into()),
        }
    }
}

impl Type<Otterbrix> for f32 {
    fn type_info() -> OtterbrixTypeInfo {
        OtterbrixTypeInfo::float()
    }

    fn compatible(ty: &OtterbrixTypeInfo) -> bool {
        matches!(
            ty.0,
            OtterbrixKind::Float | OtterbrixKind::Integer | OtterbrixKind::Unsigned
        )
    }
}

impl<'q> Encode<'q, Otterbrix> for f32 {
    fn encode_by_ref(&self, buf: &mut OtterbrixArgumentBuffer<'q>) -> Result<IsNull, BoxDynError> {
        buf.push(OtterbrixArgumentValue::Double(f64::from(*self)));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Otterbrix> for f32 {
    fn decode(value: OtterbrixValueRef<'r>) -> Result<Self, BoxDynError> {
        <f64 as Decode<'r, Otterbrix>>::decode(value).map(|d| d as f32)
    }
}

impl Type<Otterbrix> for f64 {
    fn type_info() -> OtterbrixTypeInfo {
        OtterbrixTypeInfo::float()
    }

    fn compatible(ty: &OtterbrixTypeInfo) -> bool {
        matches!(
            ty.0,
            OtterbrixKind::Float | OtterbrixKind::Integer | OtterbrixKind::Unsigned
        )
    }
}

impl<'q> Encode<'q, Otterbrix> for f64 {
    fn encode_by_ref(&self, buf: &mut OtterbrixArgumentBuffer<'q>) -> Result<IsNull, BoxDynError> {
        buf.push(OtterbrixArgumentValue::Double(*self));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Otterbrix> for f64 {
    fn decode(value: OtterbrixValueRef<'r>) -> Result<Self, BoxDynError> {
        match value.as_ob() {
            ObValue::Double(x) => Ok(*x),
            ObValue::Int(i) => Ok(*i as f64),
            ObValue::UInt(u) => Ok(*u as f64),
            ObValue::Null => Err("unexpected NULL".into()),
            _ => Err("cannot decode f64".into()),
        }
    }
}

impl Type<Otterbrix> for String {
    fn type_info() -> OtterbrixTypeInfo {
        OtterbrixTypeInfo::text()
    }

    fn compatible(ty: &OtterbrixTypeInfo) -> bool {
        matches!(ty.0, OtterbrixKind::Text | OtterbrixKind::Null)
    }
}

impl<'q> Encode<'q, Otterbrix> for String {
    fn encode_by_ref(&self, buf: &mut OtterbrixArgumentBuffer<'q>) -> Result<IsNull, BoxDynError> {
        buf.push(OtterbrixArgumentValue::Str(Cow::Owned(self.clone())));
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Otterbrix> for String {
    fn decode(value: OtterbrixValueRef<'r>) -> Result<Self, BoxDynError> {
        match value.as_ob() {
            ObValue::String(s) => Ok(s.clone()),
            ObValue::Null => Err("unexpected NULL".into()),
            _ => Err("cannot decode string".into()),
        }
    }
}

impl Type<Otterbrix> for &'static str {
    fn type_info() -> OtterbrixTypeInfo {
        OtterbrixTypeInfo::text()
    }

    fn compatible(ty: &OtterbrixTypeInfo) -> bool {
        matches!(ty.0, OtterbrixKind::Text | OtterbrixKind::Null)
    }
}

impl<'q> Encode<'q, Otterbrix> for &'q str {
    fn encode_by_ref(&self, buf: &mut OtterbrixArgumentBuffer<'q>) -> Result<IsNull, BoxDynError> {
        buf.push(OtterbrixArgumentValue::Str(Cow::Borrowed(self)));
        Ok(IsNull::No)
    }
}
