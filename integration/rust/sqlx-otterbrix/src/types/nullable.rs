//! `Option<T>` encoding as SQL NULL (`Encode` only: `Type`/`Decode` for `Option` come from sqlx_core).

use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;

use crate::arguments::OtterbrixArgumentBuffer;
use crate::database::Otterbrix;

impl<'q, T> Encode<'q, Otterbrix> for Option<T>
where
    T: Encode<'q, Otterbrix> + Type<Otterbrix> + 'q,
{
    fn encode_by_ref(&self, buf: &mut OtterbrixArgumentBuffer<'q>) -> Result<IsNull, BoxDynError> {
        match self {
            None => Ok(IsNull::Yes),
            Some(v) => v.encode_by_ref(buf),
        }
    }
}
