use crate::{
    OtterbrixArgumentBuffer, OtterbrixArguments, OtterbrixColumn, OtterbrixConnection,
    OtterbrixQueryResult, OtterbrixRow, OtterbrixStatement, OtterbrixTransactionManager,
    OtterbrixTypeInfo, OtterbrixValue, OtterbrixValueRef,
};
use sqlx_core::database::Database;

/// Marker type identifying the Otterbrix SQLx driver.
///
/// `Otterbrix` is a zero-sized type that implements
/// [`sqlx_core::database::Database`] and is used as the type parameter for
/// every SQLx call:
///
/// ```no_run
/// # use sqlx_otterbrix::{Otterbrix, OtterbrixConnectOptions, OtterbrixConnection};
/// # use sqlx_core::connection::Connection;
/// # async fn run() -> Result<(), sqlx_core::error::Error> {
/// # let mut conn = OtterbrixConnection::connect_with(
/// #     &OtterbrixConnectOptions::new(".")).await?;
/// let _q = sqlx_core::query::query::<Otterbrix>("SELECT 1").execute(&mut conn).await?;
/// # Ok(()) }
/// ```
///
/// The driver registers a single URL scheme, `otterbrix://`, used by
/// [`Connection::connect`](sqlx_core::connection::Connection::connect)
/// and [`OtterbrixConnectOptions::from_url`](crate::OtterbrixConnectOptions).
#[derive(Debug)]
pub struct Otterbrix;

impl Database for Otterbrix {
    type Connection = OtterbrixConnection;

    type TransactionManager = OtterbrixTransactionManager;

    type Row = OtterbrixRow;

    type QueryResult = OtterbrixQueryResult;

    type Column = OtterbrixColumn;

    type TypeInfo = OtterbrixTypeInfo;

    type Value = OtterbrixValue;
    type ValueRef<'r> = OtterbrixValueRef<'r>;

    type Arguments<'q> = OtterbrixArguments<'q>;
    type ArgumentBuffer<'q> = OtterbrixArgumentBuffer<'q>;

    type Statement<'q> = OtterbrixStatement<'q>;

    const NAME: &'static str = "Otterbrix";

    const URL_SCHEMES: &'static [&'static str] = &["otterbrix"];
}
