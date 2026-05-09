use crate::{
    OtterbrixArgumentBuffer, OtterbrixArguments, OtterbrixColumn, OtterbrixConnection,
    OtterbrixQueryResult, OtterbrixRow, OtterbrixStatement, OtterbrixTransactionManager,
    OtterbrixTypeInfo, OtterbrixValue, OtterbrixValueRef,
};
use sqlx_core::database::Database;

/// Otterbrix SQLx driver marker type.
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
