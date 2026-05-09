#![forbid(unsafe_code)]

mod arguments;
mod column;
mod connection;
mod convert;
mod database;
mod error;
mod executor;
mod options;
mod query_result;
mod row;
mod statement;
mod transaction;
mod r#type;
mod types;
mod value;

pub use arguments::{OtterbrixArgumentBuffer, OtterbrixArgumentValue, OtterbrixArguments};
pub use column::OtterbrixColumn;
pub use connection::OtterbrixConnection;
pub use database::Otterbrix;
pub use error::OtterbrixDbError;
pub use options::OtterbrixConnectOptions;
pub use query_result::OtterbrixQueryResult;
pub use r#type::OtterbrixTypeInfo;
pub use row::OtterbrixRow;
pub use statement::OtterbrixStatement;
pub use transaction::OtterbrixTransactionManager;
pub use value::{OtterbrixValue, OtterbrixValueRef};
