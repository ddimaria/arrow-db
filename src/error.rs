use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DbError>;

#[derive(Error, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum DbError {
    #[error("{0}")]
    ArrayData(String),

    #[error("Error creating RecordBatch: {0}")]
    CreateRecordBatch(String),

    #[error("Column index {0} is out of bounds in Table {1}")]
    ColumnIndexOutOfBounds(usize, String),

    #[error("{0}")]
    DataType(String),

    #[error("Table {0} already exists")]
    TableAlreadyExists(String),

    #[error("Table {0} not found")]
    TableNotFound(String),
}

// impl From<arrow::error::ArrowError> for DbError {
//     fn from(e: arrow::error::ArrowError) -> Self {
//         DbError::ArrayData(e.to_string())
//     }
// }
