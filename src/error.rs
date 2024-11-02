use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DbError>;

#[derive(Error, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum DbError {
    #[error("{0}")]
    ArrayData(String),

    #[error("Error creating Database: {0}")]
    CreateDatabase(String),

    #[error("Error creating RecordBatch: {0}")]
    CreateRecordBatch(String),

    #[error("Column index {0} is out of bounds in Table {1}")]
    ColumnIndexOutOfBounds(usize, String),

    #[error("{0}")]
    DataType(String),

    #[error("Error executing query ({0}) {1}")]
    Query(String, String),

    #[error("Table {0} already exists")]
    TableAlreadyExists(String),

    #[error("Error exporting Table {0}: {1}")]
    TableExportError(String, String),

    #[error("Error importing Table {0}: {1}")]
    TableImportError(String, String),

    #[error("Table {0} not found")]
    TableNotFound(String),
}
