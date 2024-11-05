use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DbClientError>;

#[derive(Error, Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum DbClientError {
    #[error("Error creating Client: {0}")]
    CreateClient(String),

    #[error("Error executing query: {0}")]
    Query(String),

    #[error("Error getting schema: {0}")]
    Schema(String),
}
