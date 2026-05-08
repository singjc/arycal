use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum ArycalError {
    #[error("An error occurred: {0}")]
    Custom(String),
    #[error("IO error: {0}")]
    Io(String),
    // Add other error variants as needed
}
