//! Error types for the wallet toolbox.

use thiserror::Error;

/// Result type alias using the toolbox Error type.
pub type Result<T> = std::result::Result<T, Error>;

/// Wallet toolbox error types.
#[derive(Error, Debug)]
pub enum Error {
    // ===================
    // Storage errors
    // ===================
    #[error("Storage not available")]
    StorageNotAvailable,

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Migration error: {0}")]
    MigrationError(String),

    #[error("Entity not found: {entity} with id {id}")]
    NotFound { entity: String, id: String },

    #[error("Duplicate entity: {entity} with id {id}")]
    Duplicate { entity: String, id: String },

    // ===================
    // Authentication errors
    // ===================
    #[error("Authentication required")]
    AuthenticationRequired,

    #[error("Invalid identity key: {0}")]
    InvalidIdentityKey(String),

    #[error("User not found: {0}")]
    UserNotFound(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    // ===================
    // Service errors
    // ===================
    #[error("Service error: {0}")]
    ServiceError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Broadcast failed: {0}")]
    BroadcastFailed(String),

    #[error("No services available")]
    NoServicesAvailable,

    // ===================
    // Transaction errors
    // ===================
    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("Invalid transaction status: {0}")]
    InvalidTransactionStatus(String),

    #[error("Insufficient funds: need {needed}, have {available}")]
    InsufficientFunds { needed: u64, available: u64 },

    // ===================
    // Validation errors
    // ===================
    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    // ===================
    // Sync errors
    // ===================
    #[error("Sync error: {0}")]
    SyncError(String),

    #[error("Sync conflict: {0}")]
    SyncConflict(String),

    #[error("Lock acquisition timed out: {0}")]
    LockTimeout(String),

    // ===================
    // Wrapped errors
    // ===================
    #[error("SDK error: {0}")]
    SdkError(#[from] bsv_sdk::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[cfg(feature = "sqlite")]
    #[error("SQLx error: {0}")]
    SqlxError(String),

    #[error("HTTP error: {0}")]
    HttpError(String),
}

// Manual From impl for sqlx::Error to avoid trait bound issues
#[cfg(feature = "sqlite")]
impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Error::SqlxError(err.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::HttpError(err.to_string())
    }
}
