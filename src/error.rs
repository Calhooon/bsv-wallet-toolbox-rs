//! Error types for the wallet toolbox.

use thiserror::Error;

/// Result type alias using the toolbox Error type.
pub type Result<T> = std::result::Result<T, Error>;

/// Wallet toolbox error types.
///
/// Errors are organized into categories: storage, authentication, service,
/// transaction, validation, sync, and wrapped errors from dependencies.
/// Use the [`Result`] type alias for convenient error propagation.
#[derive(Error, Debug)]
pub enum Error {
    // ===================
    // Storage errors
    // ===================
    /// The storage backend has not been initialized or is not yet available.
    #[error("Storage not available")]
    StorageNotAvailable,

    /// A general storage operation failed.
    #[error("Storage error: {0}")]
    StorageError(String),

    /// A database query or connection failed.
    #[error("Database error: {0}")]
    DatabaseError(String),

    /// A database schema migration failed.
    #[error("Migration error: {0}")]
    MigrationError(String),

    /// The requested entity was not found in storage.
    #[error("Entity not found: {entity} with id {id}")]
    NotFound { entity: String, id: String },

    /// An entity with the same key already exists in storage.
    #[error("Duplicate entity: {entity} with id {id}")]
    Duplicate { entity: String, id: String },

    // ===================
    // Authentication errors
    // ===================
    /// The operation requires authentication but none was provided.
    #[error("Authentication required")]
    AuthenticationRequired,

    /// The provided identity key is malformed or invalid.
    #[error("Invalid identity key: {0}")]
    InvalidIdentityKey(String),

    /// No user was found matching the given identity.
    #[error("User not found: {0}")]
    UserNotFound(String),

    /// The authenticated user does not have permission for this operation.
    #[error("Access denied: {0}")]
    AccessDenied(String),

    // ===================
    // Service errors
    // ===================
    /// A blockchain service provider returned an error.
    #[error("Service error: {0}")]
    ServiceError(String),

    /// A network request to a service provider failed.
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Transaction broadcasting failed across all configured providers.
    #[error("Broadcast failed: {0}")]
    BroadcastFailed(String),

    /// No service providers are configured for the requested operation.
    #[error("No services available")]
    NoServicesAvailable,

    // ===================
    // Transaction errors
    // ===================
    /// A transaction building, parsing, or processing error occurred.
    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// Transaction signing failed (e.g., derived key does not match locking script).
    #[error("Signing error: {0}")]
    SigningError(String),

    /// The transaction has an invalid or unexpected status for the requested operation.
    #[error("Invalid transaction status: {0}")]
    InvalidTransactionStatus(String),

    /// The wallet does not have enough funds to cover the transaction.
    #[error("Insufficient funds: need {needed}, have {available}")]
    InsufficientFunds { needed: u64, available: u64 },

    // ===================
    // Validation errors
    // ===================
    /// Input data failed validation (e.g., invalid format, out-of-range values).
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// A function argument is invalid or out of range.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// The requested operation is not valid in the current state.
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    // ===================
    // Sync errors
    // ===================
    /// A multi-storage synchronization operation failed.
    #[error("Sync error: {0}")]
    SyncError(String),

    /// A sync conflict was detected between storage backends.
    #[error("Sync conflict: {0}")]
    SyncConflict(String),

    /// A lock could not be acquired within the timeout period.
    #[error("Lock acquisition timed out: {0}")]
    LockTimeout(String),

    // ===================
    // Wrapped errors
    // ===================
    /// An error from the underlying `bsv-sdk` crate.
    #[error("SDK error: {0}")]
    SdkError(#[from] bsv_rs::Error),

    /// A JSON serialization or deserialization error.
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// A standard I/O error.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// A SQLx database driver error (only available with the `sqlite` feature).
    #[cfg(feature = "sqlite")]
    #[error("SQLx error: {0}")]
    SqlxError(String),

    /// An HTTP client error from `reqwest`.
    #[error("HTTP error: {0}")]
    HttpError(String),

    /// An unexpected internal error that does not fit other categories.
    #[error("Internal error: {0}")]
    Internal(String),
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
