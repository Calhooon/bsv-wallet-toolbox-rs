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
#[non_exhaustive]
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

    /// The store is busy, a transient refusal that wrote nothing: SQLite
    /// answered primary result code 5 (`SQLITE_BUSY`; `SQLITE_BUSY_SNAPSHOT`,
    /// 517, included), with `code` the extended code it gave, or the writer
    /// connection was not acquired within the store's ceiling (the writer
    /// pool's `acquire_timeout`, equal to `busy_timeout`, 5 s), with `code`
    /// 5. Code 6 (`SQLITE_LOCKED`, a shared-cache or same-connection
    /// conflict) is a deterministic bug and is not busy. The Display begins
    /// exactly `Store busy: sqlite code N`; consumers classify on that prefix.
    #[error("Store busy: sqlite code {code}")]
    StoreBusy { code: i32 },

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

    /// A BEEF's bytes are invalid: the offset of the byte bsv-rs's streaming
    /// reader names and the kind. Never a size or a count: a valid BEEF is
    /// never refused for those.
    #[error("Invalid BEEF at byte {offset}: {kind:?}")]
    InvalidBeef {
        /// The offset of the byte the refusal names.
        offset: u64,
        /// The kind, one of bsv-rs's nineteen.
        kind: bsv_rs::transaction::Kind,
    },

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

/// The `StoreBusy` code of a busy answer: the extended SQLite result code
/// when the primary code is 5 (`SQLITE_BUSY`, 517 included), or 5 when a pool
/// connection was not acquired in time (`PoolTimedOut`: the writer pool's
/// `acquire_timeout` is the store's ceiling; nothing was written). `None` for
/// every other error, `SQLITE_LOCKED` (6) included.
#[cfg(feature = "sqlite")]
pub(crate) fn sqlite_busy_code(err: &sqlx::Error) -> Option<i32> {
    let db = match err {
        sqlx::Error::PoolTimedOut => return Some(5),
        sqlx::Error::Database(db) => db,
        _ => return None,
    };
    let code: i32 = db.code()?.parse().ok()?;
    (code & 0xff == 5).then_some(code)
}

// Manual From impl for sqlx::Error to avoid trait bound issues
#[cfg(feature = "sqlite")]
impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        match sqlite_busy_code(&err) {
            Some(code) => Error::StoreBusy { code },
            None => Error::SqlxError(err.to_string()),
        }
    }
}

/// The toolbox error at the `WalletInterface` boundary. A `StoreBusy` is
/// bsv-rs's `WalletBusy`: the store refused before acting and nothing was
/// written, built or locked, so the caller may repeat the call unchanged.
/// `retry_after_ms` is `None`: the store does not know the host's retry
/// policy, and the host sets its own. An `SdkError` passes its bsv-rs error
/// through. Every other variant is `WalletError` with the toolbox Display,
/// the text the erasure sites gave before this impl.
impl From<Error> for bsv_rs::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::StoreBusy { .. } => bsv_rs::Error::WalletBusy {
                detail: err.to_string(),
                retry_after_ms: None,
            },
            Error::SdkError(e) => e,
            other => bsv_rs::Error::WalletError(other.to_string()),
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::HttpError(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_busy_is_wallet_busy_with_no_retry_policy() {
        match bsv_rs::Error::from(Error::StoreBusy { code: 517 }) {
            bsv_rs::Error::WalletBusy {
                detail,
                retry_after_ms,
            } => {
                assert_eq!(detail, "Store busy: sqlite code 517");
                assert_eq!(retry_after_ms, None);
            }
            other => panic!("expected WalletBusy, got {other:?}"),
        }
    }

    #[test]
    fn an_sdk_error_passes_through() {
        let sdk = bsv_rs::Error::WalletBusy {
            detail: "upstream".to_string(),
            retry_after_ms: Some(250),
        };
        match bsv_rs::Error::from(Error::SdkError(sdk)) {
            bsv_rs::Error::WalletBusy {
                detail,
                retry_after_ms,
            } => {
                assert_eq!(detail, "upstream");
                assert_eq!(retry_after_ms, Some(250));
            }
            other => panic!("expected the SDK error itself, got {other:?}"),
        }
    }

    #[test]
    fn every_other_error_is_wallet_error_with_its_display() {
        for err in [
            Error::ValidationError("bad".to_string()),
            Error::InsufficientFunds {
                needed: 2,
                available: 1,
            },
            Error::LockTimeout("writer".to_string()),
        ] {
            let text = err.to_string();
            match bsv_rs::Error::from(err) {
                bsv_rs::Error::WalletError(t) => assert_eq!(t, text),
                other => panic!("expected WalletError, got {other:?}"),
            }
        }
    }
}
