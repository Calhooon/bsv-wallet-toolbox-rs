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

    /// The store is busy, a transient refusal: the action wrote nothing,
    /// built nothing and locked nothing, and the caller may repeat it
    /// unchanged. `reason` says which busy it was (`StoreBusyReason`). The
    /// Display begins exactly `Store busy: `, then the reason's words;
    /// consumers classify on that prefix.
    #[error("Store busy: {reason}")]
    StoreBusy {
        /// Which busy it was.
        reason: StoreBusyReason,
    },

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

    /// `createAction`: a change coin the wallet holds cannot be spent,
    /// because its stored ancestry is incomplete (a reorg-demoted received
    /// coin whose parents were never stored), and no other set of the
    /// wallet's coins covers the action. The pre-check skipped the coin and
    /// picked again; the coins offered covered the action only with it, so
    /// this is not `InsufficientFunds`, and the caller passed no bad
    /// `inputBEEF`, so it is not a `ValidationError`. The action wrote
    /// nothing. The Display begins exactly `Unspendable coin: `, then the
    /// coin's outpoint and the ancestors not stored; a host maps the prefix
    /// apart from `Insufficient funds:` (402) and `Validation error:` (400):
    /// the wallet's own state refuses a well-formed request (409 Conflict).
    #[error(
        "Unspendable coin: {outpoint}: its stored ancestry is incomplete, ancestor(s) not stored: [{}]; createAction fetches none, and the wallet's other coins do not cover the action",
        .missing.join(",")
    )]
    UnspendableCoin {
        /// The coin, `txid.vout`.
        outpoint: String,
        /// The ancestors the wallet does not store (the coin's own txid when
        /// its transaction's bytes are not stored).
        missing: Vec<String>,
    },

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

/// Why the store answered `Error::StoreBusy` (bsv-wallet-cli #44).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StoreBusyReason {
    /// SQLite answered primary result code 5 (`SQLITE_BUSY`;
    /// `SQLITE_BUSY_SNAPSHOT`, 517, included), `code` the extended code it
    /// gave. Code 6 (`SQLITE_LOCKED`, a shared-cache or same-connection
    /// conflict) is a deterministic bug and is not busy. Display
    /// `sqlite code N`.
    SqliteBusy {
        /// The extended SQLite result code.
        code: i32,
    },
    /// The writer connection was not acquired within the store's ceiling
    /// (the writer pool's `acquire_timeout`, equal to `busy_timeout`,
    /// `STORE_BUSY_CEILING_MS`). A reader-pool timeout is not busy.
    WriterNotAcquired,
    /// `createAction`: the store moved between the pre-check and the write
    /// lock twice (a pre-checked coin taken, or a proof stored, by another
    /// writer each time), so the bounded retry ended with the action's
    /// latest pre-check covered and its locked step not.
    MovedUnderUs,
    /// `createAction`: the pre-check did not cover the action only because
    /// coins it would pick are reserved by other `createAction` calls of
    /// this process still in flight. Planned without those reservations the
    /// action is covered: not a shortfall. Answered at once, with no wait.
    InFlight,
}

impl std::fmt::Display for StoreBusyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreBusyReason::SqliteBusy { code } => write!(f, "sqlite code {code}"),
            StoreBusyReason::WriterNotAcquired => {
                f.write_str("the writer was not acquired within the store's ceiling")
            }
            StoreBusyReason::MovedUnderUs => {
                f.write_str("the store moved between the pre-check and the write lock twice")
            }
            StoreBusyReason::InFlight => f.write_str("coins held by calls in flight"),
        }
    }
}

/// The `StoreBusy` code of a busy answer: the extended SQLite result code
/// when the primary code is 5 (`SQLITE_BUSY`, 517 included). `None` for
/// every other error, `SQLITE_LOCKED` (6) included, and for `PoolTimedOut`:
/// the conversion cannot tell the pools apart, so the writer's acquire sites
/// (`begin_immediate`, `acquire_writer`) make a writer timeout `StoreBusy`
/// themselves and a reader-pool timeout stays the plain SQLx error.
#[cfg(feature = "sqlite")]
pub(crate) fn sqlite_busy_code(err: &sqlx::Error) -> Option<i32> {
    let sqlx::Error::Database(db) = err else {
        return None;
    };
    let code: i32 = db.code()?.parse().ok()?;
    (code & 0xff == 5).then_some(code)
}

// Manual From impl for sqlx::Error to avoid trait bound issues
#[cfg(feature = "sqlite")]
impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        match sqlite_busy_code(&err) {
            Some(code) => Error::StoreBusy {
                reason: StoreBusyReason::SqliteBusy { code },
            },
            None => Error::SqlxError(err.to_string()),
        }
    }
}

/// The toolbox error at the `WalletInterface` boundary. A `StoreBusy` is
/// bsv-rs's `WalletBusy`: the store refused before acting and the refused
/// step wrote nothing, so the caller may repeat it unchanged (a
/// `createAction` whose `processAction` was busy keeps its written action
/// for a `signAction` on its reference).
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
        match bsv_rs::Error::from(Error::StoreBusy {
            reason: StoreBusyReason::SqliteBusy { code: 517 },
        }) {
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

    /// Every reason keeps the `Store busy: ` prefix and is `WalletBusy`
    /// with its Display as the detail.
    #[test]
    fn every_store_busy_reason_is_wallet_busy_with_its_words() {
        for (reason, words) in [
            (StoreBusyReason::SqliteBusy { code: 5 }, "sqlite code 5"),
            (
                StoreBusyReason::WriterNotAcquired,
                "the writer was not acquired within the store's ceiling",
            ),
            (
                StoreBusyReason::MovedUnderUs,
                "the store moved between the pre-check and the write lock twice",
            ),
            (StoreBusyReason::InFlight, "coins held by calls in flight"),
        ] {
            let err = Error::StoreBusy { reason };
            assert_eq!(err.to_string(), format!("Store busy: {words}"));
            match bsv_rs::Error::from(err) {
                bsv_rs::Error::WalletBusy {
                    detail,
                    retry_after_ms,
                } => {
                    assert_eq!(detail, format!("Store busy: {words}"));
                    assert_eq!(retry_after_ms, None);
                }
                other => panic!("expected WalletBusy, got {other:?}"),
            }
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
