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

    /// The store is busy, a transient refusal: the action writes nothing,
    /// builds nothing and locks nothing; the pre-check's evidence records
    /// are idempotent and repeating is safe, so the caller may repeat it
    /// unchanged. `reason` says which busy it was (`StoreBusyReason`). The
    /// Display begins exactly `Store busy: `, then the reason's words;
    /// the boundary carries it as `WalletBusy`, classified by type.
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

    /// A provider is rate limited: it answered 429 (or its own pace has no
    /// request left), so it was not asked, or its answer is not one. A
    /// fault ("could not look"), never an answer: the ladder moves to the
    /// next rung, and the provider is skipped without a request until
    /// `retry_after_secs` have passed.
    #[error("Rate limited: {provider}: retry after {retry_after_secs}s")]
    RateLimited {
        /// The provider that is rate limited.
        provider: String,
        /// Seconds until the provider may be asked again.
        retry_after_secs: u64,
    },

    /// No explorer answered a lead question (`get_utxo_leads`): every one
    /// asked faulted. "Could not look", never "no outputs". The Display
    /// begins exactly `Could not look: no explorer answered: `, then each
    /// explorer's fault.
    #[error(
        "Could not look: no explorer answered: {}",
        .faults.iter().map(|(explorer, fault)| format!("{explorer}: {fault}")).collect::<Vec<_>>().join("; ")
    )]
    NoExplorerAnswered {
        /// Each explorer asked and its fault, in the order asked.
        faults: Vec<(String, String)>,
    },

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
    /// coin's outpoint and the ancestors not stored. The boundary carries
    /// `WalletRefusalClass::InvalidOperation`: the wallet's own state
    /// refuses a well-formed request, distinct from a funding shortfall or
    /// an invalid argument.
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

    /// A transaction this wallet retired (`failed`: a double spend's loser,
    /// a transaction the network refused or never held) is the creator of
    /// an explicit input or an ancestor the BEEF walk reaches: nothing built
    /// on it is assembled or broadcast (bsv-wallet-toolbox-rs #42 point 4).
    /// Refused before any post; the action wrote nothing. The Display begins
    /// exactly `Failed ancestor: `, then the failed transaction's txid.
    #[error(
        "Failed ancestor: {txid} is failed in this wallet; nothing built on it is assembled or broadcast"
    )]
    FailedAncestor {
        /// The failed transaction's txid.
        txid: String,
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
        /// The kind, one of bsv-rs's kinds.
        kind: bsv_rs::transaction::Kind,
    },

    /// The BEEF's bytes are well formed and the verifier refused a spend inside it (#103).
    /// The offset, input and reason are the verifier's own words; the text is
    /// `spend refused at offset N, input I of <txid>: <why>`, the reason in
    /// bsv-rs's own `Display` (bsv-rs #213).
    #[error("spend refused at offset {offset}, input {} of {txid}: {why}", input_text(*.input))]
    SpendRefused {
        /// The byte offset within the submitted BEEF of the input the verifier
        /// refused (or of the transaction, for the value rules).
        offset: u64,
        /// The spending transaction, hexadecimal.
        txid: String,
        /// The input the verifier named, when one input is named.
        input: Option<u32>,
        /// The verifier's own reason.
        why: bsv_rs::transaction::beef_stream::SpendRefusal,
    },

    /// A function argument is invalid or out of range.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// The requested operation is not valid in the current state.
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// The reference's `WERR_REVIEW_ACTIONS` (code 5) with its results
    /// (#264, the owner's ruling 18.4): an undelayed createAction whose
    /// result requires review, a double spend carrying the competing txid.
    /// The text is the reference's, `Undelayed createAction or signAction
    /// results require review.`; it reaches a `WalletInterface` caller as
    /// bsv-rs's `Error::WalletReviewActions` (bsv-rs #327, #339).
    #[error("{}", .0.message)]
    ReviewActions(Box<bsv_rs::wallet::ReviewActionsRefusal>),

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
    #[error("{}", sdk_error_display(.0))]
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

fn sdk_error_display(error: &bsv_rs::Error) -> String {
    match error {
        bsv_rs::Error::WalletRefusal { message, .. } => message.clone(),
        // The record displays the guard's string, as `WalletRefusal` does.
        bsv_rs::Error::AbortAccepted(acceptance) => acceptance.message.clone(),
        _ => format!("SDK error: {error}"),
    }
}

#[cfg(any(feature = "sqlite", test))]
impl Error {
    /// An abort refused because acceptance is stored (bsv-rs #439): the
    /// guard's own `class` and its whole Display as the message, so the
    /// boundary class, text and wire frame are the guard's as before, the
    /// typed record beside them. Retirement guards never build it.
    pub(crate) fn abort_accepted(
        self,
        reference: &str,
        class: bsv_rs::WalletRefusalClass,
        txid: Option<&str>,
        status: Option<&str>,
        accepting_word: Option<&str>,
    ) -> Self {
        let mut record = bsv_rs::AbortAcceptance::new(reference, class, self.to_string());
        record.txid = txid.map(Into::into);
        record.status = status.map(Into::into);
        record.accepting_word = accepting_word.map(Into::into);
        Self::SdkError(bsv_rs::Error::AbortAccepted(Box::new(record)))
    }

    /// Preserve the text when a site's reference class differs from the variant's default.
    pub(crate) fn into_invalid_parameter(self) -> Self {
        Self::SdkError(bsv_rs::Error::WalletRefusal {
            class: bsv_rs::WalletRefusalClass::InvalidParameter,
            message: self.to_string(),
        })
    }
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
/// bsv-rs's `WalletBusy`: the store refused before acting and the action
/// writes nothing (the pre-check's evidence records are idempotent and
/// repeating is safe), so the caller may repeat it unchanged.
/// `retry_after_ms` is `None`: the store does not know the host's retry
/// policy, and the host sets its own. An `SdkError` passes its bsv-rs error
/// through, and `ReviewActions` becomes `WalletReviewActions` with its
/// results. Request refusals carry a `WalletRefusalClass` and the toolbox
/// Display as their message. Faults stay `WalletError`; a could-not-look
/// failure is never classified as a refusal by its text.
impl From<Error> for bsv_rs::Error {
    fn from(err: Error) -> Self {
        use bsv_rs::{WalletFaultFamily, WalletRefusalClass};

        let class = match err {
            Error::ValidationError(_)
            | Error::InvalidArgument(_)
            | Error::InvalidBeef { .. }
            | Error::SpendRefused { .. }
            | Error::NotFound { .. }
            | Error::UserNotFound(_) => WalletRefusalClass::InvalidParameter,
            Error::InvalidOperation(_)
            | Error::InvalidTransactionStatus(_)
            | Error::UnspendableCoin { .. }
            | Error::FailedAncestor { .. }
            | Error::Duplicate { .. } => WalletRefusalClass::InvalidOperation,
            Error::AccessDenied(_) | Error::AuthenticationRequired => {
                WalletRefusalClass::Unauthorized
            }
            Error::InvalidIdentityKey(_) => WalletRefusalClass::InvalidPublicKey,
            Error::InsufficientFunds { .. } => WalletRefusalClass::InsufficientFunds,
            Error::StorageNotAvailable => WalletRefusalClass::NotActive,
            Error::StoreBusy { .. } => {
                return bsv_rs::Error::WalletBusy {
                    detail: err.to_string(),
                    retry_after_ms: None,
                };
            }
            Error::SdkError(e) => return e,
            Error::ReviewActions(refusal) => return bsv_rs::Error::WalletReviewActions(refusal),
            Error::NetworkError(_) | Error::HttpError(_) => {
                return wallet_fault(WalletFaultFamily::Transport, &err)
            }
            Error::ServiceError(_)
            | Error::BroadcastFailed(_)
            | Error::NoServicesAvailable
            | Error::RateLimited { .. }
            | Error::NoExplorerAnswered { .. } => {
                return wallet_fault(WalletFaultFamily::Service, &err)
            }
            #[cfg(feature = "sqlite")]
            Error::SqlxError(_) => return wallet_fault(WalletFaultFamily::Storage, &err),
            Error::StorageError(_)
            | Error::DatabaseError(_)
            | Error::MigrationError(_)
            | Error::SyncError(_)
            | Error::SyncConflict(_)
            | Error::LockTimeout(_) => return wallet_fault(WalletFaultFamily::Storage, &err),
            Error::TransactionError(_)
            | Error::SigningError(_)
            | Error::JsonError(_)
            | Error::IoError(_)
            | Error::Internal(_) => return wallet_fault(WalletFaultFamily::Internal, &err),
        };
        bsv_rs::Error::WalletRefusal {
            class,
            message: err.to_string(),
        }
    }
}

/// A fault at the boundary (bsv-rs #439): its family typed, its message the
/// toolbox Display unchanged, displayed and framed exactly as the
/// `WalletError` of the same message (wire code 1).
fn wallet_fault(family: bsv_rs::WalletFaultFamily, err: &Error) -> bsv_rs::Error {
    bsv_rs::Error::WalletFault(bsv_rs::WalletFault::new(family, err.to_string()))
}

/// A direct fault the wallet raises itself, in `family`, `message` unchanged
/// (the former `bsv_rs::Error::WalletError(message)`).
pub(crate) fn direct_fault(
    family: bsv_rs::WalletFaultFamily,
    message: impl Into<String>,
) -> bsv_rs::Error {
    bsv_rs::Error::WalletFault(bsv_rs::WalletFault::new(family, message))
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::HttpError(err.to_string())
    }
}

/// The input an [`Error::SpendRefused`] names, or `none` when the verifier
/// judged the transaction as a whole (the value rules).
fn input_text(input: Option<u32>) -> String {
    match input {
        Some(index) => index.to_string(),
        None => "none".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #264: the review refusal reaches a WalletInterface caller as bsv-rs's
    // WalletReviewActions with its results, in the reference's words.
    #[test]
    fn review_actions_is_wallet_review_actions_with_its_results() {
        let refusal =
            bsv_rs::wallet::ReviewActionsRefusal::new(vec![bsv_rs::wallet::ReviewActionResult {
                txid: [0; 32],
                status: bsv_rs::wallet::ReviewActionResultStatus::DoubleSpend,
                competing_txs: Some(vec![[7; 32]]),
                competing_beef: None,
            }]);
        let error = Error::ReviewActions(Box::new(refusal.clone()));
        assert_eq!(
            error.to_string(),
            "Undelayed createAction or signAction results require review."
        );
        match bsv_rs::Error::from(error) {
            bsv_rs::Error::WalletReviewActions(carried) => assert_eq!(*carried, refusal),
            other => panic!("expected WalletReviewActions, got {other:?}"),
        }
    }

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

    /// bsv-rs #213: the reason is bsv-rs's own `Display`, the script arm the
    /// interpreter's sentence and every other arm bsv-rs's words for its rule.
    #[test]
    fn a_spend_refusal_prints_bsv_rs_words_for_its_reason() {
        use bsv_rs::transaction::beef_stream::SpendRefusal;
        let txid = "ab".repeat(32);
        for (input, why, words) in [
            (
                Some(0),
                SpendRefusal::Script("The top stack element must be truthy after script evaluation.".to_string()),
                "The top stack element must be truthy after script evaluation.",
            ),
            (
                None,
                SpendRefusal::NonFinal { lock_time: 900_000 },
                "the transaction is not final for mining in the next block under the supplied headers: lock_time 900000",
            ),
        ] {
            let err = Error::SpendRefused {
                offset: 156,
                txid: txid.clone(),
                input,
                why,
            };
            let input = input.map_or("none".to_string(), |i| i.to_string());
            assert_eq!(
                err.to_string(),
                format!("spend refused at offset 156, input {input} of {txid}: {words}")
            );
        }
    }
}

#[cfg(test)]
mod refusal_tests;
