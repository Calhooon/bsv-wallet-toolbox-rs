//! The typed boundary, with messages that cannot choose the class (#404).

use super::{Error, StoreBusyReason};
use bsv_rs::transaction::{beef_stream::SpendRefusal, Kind};
use bsv_rs::WalletRefusalClass;

fn assert_refusal(error: Error, expected_class: WalletRefusalClass, expected_text: &str) {
    assert_eq!(error.to_string(), expected_text);
    let sdk = bsv_rs::Error::from(error);
    match &sdk {
        bsv_rs::Error::WalletRefusal { class, message } => {
            assert_eq!(*class, expected_class);
            assert_eq!(message, expected_text);
        }
        other => panic!("expected {expected_class:?}, got {other:?}"),
    }
    assert_eq!(sdk.to_string(), expected_text);
}

macro_rules! string_refusal {
    ($name:ident, $variant:ident, $class:ident, $prefix:literal) => {
        #[test]
        fn $name() {
            for words in ["probe", "changed detail without a class name"] {
                assert_refusal(
                    Error::$variant(words.to_string()),
                    WalletRefusalClass::$class,
                    &format!(concat!($prefix, "{}"), words),
                );
            }
        }
    };
}

string_refusal!(
    validation,
    ValidationError,
    InvalidParameter,
    "Validation error: "
);
string_refusal!(
    argument,
    InvalidArgument,
    InvalidParameter,
    "Invalid argument: "
);
string_refusal!(
    operation,
    InvalidOperation,
    InvalidOperation,
    "Invalid operation: "
);
string_refusal!(
    transaction_status,
    InvalidTransactionStatus,
    InvalidOperation,
    "Invalid transaction status: "
);

#[test]
fn abort_reference_status_uses_its_site_class_and_preserves_the_text() {
    for words in ["probe", "changed detail without a class name"] {
        let source = Error::InvalidTransactionStatus(words.to_string());
        let expected = source.to_string();
        let sdk = bsv_rs::Error::from(source.into_invalid_parameter());
        assert!(
            matches!(&sdk, bsv_rs::Error::WalletRefusal { class, message }
            if *class == WalletRefusalClass::InvalidParameter && message == &expected)
        );
        assert_eq!(sdk.to_string(), expected);
    }
}
string_refusal!(access, AccessDenied, Unauthorized, "Access denied: ");
string_refusal!(
    identity_key,
    InvalidIdentityKey,
    InvalidPublicKey,
    "Invalid identity key: "
);
string_refusal!(
    user_not_found,
    UserNotFound,
    InvalidParameter,
    "User not found: "
);

#[test]
fn authentication() {
    assert_refusal(
        Error::AuthenticationRequired,
        WalletRefusalClass::Unauthorized,
        "Authentication required",
    );
}

#[test]
fn storage_not_available() {
    assert_refusal(
        Error::StorageNotAvailable,
        WalletRefusalClass::NotActive,
        "Storage not available",
    );
}

#[test]
fn insufficient_funds() {
    assert_refusal(
        Error::InsufficientFunds {
            needed: 2,
            available: 1,
        },
        WalletRefusalClass::InsufficientFunds,
        "Insufficient funds: need 2, have 1",
    );
}

#[test]
fn invalid_beef() {
    assert_refusal(
        Error::InvalidBeef {
            offset: 17,
            kind: Kind::BadVersion,
        },
        WalletRefusalClass::InvalidParameter,
        "Invalid BEEF at byte 17: BadVersion",
    );
}

#[test]
fn spend_refused() {
    for words in ["probe", "changed script verdict"] {
        assert_refusal(
            Error::SpendRefused {
                offset: 17,
                txid: "tx".to_string(),
                input: Some(2),
                why: SpendRefusal::Script(words.to_string()),
            },
            WalletRefusalClass::InvalidParameter,
            &format!("spend refused at offset 17, input 2 of tx: {words}"),
        );
    }
}

#[test]
fn unspendable_coin() {
    assert_refusal(
        Error::UnspendableCoin {
            outpoint: "coin.0".to_string(),
            missing: vec!["parent".to_string()],
        },
        WalletRefusalClass::InvalidOperation,
        "Unspendable coin: coin.0: its stored ancestry is incomplete, ancestor(s) not stored: [parent]; createAction fetches none, and the wallet's other coins do not cover the action",
    );
}

#[test]
fn failed_ancestor() {
    assert_refusal(
        Error::FailedAncestor { txid: "parent".to_string() },
        WalletRefusalClass::InvalidOperation,
        "Failed ancestor: parent is failed in this wallet; nothing built on it is assembled or broadcast",
    );
}

#[test]
fn entity_not_found() {
    for entity in [
        "transaction",
        "certificate",
        "output",
        "changed entity name",
    ] {
        assert_refusal(
            Error::NotFound {
                entity: entity.to_string(),
                id: "missing".to_string(),
            },
            WalletRefusalClass::InvalidParameter,
            &format!("Entity not found: {entity} with id missing"),
        );
    }
}

#[test]
fn duplicate_entity() {
    assert_refusal(
        Error::Duplicate {
            entity: "row".to_string(),
            id: "existing".to_string(),
        },
        WalletRefusalClass::InvalidOperation,
        "Duplicate entity: row with id existing",
    );
}

fn assert_fault(error: Error) {
    let expected = error.to_string();
    match bsv_rs::Error::from(error) {
        bsv_rs::Error::WalletError(message) => assert_eq!(message, expected),
        other => panic!("expected a fault, got {other:?}"),
    }
}

macro_rules! string_fault {
    ($name:ident, $variant:ident) => {
        #[test]
        fn $name() {
            for words in ["probe", "Validation error: not a refusal"] {
                assert_fault(Error::$variant(words.to_string()));
            }
        }
    };
}

string_fault!(network_fault, NetworkError);
string_fault!(service_fault, ServiceError);
string_fault!(storage_fault, StorageError);
string_fault!(database_fault, DatabaseError);
string_fault!(http_fault, HttpError);
string_fault!(lock_fault, LockTimeout);
string_fault!(internal_fault, Internal);
string_fault!(sync_fault, SyncError);
string_fault!(sync_conflict, SyncConflict);
string_fault!(migration_fault, MigrationError);
string_fault!(broadcast_fault, BroadcastFailed);
string_fault!(transaction_fault, TransactionError);
string_fault!(signing_fault, SigningError);
#[cfg(feature = "sqlite")]
string_fault!(sqlx_fault, SqlxError);

#[test]
fn structured_faults() {
    for error in [
        Error::RateLimited {
            provider: "fixture".to_string(),
            retry_after_secs: 3,
        },
        Error::NoExplorerAnswered {
            faults: vec![("fixture".to_string(), "could not look".to_string())],
        },
        Error::NoServicesAvailable,
        Error::IoError(std::io::Error::other("fixture")),
        Error::JsonError(serde_json::from_str::<bool>("invalid").unwrap_err()),
    ] {
        assert_fault(error);
    }
}

#[test]
fn every_busy_reason_stays_a_fault() {
    for reason in [
        StoreBusyReason::SqliteBusy { code: 5 },
        StoreBusyReason::WriterNotAcquired,
        StoreBusyReason::MovedUnderUs,
        StoreBusyReason::InFlight,
    ] {
        let error = Error::StoreBusy { reason };
        let expected = error.to_string();
        assert!(matches!(
            bsv_rs::Error::from(error),
            bsv_rs::Error::WalletBusy { detail, retry_after_ms: None } if detail == expected
        ));
    }
}

#[test]
fn sdk_refusal_keeps_its_class_and_words() {
    for words in ["probe", "changed upstream message"] {
        let error = Error::SdkError(bsv_rs::Error::WalletRefusal {
            class: WalletRefusalClass::MissingParameter,
            message: words.to_string(),
        });
        assert!(matches!(
            bsv_rs::Error::from(error),
            bsv_rs::Error::WalletRefusal { class: WalletRefusalClass::MissingParameter, message }
                if message == words
        ));
    }
}

#[test]
fn sdk_transport_fault_keeps_its_type() {
    for words in ["probe", "changed upstream transport detail"] {
        let error = Error::SdkError(bsv_rs::Error::TransportFault {
            fault: bsv_rs::TransportFault::MayHaveBeenReceived,
            status: Some(503),
            detail: words.to_string(),
        });
        assert!(matches!(
            bsv_rs::Error::from(error),
            bsv_rs::Error::TransportFault {
                fault: bsv_rs::TransportFault::MayHaveBeenReceived,
                status: Some(503), detail,
            } if detail == words
        ));
    }
}
