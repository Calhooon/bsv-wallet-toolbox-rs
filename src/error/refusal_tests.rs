//! The typed boundary, with messages that cannot choose the class (#404).

use super::{Error, StoreBusyReason};
use bsv_rs::transaction::{beef_stream::SpendRefusal, Kind};
use bsv_rs::WalletFaultFamily;
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
        let wrapped = source.into_invalid_parameter();
        assert_eq!(wrapped.to_string(), expected);
        let sdk = bsv_rs::Error::from(wrapped);
        assert!(
            matches!(&sdk, bsv_rs::Error::WalletRefusal { class, message }
            if *class == WalletRefusalClass::InvalidParameter && message == &expected)
        );
        assert_eq!(sdk.to_string(), expected);
    }
}

#[test]
fn sdk_fault_display_keeps_its_wrapper() {
    let fault = Error::SdkError(bsv_rs::Error::WalletError("local SDK failure".into()));
    assert_eq!(
        fault.to_string(),
        "SDK error: wallet error: local SDK failure"
    );
    let transport = Error::SdkError(bsv_rs::Error::TransportFault {
        fault: bsv_rs::TransportFault::NeverSent,
        status: Some(503),
        detail: "unavailable".into(),
    });
    assert_eq!(
        transport.to_string(),
        "SDK error: transport fault NEVER_SENT: unavailable"
    );
}

#[test]
fn sdk_refusals_display_their_original_message() {
    for class in WalletRefusalClass::ALL {
        let wrapped = Error::SdkError(bsv_rs::Error::WalletRefusal {
            class,
            message: "sdk refusal text".into(),
        });
        assert_eq!(wrapped.to_string(), "sdk refusal text");
        let sdk = bsv_rs::Error::from(wrapped);
        assert!(
            matches!(sdk, bsv_rs::Error::WalletRefusal { class: actual, message }
            if actual == class && message == "sdk refusal text")
        );
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

/// A fault is bsv-rs's typed `WalletFault` (#439) of `family`, its message
/// the toolbox Display, its display and wire text the `WalletError`'s of the
/// same message, byte for byte.
fn assert_fault(error: Error, family: WalletFaultFamily) {
    let expected = error.to_string();
    let sdk = bsv_rs::Error::from(error);
    assert_eq!(
        sdk.to_string(),
        bsv_rs::Error::WalletError(expected.clone()).to_string()
    );
    match sdk {
        bsv_rs::Error::WalletFault(fault) => {
            assert_eq!(fault.family, family);
            assert_eq!(fault.message, expected);
        }
        other => panic!("expected a fault, got {other:?}"),
    }
}

macro_rules! string_fault {
    ($name:ident, $variant:ident, $family:ident) => {
        #[test]
        fn $name() {
            for words in ["probe", "Validation error: not a refusal"] {
                assert_fault(
                    Error::$variant(words.to_string()),
                    WalletFaultFamily::$family,
                );
            }
        }
    };
}

string_fault!(network_fault, NetworkError, Transport);
string_fault!(service_fault, ServiceError, Service);
string_fault!(storage_fault, StorageError, Storage);
string_fault!(database_fault, DatabaseError, Storage);
string_fault!(http_fault, HttpError, Transport);
string_fault!(lock_fault, LockTimeout, Storage);
string_fault!(internal_fault, Internal, Internal);
string_fault!(sync_fault, SyncError, Storage);
string_fault!(sync_conflict, SyncConflict, Storage);
string_fault!(migration_fault, MigrationError, Storage);
string_fault!(broadcast_fault, BroadcastFailed, Service);
string_fault!(transaction_fault, TransactionError, Internal);
string_fault!(signing_fault, SigningError, Internal);
#[cfg(feature = "sqlite")]
string_fault!(sqlx_fault, SqlxError, Storage);

#[test]
fn structured_faults() {
    use WalletFaultFamily::{Internal, Service};
    for (error, family) in [
        (
            Error::RateLimited {
                provider: "fixture".to_string(),
                retry_after_secs: 3,
            },
            Service,
        ),
        (
            Error::NoExplorerAnswered {
                faults: vec![("fixture".to_string(), "could not look".to_string())],
            },
            Service,
        ),
        (Error::NoServicesAvailable, Service),
        (Error::IoError(std::io::Error::other("fixture")), Internal),
        (
            Error::JsonError(serde_json::from_str::<bool>("invalid").unwrap_err()),
            Internal,
        ),
    ] {
        assert_fault(error, family);
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
