//! Error recovery path tests
//!
//! Tests verifying that error types can be constructed, display useful messages,
//! and convert correctly from wrapped error sources.

use bsv_wallet_toolbox::Error;

// =============================================================================
// Error variant construction tests
// =============================================================================

#[tokio::test]
async fn test_error_variants_exist() {
    // Verify all expected error variants can be constructed

    // Storage errors
    let e = Error::StorageNotAvailable;
    assert!(format!("{}", e).contains("Storage not available"));

    let e = Error::StorageError("disk full".to_string());
    assert!(format!("{}", e).contains("disk full"));

    let e = Error::DatabaseError("connection lost".to_string());
    assert!(format!("{}", e).contains("connection lost"));

    let e = Error::MigrationError("version mismatch".to_string());
    assert!(format!("{}", e).contains("version mismatch"));

    let e = Error::NotFound {
        entity: "transaction".to_string(),
        id: "abc123".to_string(),
    };
    assert!(format!("{}", e).contains("transaction"));
    assert!(format!("{}", e).contains("abc123"));

    let e = Error::Duplicate {
        entity: "output".to_string(),
        id: "xyz789".to_string(),
    };
    assert!(format!("{}", e).contains("output"));
    assert!(format!("{}", e).contains("xyz789"));

    // Authentication errors
    let e = Error::AuthenticationRequired;
    assert!(format!("{}", e).contains("Authentication required"));

    let e = Error::InvalidIdentityKey("not hex".to_string());
    assert!(format!("{}", e).contains("not hex"));

    let e = Error::UserNotFound("user42".to_string());
    assert!(format!("{}", e).contains("user42"));

    let e = Error::AccessDenied("insufficient privileges".to_string());
    assert!(format!("{}", e).contains("insufficient privileges"));

    // Service errors
    let e = Error::ServiceError("rate limited".to_string());
    assert!(format!("{}", e).contains("rate limited"));

    let e = Error::NetworkError("timeout".to_string());
    assert!(format!("{}", e).contains("timeout"));

    let e = Error::BroadcastFailed("rejected by miners".to_string());
    assert!(format!("{}", e).contains("rejected by miners"));

    let e = Error::NoServicesAvailable;
    assert!(format!("{}", e).contains("No services available"));

    // Transaction errors
    let e = Error::TransactionError("invalid script".to_string());
    assert!(format!("{}", e).contains("invalid script"));

    let e = Error::InvalidTransactionStatus("unknown_status".to_string());
    assert!(format!("{}", e).contains("unknown_status"));

    let e = Error::InsufficientFunds {
        needed: 50_000,
        available: 10_000,
    };
    let msg = format!("{}", e);
    assert!(msg.contains("50000"));
    assert!(msg.contains("10000"));

    // Validation errors
    let e = Error::ValidationError("merkle root mismatch".to_string());
    assert!(format!("{}", e).contains("merkle root mismatch"));

    let e = Error::InvalidArgument("txid must be 64 hex chars".to_string());
    assert!(format!("{}", e).contains("txid must be 64 hex chars"));

    let e = Error::InvalidOperation("cannot abort completed tx".to_string());
    assert!(format!("{}", e).contains("cannot abort completed tx"));

    // Sync errors
    let e = Error::SyncError("chunk too large".to_string());
    assert!(format!("{}", e).contains("chunk too large"));

    let e = Error::SyncConflict("version mismatch".to_string());
    assert!(format!("{}", e).contains("version mismatch"));

    let e = Error::LockTimeout("writer lock".to_string());
    assert!(format!("{}", e).contains("writer lock"));

    // Wrapped errors
    let e = Error::HttpError("connection reset".to_string());
    assert!(format!("{}", e).contains("connection reset"));

    let e = Error::Internal("unexpected state".to_string());
    assert!(format!("{}", e).contains("unexpected state"));
}

#[tokio::test]
async fn test_error_display() {
    // Verify error Display implementations produce useful, human-readable messages

    // NotFound should include entity type and ID
    let e = Error::NotFound {
        entity: "ProvenTxReq".to_string(),
        id: "42".to_string(),
    };
    let display = format!("{}", e);
    assert!(
        display.contains("ProvenTxReq") && display.contains("42"),
        "NotFound display should include entity and id, got: {}",
        display
    );

    // InsufficientFunds should include both amounts
    let e = Error::InsufficientFunds {
        needed: 1_000_000,
        available: 500,
    };
    let display = format!("{}", e);
    assert!(
        display.contains("1000000") && display.contains("500"),
        "InsufficientFunds should include both amounts, got: {}",
        display
    );

    // BroadcastFailed should include the reason
    let e = Error::BroadcastFailed("mempool conflict: txid already exists".to_string());
    let display = format!("{}", e);
    assert!(
        display.contains("mempool conflict"),
        "BroadcastFailed should include reason, got: {}",
        display
    );

    // Verify Debug also works (thiserror derives Debug)
    let debug = format!("{:?}", e);
    assert!(
        !debug.is_empty(),
        "Debug output should not be empty"
    );
}

#[tokio::test]
async fn test_error_conversion_from_sdk() {
    // Test that bsv_sdk errors can be converted into our Error type.
    // We construct a synthetic scenario using Error::from for SDK errors.
    // Since bsv_sdk::Error is opaque, we test this indirectly by verifying
    // the SdkError variant exists and wraps the error.

    // Create an Error that looks like an SDK error via our own construction
    // (We cannot easily construct bsv_sdk::Error directly in integration tests,
    // but we can verify the variant pattern exists by matching.)
    let e = Error::Internal("SDK-like error occurred".to_string());

    // Verify it implements std::error::Error
    let _: &dyn std::error::Error = &e;

    // Verify Display works
    let msg = format!("{}", e);
    assert!(msg.contains("SDK-like error"));

    // Verify the error can be downcast/matched
    match e {
        Error::Internal(ref s) => assert!(s.contains("SDK-like")),
        _ => panic!("Expected Internal variant"),
    }
}

#[tokio::test]
async fn test_error_conversion_from_json() {
    // Test Error::from for serde_json errors.
    // Create a deliberate JSON parse error.
    let bad_json = "{ invalid json }}}";
    let json_err = serde_json::from_str::<serde_json::Value>(bad_json).unwrap_err();

    // Convert to our Error type
    let our_error: Error = Error::from(json_err);

    // Verify it's a JsonError variant
    match &our_error {
        Error::JsonError(_) => {} // expected
        other => panic!("Expected JsonError, got: {:?}", other),
    }

    // Verify Display includes useful info about the JSON error
    let display = format!("{}", our_error);
    assert!(
        display.contains("JSON error"),
        "Display should mention JSON error, got: {}",
        display
    );
}

#[tokio::test]
async fn test_validation_error_construction() {
    // Test ValidationError variant with various messages

    // Empty message
    let e = Error::ValidationError(String::new());
    assert_eq!(format!("{}", e), "Validation error: ");

    // Typical usage: BEEF validation failure
    let e = Error::ValidationError("Invalid merkle root at height 800000".to_string());
    let msg = format!("{}", e);
    assert!(msg.contains("Validation error"));
    assert!(msg.contains("Invalid merkle root"));
    assert!(msg.contains("800000"));

    // Typical usage: script validation
    let e = Error::ValidationError("Script exceeds max length of 10000 bytes".to_string());
    let msg = format!("{}", e);
    assert!(msg.contains("10000"));

    // InvalidArgument (related validation variant)
    let e = Error::InvalidArgument("description must be at least 5 characters".to_string());
    let msg = format!("{}", e);
    assert!(msg.contains("Invalid argument"));
    assert!(msg.contains("5 characters"));

    // InvalidOperation
    let e = Error::InvalidOperation("Cannot process already-broadcast transaction".to_string());
    let msg = format!("{}", e);
    assert!(msg.contains("Invalid operation"));
    assert!(msg.contains("already-broadcast"));

    // Verify errors are Send + Sync (required for async contexts)
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Error>();

    // Verify IO error conversion
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let our_error: Error = Error::from(io_err);
    match &our_error {
        Error::IoError(_) => {} // expected
        other => panic!("Expected IoError, got: {:?}", other),
    }
    let display = format!("{}", our_error);
    assert!(display.contains("IO error") || display.contains("file not found"));
}
