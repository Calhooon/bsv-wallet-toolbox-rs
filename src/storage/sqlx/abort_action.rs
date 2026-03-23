//! Abort Action Implementation
//!
//! This module contains the implementation of the `abort_action` method
//! for the `StorageSqlx` wallet storage backend.
//!
//! Abort action cancels a pending (unsigned) transaction and releases
//! any locked UTXOs back to their spendable state.

use crate::error::{Error, Result};
use crate::storage::entities::TransactionStatus;
use chrono::Utc;
use sqlx::sqlite::SqliteConnection;
use sqlx::Row;

use bsv_rs::wallet::{AbortActionArgs, AbortActionResult};

use super::StorageSqlx;

// =============================================================================
// Constants
// =============================================================================

/// Length of a transaction ID in hex characters.
const TXID_HEX_LENGTH: usize = 64;

/// Transaction statuses that can be aborted.
const ABORTABLE_STATUSES: &[TransactionStatus] = &[
    TransactionStatus::Unsigned,
    TransactionStatus::Unprocessed,
    TransactionStatus::NoSend,
    TransactionStatus::NonFinal,
    TransactionStatus::Unfail,
];

/// Transaction statuses that cannot be aborted.
const NON_ABORTABLE_STATUSES: &[TransactionStatus] = &[
    TransactionStatus::Completed,
    TransactionStatus::Failed,
    TransactionStatus::Sending,
    TransactionStatus::Unproven,
];

// =============================================================================
// Internal Types
// =============================================================================

/// Transaction entity from database.
#[derive(Debug)]
struct TransactionRecord {
    transaction_id: i64,
    status: String,
    is_outgoing: bool,
}

// =============================================================================
// Main Implementation
// =============================================================================

/// Internal implementation of abort_action.
///
/// This function:
/// 1. Finds the transaction by reference (or txid if reference looks like a txid)
/// 2. Validates the transaction can be aborted
/// 3. Releases locked outputs back to spendable state
/// 4. Updates transaction status to 'failed'
///
/// All database operations are wrapped in a SQL transaction so that a crash
/// mid-operation does not leave the database in an inconsistent state.
/// If any step fails (via `?`), sqlx automatically rolls back on drop.
pub async fn abort_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: AbortActionArgs,
) -> Result<AbortActionResult> {
    let reference = &args.reference;

    // Begin a SQL transaction so that all mutations are atomic.
    let mut db_tx = storage
        .pool()
        .begin()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    // Step 1: Find the transaction
    let tx = find_transaction(&mut db_tx, user_id, reference).await?;

    let tx = tx.ok_or_else(|| Error::NotFound {
        entity: "transaction".to_string(),
        id: format!("reference or txid '{}'", reference),
    })?;

    // Step 2: Validate the transaction can be aborted
    validate_transaction_for_abort(&tx)?;

    // Step 3: Check that transaction outputs haven't been spent
    check_outputs_not_spent(&mut db_tx, tx.transaction_id).await?;

    // Step 4: Release locked outputs (make them spendable again)
    release_locked_outputs(&mut db_tx, tx.transaction_id).await?;

    // Step 5: Update transaction status to 'failed'
    update_transaction_status_to_failed(&mut db_tx, tx.transaction_id).await?;

    // Commit the transaction to make all changes durable.
    db_tx
        .commit()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    Ok(AbortActionResult { aborted: true })
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Find a transaction by reference or txid.
///
/// First searches by reference. If not found and the reference looks like
/// a txid (64 hex characters), also searches by txid.
async fn find_transaction(
    conn: &mut SqliteConnection,
    user_id: i64,
    reference: &str,
) -> Result<Option<TransactionRecord>> {
    // First, try to find by reference
    let tx = find_transaction_by_reference(&mut *conn, user_id, reference).await?;

    if tx.is_some() {
        return Ok(tx);
    }

    // If not found and reference looks like a txid, try finding by txid
    if is_potential_txid(reference) {
        return find_transaction_by_txid(&mut *conn, user_id, reference).await;
    }

    Ok(None)
}

/// Find transaction by reference string.
async fn find_transaction_by_reference(
    conn: &mut SqliteConnection,
    user_id: i64,
    reference: &str,
) -> Result<Option<TransactionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT transaction_id, status, is_outgoing
        FROM transactions
        WHERE user_id = ? AND reference = ?
        "#,
    )
    .bind(user_id)
    .bind(reference)
    .fetch_optional(&mut *conn)
    .await?;

    match row {
        Some(row) => Ok(Some(TransactionRecord {
            transaction_id: row.get("transaction_id"),
            status: row.get("status"),
            is_outgoing: row.get::<i32, _>("is_outgoing") != 0,
        })),
        None => Ok(None),
    }
}

/// Find transaction by txid.
async fn find_transaction_by_txid(
    conn: &mut SqliteConnection,
    user_id: i64,
    txid: &str,
) -> Result<Option<TransactionRecord>> {
    let row = sqlx::query(
        r#"
        SELECT transaction_id, status, is_outgoing
        FROM transactions
        WHERE user_id = ? AND txid = ?
        "#,
    )
    .bind(user_id)
    .bind(txid)
    .fetch_optional(&mut *conn)
    .await?;

    match row {
        Some(row) => Ok(Some(TransactionRecord {
            transaction_id: row.get("transaction_id"),
            status: row.get("status"),
            is_outgoing: row.get::<i32, _>("is_outgoing") != 0,
        })),
        None => Ok(None),
    }
}

/// Check if a string could potentially be a transaction ID.
fn is_potential_txid(s: &str) -> bool {
    s.len() == TXID_HEX_LENGTH && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Parse transaction status from string.
fn parse_status(status: &str) -> Option<TransactionStatus> {
    match status.to_lowercase().as_str() {
        "completed" => Some(TransactionStatus::Completed),
        "unprocessed" => Some(TransactionStatus::Unprocessed),
        "sending" => Some(TransactionStatus::Sending),
        "unproven" => Some(TransactionStatus::Unproven),
        "unsigned" => Some(TransactionStatus::Unsigned),
        "nosend" => Some(TransactionStatus::NoSend),
        "nonfinal" => Some(TransactionStatus::NonFinal),
        "failed" => Some(TransactionStatus::Failed),
        "unfail" => Some(TransactionStatus::Unfail),
        _ => None,
    }
}

/// Validate that the transaction can be aborted.
fn validate_transaction_for_abort(tx: &TransactionRecord) -> Result<()> {
    // Must be an outgoing transaction
    if !tx.is_outgoing {
        return Err(Error::InvalidTransactionStatus(
            "cannot abort action: must be an outgoing transaction".to_string(),
        ));
    }

    // Parse and validate status
    let status = parse_status(&tx.status).ok_or_else(|| {
        Error::InvalidTransactionStatus(format!("unknown transaction status: {}", tx.status))
    })?;

    // Check if status is abortable
    if ABORTABLE_STATUSES.contains(&status) {
        return Ok(());
    }

    // Check if status is explicitly non-abortable
    if NON_ABORTABLE_STATUSES.contains(&status) {
        return Err(Error::InvalidTransactionStatus(format!(
            "cannot abort action: action with status '{}' cannot be aborted",
            tx.status
        )));
    }

    // Unknown status - treat as non-abortable
    Err(Error::InvalidTransactionStatus(format!(
        "cannot abort action: unexpected transaction status '{}'",
        tx.status
    )))
}

/// Check that the transaction's outputs have not been spent by another transaction.
async fn check_outputs_not_spent(conn: &mut SqliteConnection, transaction_id: i64) -> Result<()> {
    // Check if any outputs created by this transaction have been spent
    let row = sqlx::query(
        r#"
        SELECT COUNT(*) as count
        FROM outputs
        WHERE transaction_id = ? AND spent_by IS NOT NULL
        "#,
    )
    .bind(transaction_id)
    .fetch_one(&mut *conn)
    .await?;

    let count: i64 = row.get("count");

    if count > 0 {
        return Err(Error::InvalidTransactionStatus(
            "cannot abort action: transaction has outputs that have been spent".to_string(),
        ));
    }

    Ok(())
}

/// Release outputs that were locked (marked as spent) by this transaction.
///
/// This sets `spendable = true` and `spent_by = NULL` for outputs that
/// were being spent by this transaction.
async fn release_locked_outputs(conn: &mut SqliteConnection, transaction_id: i64) -> Result<()> {
    let now = Utc::now();

    // Find outputs that were being spent by this transaction and release them
    // These are change outputs from previous transactions that were reserved
    // for this (now aborted) transaction.
    sqlx::query(
        r#"
        UPDATE outputs
        SET spendable = 1, spent_by = NULL, spending_description = NULL, updated_at = ?
        WHERE spent_by = ?
        "#,
    )
    .bind(now)
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Update transaction status to 'failed'.
async fn update_transaction_status_to_failed(
    conn: &mut SqliteConnection,
    transaction_id: i64,
) -> Result<()> {
    let now = Utc::now();

    sqlx::query(
        r#"
        UPDATE transactions
        SET status = 'failed', updated_at = ?
        WHERE transaction_id = ?
        "#,
    )
    .bind(now)
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::traits::AuthId;
    use crate::storage::WalletStorageWriter;

    /// Helper to create test storage
    async fn setup_test_storage() -> (StorageSqlx, i64, AuthId) {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("test-storage", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();

        let identity_key = "a".repeat(66);
        let (user, _) = storage.find_or_insert_user(&identity_key).await.unwrap();
        let auth = AuthId::with_user_id(&identity_key, user.user_id);

        (storage, user.user_id, auth)
    }

    /// Helper to insert a test transaction
    async fn insert_test_transaction(
        storage: &StorageSqlx,
        user_id: i64,
        reference: &str,
        status: &str,
        is_outgoing: bool,
        txid: Option<&str>,
    ) -> i64 {
        let now = Utc::now();

        let result = sqlx::query(
            r#"
            INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description, txid, version, lock_time, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1000, 'Test transaction', ?, 1, 0, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(status)
        .bind(reference)
        .bind(if is_outgoing { 1 } else { 0 })
        .bind(txid)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        result.last_insert_rowid()
    }

    /// Helper to insert a test output
    async fn insert_test_output(
        storage: &StorageSqlx,
        user_id: i64,
        transaction_id: i64,
        vout: i32,
        spendable: bool,
        spent_by: Option<i64>,
    ) -> i64 {
        let now = Utc::now();
        let txid = "c".repeat(64);

        let result = sqlx::query(
            r#"
            INSERT INTO outputs (user_id, transaction_id, vout, satoshis, spendable, change, provided_by, purpose, type, txid, spent_by, created_at, updated_at)
            VALUES (?, ?, ?, 1000, ?, 1, 'storage', 'change', 'P2PKH', ?, ?, ?, ?)
            "#,
        )
        .bind(user_id)
        .bind(transaction_id)
        .bind(vout)
        .bind(if spendable { 1 } else { 0 })
        .bind(&txid)
        .bind(spent_by)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();

        result.last_insert_rowid()
    }

    /// Helper to get transaction status
    async fn get_transaction_status(storage: &StorageSqlx, transaction_id: i64) -> String {
        let row = sqlx::query("SELECT status FROM transactions WHERE transaction_id = ?")
            .bind(transaction_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        row.get("status")
    }

    /// Helper to check if output is spendable
    async fn is_output_spendable(storage: &StorageSqlx, output_id: i64) -> bool {
        let row = sqlx::query("SELECT spendable FROM outputs WHERE output_id = ?")
            .bind(output_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let spendable: i32 = row.get("spendable");
        spendable != 0
    }

    /// Helper to check if output's spent_by is null
    async fn is_output_spent_by_null(storage: &StorageSqlx, output_id: i64) -> bool {
        let row = sqlx::query("SELECT spent_by FROM outputs WHERE output_id = ?")
            .bind(output_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        let spent_by: Option<i64> = row.get("spent_by");
        spent_by.is_none()
    }

    // -------------------------------------------------------------------------
    // Test 1: Abort unsigned transaction - success
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_unsigned_transaction_success() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unsigned outgoing transaction
        let reference = "test-abort-ref-1";
        let tx_id =
            insert_test_transaction(&storage, user_id, reference, "unsigned", true, None).await;

        // Create a previous transaction with outputs
        let prev_tx_id = insert_test_transaction(
            &storage,
            user_id,
            "prev-tx-ref",
            "completed",
            true,
            Some(&"b".repeat(64)),
        )
        .await;

        // Create an output from the previous transaction that is being spent by our unsigned tx
        let output_id =
            insert_test_output(&storage, user_id, prev_tx_id, 0, false, Some(tx_id)).await;

        // Verify initial state
        assert!(!is_output_spendable(&storage, output_id).await);
        assert!(!is_output_spent_by_null(&storage, output_id).await);

        // Abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await
        .unwrap();

        // Verify result
        assert!(result.aborted);

        // Verify transaction status changed to 'failed'
        assert_eq!(get_transaction_status(&storage, tx_id).await, "failed");

        // Verify output is now spendable and spent_by is null
        assert!(is_output_spendable(&storage, output_id).await);
        assert!(is_output_spent_by_null(&storage, output_id).await);
    }

    // -------------------------------------------------------------------------
    // Test 2: Abort signed/completed transaction - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_completed_transaction_fails() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create a completed outgoing transaction
        let reference = "test-abort-ref-2";
        let _tx_id = insert_test_transaction(
            &storage,
            user_id,
            reference,
            "completed",
            true,
            Some(&"d".repeat(64)),
        )
        .await;

        // Try to abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await;

        // Should fail with InvalidTransactionStatus
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("cannot be aborted"));
                assert!(msg.contains("completed"));
            }
            _ => panic!("Expected InvalidTransactionStatus error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 3: Abort non-existent reference - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_nonexistent_reference_fails() {
        let (storage, _user_id, auth) = setup_test_storage().await;

        // Try to abort a non-existent transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: "nonexistent-reference".to_string(),
            },
        )
        .await;

        // Should fail with NotFound
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::NotFound { entity, .. } => {
                assert_eq!(entity, "transaction");
            }
            _ => panic!("Expected NotFound error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 4: Abort another user's transaction - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_other_users_transaction_fails() {
        let (storage, user_id, _auth) = setup_test_storage().await;

        // Create an unsigned outgoing transaction for user 1
        let reference = "test-abort-ref-4";
        let _tx_id =
            insert_test_transaction(&storage, user_id, reference, "unsigned", true, None).await;

        // Create a second user
        let identity_key_2 = "b".repeat(66);
        let (user2, _) = storage.find_or_insert_user(&identity_key_2).await.unwrap();

        // Try to abort the transaction as user 2
        let result = abort_action_internal(
            &storage,
            user2.user_id,
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await;

        // Should fail with NotFound (user 2 can't see user 1's transaction)
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::NotFound { entity, .. } => {
                assert_eq!(entity, "transaction");
            }
            _ => panic!("Expected NotFound error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 5: Verify outputs released after abort
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_outputs_released_after_abort() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unsigned outgoing transaction
        let reference = "test-abort-ref-5";
        let tx_id =
            insert_test_transaction(&storage, user_id, reference, "unsigned", true, None).await;

        // Create a previous completed transaction
        let prev_tx_id = insert_test_transaction(
            &storage,
            user_id,
            "prev-tx-ref-5",
            "completed",
            true,
            Some(&"e".repeat(64)),
        )
        .await;

        // Create multiple outputs that are being spent by the unsigned tx
        let output1_id =
            insert_test_output(&storage, user_id, prev_tx_id, 0, false, Some(tx_id)).await;
        let output2_id =
            insert_test_output(&storage, user_id, prev_tx_id, 1, false, Some(tx_id)).await;
        let output3_id =
            insert_test_output(&storage, user_id, prev_tx_id, 2, false, Some(tx_id)).await;

        // Also create an output that is NOT being spent (should remain unchanged)
        let output4_id = insert_test_output(&storage, user_id, prev_tx_id, 3, true, None).await;

        // Verify initial state
        assert!(!is_output_spendable(&storage, output1_id).await);
        assert!(!is_output_spendable(&storage, output2_id).await);
        assert!(!is_output_spendable(&storage, output3_id).await);
        assert!(is_output_spendable(&storage, output4_id).await);

        // Abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await
        .unwrap();

        assert!(result.aborted);

        // Verify all locked outputs are now released
        assert!(is_output_spendable(&storage, output1_id).await);
        assert!(is_output_spendable(&storage, output2_id).await);
        assert!(is_output_spendable(&storage, output3_id).await);
        assert!(is_output_spent_by_null(&storage, output1_id).await);
        assert!(is_output_spent_by_null(&storage, output2_id).await);
        assert!(is_output_spent_by_null(&storage, output3_id).await);

        // Verify the unrelated output is still unchanged
        assert!(is_output_spendable(&storage, output4_id).await);
        assert!(is_output_spent_by_null(&storage, output4_id).await);
    }

    // -------------------------------------------------------------------------
    // Test 6: Abort unprocessed transaction - should succeed
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_unprocessed_transaction_success() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unprocessed outgoing transaction
        let reference = "test-abort-ref-6";
        let tx_id =
            insert_test_transaction(&storage, user_id, reference, "unprocessed", true, None).await;

        // Abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await
        .unwrap();

        assert!(result.aborted);
        assert_eq!(get_transaction_status(&storage, tx_id).await, "failed");
    }

    // -------------------------------------------------------------------------
    // Test 7: Abort incoming transaction - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_incoming_transaction_fails() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unsigned incoming transaction (is_outgoing = false)
        let reference = "test-abort-ref-7";
        let _tx_id = insert_test_transaction(
            &storage, user_id, reference, "unsigned", false, // incoming
            None,
        )
        .await;

        // Try to abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await;

        // Should fail - can't abort incoming transactions
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("must be an outgoing transaction"));
            }
            _ => panic!("Expected InvalidTransactionStatus error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 7b: Abort incoming transaction by txid - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_incoming_transaction_by_txid_fails() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unsigned incoming transaction with a txid
        let reference = "test-abort-ref-7b";
        let txid = "5".repeat(64);
        let _tx_id = insert_test_transaction(
            &storage,
            user_id,
            reference,
            "unsigned",
            false, // incoming
            Some(&txid),
        )
        .await;

        // Try to abort the transaction using txid
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: txid.clone(),
            },
        )
        .await;

        // Should fail - can't abort incoming transactions
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("must be an outgoing transaction"));
            }
            _ => panic!("Expected InvalidTransactionStatus error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 8: Abort failed transaction - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_failed_transaction_fails() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create a failed outgoing transaction
        let reference = "test-abort-ref-8";
        let _tx_id =
            insert_test_transaction(&storage, user_id, reference, "failed", true, None).await;

        // Try to abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await;

        // Should fail - can't abort already failed transactions
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("cannot be aborted"));
            }
            _ => panic!("Expected InvalidTransactionStatus error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 9: Abort by txid - should succeed
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_by_txid_success() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unsigned outgoing transaction with a txid
        let reference = "test-abort-ref-9";
        let txid = "f".repeat(64);
        let tx_id =
            insert_test_transaction(&storage, user_id, reference, "unsigned", true, Some(&txid))
                .await;

        // Abort using the txid as reference
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: txid.clone(),
            },
        )
        .await
        .unwrap();

        assert!(result.aborted);
        assert_eq!(get_transaction_status(&storage, tx_id).await, "failed");
    }

    // -------------------------------------------------------------------------
    // Test 10: Abort nosend transaction - should succeed
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_nosend_transaction_success() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create a nosend outgoing transaction
        let reference = "test-abort-ref-10";
        let tx_id =
            insert_test_transaction(&storage, user_id, reference, "nosend", true, None).await;

        // Abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await
        .unwrap();

        assert!(result.aborted);
        assert_eq!(get_transaction_status(&storage, tx_id).await, "failed");
    }

    // -------------------------------------------------------------------------
    // Test 11: Abort sending transaction - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_sending_transaction_fails() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create a sending outgoing transaction
        let reference = "test-abort-ref-11";
        let _tx_id = insert_test_transaction(
            &storage,
            user_id,
            reference,
            "sending",
            true,
            Some(&"a".repeat(64)),
        )
        .await;

        // Try to abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await;

        // Should fail - can't abort transactions that are being sent
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("cannot be aborted"));
            }
            _ => panic!("Expected InvalidTransactionStatus error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 12: Abort unproven transaction - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_unproven_transaction_fails() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unproven outgoing transaction
        let reference = "test-abort-ref-12";
        let _tx_id = insert_test_transaction(
            &storage,
            user_id,
            reference,
            "unproven",
            true,
            Some(&"1".repeat(64)),
        )
        .await;

        // Try to abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await;

        // Should fail - can't abort transactions that are unproven (already broadcast)
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("cannot be aborted"));
            }
            _ => panic!("Expected InvalidTransactionStatus error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 13: Abort unfail transaction - should succeed
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_unfail_transaction_success() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unfail outgoing transaction
        let reference = "test-abort-ref-13";
        let tx_id =
            insert_test_transaction(&storage, user_id, reference, "unfail", true, None).await;

        // Abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await
        .unwrap();

        assert!(result.aborted);
        assert_eq!(get_transaction_status(&storage, tx_id).await, "failed");
    }

    // -------------------------------------------------------------------------
    // Test 14: Abort non-existent txid - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_nonexistent_txid_fails() {
        let (storage, _user_id, auth) = setup_test_storage().await;

        // Try to abort with a txid that doesn't exist (64 hex chars)
        let fake_txid = "1234567890123456789012345678901234567890123456789012345678901234";
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: fake_txid.to_string(),
            },
        )
        .await;

        // Should fail with NotFound
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::NotFound { entity, .. } => {
                assert_eq!(entity, "transaction");
            }
            _ => panic!("Expected NotFound error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 15: Abort another user's transaction by txid - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_other_users_transaction_by_txid_fails() {
        let (storage, user_id, _auth) = setup_test_storage().await;

        // Create an unsigned outgoing transaction for user 1 with a txid
        let reference = "test-abort-ref-15";
        let txid = "2".repeat(64);
        let _tx_id =
            insert_test_transaction(&storage, user_id, reference, "unsigned", true, Some(&txid))
                .await;

        // Create a second user
        let identity_key_2 = "c".repeat(66);
        let (user2, _) = storage.find_or_insert_user(&identity_key_2).await.unwrap();

        // Try to abort the transaction as user 2 using txid
        let result = abort_action_internal(
            &storage,
            user2.user_id,
            AbortActionArgs {
                reference: txid.clone(),
            },
        )
        .await;

        // Should fail with NotFound (user 2 can't see user 1's transaction)
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::NotFound { entity, .. } => {
                assert_eq!(entity, "transaction");
            }
            _ => panic!("Expected NotFound error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 16: Abort unproven transaction by txid - should fail
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_unproven_transaction_by_txid_fails() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unproven outgoing transaction with a txid
        let reference = "test-abort-ref-16";
        let txid = "3".repeat(64);
        let _tx_id =
            insert_test_transaction(&storage, user_id, reference, "unproven", true, Some(&txid))
                .await;

        // Try to abort the transaction using txid
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: txid.clone(),
            },
        )
        .await;

        // Should fail - can't abort unproven transactions
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("cannot be aborted"));
            }
            _ => panic!("Expected InvalidTransactionStatus error, got: {:?}", err),
        }
    }

    // -------------------------------------------------------------------------
    // Test 17: Verify funds available after abort (can create new action)
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_funds_available_after_abort() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create a completed transaction with a spendable output
        let prev_tx_id = insert_test_transaction(
            &storage,
            user_id,
            "prev-tx-ref-17",
            "completed",
            false, // incoming - this is how we get funds
            Some(&"4".repeat(64)),
        )
        .await;

        // Create a spendable output from that transaction
        let output_id = insert_test_output(&storage, user_id, prev_tx_id, 0, true, None).await;

        // Verify output is initially spendable
        assert!(is_output_spendable(&storage, output_id).await);

        // Create an unsigned outgoing transaction that "spends" the output
        let reference = "test-abort-ref-17";
        let tx_id =
            insert_test_transaction(&storage, user_id, reference, "unsigned", true, None).await;

        // Mark the output as being spent by the unsigned transaction
        let now = Utc::now();
        sqlx::query(
            "UPDATE outputs SET spendable = 0, spent_by = ?, updated_at = ? WHERE output_id = ?",
        )
        .bind(tx_id)
        .bind(now)
        .bind(output_id)
        .execute(storage.pool())
        .await
        .unwrap();

        // Verify output is no longer spendable
        assert!(!is_output_spendable(&storage, output_id).await);

        // Abort the transaction
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await
        .unwrap();

        assert!(result.aborted);

        // Verify the output is now spendable again (funds available)
        assert!(is_output_spendable(&storage, output_id).await);
        assert!(is_output_spent_by_null(&storage, output_id).await);

        // Count total spendable satoshis for user
        let row = sqlx::query(
            "SELECT COALESCE(SUM(satoshis), 0) as total FROM outputs WHERE user_id = ? AND spendable = 1"
        )
        .bind(user_id)
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let total_spendable: i64 = row.get("total");

        // Should have 1000 satoshis available (from our test output)
        assert_eq!(total_spendable, 1000);
    }

    // -------------------------------------------------------------------------
    // Test: is_potential_txid helper
    // -------------------------------------------------------------------------

    #[test]
    fn test_is_potential_txid() {
        // Valid txid
        assert!(is_potential_txid(&"a".repeat(64)));
        assert!(is_potential_txid(&"0".repeat(64)));
        assert!(is_potential_txid(&"f".repeat(64)));
        assert!(is_potential_txid(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        ));

        // Invalid - wrong length
        assert!(!is_potential_txid(&"a".repeat(63)));
        assert!(!is_potential_txid(&"a".repeat(65)));
        assert!(!is_potential_txid(""));

        // Invalid - non-hex characters
        assert!(!is_potential_txid(&"g".repeat(64)));
        assert!(!is_potential_txid(&"z".repeat(64)));
        assert!(!is_potential_txid(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdeg"
        ));
    }
}
