//! Abort Action Implementation
//!
//! This module contains the implementation of the `abort_action` method
//! for the `StorageSqlx` wallet storage backend.
//!
//! Abort action cancels a pending (unsigned) transaction and releases
//! any locked UTXOs back to their spendable state.
//!
//! # Aborting a broadcast transaction (0.3.60)
//!
//! An `unproven` (or `sending`) transaction has been handed to a
//! broadcaster, which is not the same as the network having it. When the
//! application learns that such a transaction can never mine (2026-09-03,
//! beta: a pf head spend that lost its head race is a double spend of the
//! head, the overlay says so on the first submit, and the wallet would
//! otherwise keep the transaction as `unproven` on the broadcaster's SEEN
//! and fund the next action from its change), it may abort it. The abort
//! is honoured only when the wallet holds NO chain evidence for the
//! transaction: a broadcaster's `accepted` / `seen` (Arcade's
//! `SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES`) and the network
//! pseudo-provider's `seen` are that reporter's word, not the chain's; only
//! a `chain|seen` / `chain|mined` row (the chain index or a validated proof)
//! or a `mined` row from anyone is chain evidence, and then the abort is
//! refused with a clear error. A proven descendant refuses it too.
//!
//! An honoured broadcast abort fails the transaction, releases its inputs
//! (the caller vouches that the transaction will never mine, so nothing it
//! spent is spent), invalidates its own outputs, records `rejected` in the
//! broadcast memory (no provider skips it, the reconciler never resurrects
//! it), and retires every unproven descendant through the poisoned-chain
//! path (`retire_poisoned_descendants`): change built on the aborted
//! transaction is never spent, and a descendant's outside inputs are
//! released only on chain verification (kept and re-checked otherwise).

use crate::error::{Error, Result};
use crate::services::broadcast_memory::{BroadcastStatus, BROADCAST_PROVIDER_CHAIN};
use crate::storage::entities::TransactionStatus;
use crate::storage::traits::WalletStorageReader;
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

/// Transaction statuses that can be aborted before any broadcast.
const ABORTABLE_STATUSES: &[TransactionStatus] = &[
    TransactionStatus::Unsigned,
    TransactionStatus::Unprocessed,
    TransactionStatus::NoSend,
    TransactionStatus::NonFinal,
    TransactionStatus::Unfail,
];

/// Transaction statuses of a broadcast (or broadcasting) transaction:
/// abortable only without chain evidence (see the module docs).
const BROADCAST_STATUSES: &[TransactionStatus] =
    &[TransactionStatus::Sending, TransactionStatus::Unproven];

/// Transaction statuses that cannot be aborted.
const NON_ABORTABLE_STATUSES: &[TransactionStatus] =
    &[TransactionStatus::Completed, TransactionStatus::Failed];

// =============================================================================
// Internal Types
// =============================================================================

/// Transaction entity from database.
#[derive(Debug)]
struct TransactionRecord {
    transaction_id: i64,
    status: String,
    is_outgoing: bool,
    txid: Option<String>,
}

/// Which abort a transaction's status calls for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbortKind {
    /// Never broadcast: release the inputs, fail the transaction.
    Pending,
    /// Broadcast: honoured only without chain evidence, and the
    /// descendants go with it.
    Broadcast,
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
/// For a broadcast transaction (`unproven` / `sending`) see the module docs:
/// the chain-evidence check comes first, the descendants are retired after.
///
/// The transaction's own mutations are wrapped in a SQL transaction so that
/// a crash mid-operation does not leave the database in an inconsistent
/// state. If any step fails (via `?`), sqlx automatically rolls back on
/// drop. The descendants of a broadcast abort are retired after the commit
/// (the reconciler's sweep picks up a failed root with unproven descendants
/// if the process dies in between).
pub async fn abort_action_internal(
    storage: &StorageSqlx,
    user_id: i64,
    args: AbortActionArgs,
) -> Result<AbortActionResult> {
    let reference = &args.reference;

    // Step 1: Find the transaction (read-only, outside the write
    // transaction: the broadcast checks below read the memory and the
    // descendants on the pool).
    let tx = {
        let mut conn = storage.pool().acquire().await?;
        find_transaction(&mut conn, user_id, reference).await?
    };

    let tx = tx.ok_or_else(|| Error::NotFound {
        entity: "transaction".to_string(),
        id: format!("reference or txid '{}'", reference),
    })?;

    // Step 2: Validate the transaction can be aborted
    let kind = validate_transaction_for_abort(&tx)?;

    match kind {
        AbortKind::Pending => abort_pending(storage, &tx).await,
        AbortKind::Broadcast => abort_broadcast(storage, &tx).await,
    }
}

/// The abort of a never-broadcast transaction.
async fn abort_pending(storage: &StorageSqlx, tx: &TransactionRecord) -> Result<AbortActionResult> {
    // Begin a SQL transaction so that all mutations are atomic.
    let mut db_tx = storage
        .pool()
        .begin()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

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

/// The abort of a broadcast transaction (see the module docs).
async fn abort_broadcast(
    storage: &StorageSqlx,
    tx: &TransactionRecord,
) -> Result<AbortActionResult> {
    let txid = tx.txid.as_deref().ok_or_else(|| {
        Error::InvalidTransactionStatus(format!(
            "cannot abort action: transaction with status '{}' has no txid, so its chain evidence cannot be checked",
            tx.status
        ))
    })?;

    // Only a transaction the chain does not know may be aborted.
    if let Some(evidence) = chain_evidence_for(storage, txid).await? {
        return Err(Error::InvalidTransactionStatus(format!(
            "cannot abort action: the network has transaction {} ({}); only a broadcast transaction with no chain evidence can be aborted",
            txid, evidence
        )));
    }

    // Everything built on it goes with it, unless something is proven,
    // which means the transaction is on chain after all.
    let descendants = storage.poisoned_descendants(txid).await?;
    if let Some(proven) = descendants.iter().find(|d| d.status == "completed") {
        return Err(Error::InvalidTransactionStatus(format!(
            "cannot abort action: transaction {} has a proven descendant {}, so it is on chain",
            txid, proven.txid
        )));
    }

    let now = Utc::now();
    let mut db_tx = storage
        .pool()
        .begin()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    // Its inputs come back: the caller vouches the transaction never mines.
    release_locked_outputs(&mut db_tx, tx.transaction_id).await?;

    // Its own outputs never fund anything again.
    sqlx::query("UPDATE outputs SET spendable = 0, updated_at = ? WHERE transaction_id = ?")
        .bind(now)
        .bind(tx.transaction_id)
        .execute(&mut *db_tx)
        .await?;

    update_transaction_status_to_failed(&mut db_tx, tx.transaction_id).await?;

    // The req is invalid: nothing re-sends it, the proof fetcher stops
    // asking for it.
    sqlx::query(
        "UPDATE proven_tx_reqs SET status = 'invalid', attempts = attempts + 1, updated_at = ? \
         WHERE txid = ? AND status NOT IN ('completed')",
    )
    .bind(now)
    .bind(txid)
    .execute(&mut *db_tx)
    .await?;

    db_tx
        .commit()
        .await
        .map_err(|e| Error::DatabaseError(e.to_string()))?;

    // No provider ever skips it again, and the reconciler treats it as a
    // rejected root rather than a candidate.
    storage.mark_broadcast_rejected_quiet(txid).await;

    // The descendants: change chained on the aborted transaction, and
    // whatever was built on that. The chain oracle, when the wallet has
    // one, decides their outside inputs; without one they stay locked and
    // re-checked.
    let services = storage.get_services().ok();
    let (restored, kept) = storage
        .retire_poisoned_descendants(services.as_deref(), txid, tx.transaction_id, now)
        .await?;

    tracing::warn!(
        txid = %txid,
        was = %tx.status,
        descendants = descendants.len(),
        descendant_inputs_restored = restored,
        descendant_inputs_kept = kept,
        "abort_action: broadcast transaction aborted on the caller's word (no chain evidence); inputs released, descendants retired"
    );

    Ok(AbortActionResult { aborted: true })
}

/// The chain evidence the wallet holds for `txid`, described, or `None`.
///
/// Chain evidence is a validated proof (`proven_txs`), a `mined` row from
/// any provider, or the chain pseudo-provider's `seen`. A broadcaster's
/// `accepted` / `seen` and the network pseudo-provider's `seen` are not,
/// however often they were refreshed.
pub(crate) async fn chain_evidence_for(
    storage: &StorageSqlx,
    txid: &str,
) -> Result<Option<String>> {
    let proven: Option<(i64,)> =
        sqlx::query_as("SELECT proven_tx_id FROM proven_txs WHERE txid = ?")
            .bind(txid)
            .fetch_optional(storage.pool())
            .await?;
    if proven.is_some() {
        return Ok(Some("a validated merkle proof".to_string()));
    }
    let records = storage
        .broadcast_records(None, std::slice::from_ref(&txid.to_string()))
        .await?;
    for record in &records {
        match record.ladder_status() {
            Some(BroadcastStatus::Mined) => {
                return Ok(Some(format!("{} reports it mined", record.provider)));
            }
            Some(BroadcastStatus::Seen) if record.provider == BROADCAST_PROVIDER_CHAIN => {
                return Ok(Some("the chain index has seen it".to_string()));
            }
            _ => {}
        }
    }
    Ok(None)
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
        SELECT transaction_id, status, is_outgoing, txid
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
            txid: row.get("txid"),
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
        SELECT transaction_id, status, is_outgoing, txid
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
            txid: row.get("txid"),
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

/// Validate that the transaction can be aborted, and how.
fn validate_transaction_for_abort(tx: &TransactionRecord) -> Result<AbortKind> {
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
        return Ok(AbortKind::Pending);
    }

    // Broadcast: abortable without chain evidence (checked by the caller).
    if BROADCAST_STATUSES.contains(&status) {
        return Ok(AbortKind::Broadcast);
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
    use crate::services::broadcast_memory::{
        BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_ACCEPTED, BROADCAST_STATUS_MINED,
        BROADCAST_STATUS_REJECTED, BROADCAST_STATUS_SEEN, PROVIDER_ARCADE_V2, PROVIDER_TAAL_ARC,
    };
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
    // Test 11: Abort sending transaction without chain evidence - succeeds
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_sending_transaction_without_chain_evidence_succeeds() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // A sending outgoing transaction the broadcaster merely accepted.
        let reference = "test-abort-ref-11";
        let txid = "a".repeat(64);
        let tx_id =
            insert_test_transaction(&storage, user_id, reference, "sending", true, Some(&txid))
                .await;
        storage
            .record_broadcast_status(&txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();

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
    // Test 12: Abort unproven transaction - the broadcaster's word is not
    // chain evidence, the abort is honoured and the chain built on it retired
    // -------------------------------------------------------------------------

    /// A proven_tx_req for `txid` (what the broadcast path leaves behind).
    async fn insert_test_req(storage: &StorageSqlx, txid: &str, status: &str) {
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, raw_tx, created_at, updated_at) \
             VALUES (?, ?, 0, '{}', 0, '{}', X'01000000', ?, ?)",
        )
        .bind(txid)
        .bind(status)
        .bind(now)
        .bind(now)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    async fn req_status(storage: &StorageSqlx, txid: &str) -> String {
        sqlx::query_scalar("SELECT status FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_one(storage.pool())
            .await
            .unwrap()
    }

    async fn memory_statuses(storage: &StorageSqlx, txid: &str) -> Vec<(String, String)> {
        let mut rows: Vec<(String, String)> = storage
            .broadcast_records(None, std::slice::from_ref(&txid.to_string()))
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.provider, r.status))
            .collect();
        rows.sort();
        rows
    }

    async fn locked_check_verdict(storage: &StorageSqlx, output_id: i64) -> Option<String> {
        sqlx::query_scalar("SELECT last_verdict FROM locked_input_checks WHERE output_id = ?")
            .bind(output_id)
            .fetch_optional(storage.pool())
            .await
            .unwrap()
    }

    /// The 2026-09-03 shape: a completed coin funds the aborted head spend
    /// `A`; `A`'s change funds a descendant `D` together with another
    /// completed coin; `D` has change of its own.
    struct BroadcastChain {
        a: i64,
        a_txid: String,
        d: i64,
        d_txid: String,
        /// Completed coin spent by A.
        coin_a: i64,
        /// A's change, spent by D.
        change_a: i64,
        /// Completed coin spent by D (outside the poisoned set).
        coin_d: i64,
        /// D's change.
        change_d: i64,
    }

    async fn seed_broadcast_chain(
        storage: &StorageSqlx,
        user_id: i64,
        reference: &str,
    ) -> BroadcastChain {
        let a_txid = "1".repeat(64);
        let d_txid = "2".repeat(64);
        let funding = insert_test_transaction(
            storage,
            user_id,
            "funding-ref",
            "completed",
            true,
            Some(&"f".repeat(64)),
        )
        .await;
        let a =
            insert_test_transaction(storage, user_id, reference, "unproven", true, Some(&a_txid))
                .await;
        let d = insert_test_transaction(
            storage,
            user_id,
            "desc-ref",
            "unproven",
            true,
            Some(&d_txid),
        )
        .await;
        let coin_a = insert_test_output(storage, user_id, funding, 0, false, Some(a)).await;
        let coin_d = insert_test_output(storage, user_id, funding, 1, false, Some(d)).await;
        let change_a = insert_test_output(storage, user_id, a, 0, false, Some(d)).await;
        let change_d = insert_test_output(storage, user_id, d, 0, true, None).await;
        insert_test_req(storage, &a_txid, "unmined").await;
        insert_test_req(storage, &d_txid, "unmined").await;
        BroadcastChain {
            a,
            a_txid,
            d,
            d_txid,
            coin_a,
            change_a,
            coin_d,
            change_d,
        }
    }

    #[tokio::test]
    async fn test_abort_unproven_without_chain_evidence_releases_inputs_and_retires_descendants() {
        let (storage, user_id, auth) = setup_test_storage().await;
        let reference = "test-abort-ref-12";
        let c = seed_broadcast_chain(&storage, user_id, reference).await;

        // Arcade saw it, several times; a peer node saw it too. None of that
        // is the chain's word.
        for _ in 0..4 {
            storage
                .record_broadcast_status(&c.a_txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN)
                .await
                .unwrap();
        }
        storage
            .record_broadcast_status(&c.a_txid, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        storage
            .record_broadcast_status(&c.d_txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        assert!(chain_evidence_for(&storage, &c.a_txid)
            .await
            .unwrap()
            .is_none());

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

        // The aborted transaction: failed, req invalid, its input released,
        // its change dead.
        assert_eq!(get_transaction_status(&storage, c.a).await, "failed");
        assert_eq!(req_status(&storage, &c.a_txid).await, "invalid");
        assert!(is_output_spendable(&storage, c.coin_a).await);
        assert!(is_output_spent_by_null(&storage, c.coin_a).await);
        assert!(!is_output_spendable(&storage, c.change_a).await);

        // The descendant: failed, req invalid, its own change dead, the
        // poisoned input left as it was, and the outside coin KEPT locked
        // (no chain oracle in this storage) with a re-check scheduled.
        assert_eq!(get_transaction_status(&storage, c.d).await, "failed");
        assert_eq!(req_status(&storage, &c.d_txid).await, "invalid");
        assert!(!is_output_spendable(&storage, c.change_d).await);
        assert!(!is_output_spent_by_null(&storage, c.change_a).await);
        assert!(!is_output_spendable(&storage, c.coin_d).await);
        assert!(!is_output_spent_by_null(&storage, c.coin_d).await);
        assert_eq!(
            locked_check_verdict(&storage, c.coin_d).await.as_deref(),
            Some("unknown")
        );

        // The memory forgets both: every row rejected, plus the network's.
        assert_eq!(
            memory_statuses(&storage, &c.a_txid).await,
            vec![
                (
                    PROVIDER_ARCADE_V2.to_string(),
                    BROADCAST_STATUS_REJECTED.to_string()
                ),
                (
                    BROADCAST_PROVIDER_NETWORK.to_string(),
                    BROADCAST_STATUS_REJECTED.to_string()
                ),
            ]
        );
        assert_eq!(
            memory_statuses(&storage, &c.d_txid).await,
            vec![
                (
                    PROVIDER_ARCADE_V2.to_string(),
                    BROADCAST_STATUS_REJECTED.to_string()
                ),
                (
                    BROADCAST_PROVIDER_NETWORK.to_string(),
                    BROADCAST_STATUS_REJECTED.to_string()
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_abort_unproven_with_chain_seen_row_is_refused() {
        let (storage, user_id, auth) = setup_test_storage().await;
        let reference = "test-abort-ref-12b";
        let c = seed_broadcast_chain(&storage, user_id, reference).await;
        storage
            .record_broadcast_status(&c.a_txid, BROADCAST_PROVIDER_CHAIN, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();

        let err = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await
        .unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("the network has transaction"), "{}", msg);
                assert!(msg.contains("chain index has seen it"), "{}", msg);
            }
            other => panic!("Expected InvalidTransactionStatus, got: {:?}", other),
        }
        // Nothing moved.
        assert_eq!(get_transaction_status(&storage, c.a).await, "unproven");
        assert_eq!(get_transaction_status(&storage, c.d).await, "unproven");
        assert!(!is_output_spendable(&storage, c.coin_a).await);
        assert!(is_output_spendable(&storage, c.change_d).await);
        assert_eq!(req_status(&storage, &c.a_txid).await, "unmined");
    }

    #[tokio::test]
    async fn test_abort_unproven_with_a_mined_row_from_any_provider_is_refused() {
        let (storage, user_id, auth) = setup_test_storage().await;
        let reference = "test-abort-ref-12c";
        let c = seed_broadcast_chain(&storage, user_id, reference).await;
        storage
            .record_broadcast_status(&c.a_txid, PROVIDER_ARCADE_V2, BROADCAST_STATUS_MINED)
            .await
            .unwrap();

        let err = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await
        .unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("reports it mined"), "{}", msg);
            }
            other => panic!("Expected InvalidTransactionStatus, got: {:?}", other),
        }
        assert_eq!(get_transaction_status(&storage, c.a).await, "unproven");
    }

    #[tokio::test]
    async fn test_abort_unproven_with_a_proven_descendant_is_refused() {
        let (storage, user_id, auth) = setup_test_storage().await;
        let reference = "test-abort-ref-12d";
        let c = seed_broadcast_chain(&storage, user_id, reference).await;
        sqlx::query("UPDATE transactions SET status = 'completed' WHERE transaction_id = ?")
            .bind(c.d)
            .execute(storage.pool())
            .await
            .unwrap();

        let err = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: reference.to_string(),
            },
        )
        .await
        .unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("proven descendant"), "{}", msg);
                assert!(msg.contains(&c.d_txid), "{}", msg);
            }
            other => panic!("Expected InvalidTransactionStatus, got: {:?}", other),
        }
        assert_eq!(get_transaction_status(&storage, c.a).await, "unproven");
        assert!(!is_output_spendable(&storage, c.coin_a).await);
    }

    #[tokio::test]
    async fn test_a_broadcaster_status_never_counts_as_chain_evidence_however_often_refreshed() {
        let (storage, _user_id, _auth) = setup_test_storage().await;
        let txid = "9".repeat(64);
        // Every non-chain report there is, each refreshed many times.
        for _ in 0..6 {
            for provider in [PROVIDER_ARCADE_V2, PROVIDER_TAAL_ARC] {
                storage
                    .record_broadcast_status(&txid, provider, BROADCAST_STATUS_ACCEPTED)
                    .await
                    .unwrap();
                storage
                    .record_broadcast_status(&txid, provider, BROADCAST_STATUS_SEEN)
                    .await
                    .unwrap();
            }
            storage
                .record_broadcast_status(&txid, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_SEEN)
                .await
                .unwrap();
            // The refresh moved seen_at to now every time: still not the chain.
            assert!(
                chain_evidence_for(&storage, &txid).await.unwrap().is_none(),
                "a broadcaster's seen is that broadcaster's row and nothing more"
            );
        }
        // The chain index's word is.
        storage
            .record_broadcast_status(&txid, BROADCAST_PROVIDER_CHAIN, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        assert_eq!(
            chain_evidence_for(&storage, &txid)
                .await
                .unwrap()
                .as_deref(),
            Some("the chain index has seen it")
        );
        // So is a proof.
        let proven = "8".repeat(64);
        sqlx::query(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx, created_at, updated_at) \
             VALUES (?, 1, 0, 'h', 'r', X'00', X'00', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(&proven)
        .execute(storage.pool())
        .await
        .unwrap();
        assert_eq!(
            chain_evidence_for(&storage, &proven)
                .await
                .unwrap()
                .as_deref(),
            Some("a validated merkle proof")
        );
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
    // Test 16: Abort unproven transaction by txid - honoured without chain
    // evidence, refused with it
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_abort_unproven_transaction_by_txid_follows_the_chain_evidence() {
        let (storage, user_id, auth) = setup_test_storage().await;

        // Create an unproven outgoing transaction with a txid
        let reference = "test-abort-ref-16";
        let txid = "3".repeat(64);
        let tx_id =
            insert_test_transaction(&storage, user_id, reference, "unproven", true, Some(&txid))
                .await;
        storage
            .record_broadcast_status(&txid, BROADCAST_PROVIDER_CHAIN, BROADCAST_STATUS_MINED)
            .await
            .unwrap();

        // Refused while the chain knows it.
        let err = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: txid.clone(),
            },
        )
        .await
        .unwrap_err();
        match err {
            Error::InvalidTransactionStatus(msg) => {
                assert!(msg.contains("the network has transaction"), "{}", msg);
            }
            other => panic!("Expected InvalidTransactionStatus, got: {:?}", other),
        }
        assert_eq!(get_transaction_status(&storage, tx_id).await, "unproven");

        // Honoured for a transaction the chain never saw (found by txid).
        let other_ref = "test-abort-ref-16b";
        let other_txid = "4".repeat(64);
        let other_id = insert_test_transaction(
            &storage,
            user_id,
            other_ref,
            "unproven",
            true,
            Some(&other_txid),
        )
        .await;
        let result = abort_action_internal(
            &storage,
            auth.user_id.unwrap(),
            AbortActionArgs {
                reference: other_txid.clone(),
            },
        )
        .await
        .unwrap();
        assert!(result.aborted);
        assert_eq!(get_transaction_status(&storage, other_id).await, "failed");
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
