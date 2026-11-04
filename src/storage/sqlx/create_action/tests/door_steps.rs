//! #604: the door's steps, each from its rule (`docs/ARCHITECTURE.md`
//! section 1, the `createAction` row).
use super::super::test_support::synth_tx;
use super::*;
use crate::storage::traits::{CreateActionDoor, SignAndProcess, StorageProcessActionArgs};
use bsv_rs::transaction::{BeefTx, MockChainTracker};

async fn funded_store() -> (StorageSqlx, i64) {
    let (storage, user_id) = caller_complete_storage().await;
    seed_change_output(&storage, user_id, 100_000).await;
    (storage, user_id)
}

/// The store's one change coin, unspent but not marked spendable.
async fn unmark_change(storage: &StorageSqlx) {
    sqlx::query("UPDATE outputs SET spendable = 0 WHERE change = 1")
        .execute(storage.writer())
        .await
        .unwrap();
}

fn payment(options: Option<CreateActionOptions>) -> bsv_rs::wallet::CreateActionArgs {
    bsv_rs::wallet::CreateActionArgs {
        description: "A door step test".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![p2pkh_output(1_000, "door step payment")]),
        lock_time: None,
        version: None,
        labels: None,
        options,
    }
}

fn delayed(accept_delayed_broadcast: bool) -> CreateActionOptions {
    CreateActionOptions {
        accept_delayed_broadcast: Some(accept_delayed_broadcast),
        ..CreateActionOptions::default()
    }
}

fn change_outputs(created: &StorageCreateActionResult) -> usize {
    created
        .outputs
        .iter()
        .filter(|o| o.purpose.as_deref() == Some("change"))
        .count()
}

/// The delayed route may take a change coin not yet marked spendable
/// (the allocation's `require_spendable` is `!is_delayed`, the reference's
/// `excludeSending = !isDelayed`).
#[tokio::test]
async fn the_delayed_route_may_spend_a_change_coin_not_marked_spendable() {
    let (storage, user_id) = funded_store().await;
    unmark_change(&storage).await;

    let created = create_action_internal(&storage, None, user_id, payment(Some(delayed(true))))
        .await
        .expect("a delayed action spends the unmarked change coin");
    assert_eq!(
        created.inputs.len(),
        1,
        "the unmarked coin is the one input"
    );
}

/// The immediate route spends only a change coin marked spendable.
#[tokio::test]
async fn the_immediate_route_spends_only_a_change_coin_marked_spendable() {
    let (storage, user_id) = funded_store().await;
    unmark_change(&storage).await;

    let refused =
        create_action_internal(&storage, None, user_id, payment(Some(delayed(false)))).await;
    assert!(
        matches!(refused, Err(Error::InsufficientFunds { .. })),
        "the unmarked coin is never picked on the immediate route: {refused:?}"
    );
}

/// The write-locked step counts the change coins the immediate route may
/// spend, the spendable ones only, as the pre-check does: a coin not marked
/// spendable never stands in for a change output the basket still wants.
#[tokio::test]
async fn the_step_counts_only_the_change_coins_its_route_may_spend() {
    async fn made(with_unmarked: bool) -> usize {
        let (storage, user_id) = funded_store().await;
        sqlx::query("UPDATE output_baskets SET number_of_desired_utxos = 2")
            .execute(storage.writer())
            .await
            .unwrap();
        if with_unmarked {
            let (_, unmarked) =
                seed_tagged_change_output(&storage, user_id, 50_000, "completed", "604").await;
            sqlx::query("UPDATE outputs SET spendable = 0 WHERE output_id = ?")
                .bind(unmarked)
                .execute(storage.writer())
                .await
                .unwrap();
        }
        let created =
            create_action_internal(&storage, None, user_id, payment(Some(delayed(false))))
                .await
                .unwrap();
        change_outputs(&created)
    }

    assert_eq!(
        made(true).await,
        made(false).await,
        "an unmarked coin changes nothing on the immediate route"
    );
}

/// With no caller tracker, the step's assembly reads every stored proof
/// against the storage's own tracker's answers (`reads_proofs`): a checked
/// stored bump whose root that tracker refutes never enters the BEEF, and
/// the transaction rides as raw bytes above its proven parent (ruling 7c).
#[tokio::test]
async fn without_a_caller_tracker_the_step_reads_stored_proofs_against_the_storages_own() {
    let (storage, user_id) = caller_complete_storage().await;
    let (parent_raw, parent) = synth_tx(&"11".repeat(32), 0, 200_000);
    seed_proven_chain_tx(&storage, &parent, &parent_raw, 900_000).await;
    let (coin_raw, coin) = synth_tx(&parent, 0, 100_000);
    seed_proven_chain_tx(&storage, &coin, &coin_raw, 900_001).await;
    // The coin's proof was checked once; the headers have moved since.
    sqlx::query(
        "INSERT INTO proof_root_checks (txid, height, merkle_root, checked_at) VALUES (?, 900001, ?, ?)",
    )
    .bind(&coin)
    .bind(&coin)
    .bind(Utc::now())
    .execute(storage.writer())
    .await
    .unwrap();
    let (transaction_id, output_id) =
        seed_tagged_change_output(&storage, user_id, 100_000, "completed", "6041").await;
    // The seeded coin made the coin transaction's own.
    sqlx::query("UPDATE transactions SET txid = ?, raw_tx = ? WHERE transaction_id = ?")
        .bind(&coin)
        .bind(&coin_raw)
        .bind(transaction_id)
        .execute(storage.writer())
        .await
        .unwrap();
    sqlx::query("UPDATE outputs SET txid = ? WHERE output_id = ?")
        .bind(&coin)
        .bind(output_id)
        .execute(storage.writer())
        .await
        .unwrap();

    let mut tracker = MockChainTracker::new(900_100);
    tracker.add_root(900_000, parent.clone());
    storage
        .set_chain_tracker(std::sync::Arc::new(tracker))
        .await;

    let created = create_action_internal(&storage, None, user_id, payment(None))
        .await
        .unwrap();
    let beef = Beef::from_binary(&created.input_beef.expect("the BEEF")).unwrap();
    let entry = beef.find_txid(&coin).expect("the coin's transaction rides");
    assert!(
        !entry.has_proof(),
        "the refuted stored bump never enters the BEEF"
    );
    assert!(
        beef.find_txid(&parent).is_some_and(BeefTx::has_proof),
        "the proven parent carries the coin's ancestry"
    );
}

/// A raw transaction in the template's shape: a placeholder input, the
/// template's outputs. Local CPU; nothing is signed or broadcast.
fn template_bytes(created: &StorageCreateActionResult) -> Vec<u8> {
    let mut raw = vec![1, 0, 0, 0, 1];
    raw.extend_from_slice(&[0u8; 32]);
    raw.extend_from_slice(&[0xff; 4]);
    raw.push(0);
    raw.extend_from_slice(&[0xff; 4]);
    raw.push(u8::try_from(created.outputs.len()).unwrap());
    for output in &created.outputs {
        raw.extend_from_slice(&output.satoshis.to_le_bytes());
        let script = hex::decode(&output.locking_script).unwrap();
        raw.push(u8::try_from(script.len()).unwrap());
        raw.extend_from_slice(&script);
    }
    raw.extend_from_slice(&[0; 4]);
    raw
}

/// `SignAndProcess`'s contract: the door sets `is_new_tx` itself (with the
/// reference, the txid and the raw bytes), whatever the host's flags say, so
/// the signed action is processed as the new transaction it is.
#[tokio::test]
async fn the_door_processes_its_signed_action_as_a_new_transaction() {
    let (storage, user_id) = funded_store().await;
    let sign = |created: &StorageCreateActionResult| Ok(template_bytes(created));
    let door = CreateActionDoor {
        sign_and_process: Some(SignAndProcess {
            sign: &sign,
            // The host's flags name an existing transaction.
            process: StorageProcessActionArgs {
                is_new_tx: false,
                send_with: vec![],
                raw_tx: None,
                txid: None,
                reference: None,
                is_delayed: false,
                is_no_send: false,
                is_send_with: false,
            },
        }),
        ..CreateActionDoor::default()
    };

    let done = create_action_at_door_internal(&storage, None, user_id, payment(None), door)
        .await
        .map_err(DoorRefusal::into_error)
        .expect("the door signs and processes");
    let processed = done.processed.expect("processed in the step");
    let stored: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT raw_tx FROM transactions WHERE reference = ?")
            .bind(&done.created.reference)
            .fetch_one(storage.pool())
            .await
            .unwrap();
    assert_eq!(
        stored.as_deref(),
        Some(processed.raw_tx.as_slice()),
        "processAction's new-transaction writes stored the signed bytes"
    );
}
