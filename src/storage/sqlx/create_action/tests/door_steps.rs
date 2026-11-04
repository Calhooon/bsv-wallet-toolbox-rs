//! #604: the door's steps, each from its rule (`docs/ARCHITECTURE.md`
//! section 1, the `createAction` row).
use super::super::test_support::synth_tx;
use super::*;
use crate::storage::traits::{CreateActionDoor, SignAndProcess, StorageProcessActionArgs};
use bsv_rs::transaction::MockChainTracker;

async fn funded_store() -> (StorageSqlx, i64) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    seed_change_output(&storage, user.user_id, 100_000).await;
    (storage, user.user_id)
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

fn delayed(accept_delayed_broadcast: bool) -> Option<CreateActionOptions> {
    Some(CreateActionOptions {
        accept_delayed_broadcast: Some(accept_delayed_broadcast),
        ..CreateActionOptions::default()
    })
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

    let created = create_action_internal(&storage, None, user_id, payment(delayed(true)))
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

    let refused = create_action_internal(&storage, None, user_id, payment(delayed(false))).await;
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
        let created = create_action_internal(&storage, None, user_id, payment(delayed(false)))
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
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("test-wallet", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage
        .find_or_insert_user("02user_identity_key")
        .await
        .unwrap();
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap();

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
    let now = Utc::now();
    let transaction_id = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at) \
         VALUES (?, 'completed', 'coin_ref', 0, 100000, 1, 0, 'the coin', ?, ?, ?, ?)",
    )
    .bind(user.user_id)
    .bind(&coin)
    .bind(&coin_raw)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, purpose, output_description, created_at, updated_at) \
         VALUES (?, ?, ?, 0, 100000, ?, ?, 'P2PKH', 1, 1, 'prefix123', 'suffix456', 'storage', 'change', 'the coin', ?, ?)",
    )
    .bind(user.user_id)
    .bind(transaction_id)
    .bind(basket.basket_id)
    .bind(hex::decode("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac").unwrap())
    .bind(&coin)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap();

    let mut tracker = MockChainTracker::new(900_100);
    tracker.add_root(900_000, parent.clone());
    storage
        .set_chain_tracker(std::sync::Arc::new(tracker))
        .await;

    let created = create_action_internal(&storage, None, user.user_id, payment(None))
        .await
        .unwrap();
    let beef = Beef::from_binary(&created.input_beef.expect("the BEEF")).unwrap();
    let entry = beef.find_txid(&coin).expect("the coin's transaction rides");
    assert!(
        !entry.has_proof(),
        "the refuted stored bump never enters the BEEF"
    );
    assert!(
        beef.find_txid(&parent).is_some_and(|p| p.has_proof()),
        "the proven parent carries the coin's ancestry"
    );
}

/// A raw transaction in the template's shape: a placeholder input, the
/// template's outputs. Local CPU; nothing is signed or broadcast.
fn template_bytes(created: &StorageCreateActionResult) -> Result<Vec<u8>> {
    let mut raw = vec![1, 0, 0, 0, 1];
    raw.extend_from_slice(&[0u8; 32]);
    raw.extend_from_slice(&[0xff; 4]);
    raw.push(0);
    raw.extend_from_slice(&[0xff; 4]);
    raw.push(created.outputs.len() as u8);
    for output in &created.outputs {
        raw.extend_from_slice(&output.satoshis.to_le_bytes());
        let script = hex::decode(&output.locking_script).unwrap();
        raw.push(script.len() as u8);
        raw.extend_from_slice(&script);
    }
    raw.extend_from_slice(&[0; 4]);
    Ok(raw)
}

/// `SignAndProcess`'s contract: the door sets `is_new_tx` itself (with the
/// reference, the txid and the raw bytes), whatever the host's flags say, so
/// the signed action is processed as the new transaction it is.
#[tokio::test]
async fn the_door_processes_its_signed_action_as_a_new_transaction() {
    let (storage, user_id) = funded_store().await;
    let sign = template_bytes;
    let door = CreateActionDoor {
        sign_and_process: Some(SignAndProcess {
            sign: &sign,
            process: StorageProcessActionArgs {
                is_new_tx: false,
                is_send_with: false,
                is_no_send: false,
                is_delayed: false,
                reference: None,
                txid: None,
                raw_tx: None,
                send_with: vec![],
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
