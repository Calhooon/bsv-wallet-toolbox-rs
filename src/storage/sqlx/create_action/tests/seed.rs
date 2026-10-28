use super::*;
use crate::storage::sqlx::StorageSqlxOptions;
use crate::storage::{AuthId, WalletStorageWriter};
use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, ProtoWallet};

async fn setup(seed: Option<[u8; 32]>) -> (StorageSqlx, AuthId) {
    let storage =
        StorageSqlx::new_with_options("sqlite::memory:", StorageSqlxOptions { rng_seed: seed })
            .await
            .unwrap();
    storage.migrate("rng-test", "fixture").await.unwrap();
    storage.make_available().await.unwrap();
    let identity = ProtoWallet::new(None).identity_key_hex();
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
    (storage, AuthId::with_user_id(identity, user.user_id))
}

pub(super) fn action_args() -> CreateActionArgs {
    CreateActionArgs {
        description: "Seeded payment".into(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![p2pkh_output(1_000, "payment")]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            no_send: Some(true),
            sign_and_process: Some(false),
            ..CreateActionOptions::default()
        }),
    }
}

async fn create(seed: Option<[u8; 32]>) -> StorageCreateActionResult {
    let (storage, auth) = setup(seed).await;
    seed_change_output(&storage, auth.user_id.unwrap(), 100_000).await;
    storage.create_action(&auth, action_args()).await.unwrap()
}

#[tokio::test]
async fn seeded_create_action_repeats_references_and_derivations() {
    let left = create(Some([42; 32])).await;
    let right = create(Some([42; 32])).await;
    assert_eq!(left.reference, right.reference);
    assert_eq!(left.derivation_prefix, right.derivation_prefix);
    let suffixes = |result: &StorageCreateActionResult| {
        result
            .outputs
            .iter()
            .filter_map(|output| output.derivation_suffix.clone())
            .collect::<Vec<_>>()
    };
    assert!(!suffixes(&left).is_empty());
    assert_eq!(suffixes(&left), suffixes(&right));
}

async fn process_batch(seed: Option<[u8; 32]>, new_transaction: bool) -> Vec<String> {
    let (storage, auth) = setup(seed).await;
    let mut handles = Vec::new();
    for index in 1..=2 {
        let txid = format!("{index:064x}");
        let reference = format!("stored-{index}");
        sqlx::query("INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, txid, description) VALUES (?, 'nosend', ?, 1, 1, ?, 'seeded request')")
            .bind(auth.user_id).bind(&reference).bind(&txid)
            .execute(storage.writer()).await.unwrap();
        sqlx::query("INSERT INTO proven_tx_reqs (txid, status, raw_tx, input_beef) VALUES (?, 'nosend', ?, ?)")
            .bind(&txid).bind(vec![1u8]).bind(vec![1u8])
            .execute(storage.writer()).await.unwrap();
        handles.push(txid);
    }
    let raw_tx = new_transaction.then(seed_raw_tx);
    let txid = raw_tx.as_deref().map(txid_of);
    if new_transaction {
        sqlx::query("INSERT INTO transactions (user_id, status, reference, is_outgoing, input_beef, description) VALUES (?, 'unsigned', 'new-action', 1, ?, 'new seeded request')")
            .bind(auth.user_id).bind(Beef::new().to_binary())
            .execute(storage.writer()).await.unwrap();
    }
    let result = storage
        .process_action(
            &auth,
            StorageProcessActionArgs {
                is_new_tx: new_transaction,
                is_send_with: true,
                is_no_send: false,
                is_delayed: true,
                reference: new_transaction.then(|| "new-action".to_string()),
                txid,
                raw_tx,
                send_with: handles,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        result.send_with_results.unwrap().len(),
        2 + usize::from(new_transaction)
    );
    sqlx::query_scalar("SELECT batch FROM proven_tx_reqs ORDER BY txid")
        .fetch_all(storage.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn seeded_process_action_repeats_batch_references() {
    let left = process_batch(Some([42; 32]), false).await;
    let right = process_batch(Some([42; 32]), false).await;
    assert_eq!(left.len(), 2);
    assert!(!left[0].is_empty());
    assert_eq!(left[0], left[1]);
    assert_eq!(left, right);
}

#[tokio::test]
async fn seeded_new_process_action_repeats_batch_references() {
    let expected = process_batch(Some([42; 32]), true).await;
    assert_eq!(expected.len(), 3);
    assert!(expected.iter().all(|batch| batch == &expected[0]));
    assert_eq!(expected, process_batch(Some([42; 32]), true).await);
}

#[tokio::test]
async fn unseeded_create_action_references_differ() {
    assert_ne!(create(None).await.reference, create(None).await.reference);
}

#[tokio::test]
async fn seeded_wallet_actions_repeat_references_derivations_and_unsigned_bytes() {
    use crate::services::mock::MockWalletServices;
    use crate::wallet::{Wallet, WalletOptions};
    use bsv_rs::wallet::WalletInterface;

    let mut observations = Vec::new();
    for _ in 0..2 {
        let (storage, auth) = setup(Some([42; 32])).await;
        seed_change_output(&storage, auth.user_id.unwrap(), 100_000).await;
        let wallet = Wallet::with_options(
            None,
            storage,
            MockWalletServices::new(),
            WalletOptions {
                rng_seed: Some([11; 32]),
                ..WalletOptions::default()
            },
        )
        .await
        .unwrap();
        let result = WalletInterface::create_action(&wallet, action_args(), Some("rng-test"))
            .await
            .unwrap();
        let signable = result.signable_transaction.unwrap();
        let derivations: Vec<(String, String)> = sqlx::query_as(
            "SELECT derivation_prefix, derivation_suffix FROM outputs WHERE \"change\" = 1 ORDER BY output_id",
        )
        .fetch_all(wallet.storage().pool())
        .await
        .unwrap();
        assert!(derivations.len() > 1);
        observations.push((signable.reference, signable.tx, derivations));
    }
    assert_eq!(observations[0], observations[1]);
}
