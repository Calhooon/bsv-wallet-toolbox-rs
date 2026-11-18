//! The wallet charter's R1 (d) at the storage's two selection queries: a
//! received coin (`purpose = 'receive'`) whose transaction is not
//! `completed` is counted and picked only when the action names the
//! received rule; the wallet's own change and a completed received coin are
//! counted and picked as before. The count (`count_change_inputs_under_rule`,
//! which sets how many change outputs the action targets) and the pick
//! (`allocate_change_input_by`, the dry run and the allocation under the
//! lock) agree on every case.

use super::*;
use crate::storage::sqlx::StorageSqlx;
use crate::storage::WalletStorageWriter;

/// A store holding one 100 000 satoshi change coin of `purpose` whose
/// transaction is `status`: the store, the user, the default basket and the
/// coin's output id.
async fn one_coin(purpose: &str, status: &str) -> (StorageSqlx, i64, i64, i64) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("received-rule", "02test_key")
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage.find_or_insert_user("02user_key").await.unwrap();
    let basket = storage
        .find_or_create_default_basket(user.user_id)
        .await
        .unwrap();
    let now = Utc::now();
    let txid = "ab".repeat(32);
    let transaction_id = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, \
         lock_time, description, txid, created_at, updated_at) \
         VALUES (?, ?, 'parent', 0, 100000, 1, 0, 'parent', ?, ?, ?)",
    )
    .bind(user.user_id)
    .bind(status)
    .bind(&txid)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    let output_id = sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, \
         txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, \
         purpose, output_description, created_at, updated_at) \
         VALUES (?, ?, ?, 0, 100000, ?, ?, 'P2PKH', 1, 1, 'p', 's', 'storage', ?, 'coin', ?, ?)",
    )
    .bind(user.user_id)
    .bind(transaction_id)
    .bind(basket.basket_id)
    .bind(vec![0x76u8, 0xa9, 0x14])
    .bind(&txid)
    .bind(purpose)
    .bind(now)
    .bind(now)
    .execute(storage.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    (storage, user.user_id, basket.basket_id, output_id)
}

/// The count, the dry run's pick and the allocation's pick for one coin.
async fn count_and_picks(purpose: &str, status: &str, allow_received: bool) -> (usize, bool, bool) {
    let (storage, user_id, basket_id, output_id) = one_coin(purpose, status).await;
    let mut conn = storage.pool().acquire().await.unwrap();
    let count = count_change_inputs_under_rule(&mut conn, user_id, basket_id, allow_received)
        .await
        .unwrap();
    let mut reserved = PickReservation::new(Default::default());
    let dry = allocate_change_input_by(
        &mut conn,
        user_id,
        basket_id,
        0,
        1_000,
        &mut ChangePick::DryRun {
            reserved: &mut reserved,
            skip: &[],
            offer: None,
            allow_received,
        },
    )
    .await
    .unwrap()
    .is_some();
    drop(conn);
    let mut tx = storage.write_tx().await.unwrap();
    let spender = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, \
         lock_time, description, created_at, updated_at) \
         VALUES (?, 'unsigned', 'spender', 1, 0, 1, 0, 'spender', ?, ?)",
    )
    .bind(user_id)
    .bind(Utc::now())
    .bind(Utc::now())
    .execute(&mut *tx)
    .await
    .unwrap()
    .last_insert_rowid();
    let within = allocate_change_input_by(
        &mut tx,
        user_id,
        basket_id,
        spender,
        1_000,
        &mut ChangePick::Within {
            allowed: &[output_id],
            allow_received,
        },
    )
    .await
    .unwrap()
    .is_some();
    (count, dry, within)
}

#[tokio::test]
async fn the_count_and_both_picks_agree_on_every_case() {
    // (purpose, status, rule, taken)
    let cases = [
        ("receive", "unproven", false, false),
        ("receive", "unproven", true, true),
        ("receive", "completed", false, true),
        ("receive", "completed", true, true),
        ("change", "unproven", false, true),
        ("change", "unproven", true, true),
        ("change", "completed", false, true),
    ];
    for (purpose, status, rule, taken) in cases {
        let got = count_and_picks(purpose, status, rule).await;
        println!("purpose={purpose} status={status} rule={rule}: {got:?}");
        assert_eq!(
            got,
            (usize::from(taken), taken, taken),
            "purpose={purpose} status={status} rule={rule}"
        );
    }
}

fn labelled(labels: &[&str]) -> bsv_rs::wallet::CreateActionArgs {
    bsv_rs::wallet::CreateActionArgs {
        description: "a labelled action".to_string(),
        input_beef: None,
        inputs: None,
        outputs: None,
        lock_time: None,
        version: None,
        labels: Some(labels.iter().map(|l| l.to_string()).collect()),
        options: None,
    }
}

#[test]
fn the_rule_is_read_from_the_labels_trimmed_and_lowercased() {
    for named in [
        &["underReceivedRule"][..],
        &["underreceivedrule"],
        &["UNDERRECEIVEDRULE"],
        &[" underReceivedRule\t"],
        &["\u{feff}underReceivedRule"],
        &["other", "underReceivedRule"],
    ] {
        assert!(names_received_rule(&labelled(named)), "{named:?}");
    }
    for not_named in [
        &[][..],
        &["under received rule"],
        &["underReceivedRules"],
        &["underReceived"],
        &["\u{85}underReceivedRule"],
    ] {
        assert!(!names_received_rule(&labelled(not_named)), "{not_named:?}");
    }
    let mut none = labelled(&[]);
    none.labels = None;
    assert!(!names_received_rule(&none));
}
