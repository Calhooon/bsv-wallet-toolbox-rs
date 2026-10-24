//! The change pick is a function of the store's rows, never of a byte length
//! (#105, found by bsv-wallet-cli's differential harness, CLI #70): two coins
//! of equal value and status whose transactions' `input_beef` differ by one
//! byte (a DER signature's length moves it between runs) must resolve to the
//! same coin whichever BEEF is the longer.

use chrono::Utc;

use super::create_action::allocate_change_input;
use super::StorageSqlx;
use crate::storage::traits::WalletStorageWriter;

const SATS: i64 = 2_000;

/// Plants one `completed` change coin of `SATS` in the default basket whose
/// transaction carries `beef_len` bytes of `input_beef`; returns its output id.
async fn plant(s: &StorageSqlx, user_id: i64, basket_id: i64, tag: &str, beef_len: usize) -> i64 {
    let now = Utc::now();
    let tx_id: i64 = sqlx::query_scalar(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, input_beef, created_at, updated_at) \
         VALUES (?, 'completed', ?, 0, ?, 1, 0, 'tiebreak coin', ?, ?, ?, ?) RETURNING transaction_id",
    )
    .bind(user_id)
    .bind(format!("ref-{tag}"))
    .bind(SATS)
    .bind(format!("{tag:0>64}"))
    .bind(vec![0u8; beef_len])
    .bind(now)
    .bind(now)
    .fetch_one(s.writer())
    .await
    .unwrap();
    sqlx::query_scalar(
        "INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid, locking_script, derivation_prefix, derivation_suffix, created_at, updated_at) \
         VALUES (?, ?, ?, 1, 1, 0, ?, 'storage', 'change', 'P2PKH', ?, X'76a914000000000000000000000000000000000000000088ac', 'prefix', 'suffix', ?, ?) RETURNING output_id",
    )
    .bind(user_id)
    .bind(tx_id)
    .bind(basket_id)
    .bind(SATS)
    .bind(format!("{tag:0>64}"))
    .bind(now)
    .bind(now)
    .fetch_one(s.writer())
    .await
    .unwrap()
}

/// Plants the two coins with the given BEEF lengths, picks for `target`, and
/// returns (lower output id, higher output id, picked output id).
async fn pick_with(a_len: usize, b_len: usize, target: u64) -> (i64, i64, i64) {
    let s = StorageSqlx::in_memory().await.unwrap();
    s.migrate("test-wallet", "02test_key").await.unwrap();
    s.make_available().await.unwrap();
    let (user, _) = s.find_or_insert_user("02tiebreak").await.unwrap();
    let basket = s.find_or_create_default_basket(user.user_id).await.unwrap();
    let a = plant(&s, user.user_id, basket.basket_id, "a1", a_len).await;
    let b = plant(&s, user.user_id, basket.basket_id, "b2", b_len).await;
    let spender: i64 = sqlx::query_scalar(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, created_at, updated_at) \
         VALUES (?, 'unsigned', 'ref-spender', 1, 0, 1, 0, 'spender', ?, ?, ?) RETURNING transaction_id",
    )
    .bind(user.user_id)
    .bind(format!("{:0>64}", "5e"))
    .bind(Utc::now())
    .bind(Utc::now())
    .fetch_one(s.writer())
    .await
    .unwrap();
    let mut conn = s.writer().acquire().await.unwrap();
    allocate_change_input(
        &mut conn,
        user.user_id,
        basket.basket_id,
        spender,
        target,
        true,
    )
    .await
    .unwrap()
    .expect("a coin is picked");
    // The pick is the coin the allocation marked spent by the spender.
    let picked: i64 = sqlx::query_scalar("SELECT output_id FROM outputs WHERE spent_by = ?")
        .bind(spender)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    (a.min(b), a.max(b), picked)
}

#[tokio::test]
async fn equal_coins_pick_the_lower_output_id_whichever_beef_is_longer() {
    for target in [SATS as u64, 500] {
        // The lower id's BEEF one byte longer, then one byte shorter.
        for (a_len, b_len) in [(1_208, 1_207), (1_207, 1_208)] {
            let (low, high, picked) = pick_with(a_len, b_len, target).await;
            assert_ne!(low, high);
            assert_eq!(
                picked, low,
                "target {target}, BEEF lengths {a_len}/{b_len}: \
                 the pick must be the lower output id, not the shorter BEEF"
            );
        }
    }
}
