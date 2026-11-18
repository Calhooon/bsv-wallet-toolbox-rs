//! A seed shared by the `sendWith` tests: proven P2PKH change coins of a
//! wallet's own, written straight into the store, each under a proven
//! parent at a height of its own, so any number of them assemble into one
//! BEEF.
//!
//! Each test file uses part of this module, hence `dead_code` allowed.

#![allow(dead_code)]

use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{Counterparty, KeyDeriverApi, ProtoWallet, Protocol, SecurityLevel};
use bsv_wallet_toolbox_rs::StorageSqlx;

const BRC29_PROTOCOL: &str = "3241645161d8";
const SEED_PREFIX: &str = "c2VuZHdpdGg=";

pub fn p2pkh_lock(pubkey: &[u8]) -> Vec<u8> {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&hash160(pubkey));
    script.extend([0x88, 0xac]);
    script
}

/// `coins` (four at most) proven change coins of `root`'s, user `user_id`,
/// of `satoshis`, `satoshis + 1`, and so on.
pub async fn seed_coins(
    storage: &StorageSqlx,
    root: &PrivateKey,
    user_id: i64,
    coins: u32,
    satoshis: i64,
) {
    let basket = storage
        .find_or_create_default_basket(user_id)
        .await
        .expect("basket");
    let proto = ProtoWallet::new(Some(root.clone()));
    for i in 0..coins {
        let suffix = format!("Y29pbi0{}", ["w", "x", "y", "z"][i as usize]);
        let sk = proto
            .key_deriver()
            .derive_private_key(
                &Protocol::new(SecurityLevel::Counterparty, BRC29_PROTOCOL),
                &format!("{SEED_PREFIX} {suffix}"),
                &Counterparty::Self_,
            )
            .expect("derive funding key");
        let funding_lock = p2pkh_lock(&sk.public_key().to_compressed());
        let satoshis = satoshis + i64::from(i);
        let height = 100_000 + u32::try_from(user_id).unwrap() * 10 + i;
        let parent = Transaction::with_params(
            1,
            vec![TransactionInput {
                source_transaction: None,
                source_txid: Some("00".repeat(32)),
                source_output_index: 0xffff_ffff,
                unlocking_script: Some(UnlockingScript::from_hex("00").unwrap()),
                unlocking_script_template: None,
                sequence: 0xffff_ffff,
            }],
            vec![TransactionOutput {
                satoshis: Some(u64::try_from(satoshis).unwrap()),
                locking_script: LockingScript::from_binary(&funding_lock).unwrap(),
                change: false,
            }],
            0,
        );
        let parent_txid = parent.id();
        let now = chrono::Utc::now();
        let bump = MerklePath::from_coinbase_txid(&parent_txid, height);
        sqlx::query(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, \
             raw_tx, created_at, updated_at) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&parent_txid)
        .bind(i64::from(height))
        .bind(format!("{:x}{i}", user_id % 16).repeat(32))
        .bind(&parent_txid)
        .bind(bump.to_binary().unwrap())
        .bind(parent.to_binary())
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .expect("proven funding parent");
        let parent_id = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, \
             lock_time, description, txid, raw_tx, created_at, updated_at) \
             VALUES (?, 'completed', ?, 0, ?, 1, 0, 'seeded parent', ?, ?, ?, ?)",
        )
        .bind(user_id)
        .bind(format!("parent-ref-{user_id}-{i}"))
        .bind(satoshis)
        .bind(&parent_txid)
        .bind(parent.to_binary())
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .expect("insert parent")
        .last_insert_rowid();
        sqlx::query(
            "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, \
             txid, type, spendable, change, derivation_prefix, derivation_suffix, provided_by, \
             purpose, output_description, created_at, updated_at) \
             VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, ?, ?, 'storage', 'change', 'wallet funding', ?, ?)",
        )
        .bind(user_id)
        .bind(parent_id)
        .bind(basket.basket_id)
        .bind(satoshis)
        .bind(&funding_lock)
        .bind(&parent_txid)
        .bind(SEED_PREFIX)
        .bind(&suffix)
        .bind(now)
        .bind(now)
        .execute(storage.writer())
        .await
        .expect("insert funding output");
    }
}
