use std::collections::HashSet;
use std::sync::Arc;

use bsv_rs::primitives::{hash160, sha256d, PrivateKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, ChainTracker, ChainTrackerError, MerklePath, Transaction};
use bsv_rs::transaction::{TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    BasketInsertion, Counterparty, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, KeyDeriverApi, ListOutputsArgs, ProtoWallet,
    Protocol, SecurityLevel,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::services::traits::BlockHeader;
use bsv_wallet_toolbox_rs::{AuthId, MonitorStorage, StorageSqlx, Wallet, WalletStorageWriter};

pub const HEIGHT: u32 = 900_000;
pub const SATOSHIS: u64 = 100_000;
pub const ORIGINATOR: &str = "bench.local";
const PREFIX: &str = "YmVuY2gtcHJlZml4";
const SUFFIX: &str = "YmVuY2gtc3VmZml4";

pub struct Store {
    pub wallet: Wallet<StorageSqlx, MockWalletServices>,
    pub auth: AuthId,
    pub basket: i64,
    pub coins: Vec<i64>,
    last_transaction: i64,
}

struct StoredRoots(HashSet<String>);

#[async_trait::async_trait]
impl ChainTracker for StoredRoots {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        Ok(height == HEIGHT && self.0.contains(root))
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(HEIGHT)
    }

    /// The mining context the spend verifier needs since #103: a synthetic tip
    /// at HEIGHT, well past the CSV and Genesis activations, so the door never
    /// holds for a missing tip and the bench measures the verifier's work.
    async fn mining_tip(
        &self,
    ) -> Result<Option<bsv_rs::transaction::beef_stream::ChainTip>, ChainTrackerError> {
        #[allow(deprecated)]
        let tip = bsv_rs::transaction::beef_stream::ChainTip {
            height: HEIGHT,
            median_time_past: 1_700_000_000,
            // The deprecated copy (bsv-rs 0.9.0-beta.4) from ChainParams; the
            // verifiers read ChainParams alone (#451).
            genesis_height: bsv_rs::block::ChainParams::mainnet().genesis_height,
            csv_height: 419_328,
            next_block_time: None,
        };
        Ok(Some(tip))
    }
}

/// A distinct synthetic funding transaction for each coin. Its proof is a
/// single-leaf fixture checked against StoredRoots, never a network header.
pub fn funding_transaction(tag: usize, script: &[u8]) -> Transaction {
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(format!("{tag:064x}"), u32::MAX);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").expect("fixture unlock"));
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(SATOSHIS),
        locking_script: LockingScript::from_binary(script).expect("fixture lock"),
        change: false,
    });
    tx
}

async fn store_proof(conn: &mut sqlx::SqliteConnection, tx: &Transaction) {
    let txid = tx.id();
    let bump = MerklePath::from_coinbase_txid(&txid, HEIGHT);
    sqlx::query(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) \
         VALUES (?, ?, 0, ?, ?, ?, ?)",
    )
    .bind(&txid)
    .bind(i64::from(HEIGHT))
    .bind("ab".repeat(32))
    .bind(&txid)
    .bind(bump.to_binary())
    .bind(tx.to_binary())
    .execute(&mut *conn)
    .await
    .expect("store synthetic proof");
    sqlx::query("INSERT INTO proof_root_checks (txid, height, merkle_root) VALUES (?, ?, ?)")
        .bind(&txid)
        .bind(i64::from(HEIGHT))
        .bind(&txid)
        .execute(conn)
        .await
        .expect("record checked root");
}

/// Shared by all storage benches. Initialization is timed only by seed_10000.
pub async fn seed(count: usize) -> Store {
    let storage = StorageSqlx::in_memory().await.expect("in-memory store");
    let root = PrivateKey::from_hex(&format!("{:064x}", 1)).expect("synthetic key");
    let identity = root.public_key().to_hex();
    storage
        .migrate("door-bench", &identity)
        .await
        .expect("migrate");
    storage.make_available().await.expect("make available");
    let wallet = Wallet::new(Some(root.clone()), storage, MockWalletServices::new())
        .await
        .expect("wallet");
    let (user, _) = wallet
        .storage()
        .find_or_insert_user(&identity)
        .await
        .expect("user");
    let auth = AuthId::with_user_id(identity, user.user_id);
    let basket = wallet
        .storage()
        .find_or_create_default_basket(user.user_id)
        .await
        .expect("default basket")
        .basket_id;
    let proto = ProtoWallet::new(Some(root));
    let key = proto
        .key_deriver()
        .derive_private_key(
            &Protocol::new(SecurityLevel::Counterparty, "3241645161d8"),
            &format!("{PREFIX} {SUFFIX}"),
            &Counterparty::Self_,
        )
        .expect("funding key");
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend(hash160(&key.public_key().to_compressed()));
    script.extend([0x88, 0xac]);

    let rows: Vec<_> = (0..count)
        .map(|i| {
            let tx = funding_transaction(i, &script);
            let txid = tx.id();
            let proof = MerklePath::from_coinbase_txid(&txid, HEIGHT).to_binary();
            (i as i64 + 1, txid, tx.to_binary(), proof)
        })
        .collect();
    let mut roots: HashSet<_> = rows.iter().map(|row| row.1.clone()).collect();
    let mut writer = wallet.storage().write_tx().await.expect("seed transaction");
    // 64 rows keeps even the output insert below 999 bind parameters.
    // Explicit fixture IDs let all four tables be loaded in batches.
    for chunk in rows.chunks(64) {
        let mut query = sqlx::QueryBuilder::new(
            "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) "
        );
        query.push_values(chunk, |mut row, (_, txid, raw, proof)| {
            row.push_bind(txid)
                .push_bind(i64::from(HEIGHT))
                .push_bind(0)
                .push_bind("ab".repeat(32))
                .push_bind(txid)
                .push_bind(proof)
                .push_bind(raw);
        });
        query
            .build()
            .execute(&mut *writer)
            .await
            .expect("funding proofs");
        let mut query =
            sqlx::QueryBuilder::new("INSERT INTO proof_root_checks (txid, height, merkle_root) ");
        query.push_values(chunk, |mut row, (_, txid, _, _)| {
            row.push_bind(txid)
                .push_bind(i64::from(HEIGHT))
                .push_bind(txid);
        });
        query
            .build()
            .execute(&mut *writer)
            .await
            .expect("checked roots");
        let mut query = sqlx::QueryBuilder::new(
            "INSERT INTO transactions (transaction_id, user_id, status, reference, is_outgoing, \
             satoshis, version, lock_time, description, txid, raw_tx) ",
        );
        query.push_values(chunk, |mut row, (id, txid, raw, _)| {
            row.push_bind(id)
                .push_bind(user.user_id)
                .push_bind("completed")
                .push_bind(format!("funding-{id}"))
                .push_bind(false)
                .push_bind(SATOSHIS as i64)
                .push_bind(1)
                .push_bind(0)
                .push_bind("synthetic funding")
                .push_bind(txid)
                .push_bind(raw);
        });
        query
            .build()
            .execute(&mut *writer)
            .await
            .expect("funding transactions");
        let mut query = sqlx::QueryBuilder::new(
            "INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, vout, satoshis, \
             locking_script, txid, type, spendable, change, derivation_prefix, derivation_suffix, \
             provided_by, purpose) ",
        );
        query.push_values(chunk, |mut row, (id, txid, _, _)| {
            row.push_bind(id)
                .push_bind(user.user_id)
                .push_bind(id)
                .push_bind(basket)
                .push_bind(0)
                .push_bind(SATOSHIS as i64)
                .push_bind(&script)
                .push_bind(txid)
                .push_bind("P2PKH")
                .push_bind(true)
                .push_bind(true)
                .push_bind(PREFIX)
                .push_bind(SUFFIX)
                .push_bind("storage")
                .push_bind("change");
        });
        query
            .build()
            .execute(&mut *writer)
            .await
            .expect("funding outputs");
    }
    // The internalize benchmark receives a transaction with a proof already
    // stored, but without an owned transaction/output row yet.
    let incoming = funding_transaction(count, &[0x51]);
    store_proof(&mut writer, &incoming).await;
    let header = BlockHeader {
        version: 1,
        previous_hash: "00".repeat(32),
        merkle_root: incoming.id(),
        time: 1_700_000_000,
        bits: 486_604_799,
        nonce: 1,
        hash: String::new(),
        height: HEIGHT,
    };
    let mut block_hash = sha256d(&header.to_binary());
    block_hash.reverse();
    sqlx::query("UPDATE proven_txs SET block_hash = ? WHERE txid = ?")
        .bind(hex::encode(block_hash))
        .bind(incoming.id())
        .execute(&mut *writer)
        .await
        .expect("incoming proof header");
    roots.insert(incoming.id());
    writer.commit().await.expect("commit seed");
    wallet.services().set_header_for_height(header);
    wallet
        .storage()
        .set_max_acceptable_proof_height(HEIGHT)
        .await
        .expect("aged proof gate");
    wallet
        .storage()
        .set_chain_tracker(Arc::new(StoredRoots(roots)))
        .await;
    wallet.services().reset_history();
    Store {
        wallet,
        auth,
        basket,
        coins: rows.iter().map(|row| row.0).collect(),
        last_transaction: count as i64,
    }
}

impl Store {
    /// Restore the exact eligible coin set between measured calls. Never
    /// include this cleanup in the elapsed time returned to Criterion.
    pub async fn reset(&self) {
        let mut writer = self
            .wallet
            .storage()
            .write_tx()
            .await
            .expect("reset transaction");
        sqlx::query("UPDATE outputs SET spendable = 1, spent_by = NULL WHERE spent_by IS NOT NULL")
            .execute(&mut *writer)
            .await
            .expect("release funding coins");
        sqlx::query("DELETE FROM proven_tx_reqs")
            .execute(&mut *writer)
            .await
            .expect("remove requests");
        sqlx::query("DELETE FROM outputs WHERE transaction_id > ?")
            .bind(self.last_transaction)
            .execute(&mut *writer)
            .await
            .expect("remove action outputs");
        sqlx::query("DELETE FROM transactions WHERE transaction_id > ?")
            .bind(self.last_transaction)
            .execute(&mut *writer)
            .await
            .expect("remove actions");
        writer.commit().await.expect("commit reset");
        self.wallet.services().reset_history();
    }

    pub async fn assert_clean(&self) {
        let (coins, free): (i64, i64) =
            sqlx::query_as("SELECT COUNT(*), SUM(spendable = 1 AND spent_by IS NULL) FROM outputs")
                .fetch_one(self.wallet.storage().pool())
                .await
                .expect("count coins");
        assert_eq!(
            (coins, free),
            (self.coins.len() as i64, self.coins.len() as i64)
        );
    }

    pub fn internalize_args(&self) -> InternalizeActionArgs {
        let tx = funding_transaction(self.coins.len(), &[0x51]);
        let txid = tx.id();
        let mut beef = Beef::new();
        let index = beef.merge_bump(MerklePath::from_coinbase_txid(&txid, HEIGHT));
        beef.merge_raw_tx(tx.to_binary(), Some(index));
        InternalizeActionArgs {
            tx: beef.to_binary_atomic(&txid).expect("atomic proof"),
            outputs: vec![InternalizeOutput {
                output_index: 0,
                protocol: "basket insertion".into(),
                payment_remittance: None,
                insertion_remittance: Some(BasketInsertion {
                    basket: "default".into(),
                    custom_instructions: None,
                    tags: None,
                }),
            }],
            description: "synthetic proof intake".into(),
            labels: None,
            seek_permission: None,
        }
    }
}

pub fn payment_args() -> CreateActionArgs {
    CreateActionArgs {
        description: "synthetic benchmark payment".into(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: vec![0x51],
            satoshis: 1_000,
            output_description: "benchmark output".into(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            no_send: Some(true),
            ..Default::default()
        }),
    }
}

pub fn list_args() -> ListOutputsArgs {
    ListOutputsArgs {
        basket: "default".into(),
        tags: None,
        tag_query_mode: None,
        include: None,
        include_custom_instructions: None,
        include_tags: None,
        include_labels: None,
        limit: Some(10_000),
        offset: None,
        seek_permission: None,
    }
}
