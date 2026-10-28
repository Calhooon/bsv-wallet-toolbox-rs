use super::*;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    AlwaysValidChainTracker, Transaction, TransactionInput, TransactionOutput,
};
use bsv_rs::wallet::{CreateActionInput, Outpoint, TrustSelf};

const LOCK: &str = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac";

/// A one-input, one-output transaction spending `parent:0` (or a
/// coinbase-shaped outpoint when `parent` is `None`), unique by `tag`.
fn tx_spending(parent: Option<&str>, tag: u8, satoshis: u64) -> Transaction {
    Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some(
                parent.map_or_else(|| format!("{:02x}", tag).repeat(32), str::to_string),
            ),
            source_output_index: if parent.is_some() { 0 } else { 0xffff_ffff },
            unlocking_script: Some(UnlockingScript::from_hex(&format!("01{:02x}", tag)).unwrap()),
            unlocking_script_template: None,
            sequence: 0xffff_ffff,
        }],
        vec![TransactionOutput {
            satoshis: Some(satoshis),
            locking_script: LockingScript::from_hex(LOCK).unwrap(),
            change: false,
        }],
        0,
    )
}

/// A one-leaf BUMP proving `txid` at `height`.
fn bump_for(txid: &str, height: u32) -> MerklePath {
    MerklePath {
        block_height: height,
        path: vec![vec![bsv_rs::transaction::MerklePathLeaf {
            offset: 0,
            hash: Some(txid.to_string()),
            txid: true,
            duplicate: false,
        }]],
    }
}

async fn setup() -> (StorageSqlx, i64) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("trust-self", "02test_key").await.unwrap();
    storage.make_available().await.unwrap();
    let (user, _) = storage
        .find_or_insert_user("02trust_self_user")
        .await
        .unwrap();
    (storage, user.user_id)
}

/// The wallet's own record of `tx`: its bytes, its output 0 (the
/// user's, spendable, not change) and, when `proven`, its proof.
async fn seed_known(storage: &StorageSqlx, user_id: i64, tx: &Transaction, proven: bool) {
    let txid = tx.id();
    let raw = tx.to_binary();
    if proven {
        seed_proven_tx(storage, &txid, &raw, &bump_for(&txid, 900).to_binary()).await;
    }
    let now = Utc::now();
    let row = sqlx::query(
                r#"
                INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
                VALUES (?, 'completed', ?, 0, ?, 1, 0, 'known to the wallet', ?, ?, ?, ?)
                "#,
            )
            .bind(user_id)
            .bind(format!("ref-{}", &txid[..8]))
            .bind(tx.outputs[0].satoshis.unwrap() as i64)
            .bind(&txid)
            .bind(&raw)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap();
    sqlx::query(
                r#"
                INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                                     txid, type, spendable, change, provided_by, purpose,
                                     output_description, created_at, updated_at)
                VALUES (?, ?, NULL, 0, ?, ?, ?, 'custom', 1, 0, 'you', '', 'known output', ?, ?)
                "#,
            )
            .bind(user_id)
            .bind(row.last_insert_rowid())
            .bind(tx.outputs[0].satoshis.unwrap() as i64)
            .bind(hex::decode(LOCK).unwrap())
            .bind(&txid)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap();
}

fn input(txid: &str) -> CreateActionInput {
    CreateActionInput {
        outpoint: Outpoint::from_string(&format!("{}.0", txid)).unwrap(),
        input_description: "an input".to_string(),
        unlocking_script: None,
        unlocking_script_length: Some(107),
        sequence_number: None,
    }
}

fn args(inputs: Vec<CreateActionInput>, beef: Vec<u8>) -> bsv_rs::wallet::CreateActionArgs {
    bsv_rs::wallet::CreateActionArgs {
        description: "trust self known".to_string(),
        input_beef: Some(beef),
        inputs: Some(inputs),
        outputs: Some(vec![CreateActionOutput {
            locking_script: hex::decode(LOCK).unwrap(),
            satoshis: 1_000,
            output_description: "an output".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            trust_self: Some(TrustSelf::Known),
            ..Default::default()
        }),
    }
}

/// The caller's BEEF: `proven` with its own BUMP, then a txid-only
/// entry for each of `txid_only`.
fn caller_beef(proven: &Transaction, txid_only: &[String]) -> Vec<u8> {
    let mut beef = Beef::new();
    let bi = beef.merge_bump(bump_for(&proven.id(), 800));
    beef.merge_raw_tx(proven.to_binary(), Some(bi));
    for t in txid_only {
        beef.merge_txid_only(t.clone());
    }
    beef.to_binary()
}

async fn count(storage: &StorageSqlx, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

/// A change coin of the default basket on `funding`'s output 0
/// (`funding` recorded `completed`, with its proof when `proven`).
async fn seed_change_coin(
    storage: &StorageSqlx,
    user_id: i64,
    funding: &Transaction,
    proven: bool,
) -> i64 {
    let basket = storage
        .find_or_create_default_basket(user_id)
        .await
        .unwrap();
    let txid = funding.id();
    let raw = funding.to_binary();
    if proven {
        seed_proven_tx(storage, &txid, &raw, &bump_for(&txid, 900).to_binary()).await;
    }
    let now = Utc::now();
    let row = sqlx::query(
                r#"
                INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx, created_at, updated_at)
                VALUES (?, 'completed', ?, 0, ?, 1, 0, 'funding', ?, ?, ?, ?)
                "#,
            )
            .bind(user_id)
            .bind(format!("fund-{}", &txid[..8]))
            .bind(funding.outputs[0].satoshis.unwrap() as i64)
            .bind(&txid)
            .bind(&raw)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap();
    sqlx::query(
                r#"
                INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script,
                                     txid, type, spendable, change, derivation_prefix, derivation_suffix,
                                     provided_by, purpose, output_description, created_at, updated_at)
                VALUES (?, ?, ?, 0, ?, ?, ?, 'P2PKH', 1, 1, 'prefix', 'suffix', 'storage', 'change', 'change', ?, ?)
                "#,
            )
            .bind(user_id)
            .bind(row.last_insert_rowid())
            .bind(basket.basket_id)
            .bind(funding.outputs[0].satoshis.unwrap() as i64)
            .bind(hex::decode(LOCK).unwrap())
            .bind(&txid)
            .bind(now)
            .bind(now)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid()
}

/// One change coin is enough: the default basket wants one.
async fn want_one_change_coin(storage: &StorageSqlx) {
    sqlx::query("UPDATE output_baskets SET number_of_desired_utxos = 1")
        .execute(storage.writer())
        .await
        .unwrap();
}

/// A wallet-funded action: one output to a new basket, with a label
/// and a tag, so a written action would leave rows in every table.
fn funded_args() -> bsv_rs::wallet::CreateActionArgs {
    bsv_rs::wallet::CreateActionArgs {
        description: "the door in one step".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: hex::decode(LOCK).unwrap(),
            satoshis: 1_000,
            output_description: "an output".to_string(),
            basket: Some("a-new-basket".to_string()),
            custom_instructions: None,
            tags: Some(vec!["a-new-tag".to_string()]),
        }]),
        lock_time: None,
        version: None,
        labels: Some(vec!["a-new-label".to_string()]),
        options: None,
    }
}

/// Everything an action writes, as text: the coins, the rows, the
/// labels, tags, baskets and their links, and the sequence counter.
async fn the_store(storage: &StorageSqlx) -> Vec<Option<String>> {
    let mut seen = Vec::new();
    for sql in [
                "SELECT group_concat(output_id || ':' || spendable || ':' || ifnull(spent_by, '-'), ',') FROM outputs",
                "SELECT group_concat(transaction_id || ':' || status, ',') FROM transactions",
                "SELECT group_concat(label, ',') FROM tx_labels",
                "SELECT group_concat(transaction_id || ':' || tx_label_id, ',') FROM tx_labels_map",
                "SELECT group_concat(tag, ',') FROM output_tags",
                "SELECT group_concat(output_id || ':' || output_tag_id, ',') FROM output_tags_map",
                "SELECT group_concat(name, ',') FROM output_baskets",
                "SELECT group_concat(name || ':' || seq, ',') FROM sqlite_sequence",
            ] {
                seen.push(
                    sqlx::query_scalar::<_, Option<String>>(sql)
                        .fetch_one(storage.pool())
                        .await
                        .unwrap(),
                );
            }
    seen
}

// =====================================================================
// Concurrent createActions in one process (review 2 F1 of #44)
// =====================================================================

/// Waits 30 ms on every root (the reviewer's scratch tracker), so
/// concurrent pre-checks overlap, and records the coins reserved by
/// every call at each ask.
struct SlowTracker {
    in_flight: std::sync::Arc<InFlightPicks>,
    seen_reserved: std::sync::Mutex<Vec<usize>>,
}

impl SlowTracker {
    fn new(storage: &StorageSqlx) -> Self {
        Self {
            in_flight: storage.in_flight_picks().clone(),
            seen_reserved: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl ChainTracker for SlowTracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, bsv_rs::transaction::ChainTrackerError> {
        self.seen_reserved
            .lock()
            .unwrap()
            .push(self.in_flight.reserved().len());
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        Ok(true)
    }

    async fn current_height(
        &self,
    ) -> std::result::Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Ok(100_000)
    }
}

/// Eight proven 900,000-sat change coins; one wanted.
async fn eight_coins(storage: &StorageSqlx, user_id: i64) {
    want_one_change_coin(storage).await;
    for i in 0..8u8 {
        let t = tx_spending(None, 0x40 + i, 900_000);
        seed_change_coin(storage, user_id, &t, true).await;
    }
}

// =====================================================================
// The host's offer (bsv-wallet-cli #44, bsv-low #828)
// =====================================================================

/// Eight 900,000-sat coins; their output ids, in seeding order.
async fn eight_coin_ids(storage: &StorageSqlx, user_id: i64) -> Vec<i64> {
    want_one_change_coin(storage).await;
    let mut ids = Vec::new();
    for i in 0..8u8 {
        let t = tx_spending(None, 0x70 + i, 900_000);
        ids.push(seed_change_coin(storage, user_id, &t, true).await);
    }
    ids
}

/// `funded_args` through the door with `offer`.
async fn offering(
    storage: &StorageSqlx,
    tracker: &dyn ChainTracker,
    user_id: i64,
    offer: &[i64],
) -> Result<StorageCreateActionResult> {
    create_action_at_door_internal(
        storage,
        Some(tracker),
        user_id,
        funded_args(),
        CreateActionDoor {
            offer: Some(offer),
            must_spend: &[],
            sign_and_process: None,
        },
    )
    .await
    .map(|done| done.created)
    .map_err(DoorRefusal::into_error)
}

/// The coins allocated to some action.
async fn allocated(storage: &StorageSqlx) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT output_id FROM outputs WHERE spent_by IS NOT NULL ORDER BY output_id",
    )
    .fetch_all(storage.pool())
    .await
    .unwrap()
}
#[cfg(test)]
mod known;

#[cfg(test)]
mod precheck;

#[cfg(test)]
mod races;

#[cfg(test)]
mod concurrency;

#[cfg(test)]
mod unspendable;

#[cfg(test)]
mod offers;
