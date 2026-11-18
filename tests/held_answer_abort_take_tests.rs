//! A take heard after the wallet's own abort recovers the transaction, in
//! the one write that applies the take (the keeper's reading 18 of
//! section 30, the broadcaster charter's B4 (d)).
//!
//! The state: the wallet's own action is posted; `abortAction` lands while
//! the post is in flight (the transaction `failed`, its request `invalid`,
//! its inputs released); then an instance's take arrives. The rule: a
//! request `unmined` beside a `failed` row with its inputs released never
//! stands. A `failed` the host's abort wrote (its mark on the request's
//! history) goes back to `unproven` with each input still free marked
//! spent by it again and the outputs it closed opened again, the cause on
//! the request's history (`takenAfterAbort`); an input another action
//! spent meanwhile stays with that action. A `failed` on evidence (a
//! competitor's checked proof, the wallet's own refusal, a received
//! transaction) is not recovered by a take.
//!
//! The matrix: the take's writer (the door for another user's
//! presentation, send-waiting with the abort landing during its post, the
//! retire path's status source, the unfail task, the wallet's own post) by
//! the row's cause and its inputs (the abort with every input free, with
//! one spent meanwhile by another action, with one spent by an action
//! that is itself proven; a `failed` on a competitor's checked proof; on
//! the wallet's own refusal; a received transaction of another user).
//! Real SQLite, a mock broadcaster that takes the transaction, a status
//! source that knows it; nothing is sent.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, ProtocolEra};
use bsv_rs::transaction::{ChainTracker, ChainTrackerError, MerklePath};
use bsv_rs::wallet::{AbortActionArgs, ListActionsArgs, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::services::{
    BlockHeader, FiatCurrency, GetBeefResult, GetMerklePathResult, GetRawTxResult,
    GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput, PostBeefResult, TxStatusDetail,
    UtxoVerdict, WalletServices,
};
use bsv_wallet_toolbox_rs::{
    AuthId, BroadcastOutcome, GetStatusForTxidsResult, MonitorStorage, Result, StorageSqlx, Wallet,
    WalletStorageReader, WalletStorageWriter,
};

const PREFIX: &str = "dGFrZW4=";
const SUFFIX: &str = "YWJvcnQ=";
const ORIGIN: &str = "test.local";
const PROVEN_HEIGHT: u32 = ProtocolEra::MAINNET_GENESIS_HEIGHT + 150;

/// Every root checks.
struct Headers;

#[async_trait]
impl ChainTracker for Headers {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        Ok(true)
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(PROVEN_HEIGHT + 100)
    }
}

/// The take's writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    Door,
    SendWaiting,
    Retire,
    Unfail,
    OwnPost,
}

const ACTS: [Act; 5] = [
    Act::Door,
    Act::SendWaiting,
    Act::Retire,
    Act::Unfail,
    Act::OwnPost,
];

/// The row's cause, and for the abort what its input 0 reads by the take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cause {
    /// The host's abort; every input still free.
    AbortFree,
    /// The host's abort; input 0 spent meanwhile by another (unproven) action.
    AbortSpentByOther,
    /// The host's abort; input 0 spent by another action that is proven.
    AbortSpentByProven,
    /// `failed` on a competitor's checked proof (request `doubleSpend`).
    Competitor,
    /// `failed` on the wallet's own refusal (request `invalid`, no mark).
    Refusal,
    /// A received transaction of another user, `failed`.
    Received,
}

const CAUSES: [Cause; 6] = [
    Cause::AbortFree,
    Cause::AbortSpentByOther,
    Cause::AbortSpentByProven,
    Cause::Competitor,
    Cause::Refusal,
    Cause::Received,
];

impl Cause {
    fn aborts(self) -> bool {
        matches!(
            self,
            Cause::AbortFree | Cause::AbortSpentByOther | Cause::AbortSpentByProven
        )
    }
}

/// The fixture: the sender's (X's) action spending its outputs 0 and 1 of
/// the parent, paying the wallet's user.
struct Fixture {
    store: StorageSqlx,
    txid: String,
    x: AuthId,
    reference: String,
    parent_txid: String,
    /// X's output rows at parent:0 and parent:1 (the action's inputs).
    inputs: [i64; 2],
    /// X's action's own output (its change).
    own_output: i64,
    /// The proven spender's bytes.
    competitor: Vec<u8>,
}

async fn insert_tx(
    store: &StorageSqlx,
    user: i64,
    txid: &str,
    status: &str,
    raw: &[u8],
    outgoing: bool,
) -> i64 {
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO transactions (user_id, txid, status, reference, description, satoshis, \
         version, lock_time, raw_tx, is_outgoing, created_at, updated_at) \
         VALUES (?, ?, ?, ?, 'a cell of the matrix', 0, 1, 0, ?, ?, ?, ?)",
    )
    .bind(user)
    .bind(txid)
    .bind(status)
    .bind(format!("{}-{user}-{status}", &txid[..16]))
    .bind(raw)
    .bind(outgoing)
    .bind(now)
    .bind(now)
    .execute(store.writer())
    .await
    .unwrap()
    .last_insert_rowid()
}

async fn insert_output(
    store: &StorageSqlx,
    user: i64,
    transaction_id: i64,
    txid: &str,
    vout: i64,
    satoshis: i64,
    (spendable, spent_by): (bool, Option<i64>),
) -> i64 {
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, spendable, change, vout, satoshis, \
         provided_by, purpose, type, txid, spent_by) \
         VALUES (?, ?, ?, 1, ?, ?, 'storage', 'change', 'P2PKH', ?, ?)",
    )
    .bind(user)
    .bind(transaction_id)
    .bind(spendable)
    .bind(vout)
    .bind(satoshis)
    .bind(txid)
    .bind(spent_by)
    .execute(store.writer())
    .await
    .unwrap()
    .last_insert_rowid()
}

/// A proven transaction `competitor` (row of `user`, `completed`) with its
/// stored checked proof.
async fn prove(store: &StorageSqlx, user: i64, competitor: &[u8]) -> i64 {
    let id = bsv_rs::transaction::Transaction::from_binary(competitor)
        .unwrap()
        .id();
    let (root, path) = {
        let path = MerklePath::from_coinbase_txid(&id, PROVEN_HEIGHT);
        (
            path.compute_root(Some(&id)).unwrap(),
            path.to_binary().unwrap(),
        )
    };
    let row = insert_tx(store, user, &id, "completed", competitor, true).await;
    sqlx::query(
        "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) \
         VALUES (?, ?, 0, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(PROVEN_HEIGHT)
    .bind("bb".repeat(32))
    .bind(root)
    .bind(path)
    .bind(competitor)
    .execute(store.writer())
    .await
    .unwrap();
    row
}

/// What lands while the post is in flight: the host's abort, then the
/// meanwhile spend of input 0.
async fn abort_now(f: &Fixture, cause: Cause) {
    f.store
        .abort_action(
            &f.x,
            AbortActionArgs {
                reference: f.reference.clone(),
            },
        )
        .await
        .expect("the abort lands while the post is in flight");
    let x = f.x.user_id.unwrap();
    let spender = match cause {
        Cause::AbortSpentByOther => {
            Some(insert_tx(&f.store, x, &"0d".repeat(32), "sending", &[1], true).await)
        }
        Cause::AbortSpentByProven => Some(prove(&f.store, x, &f.competitor).await),
        _ => None,
    };
    if let Some(spender) = spender {
        sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?")
            .bind(spender)
            .bind(f.inputs[0])
            .execute(f.store.writer())
            .await
            .unwrap();
    }
}

/// Services whose post (or status read) lands the abort ahead of its answer, once.
struct AbortsInFlight {
    inner: MockWalletServices,
    fixture: tokio::sync::OnceCell<(Arc<Fixture>, Cause)>,
    armed: AtomicBool,
}

impl AbortsInFlight {
    async fn land(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            if let Some((f, cause)) = self.fixture.get() {
                abort_now(f, *cause).await;
            }
        }
    }
}

#[async_trait]
impl WalletServices for AbortsInFlight {
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker> {
        self.inner.get_chain_tracker().await
    }
    async fn get_height(&self) -> Result<u32> {
        self.inner.get_height().await
    }
    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>> {
        self.inner.get_header_for_height(height).await
    }
    async fn hash_to_header(&self, hash: &str) -> Result<BlockHeader> {
        self.inner.hash_to_header(hash).await
    }
    async fn get_raw_tx(&self, txid: &str, use_next: bool) -> Result<GetRawTxResult> {
        self.inner.get_raw_tx(txid, use_next).await
    }
    async fn get_merkle_path(&self, txid: &str, use_next: bool) -> Result<GetMerklePathResult> {
        self.inner.get_merkle_path(txid, use_next).await
    }
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<Vec<PostBeefResult>> {
        self.land().await;
        self.inner.post_beef(beef, txids).await
    }
    async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
        use_next: bool,
    ) -> Result<GetUtxoStatusResult> {
        self.inner
            .get_utxo_status(output, output_format, outpoint, use_next)
            .await
    }
    async fn get_status_for_txids(
        &self,
        txids: &[String],
        use_next: bool,
    ) -> Result<GetStatusForTxidsResult> {
        self.land().await;
        self.inner.get_status_for_txids(txids, use_next).await
    }
    async fn get_bsv_exchange_rate(&self) -> Result<f64> {
        self.inner.get_bsv_exchange_rate().await
    }
    async fn get_fiat_exchange_rate(
        &self,
        currency: FiatCurrency,
        base: Option<FiatCurrency>,
    ) -> Result<f64> {
        self.inner.get_fiat_exchange_rate(currency, base).await
    }
    fn hash_output_script(&self, script: &[u8]) -> String {
        self.inner.hash_output_script(script)
    }
    async fn is_utxo(&self, txid: &str, vout: u32, locking_script: &[u8]) -> UtxoVerdict {
        self.inner.is_utxo(txid, vout, locking_script).await
    }
    async fn n_lock_time_is_final(&self, n_lock_time: u32) -> Result<bool> {
        self.inner.n_lock_time_is_final(n_lock_time).await
    }
    async fn n_lock_time_is_final_for_tx(&self, input: NLockTimeInput) -> Result<bool> {
        self.inner.n_lock_time_is_final_for_tx(input).await
    }
    async fn get_beef(&self, txid: &str, known_txids: &[String]) -> Result<GetBeefResult> {
        self.inner.get_beef(txid, known_txids).await
    }
}

type AbortWallet = Wallet<StorageSqlx, AbortsInFlight>;

/// One cell's reading.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reading {
    /// X's row (or the received row's) word.
    row: Option<String>,
    request: Option<String>,
    /// Each input: (spendable, who spends it: `own`, `other`, `none`).
    inputs: Vec<(bool, String)>,
    own_output_spendable: Option<bool>,
    taken_after_abort: bool,
    take_not_counted: bool,
    /// X's spendable change.
    balance: i64,
    /// X's action as `listActions` lists it.
    listed: Option<String>,
}

impl Reading {
    /// The state the rule makes impossible: the request `unmined` beside a
    /// `failed` row with an input released.
    fn half(&self) -> bool {
        self.request.as_deref() == Some("unmined")
            && self.row.as_deref() == Some("failed")
            && self.inputs.iter().any(|(spendable, _)| *spendable)
    }
}

/// The cell's rows: X's action (or the received row) by `cause`.
async fn seed(store: &StorageSqlx, parts: &common::SpendParts, cause: Cause) -> Fixture {
    let store = store.clone();
    let txid = parts.txid.clone();
    // X: the parent, its outputs 0 and 1 locked by the action, the action
    // `sending` with its change, its request `sending` two hours old.
    let x_key = PrivateKey::random().public_key().to_hex();
    let (x_user, _) = store.find_or_insert_user(&x_key).await.unwrap();
    let x = AuthId::with_user_id(&x_key, x_user.user_id);
    let xid = x_user.user_id;
    let then = chrono::Utc::now() - chrono::Duration::hours(2);
    let (row_word, req_word) = match cause {
        c if c.aborts() => ("sending", "sending"),
        Cause::Competitor => ("failed", "doubleSpend"),
        _ => ("failed", "invalid"),
    };
    let received = cause == Cause::Received;
    let parent = insert_tx(&store, xid, &parts.parent_txid, "completed", &[1], false).await;
    let action = if received {
        None
    } else {
        Some(insert_tx(&store, xid, &txid, row_word, &parts.raw, true).await)
    };
    let mut inputs = [0i64; 2];
    for (vout, slot) in inputs.iter_mut().enumerate() {
        // A received transaction's inputs are its payer's coins, consumed
        // by the payer's spend with no spender recorded here.
        *slot = insert_output(
            &store,
            xid,
            parent,
            &parts.parent_txid,
            i64::try_from(vout).unwrap(),
            1000,
            (false, action),
        )
        .await;
    }
    let own_output = match action {
        Some(action) => {
            let spendable = cause.aborts();
            insert_output(&store, xid, action, &txid, 1, 500, (spendable, None)).await
        }
        None => 0,
    };
    if received {
        let y_key = PrivateKey::random().public_key().to_hex();
        let (y, _) = store.find_or_insert_user(&y_key).await.unwrap();
        insert_tx(&store, y.user_id, &txid, "failed", &parts.raw, false).await;
    }
    sqlx::query(
        "INSERT INTO proven_tx_reqs (txid, status, attempts, history, notified, notify, \
         raw_tx, input_beef, created_at, updated_at) VALUES (?, ?, 0, '{}', 0, '{}', ?, ?, ?, ?)",
    )
    .bind(&txid)
    .bind(req_word)
    .bind(&parts.raw)
    .bind(&parts.beef)
    .bind(then)
    .bind(then)
    .execute(store.writer())
    .await
    .unwrap();
    let competitor = common::competitor_of(&parts.parent_txid, 0).to_binary();
    if cause == Cause::Competitor {
        // The payer's own row of the coin, spent by the proven competitor.
        let p_key = PrivateKey::random().public_key().to_hex();
        let (p, _) = store.find_or_insert_user(&p_key).await.unwrap();
        let p_parent = insert_tx(
            &store,
            p.user_id,
            &parts.parent_txid,
            "completed",
            &[1],
            false,
        )
        .await;
        let c_row = prove(&store, p.user_id, &competitor).await;
        insert_output(
            &store,
            p.user_id,
            p_parent,
            &parts.parent_txid,
            0,
            1000,
            (false, Some(c_row)),
        )
        .await;
    }
    Fixture {
        store: store.clone(),
        txid: txid.clone(),
        x: x.clone(),
        reference: format!("{}-{xid}-sending", &txid[..16]),
        parent_txid: parts.parent_txid.clone(),
        inputs,
        own_output,
        competitor,
    }
}

/// The take's writer, run.
async fn act_on(
    wallet: &AbortWallet,
    act: Act,
    parts: &common::SpendParts,
    sender: &bsv_rs::primitives::PublicKey,
    taken: PostBeefResult,
) {
    let store = wallet.storage();
    let txid = parts.txid.clone();
    match act {
        Act::Door => {
            let _ = wallet
                .internalize_action(
                    common::args(parts.beef.clone(), common::payment(sender, PREFIX, SUFFIX)),
                    Some(ORIGIN),
                )
                .await;
        }
        Act::SendWaiting => {
            store
                .send_waiting_transactions(std::time::Duration::ZERO)
                .await
                .unwrap();
        }
        Act::Retire => {
            let _ = store
                .retire_undeliverable_txid(wallet.services(), &txid, "invalid")
                .await;
        }
        Act::Unfail => store.un_fail().await.unwrap(),
        Act::OwnPost => {
            let _ = store
                .update_transaction_status_after_post(
                    &txid,
                    &BroadcastOutcome::Success,
                    &[taken],
                    None,
                )
                .await;
        }
    }
}

async fn cell(act: Act, cause: Cause) -> Reading {
    let root = PrivateKey::random();
    let sender = PrivateKey::random().public_key();
    let key = common::derived_key(&root, &sender, PREFIX, SUFFIX);
    let parts =
        common::beef_paying_parts(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
    let txid = parts.txid.clone();
    let mut taken = common::accepted(&txid);
    taken.name = "arcade".to_string();
    let inner = MockWalletServices::builder()
        .post_beef_response(MockResponse::Success(vec![taken.clone()]))
        .get_status_for_txids_response(MockResponse::Success(GetStatusForTxidsResult {
            name: "status".to_string(),
            status: "success".to_string(),
            error: None,
            results: vec![TxStatusDetail {
                txid: txid.clone(),
                status: "known".to_string(),
                ..Default::default()
            }],
        }))
        .build();
    let services = AbortsInFlight {
        inner,
        fixture: tokio::sync::OnceCell::new(),
        armed: AtomicBool::new(false),
    };
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("abort-take", &root.public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    storage.set_chain_tracker(Arc::new(Headers)).await;
    let store = storage.clone();
    let wallet = Wallet::new(Some(root.clone()), storage, services)
        .await
        .unwrap();

    let fixture = Arc::new(seed(&store, &parts, cause).await);
    let txid = fixture.txid.clone();
    let then = chrono::Utc::now() - chrono::Duration::hours(2);

    // The abort: during send-waiting's post (its request was picked
    // before), else before the take arrives.
    if cause.aborts() {
        if act == Act::SendWaiting {
            wallet
                .services()
                .fixture
                .set((fixture.clone(), cause))
                .ok()
                .unwrap();
            wallet.services().armed.store(true, Ordering::SeqCst);
        } else {
            abort_now(&fixture, cause).await;
        }
    }
    if act == Act::Unfail {
        sqlx::query("UPDATE proven_tx_reqs SET updated_at = ? WHERE txid = ?")
            .bind(then)
            .bind(&txid)
            .execute(store.writer())
            .await
            .unwrap();
    }

    act_on(&wallet, act, &parts, &sender, taken).await;
    read(&fixture, cause == Cause::Received).await
}

async fn read(f: &Fixture, received: bool) -> Reading {
    let store = &f.store;
    let xid = f.x.user_id.unwrap();
    let row: Option<String> = sqlx::query_scalar(if received {
        "SELECT status FROM transactions WHERE txid = ? AND is_outgoing = 0 LIMIT 1"
    } else {
        "SELECT status FROM transactions WHERE txid = ? AND is_outgoing = 1 LIMIT 1"
    })
    .bind(&f.txid)
    .fetch_optional(store.pool())
    .await
    .unwrap();
    let action: Option<i64> = sqlx::query_scalar(
        "SELECT transaction_id FROM transactions WHERE txid = ? AND user_id = ?",
    )
    .bind(&f.txid)
    .bind(xid)
    .fetch_optional(store.pool())
    .await
    .unwrap();
    let (request, history): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT status, history FROM proven_tx_reqs WHERE txid = ?")
            .bind(&f.txid)
            .fetch_optional(store.pool())
            .await
            .unwrap()
            .map_or((None, None), |(s, h): (String, String)| (Some(s), Some(h)));
    let history = history.unwrap_or_default();
    let mut inputs = Vec::new();
    for id in f.inputs {
        let (spendable, spent_by): (bool, Option<i64>) =
            sqlx::query_as("SELECT spendable, spent_by FROM outputs WHERE output_id = ?")
                .bind(id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        let who = match spent_by {
            None => "none",
            Some(s) if Some(s) == action => "own",
            Some(_) => "other",
        };
        inputs.push((spendable, who.to_string()));
    }
    let own_output_spendable: Option<bool> =
        sqlx::query_scalar("SELECT spendable FROM outputs WHERE output_id = ?")
            .bind(f.own_output)
            .fetch_optional(store.pool())
            .await
            .unwrap();
    let balance: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(satoshis), 0) FROM outputs WHERE user_id = ? AND spendable = 1 AND change = 1",
    )
    .bind(xid)
    .fetch_one(store.pool())
    .await
    .unwrap();
    let listed = store
        .list_actions(
            &f.x,
            ListActionsArgs {
                labels: vec![],
                label_query_mode: None,
                include_labels: None,
                include_inputs: None,
                include_input_source_locking_scripts: None,
                include_input_unlocking_scripts: None,
                include_outputs: None,
                include_output_locking_scripts: None,
                limit: None,
                offset: None,
                seek_permission: None,
            },
        )
        .await
        .unwrap()
        .actions
        .iter()
        .find(|a| hex::encode(a.txid) == f.txid)
        .map(|a| format!("{:?}", a.status));
    let _ = &f.parent_txid;
    Reading {
        row,
        request,
        inputs,
        own_output_spendable,
        taken_after_abort: history.contains("takenAfterAbort"),
        take_not_counted: history.contains("takeNotCounted"),
        balance,
        listed,
    }
}

/// The cell's rule; `None` when it holds, else what is wrong.
fn judge(act: Act, cause: Cause, got: &Reading) -> Option<String> {
    if got.half() {
        return Some(format!(
            "{act:?} {cause:?}: the half state, the request unmined beside a failed row with an input released: {got:?}"
        ));
    }
    let want_inputs = |zero: &str| vec![(false, zero.to_string()), (false, "own".to_string())];
    let ok = match cause {
        Cause::AbortFree | Cause::AbortSpentByOther => {
            let zero = if cause == Cause::AbortFree {
                "own"
            } else {
                "other"
            };
            got.row.as_deref() == Some("unproven")
                && got.request.as_deref() == Some("unmined")
                && got.inputs == want_inputs(zero)
                && got.own_output_spendable == Some(true)
                && got.taken_after_abort
                && got.balance == 500
        }
        // The guard holds: the proven spender's proof stands.
        Cause::AbortSpentByProven => {
            got.row.as_deref() == Some("failed")
                && got.request.as_deref() == Some("invalid")
                && got.inputs[0] == (false, "other".to_string())
                && !got.taken_after_abort
        }
        // The retire path's write after its refused promotion writes its own
        // `invalid` over the verdict's word, as it did.
        Cause::Competitor => {
            got.row.as_deref() == Some("failed")
                && got.request.as_deref()
                    == Some(if act == Act::Retire {
                        "invalid"
                    } else {
                        "doubleSpend"
                    })
                && got.inputs == want_inputs("own")
                && !got.taken_after_abort
        }
        // No mark: a take does not lift the row (its inputs stay locked);
        // the unfail task's recovery on chain evidence does, as it did.
        Cause::Refusal => {
            let lifted = act == Act::Unfail;
            let request = match act {
                Act::SendWaiting => "invalid",
                _ => "unmined",
            };
            got.row.as_deref() == Some(if lifted { "unproven" } else { "failed" })
                && got.request.as_deref() == Some(request)
                && got.inputs == want_inputs("own")
                && !got.taken_after_abort
        }
        Cause::Received => {
            let lifted = act == Act::Unfail;
            let request = match act {
                Act::SendWaiting => "invalid",
                _ => "unmined",
            };
            got.row.as_deref() == Some(if lifted { "unproven" } else { "failed" })
                && got.request.as_deref() == Some(request)
                && got.inputs.iter().all(|(s, w)| !*s && w == "none")
                && !got.taken_after_abort
        }
    };
    (!ok).then(|| format!("{act:?} {cause:?}: {got:?}"))
}

async fn matrix(act: Act) {
    let mut wrong = Vec::new();
    for cause in CAUSES {
        let got = cell(act, cause).await;
        println!(
            "MATRIX {act:?} {cause:?} -> row {:?} request {:?} inputs {:?} own output {:?} takenAfterAbort {} takeNotCounted {} balance {} listActions {:?}",
            got.row,
            got.request,
            got.inputs,
            got.own_output_spendable,
            got.taken_after_abort,
            got.take_not_counted,
            got.balance,
            got.listed
        );
        if let Some(w) = judge(act, cause, &got) {
            wrong.push(w);
        }
    }
    assert!(wrong.is_empty(), "{act:?}:\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn a_take_at_the_door_after_the_hosts_abort_recovers_it() {
    matrix(Act::Door).await;
}

#[tokio::test]
async fn a_take_heard_by_send_waiting_after_the_hosts_abort_recovers_it() {
    matrix(Act::SendWaiting).await;
}

#[tokio::test]
async fn a_take_heard_by_the_retire_path_after_the_hosts_abort_recovers_it() {
    matrix(Act::Retire).await;
}

#[tokio::test]
async fn a_take_heard_by_unfail_after_the_hosts_abort_recovers_it() {
    matrix(Act::Unfail).await;
}

#[tokio::test]
async fn a_take_of_the_wallets_own_post_after_the_hosts_abort_recovers_it() {
    matrix(Act::OwnPost).await;
}

/// The recovered action reads `unproven` to the host, and a second take
/// recovers nothing twice.
#[tokio::test]
async fn the_host_reads_its_abort_undone_and_why() {
    let got = cell(Act::OwnPost, Cause::AbortFree).await;
    assert_eq!(got.listed.as_deref(), Some("Unproven"), "{got:?}");
    assert!(got.taken_after_abort, "{got:?}");
}

#[test]
fn the_matrix_names_every_act_and_cause() {
    assert_eq!(ACTS.len() * CAUSES.len(), 30);
}
