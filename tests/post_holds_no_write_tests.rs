//! No store write is held across a post, on any road (the host's ask).
//!
//! A host wraps the services object it hands to every `Wallet` and
//! `Monitor`; its wrapper writes a row of its own table in the same SQLite
//! file, through the store's one writer, before it passes each `post_beef`
//! on. That is sound only if the crate holds neither the writer connection
//! nor a write transaction across the call. Each road below is driven
//! through its real door or task with such a wrapper (`HostServices`): its
//! `post_beef` takes `write_tx()` on a clone of the same `StorageSqlx` (one
//! store a file a process), writes and commits a row of `host_posts`, and
//! records how long the acquire took. The witness (`witness`) asserts the
//! post was reached, the row is there, the acquire did not wait (well
//! under `STORE_BUSY_CEILING_MS`), and the door's answer and rows read as
//! the same road's run with the wrapper disarmed. One in-memory store per
//! run, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;
#[path = "common/sendwith_seed.rs"]
mod seed;

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{ChainTracker, Transaction};
use bsv_rs::wallet::{
    CreateActionArgs, CreateActionOptions, CreateActionOutput, SignActionArgs, WalletInterface,
};
use bsv_wallet_toolbox_rs::monitor::reask_lost_proof;
use bsv_wallet_toolbox_rs::monitor::tasks::SendWaitingTask;
use bsv_wallet_toolbox_rs::services::mock::{MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::services::{
    BlockHeader, BoundSpender, BroadcastStatus, FiatCurrency, GetBeefResult, GetMerklePathResult,
    GetRawTxResult, GetUtxoStatusOutputFormat, GetUtxoStatusResult, NLockTimeInput, PostBeefResult,
    UtxoLeads, UtxoVerdict, WalletServices,
};
use bsv_wallet_toolbox_rs::storage::sqlx::STORE_BUSY_CEILING_MS;
use bsv_wallet_toolbox_rs::storage::StorageProcessActionArgs;
use bsv_wallet_toolbox_rs::{
    GetStatusForTxidsResult, MonitorTask, Result, StorageSqlx, Wallet, WalletStorageWriter,
};

const ORIGIN: &str = "test.local";
const SIK: &str = "2222222222222222222222222222222222222222222222222222222222222222";

/// The bound on the wrapper's acquire: a tenth of the store's busy ceiling.
/// A door holding the writer across its post makes the acquire wait the
/// whole ceiling and fail `StoreBusy`.
const NO_WAIT: Duration = Duration::from_millis(STORE_BUSY_CEILING_MS / 10);

/// One post the wrapper saw: how long its write took to acquire the
/// writer, and whether its row committed.
#[derive(Debug, Clone)]
struct Hold {
    acquire: Duration,
    written: std::result::Result<(), String>,
}

/// What the wrapper shares with the test.
struct HostState {
    store: OnceLock<StorageSqlx>,
    armed: bool,
    holds: Mutex<Vec<Hold>>,
}

impl HostState {
    fn take(&self) -> Vec<Hold> {
        std::mem::take(&mut self.holds.lock().unwrap())
    }
}

/// A host's services object: before it passes a post on it writes a row of
/// its own table through the store's writer.
struct HostServices {
    inner: MockWalletServices,
    state: Arc<HostState>,
}

impl HostServices {
    async fn write_before_post(&self, txids: &[String]) {
        let Some(store) = self.state.store.get().filter(|_| self.state.armed) else {
            return;
        };
        let started = Instant::now();
        let tx = store.write_tx().await;
        let acquire = started.elapsed();
        let written = match tx {
            Ok(mut tx) => {
                let wrote = ::sqlx::query("INSERT INTO host_posts (txids) VALUES (?)")
                    .bind(txids.join(","))
                    .execute(&mut *tx)
                    .await;
                match wrote {
                    Ok(_) => tx.commit().await.map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                }
            }
            Err(e) => Err(e.to_string()),
        };
        self.state
            .holds
            .lock()
            .unwrap()
            .push(Hold { acquire, written });
    }
}

#[async_trait]
impl WalletServices for HostServices {
    async fn get_chain_tracker(&self) -> Result<&dyn ChainTracker> {
        self.inner.get_chain_tracker().await
    }
    async fn get_height(&self) -> Result<u32> {
        self.inner.get_height().await
    }
    async fn get_header_for_height(&self, height: u32) -> Result<Vec<u8>> {
        self.inner.get_header_for_height(height).await
    }
    async fn get_chain_tip_header(&self) -> Result<BlockHeader> {
        self.inner.get_chain_tip_header().await
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
        self.write_before_post(txids).await;
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
    async fn is_utxo_with_spender(
        &self,
        txid: &str,
        vout: u32,
        locking_script: &[u8],
    ) -> (UtxoVerdict, Option<BoundSpender>) {
        self.inner
            .is_utxo_with_spender(txid, vout, locking_script)
            .await
    }
    async fn get_utxo_leads(
        &self,
        script_hash: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
    ) -> Result<UtxoLeads> {
        self.inner.get_utxo_leads(script_hash, output_format).await
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
    async fn get_broadcaster_statuses(&self, txid: &str) -> Vec<(String, BroadcastStatus)> {
        self.inner.get_broadcaster_statuses(txid).await
    }
}

type W = Wallet<StorageSqlx, HostServices>;

/// One run of a road: a wallet over a fresh store whose services are the
/// host's wrapper (armed or not), funded by three proven coins of its own.
struct Scene {
    store: StorageSqlx,
    wallet: W,
    host: Arc<HostState>,
}

async fn scene(armed: bool, mock: MockWalletServices) -> Scene {
    scene_of(armed, mock, PrivateKey::random()).await
}

async fn scene_of(armed: bool, mock: MockWalletServices, root: PrivateKey) -> Scene {
    let store = StorageSqlx::in_memory().await.unwrap();
    store.migrate("post-holds-no-write", SIK).await.unwrap();
    store.make_available().await.unwrap();
    ::sqlx::query("CREATE TABLE host_posts (id INTEGER PRIMARY KEY, txids TEXT NOT NULL)")
        .execute(store.writer())
        .await
        .unwrap();
    let host = Arc::new(HostState {
        store: OnceLock::new(),
        armed,
        holds: Mutex::new(Vec::new()),
    });
    // The host's handle: a clone of the one store, never a second open.
    host.store.set(store.clone()).ok().unwrap();
    let services = HostServices {
        inner: mock,
        state: host.clone(),
    };
    let wallet = Wallet::new(Some(root.clone()), store.clone(), services)
        .await
        .unwrap();
    let (user, _) = store
        .find_or_insert_user(&root.public_key().to_hex())
        .await
        .unwrap();
    seed::seed_coins(&store, &root, user.user_id, 3, 9500).await;
    ::sqlx::query(
        "UPDATE output_baskets SET number_of_desired_utxos = 1, minimum_desired_utxo_value = 1000",
    )
    .execute(store.writer())
    .await
    .unwrap();
    Scene {
        store,
        wallet,
        host,
    }
}

fn accepting() -> MockWalletServices {
    MockWalletServices::builder().post_beef_success().build()
}

/// A payment of 1000 to a key of neither party, signed now or deferred,
/// sent now, queued or not sent.
fn pay(sign_now: bool, delayed: bool, no_send: bool) -> CreateActionArgs {
    CreateActionArgs {
        description: "a payment from the wallet".to_string(),
        input_beef: None,
        inputs: None,
        outputs: Some(vec![CreateActionOutput {
            locking_script: common::other_lock().to_binary(),
            satoshis: 1000,
            output_description: "a payment".to_string(),
            basket: None,
            custom_instructions: None,
            tags: None,
        }]),
        lock_time: None,
        version: None,
        labels: None,
        options: Some(CreateActionOptions {
            sign_and_process: Some(sign_now),
            accept_delayed_broadcast: Some(delayed),
            randomize_outputs: Some(false),
            no_send: Some(no_send),
            ..Default::default()
        }),
    }
}

/// Creates and signs a payment, returning its txid.
async fn created(s: &Scene, delayed: bool, no_send: bool) -> String {
    let made = s
        .wallet
        .create_action(pay(true, delayed, no_send), Some(ORIGIN))
        .await
        .expect("the action");
    Transaction::from_binary(&made.tx.expect("the signed transaction"))
        .unwrap()
        .id()
}

/// The words the store holds for `txid`: its transaction rows and its
/// request. Txid-free, so two runs of a road compare.
async fn words(store: &StorageSqlx, txid: &str) -> String {
    let tx: Vec<(String,)> =
        ::sqlx::query_as("SELECT status FROM transactions WHERE txid = ? ORDER BY transaction_id")
            .bind(txid)
            .fetch_all(store.pool())
            .await
            .unwrap();
    let req: Vec<(String, i64)> =
        ::sqlx::query_as("SELECT status, attempts FROM proven_tx_reqs WHERE txid = ?")
            .bind(txid)
            .fetch_all(store.pool())
            .await
            .unwrap();
    format!("tx {tx:?} req {req:?}")
}

fn send_words(results: Option<Vec<bsv_rs::wallet::SendWithResult>>) -> String {
    let words: Vec<String> = results
        .unwrap_or_default()
        .into_iter()
        .map(|r| format!("{:?}", r.status))
        .collect();
    words.join(",")
}

/// THE WITNESS, one for every road: `run` drives the road once with the
/// wrapper disarmed and once armed, each on a fresh store, and answers the
/// door's reading. Armed, every post the road made reached the wrapper,
/// whose write took the writer without waiting and committed its row; and
/// the reading is the disarmed run's.
async fn witness<F, Fut>(road: &str, posts: usize, run: F)
where
    F: Fn(bool) -> Fut,
    Fut: std::future::Future<Output = (Scene, String)>,
{
    let (_, plain) = run(false).await;
    let (s, held) = run(true).await;
    let holds = s.host.take();
    assert_eq!(holds.len(), posts, "{road}: the posts the wrapper saw");
    for hold in &holds {
        assert!(
            hold.written.is_ok(),
            "{road}: the host's write beside the post: {:?}",
            hold.written
        );
        assert!(
            hold.acquire < NO_WAIT,
            "{road}: the host's write waited {:?} for the writer (bound {NO_WAIT:?})",
            hold.acquire
        );
    }
    let (rows,): (i64,) = ::sqlx::query_as("SELECT COUNT(*) FROM host_posts")
        .fetch_one(s.store.pool())
        .await
        .unwrap();
    assert_eq!(
        usize::try_from(rows).ok(),
        Some(posts),
        "{road}: the host's rows"
    );
    assert_eq!(held, plain, "{road}: the door's answer and rows");
    eprintln!(
        "{road}: {} post(s), acquire {:?}; {held}",
        holds.len(),
        holds.iter().map(|h| h.acquire).collect::<Vec<_>>()
    );
}

/// `createAction`'s post after its one commit (`Wallet::create_action_at`).
#[tokio::test]
async fn create_action_post_holds_no_write() {
    witness("createAction", 1, |armed| async move {
        let s = scene(armed, accepting()).await;
        let made = s
            .wallet
            .create_action(pay(true, false, false), Some(ORIGIN))
            .await
            .expect("the action");
        let txid = Transaction::from_binary(made.tx.as_ref().unwrap())
            .unwrap()
            .id();
        let reading = format!(
            "{} sendWith [{}]",
            words(&s.store, &txid).await,
            send_words(made.send_with_results)
        );
        (s, reading)
    })
    .await;
}

/// `signAction`'s post after `process_action_internal`'s commit.
#[tokio::test]
async fn sign_action_post_holds_no_write() {
    witness("signAction", 1, |armed| async move {
        let s = scene(armed, accepting()).await;
        let made = s
            .wallet
            .create_action(pay(false, false, false), Some(ORIGIN))
            .await
            .expect("a signable transaction");
        let reference = made.signable_transaction.expect("signable").reference;
        let signed = s
            .wallet
            .sign_action(
                SignActionArgs {
                    spends: std::collections::HashMap::default(),
                    reference: bsv_rs::primitives::to_base64(&reference),
                    options: None,
                },
                Some(ORIGIN),
            )
            .await
            .expect("signed");
        let txid = hex::encode(signed.txid.expect("the txid"));
        let reading = format!(
            "{} sendWith [{}]",
            words(&s.store, &txid).await,
            send_words(signed.send_with_results)
        );
        (s, reading)
    })
    .await;
}

/// `internalizeAction`'s post of a new body with no proof, after the
/// door's commit (`broadcast_new_internalized_req`).
#[tokio::test]
async fn internalize_action_post_holds_no_write() {
    witness("internalizeAction", 1, |armed| async move {
        let sender = PrivateKey::random().public_key();
        let root = PrivateKey::random();
        let key = common::derived_key(&root, &sender, "cHJl", "c3Vm");
        let (beef, txid) =
            common::beef_paying(LockingScript::from_binary(&common::p2pkh_bytes(&key)).unwrap());
        let mock = MockWalletServices::builder()
            .post_beef_response(MockResponse::Success(vec![common::accepted(&txid)]))
            .build();
        let s = scene_of(armed, mock, root).await;
        let answer = s
            .wallet
            .internalize_action(
                common::args(beef, common::payment(&sender, "cHJl", "c3Vm")),
                Some(ORIGIN),
            )
            .await;
        let reading = format!(
            "{} answer {}",
            words(&s.store, &txid).await,
            common::read_answer(answer)
        );
        (s, reading)
    })
    .await;
}

/// The stored-action door (`Wallet::process_action`, the store's
/// `post_stored_actions`): a `noSend` action named in `sendWith` alone.
#[tokio::test]
async fn stored_action_post_holds_no_write() {
    witness("processAction stored", 1, |armed| async move {
        let s = scene(armed, accepting()).await;
        let txid = created(&s, false, true).await;
        let posted = s
            .wallet
            .process_action(
                StorageProcessActionArgs {
                    is_new_tx: false,
                    is_send_with: true,
                    is_no_send: false,
                    is_delayed: false,
                    reference: None,
                    txid: None,
                    raw_tx: None,
                    send_with: vec![txid.clone()],
                },
                Some(ORIGIN),
            )
            .await
            .expect("the stored post");
        let sent: Vec<String> = posted
            .send_with_results
            .unwrap_or_default()
            .into_iter()
            .map(|r| format!("{:?}", r.status))
            .collect();
        let reading = format!("{} sendWith {sent:?}", words(&s.store, &txid).await);
        (s, reading)
    })
    .await;
}

/// The monitor's send-waiting pass (`SendWaitingTask`, the store's
/// `send_waiting_transactions`): a queued action posted by the task.
#[tokio::test]
async fn send_waiting_post_holds_no_write() {
    witness("sendWaiting", 1, |armed| async move {
        let s = scene(armed, accepting()).await;
        let txid = created(&s, true, false).await;
        // Queued an hour ago: past the task's age and re-ask pause.
        let hour_ago = chrono::Utc::now() - chrono::Duration::hours(1);
        ::sqlx::query("UPDATE proven_tx_reqs SET created_at = ?, updated_at = ? WHERE txid = ?")
            .bind(hour_ago)
            .bind(hour_ago)
            .bind(&txid)
            .execute(s.store.writer())
            .await
            .unwrap();
        let task = SendWaitingTask::new(
            Arc::new(s.store.clone()),
            Arc::new(MockWalletServices::builder().build()),
        );
        let ran = task.run().await.expect("the pass");
        let reading = format!(
            "{} processed {} errors {:?}",
            words(&s.store, &txid).await,
            ran.items_processed,
            ran.errors
        );
        (s, reading)
    })
    .await;
}

/// The lost-proof announce (`reask_lost_proof`, which the reorg task's
/// `reprove_moved_anchor` and a host's re-proof through `reprove_anchor`
/// reach; the store's `announce_lost_proof`): no broadcaster knows the
/// transaction, so it is announced once.
#[tokio::test]
async fn lost_proof_announce_holds_no_write() {
    witness("lostProofAnnounce", 1, |armed| async move {
        let mock = MockWalletServices::builder()
            .post_beef_success()
            .broadcaster_statuses(vec![("arc".to_string(), BroadcastStatus::Unknown)])
            .build();
        let s = scene(armed, mock).await;
        let txid = created(&s, true, false).await;
        let reask = reask_lost_proof(&s.store, s.wallet.services(), &txid).await;
        let reading = format!("{} reask {reask:?}", words(&s.store, &txid).await);
        (s, reading)
    })
    .await;
}

/// The ceiling the bound is a fraction of is the store's writer wait.
#[test]
fn the_bound_is_a_tenth_of_the_store_busy_ceiling() {
    assert_eq!(STORE_BUSY_CEILING_MS, 5000);
    assert_eq!(NO_WAIT, Duration::from_millis(500));
}
