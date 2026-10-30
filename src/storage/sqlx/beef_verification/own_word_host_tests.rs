//! Host-verifier witnesses, using the own-word fixtures and the live door.

use super::*;
use crate::{SpendRequest, SpendVerificationContext, SpendVerifier};
use std::sync::Mutex;

type Item = (SpendRequest, SpendVerificationContext);

struct RecordingVerifier {
    verdict: bool,
    skip: Option<usize>,
    considered: Mutex<Vec<usize>>,
    verified: Mutex<Vec<Item>>,
}

impl RecordingVerifier {
    fn new(verdict: bool) -> Self {
        Self {
            verdict,
            skip: None,
            considered: Mutex::new(Vec::new()),
            verified: Mutex::new(Vec::new()),
        }
    }
}

impl SpendVerifier for RecordingVerifier {
    fn verify_spend(
        &self,
        spend: &SpendRequest,
        context: &SpendVerificationContext,
    ) -> Result<bool> {
        self.verified
            .lock()
            .unwrap()
            .push((spend.clone(), *context));
        Ok(self.verdict)
    }

    fn should_verify_spend(&self, spend: &SpendRequest, _: &SpendVerificationContext) -> bool {
        self.considered.lock().unwrap().push(spend.vin);
        self.skip != Some(spend.vin)
    }
}

struct BatchVerifier {
    single: RecordingVerifier,
    batches: Mutex<Vec<Vec<Item>>>,
    verdicts: Result<Vec<bool>>,
}

impl SpendVerifier for BatchVerifier {
    fn verify_spend(&self, _: &SpendRequest, _: &SpendVerificationContext) -> Result<bool> {
        panic!("batch override must receive the selected inputs")
    }

    fn should_verify_spend(
        &self,
        spend: &SpendRequest,
        context: &SpendVerificationContext,
    ) -> bool {
        self.single.should_verify_spend(spend, context)
    }

    fn verify_spends_batch(&self, items: &[Item]) -> Result<Vec<bool>> {
        self.batches.lock().unwrap().push(items.to_vec());
        match &self.verdicts {
            Ok(verdicts) => Ok(verdicts.clone()),
            Err(_) => Err(Error::Internal("test backend unavailable".into())),
        }
    }
}

fn host_judge(
    child: &Transaction,
    parent: &Transaction,
    proven_height: Option<u64>,
    verifier: &dyn SpendVerifier,
) -> Result<()> {
    let sources = HashMap::from([(
        parent.id(),
        SourceTx {
            raw: parent.to_binary(),
            proven_height,
        },
    )]);
    verify_own(
        &child.to_binary(),
        &sources,
        Some(tip()),
        "main",
        Some(verifier),
    )
}

fn three_inputs() -> (Transaction, Transaction) {
    let mut parent = source(&[0x51]);
    parent.outputs = vec![parent.outputs[0].clone(); 3];
    let mut child = spend(&parent, 0, Some(&[]));
    for vout in 1..3 {
        let mut input = TransactionInput::new(parent.id(), vout);
        input.unlocking_script = Some(UnlockingScript::from_binary(&[]).unwrap());
        child.inputs.push(input);
    }
    (parent, child)
}

fn batch(verdicts: Result<Vec<bool>>) -> BatchVerifier {
    BatchVerifier {
        single: RecordingVerifier {
            skip: Some(1),
            ..RecordingVerifier::new(true)
        },
        batches: Mutex::new(Vec::new()),
        verdicts,
    }
}

#[test]
fn host_true_receives_the_input_and_both_words() {
    let (parent, child) = p2pkh_spend(0x01);
    let host = RecordingVerifier::new(true);
    host_judge(&child, &parent, Some(u64::from(GENESIS)), &host).unwrap();
    assert!(host.is_ready());
    let calls = host.verified.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let (spend, context) = &calls[0];
    assert_eq!(display_hex(&spend.txid), child.id());
    assert_eq!(spend.body.raw, child.to_binary());
    assert_eq!(spend.vin, 0);
    assert_eq!(spend.satoshis, SATOSHIS);
    assert_eq!(&*spend.script, parent.outputs[0].locking_script.to_binary());
    assert_eq!(context.proven_height, Some(u64::from(GENESIS)));
    assert_eq!(
        context.word,
        bsv_rs::script::ScriptFlags::standard(ProtocolEra::PostChronicle)
            .without(bsv_rs::script::ScriptFlags::UTXO_AFTER_CHRONICLE)
    );
    eprintln!("host true: input 0, spend PostChronicle, coin PostGenesis, height {GENESIS}");
}

#[test]
fn host_false_refuses_an_interpreter_valid_spend() {
    let parent = source(&[0x51]);
    let child = spend(&parent, 0, Some(&[]));
    judge(&child, &parent, None).unwrap();
    let error = host_judge(&child, &parent, None, &RecordingVerifier::new(false)).unwrap_err();
    let expected = format!(
        "spend refused at offset 5, input 0 of {}: host script verifier refused the spend",
        child.id()
    );
    assert_eq!(error.to_string(), expected);
    eprintln!("{error}");
    let boundary = bsv_rs::Error::from(error);
    assert!(matches!(
        boundary,
        bsv_rs::Error::WalletRefusal {
            class: bsv_rs::WalletRefusalClass::InvalidParameter,
            ..
        }
    ));
    assert_eq!(boundary.to_string(), expected);
}

#[test]
fn host_unselected_input_keeps_the_interpreter_verdict() {
    let host = RecordingVerifier {
        skip: Some(0),
        ..RecordingVerifier::new(true)
    };
    let (parent, child) = p2pkh_spend(0x01);
    let expected = judge(&child, &parent, Some(GENESIS))
        .unwrap_err()
        .to_string();
    let actual = host_judge(&child, &parent, Some(u64::from(GENESIS)), &host).unwrap_err();
    assert_eq!(actual.to_string(), expected);
    assert!(host.verified.lock().unwrap().is_empty());
    let parent = source(&[0x51]);
    host_judge(&spend(&parent, 0, Some(&[])), &parent, None, &host).unwrap();
    assert!(host.verified.lock().unwrap().is_empty());
}

#[test]
fn host_batch_gets_selected_inputs_in_one_call() {
    let (parent, child) = three_inputs();
    let host = batch(Ok(vec![true, true]));
    host_judge(&child, &parent, None, &host).unwrap();
    assert_eq!(*host.single.considered.lock().unwrap(), vec![0, 1, 2]);
    let batches = host.batches.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .iter()
            .map(|(spend, _)| spend.vin)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert!(batches[0]
        .iter()
        .all(|(_, context)| context.proven_height.is_none()));
    assert!(Arc::ptr_eq(&batches[0][0].0.body, &batches[0][1].0.body));
    eprintln!("host batch: one call, selected inputs [0, 2], input 1 interpreted");
}

#[test]
fn host_batch_wrong_count_refuses_the_whole_batch() {
    let (parent, child) = three_inputs();
    for verdicts in [vec![true], vec![true, true, true]] {
        let host = batch(Ok(verdicts));
        let error = host_judge(&child, &parent, None, &host).unwrap_err();
        assert!(error.to_string().contains("host script verifier returned"));
        assert!(matches!(error, Error::SpendRefused { input: Some(0), .. }));
        eprintln!("{error}");
    }
}

#[test]
fn host_batch_false_maps_to_the_selected_input() {
    let (parent, child) = three_inputs();
    let error = host_judge(&child, &parent, None, &batch(Ok(vec![true, false]))).unwrap_err();
    assert!(matches!(error, Error::SpendRefused { input: Some(2), .. }));
}

#[test]
fn host_batch_error_refuses_the_whole_batch() {
    let (parent, child) = three_inputs();
    let host = batch(Err(Error::Internal("test backend unavailable".into())));
    let error = host_judge(&child, &parent, None, &host).unwrap_err();
    assert!(matches!(error, Error::SpendRefused { input: Some(0), .. }));
    assert!(error
        .to_string()
        .contains("host script verifier failed: Internal error: test backend unavailable"));
}

#[test]
fn host_receives_no_input_when_own_word_skips_it() {
    let (parent, child) = p2pkh_spend(0x01);
    let host = RecordingVerifier::new(false);
    verify_own(
        &child.to_binary(),
        &HashMap::new(),
        Some(tip()),
        "main",
        Some(&host),
    )
    .unwrap();
    host_judge(&child, &parent, Some(u64::from(GENESIS - 1)), &host).unwrap();
    assert!(host.considered.lock().unwrap().is_empty());
}

#[test]
fn host_options_default_to_none_and_clone_the_same_backend() {
    use crate::storage::sqlx::StorageSqlxOptions;
    assert!(crate::WalletOptions::default().script_verifier.is_none());
    assert!(StorageSqlxOptions::default().script_verifier.is_none());
    let options = StorageSqlxOptions {
        script_verifier: Some(Arc::new(RecordingVerifier::new(true))),
        ..Default::default()
    };
    assert!(Arc::ptr_eq(
        options.script_verifier.as_ref().unwrap(),
        options.clone().script_verifier.as_ref().unwrap()
    ));
}

fn proven_beef() -> (Transaction, Beef, bsv_rs::transaction::MockChainTracker) {
    use bsv_rs::transaction::{MerklePath, MockChainTracker};
    let parent = source(&[0x51]);
    let child = spend(&parent, 0, Some(&[]));
    let bump = MerklePath::from_coinbase_txid(&parent.id(), GENESIS);
    let mut tracker = MockChainTracker::new(tip().height);
    tracker.add_root(GENESIS, bump.compute_root(None).unwrap());
    let mut beef = Beef::new();
    let index = beef.merge_bump(bump);
    beef.merge_raw_tx(parent.to_binary(), Some(index));
    beef.merge_raw_tx(child.to_binary(), None);
    (child, beef, tracker)
}

async fn host_store(host: Arc<dyn SpendVerifier>) -> (crate::StorageSqlx, i64) {
    use crate::storage::WalletStorageWriter;
    let store = crate::StorageSqlx::new_with_options(
        "sqlite::memory:",
        crate::storage::sqlx::StorageSqlxOptions {
            script_verifier: Some(host),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    store
        .migrate("host-verifier", "host-verifier-test-key")
        .await
        .unwrap();
    store.make_available().await.unwrap();
    let (user, _) = store
        .find_or_insert_user("host-verifier-test-user")
        .await
        .unwrap();
    (store, user.user_id)
}

#[tokio::test]
async fn host_storage_internalize_refuses_before_writes_through_a_clone() {
    use crate::storage::sqlx::internalize_action::internalize_action_internal;
    let host = Arc::new(RecordingVerifier::new(false));
    let (storage, user_id) = host_store(host.clone()).await;
    let (child, mut beef, tracker) = proven_beef();
    storage.set_chain_tracker(Arc::new(tracker)).await;
    let args = serde_json::from_value(serde_json::json!({
        "tx": beef.to_binary_atomic(&child.id()).unwrap(),
        "description": "host verifier fixture",
        "outputs": [{"outputIndex": 0, "protocol": "basket insertion",
            "insertionRemittance": {"basket": "host-test"}}]
    }))
    .unwrap();
    let error = internalize_action_internal(&storage.clone(), user_id, args)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::SpendRefused { input: Some(0), .. }));
    assert!(error
        .to_string()
        .ends_with("host script verifier refused the spend"));
    assert_eq!(host.verified.lock().unwrap().len(), 1);
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0);
    eprintln!("internalize: host refusal, zero action rows, shared verifier through clone");
}

#[tokio::test]
async fn host_live_pass_preserves_value_refusals() {
    let (mut child, mut beef, tracker) = proven_beef();
    child.outputs[0].satoshis = Some(SATOSHIS + 1);
    child.invalidate_caches();
    // Only the proven source and this overspending child belong in the frame.
    beef.txs.pop();
    beef.merge_raw_tx(child.to_binary(), None);
    let roots = beef_roots(&beef);
    let error = super::super::super::verify_beef_spends_with_verifier(
        &beef.to_binary(),
        &roots,
        &tracker,
        "main",
        Some(&RecordingVerifier::new(true)),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        Error::SpendRefused {
            why: SpendRefusal::CreatesValue,
            ..
        }
    ));
}

fn beef_roots(beef: &Beef) -> Vec<(u64, Hash32)> {
    beef.bumps
        .iter()
        .map(|bump| {
            let root = bsv_rs::primitives::from_hex(&bump.compute_root(None).unwrap()).unwrap();
            let mut wire: [u8; 32] = root.try_into().unwrap();
            wire.reverse();
            (u64::from(bump.block_height), wire)
        })
        .collect()
}

#[tokio::test]
async fn host_live_batch_spans_all_selected_transactions() {
    let (child, mut beef, tracker) = proven_beef();
    let grandchild = spend(&child, 0, Some(&[]));
    beef.merge_raw_tx(grandchild.to_binary(), None);
    let host = batch(Ok(vec![true, true]));
    super::super::super::verify_beef_spends_with_verifier(
        &beef.to_binary(),
        &beef_roots(&beef),
        &tracker,
        "main",
        Some(&host),
    )
    .await
    .unwrap();
    let batches = host.batches.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 2);
    assert_eq!(display_hex(&batches[0][0].0.txid), child.id());
    assert_eq!(display_hex(&batches[0][1].0.txid), grandchild.id());
    assert_eq!(batches[0][0].1.proven_height, Some(u64::from(GENESIS)));
    assert_eq!(batches[0][1].1.proven_height, None);
    eprintln!("live batch: one call for two transactions, both coin contexts retained");
}

#[cfg(test)]
#[path = "work_budget_tests.rs"]
mod work_budget;
