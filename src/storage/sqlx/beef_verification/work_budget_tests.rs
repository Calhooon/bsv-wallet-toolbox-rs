//! The work budget (bsv-rs #472, the keeper's word on #487): 2^31 units per
//! input at the doors and the signer, and 2^31 per transaction at the doors,
//! summed over the inputs. The witnesses lower both through the test seam
//! (`live_word::WORK_OVERRIDE`); no test could exhaust 2^31.

use super::*;
use crate::storage::sqlx::beef_verification::live_word::WORK_OVERRIDE;
use bsv_rs::script::NotJudgedCause;
use bsv_rs::transaction::{MerklePath, MockChainTracker};

/// Runs `f` under the budgets `(per input, per transaction)`.
fn with_work<T>(budgets: (u64, u64), f: impl FnOnce() -> T) -> T {
    WORK_OVERRIDE.with(|cell| cell.set(Some(budgets)));
    let out = f();
    WORK_OVERRIDE.with(|cell| cell.set(None));
    out
}

/// A P2PKH coin with `count` outputs, proven at Genesis, and a spend of all
/// of them, each input signed ALL|FORKID.
fn p2pkh_spends(count: u32) -> (Transaction, Transaction) {
    let key = PrivateKey::random();
    let hash = bsv_rs::primitives::hash160(&key.public_key().to_compressed());
    let lock = [&[0x76, 0xa9, 0x14][..], &hash[..], &[0x88, 0xac][..]].concat();
    let mut parent = source(&lock);
    parent.outputs = vec![parent.outputs[0].clone(); count as usize];
    let mut child = spend(&parent, 0, None);
    for vout in 1..count {
        child.inputs.push(TransactionInput::new(parent.id(), vout));
    }
    let unsigned = child.to_binary();
    for vin in 0..count as usize {
        let script = P2PKH::unlock(&key, SignOutputs::All, false)
            .sign(&SigningContext::new(
                &unsigned,
                vin,
                SATOSHIS,
                &Script::from_binary(&lock).unwrap(),
            ))
            .unwrap();
        child.inputs[vin].unlocking_script = Some(script);
    }
    child.invalidate_caches();
    (parent, child)
}

/// The live door's word pass over `child` with its proven `parent`.
fn word_pass(parent: &Transaction, child: &Transaction) -> Result<()> {
    let bump = MerklePath::from_coinbase_txid(&parent.id(), GENESIS);
    let mut beef = Beef::new();
    let index = beef.merge_bump(bump);
    beef.merge_raw_tx(parent.to_binary(), Some(index));
    beef.merge_raw_tx(child.to_binary(), None);
    let headers = crate::storage::sqlx::beef_verification::VerificationHeaders {
        tip: Some(tip()),
        roots: beef_roots(&beef).into_iter().collect(),
    };
    crate::storage::sqlx::beef_verification::live_word::verify(
        &beef.to_binary(),
        &headers,
        "main",
        None,
    )
}

/// The door's whole verification, the default-mode stream pass first.
async fn door(parent: &Transaction, child: &Transaction) -> Result<()> {
    let bump = MerklePath::from_coinbase_txid(&parent.id(), GENESIS);
    let mut tracker = MockChainTracker::new(tip().height);
    tracker.add_root(GENESIS, bump.compute_root(None).unwrap());
    let mut beef = Beef::new();
    let index = beef.merge_bump(bump);
    beef.merge_raw_tx(parent.to_binary(), Some(index));
    beef.merge_raw_tx(child.to_binary(), None);
    crate::storage::sqlx::beef_verification::verify_beef_spends_with_verifier(
        &beef.to_binary(),
        &beef_roots(&beef),
        &tracker,
        "main",
        None,
    )
    .await
}

/// The cause of a not-judged refusal at `input`.
fn work_cause(result: Result<()>, input: u32) -> NotJudgedCause {
    match result.unwrap_err() {
        Error::SpendRefused {
            input: Some(at),
            why:
                SpendRefusal::NotJudged {
                    input: index,
                    cause,
                },
            ..
        } if at == input && index == input => {
            eprintln!("input {input} not judged: {cause}");
            cause
        }
        other => panic!("expected input {input} not judged, got {other:?}"),
    }
}

/// The signer SKIPS an input past its budget (#461 as amended): a fork-ID-less
/// signature it refuses under the budget passes when the budget stops first.
#[test]
fn the_signer_skips_an_input_past_its_work_budget() {
    let (parent, child) = p2pkh_spend(0x01);
    assert!(matches!(
        judge(&child, &parent, Some(GENESIS)),
        Err(Error::SpendRefused {
            why: SpendRefusal::Script(_),
            ..
        })
    ));
    with_work((3, 3), || judge(&child, &parent, Some(GENESIS))).unwrap();
}

/// The signer keeps the per-input bound alone: two inputs whose sum passes a
/// per-transaction bound are both judged and pass.
#[test]
fn the_signer_has_no_transaction_total() {
    let (parent, child) = p2pkh_spends(2);
    with_work((u64::MAX, 600), || judge(&child, &parent, Some(GENESIS))).unwrap();
}

/// The doors REFUSE the same input, not judged, never invalid.
#[test]
fn the_word_pass_refuses_an_input_past_its_work_budget() {
    let (parent, child) = p2pkh_spends(1);
    word_pass(&parent, &child).unwrap();
    let cause = work_cause(with_work((3, u64::MAX), || word_pass(&parent, &child)), 0);
    assert!(matches!(
        cause,
        NotJudgedCause::WorkBudgetExceeded {
            input: 0,
            budget: 3,
            work_done
        } if work_done <= 3
    ));
}

/// The door's default-mode stream pass carries the per-input budget too
/// (bsv-rs sets none by default), so it is bounded before the word pass.
#[tokio::test]
async fn the_door_stream_pass_carries_the_per_input_budget() {
    let (parent, child) = p2pkh_spends(1);
    door(&parent, &child).await.unwrap();
    WORK_OVERRIDE.with(|cell| cell.set(Some((3, u64::MAX))));
    let result = door(&parent, &child).await;
    WORK_OVERRIDE.with(|cell| cell.set(None));
    assert!(matches!(
        work_cause(result, 0),
        NotJudgedCause::WorkBudgetExceeded {
            input: 0,
            budget: 3,
            ..
        }
    ));
}

/// The doors bound the transaction's total: the input that carries the sum
/// past it is not judged, the total named as the budget, the work done the
/// sum over both inputs. A P2PKH input spends a few hundred units (#487's
/// corpus: 546 at most), so 600 admits one and not two.
#[test]
fn the_word_pass_bounds_the_transactions_total_work() {
    let (parent, child) = p2pkh_spends(2);
    with_work((u64::MAX, 1_200), || word_pass(&parent, &child)).unwrap();
    let cause = work_cause(with_work((u64::MAX, 600), || word_pass(&parent, &child)), 1);
    match cause {
        NotJudgedCause::WorkBudgetExceeded {
            input: 1,
            budget: 600,
            work_done,
        } => assert!((300..=600).contains(&work_done), "{work_done}"),
        other => panic!("expected the total's refusal, got {other:?}"),
    }
}
