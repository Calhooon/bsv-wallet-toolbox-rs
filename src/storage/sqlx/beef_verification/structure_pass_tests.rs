//! The door's preliminary pass reads the structure alone (the keeper's word
//! of 2026-10-30 on #493's review: structure-only, rule 22): it returns no
//! script verdict, and the one word pass judges every input under the
//! network's `ChainParams` and both work budgets. The review's two probes.

use super::*;
use bsv_rs::block::ChainParams;
use bsv_rs::transaction::beef_stream::verify_stream_two_pass_with_chain_params;
use bsv_rs::transaction::{ChainTracker, MerklePath, MockChainTracker};

/// The structure pass's verdict over `bytes`.
fn structure(bytes: &[u8], headers: &VerificationHeaders) -> Verdict {
    beef_verification::structure_pass(bytes, headers).unwrap()
}

/// Review probe 1: a stream whose input 1 carries the transaction's total
/// past 600. The structure pass returns `Valid` whatever the budgets and
/// whatever the scripts say; the word pass alone refuses input 1 at 600, and
/// an input past its own budget or with a failing script at its input.
#[tokio::test]
async fn the_structure_pass_judges_no_script_and_the_word_pass_refuses() {
    let (parent, child) = p2pkh_spends(2);
    let (bytes, _, headers) = frame(&parent, &child);
    with_work((u64::MAX, 600), || {
        assert!(matches!(structure(&bytes, &headers), Verdict::Valid { .. }));
    });
    WORK_OVERRIDE.with(|cell| cell.set(Some(budget((u64::MAX, 600)))));
    let result = door(&parent, &child).await;
    WORK_OVERRIDE.with(|cell| cell.set(None));
    assert!(matches!(
        work_cause(result, 1),
        NotJudgedCause::TransactionWorkBudgetExceeded {
            input: 1,
            limit: 600,
            ..
        }
    ));

    // Past the per-input budget: no verdict from the structure pass.
    let verdict = with_work((3, u64::MAX), || structure(&bytes, &headers));
    assert!(matches!(verdict, Verdict::Valid { .. }), "{verdict:?}");

    // A fork-ID-less signature: the structure pass runs no script.
    let (parent, child) = p2pkh_spend(0x01);
    let (bytes, _, headers) = frame(&parent, &child);
    let verdict = structure(&bytes, &headers);
    assert!(matches!(verdict, Verdict::Valid { .. }), "{verdict:?}");
    let refused = door(&parent, &child).await;
    assert!(
        matches!(
            refused,
            Err(Error::SpendRefused {
                input: Some(0),
                why: SpendRefusal::Script(_),
                ..
            })
        ),
        "{refused:?}"
    );
    eprintln!("structure pass: Valid at budget 600, at 3 and on a failing script");
}

/// Review probe 2: a mainnet spend below Genesis. bsv-rs's verifier under
/// `ChainParams` answers `NotJudged(PreGenesisSpend)`; the structure pass
/// returns `Valid` without judging, and the door answers bsv-rs's verdict,
/// for a script that passes and one that fails in the default mode.
#[tokio::test]
async fn a_pre_genesis_spend_is_not_judged_by_the_word_pass_alone() {
    const COIN: u32 = 500_000;
    const TIP: u32 = 600_000;
    for lock in [[0x51], [0x00]] {
        let parent = source(&lock);
        let child = spend(&parent, 0, Some(&[]));
        let bump = MerklePath::from_coinbase_txid(&parent.id(), COIN);
        let mut tracker = MockChainTracker::new(TIP);
        tracker.add_root(COIN, bump.compute_root(None).unwrap());
        let mut beef = Beef::new();
        let index = beef.merge_bump(bump);
        beef.merge_raw_tx(parent.to_binary(), Some(index));
        beef.merge_raw_tx(child.to_binary(), None);
        let bytes = beef.to_binary();
        let roots = beef_roots(&beef);
        let headers = VerificationHeaders {
            tip: tracker.mining_tip().await.unwrap(),
            roots: roots.iter().copied().collect(),
        };

        let sdk = verify_stream_two_pass_with_chain_params(
            std::io::Cursor::new(&bytes),
            &headers,
            None,
            &ChainParams::mainnet(),
        )
        .unwrap();
        assert!(
            matches!(
                sdk,
                Verdict::NotJudged {
                    input: 0,
                    cause: NotJudgedCause::PreGenesisSpend {
                        height: 600_001,
                        ..
                    },
                    ..
                }
            ),
            "{sdk:?}"
        );
        let verdict = structure(&bytes, &headers);
        assert!(matches!(verdict, Verdict::Valid { .. }), "{verdict:?}");
        let result = beef_verification::verify_beef_spends_with_verifier(
            &bytes, &roots, &tracker, "main", None,
        )
        .await;
        let Verdict::NotJudged { cause, .. } = sdk else {
            unreachable!()
        };
        assert_eq!(work_cause(result, 0), cause);
    }
    eprintln!("pre-Genesis: the SDK and the door NotJudged(PreGenesisSpend), structure Valid");
}
