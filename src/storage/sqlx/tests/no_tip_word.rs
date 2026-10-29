//! #464, the keeper's reading of 2026-10-29: bsv-rs's constants name no
//! activation height still ahead (Chronicle passed at 943,816), so a
//! tracker with no mining tip, or one whose tip read faults, gives the
//! spend's transaction-level word the latest era (post-Chronicle), as the
//! reference reads no tip at `verifyUnlockScripts.ts:331`. The coin's UTXO
//! bits still come from its proof height (the grandparent at Genesis + 100,
//! post-Genesis) and an unproven coin takes the same latest era. The routes
//! judge instead of holding with `Service error: ChainTracker verification
//! failed: the header service supplied no mining tip`: a signature without
//! `SIGHASH_FORKID` is refused with bsv-rs's text, a valid spend accepted.

use super::*;

const NO_TIPS: [Tip; 2] = [Tip::Missing, Tip::Fault];

/// The signAction route (`process_action_internal` for a new signed
/// transaction): the wallet's own input, spending the unproven parent's
/// coin without `SIGHASH_FORKID`, is refused before the writer, nothing
/// written, the action still `unsigned`.
#[tokio::test]
async fn process_new_tx_judges_its_own_input_at_the_latest_era_without_a_tip() {
    for tip in NO_TIPS {
        for delayed in [false, true] {
            let (storage, child, processed) = sign(Bad::Own, delayed, tip).await;
            eprintln!("{tip:?} delayed={delayed}: {processed:?}");
            assert_forkid_refusal(processed.unwrap_err(), &child);
            assert_eq!(signed_rows(&storage).await, ("unsigned".into(), 0));
        }
    }
}

/// Control: the same route with every judged input valid is signed and
/// sent, where it held before.
#[tokio::test]
async fn process_new_tx_accepts_a_valid_spend_without_a_tip() {
    for tip in NO_TIPS {
        for bad in [Bad::None, Bad::Parent] {
            let (storage, _, processed) = sign(bad, false, tip).await;
            assert_eq!(processed.unwrap(), "sending", "{tip:?}");
            assert_eq!(signed_rows(&storage).await, ("sending".into(), 1));
        }
    }
}

/// The create pre-check on the caller's inputBEEF, under `tip`.
async fn create_under(
    bad: bool,
    txid_only: bool,
    tip: Tip,
) -> (StorageSqlx, Transaction, crate::Result<()>) {
    let (storage, user_id) = store().await;
    let p2pkh = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac";
    let (parent, mut beef, tracker) = lineage(bad, p2pkh);
    let tracker = tip.tracker(tracker);
    let args = caller_args(&parent, &beef.to_binary(), txid_only);
    let created = create_action_internal(&storage, Some(tracker.as_ref()), user_id, args)
        .await
        .map(|_| ());
    (storage, parent, created)
}

/// create's pre-check judges the caller's unproven parent at the latest era
/// without a tip: the fork-ID-less spend refused before any write, a valid
/// one created.
#[tokio::test]
async fn create_pre_check_judges_at_the_latest_era_without_a_tip() {
    for tip in NO_TIPS {
        for txid_only in [false, true] {
            let (storage, parent, created) = create_under(true, txid_only, tip).await;
            eprintln!("{tip:?} returnTXIDOnly={txid_only}: {created:?}");
            assert_forkid_refusal(created.unwrap_err(), &parent);
            let actions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
                .fetch_one(storage.pool())
                .await
                .unwrap();
            assert_eq!(actions, 0);
            let (_, _, created) = create_under(false, txid_only, tip).await;
            created.unwrap();
        }
    }
}
