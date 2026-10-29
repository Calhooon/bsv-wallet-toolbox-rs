use super::*;

// =====================================================================
// A change coin whose BEEF cannot be built (review 2 F2 of #44)
// =====================================================================

/// A reorg-demoted received coin: its transaction `unproven`, with
/// no proof, and its parent never stored. Returns the coin.
async fn seed_demoted_coin(
    storage: &StorageSqlx,
    user_id: i64,
    tag: u8,
    satoshis: u64,
) -> (Transaction, Transaction) {
    let parent = tx_spending(None, tag, satoshis + 1_000);
    let funding = tx_spending(Some(&parent.id()), tag + 1, satoshis);
    seed_change_coin(storage, user_id, &funding, false).await;
    sqlx::query("UPDATE transactions SET status = 'unproven' WHERE txid = ?")
        .bind(funding.id())
        .execute(storage.writer())
        .await
        .unwrap();
    (parent, funding)
}

/// The demoted coin is the best pick (900,000 sats, the only coin at
/// or above the target), but its BEEF cannot be built: the
/// pre-check skips it and funds the action from the two proven
/// 800-sat coins. The same under `returnTXIDOnly`, which assembles and
/// stores the input BEEF as the other options do (#463).
#[tokio::test]
async fn a_precheck_skips_a_coin_whose_beef_cannot_be_built() {
    for return_txid_only in [false, true] {
        let (storage, user_id) = setup().await;
        want_one_change_coin(&storage).await;
        let (_, demoted) = seed_demoted_coin(&storage, user_id, 0x71, 900_000).await;
        let b1 = tx_spending(None, 0x75, 800);
        let b2 = tx_spending(None, 0x76, 800);
        seed_change_coin(&storage, user_id, &b1, true).await;
        seed_change_coin(&storage, user_id, &b2, true).await;
        // One root per height: b2's proof is in the next block.
        sqlx::query("UPDATE proven_txs SET height = 901, merkle_path = ? WHERE txid = ?")
            .bind(bump_for(&b2.id(), 901).to_binary())
            .bind(b2.id())
            .execute(storage.writer())
            .await
            .unwrap();

        let tracker = AlwaysValidChainTracker::new(100_000);
        let mut args = funded_args();
        args.options = Some(bsv_rs::wallet::CreateActionOptions {
            return_txid_only: Some(return_txid_only),
            ..Default::default()
        });
        let result = create_action_internal(&storage, Some(&tracker), user_id, args)
            .await
            .expect("the proven coins fund the action");
        let mut sources: Vec<String> = result
            .inputs
            .iter()
            .map(|i| i.source_txid.clone())
            .collect();
        sources.sort();
        let mut want = vec![b1.id(), b2.id()];
        want.sort();
        assert_eq!(
            sources,
            want,
            "returnTXIDOnly={return_txid_only}: the demoted coin {} is not spent",
            demoted.id()
        );
        let stored: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT input_beef FROM transactions WHERE reference = ?")
                .bind(&result.reference)
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let beef = Beef::from_binary(&stored.unwrap()).unwrap();
        assert!(beef.find_txid(&demoted.id()).is_none());
        assert!(storage.in_flight_picks().reserved().is_empty());
    }
}

/// The demoted coin alone covers the action; the proven 300-sat coin
/// does not. The refusal names the wallet's coin and its missing
/// ancestor under `Unspendable coin:`, never `inputBEEF` (the caller
/// passed none) and never `Insufficient funds` (the coins offered
/// cover the action).
#[tokio::test]
async fn an_unspendable_coin_refusal_names_the_coin_and_its_missing_ancestor() {
    let (storage, user_id) = setup().await;
    want_one_change_coin(&storage).await;
    let (parent, demoted) = seed_demoted_coin(&storage, user_id, 0x81, 900_000).await;
    let small = tx_spending(None, 0x85, 300);
    seed_change_coin(&storage, user_id, &small, true).await;
    let before = the_store(&storage).await;

    let tracker = AlwaysValidChainTracker::new(100_000);
    let err = create_action_internal(&storage, Some(&tracker), user_id, funded_args())
        .await
        .expect_err("only the unspendable coin covers the action");
    match &err {
        Error::UnspendableCoin { outpoint, missing } => {
            assert_eq!(outpoint, &format!("{}.0", demoted.id()));
            assert_eq!(missing, &vec![parent.id()]);
        }
        other => panic!("expected UnspendableCoin, got {other:?}"),
    }
    let msg = err.to_string();
    assert_eq!(
                msg,
                format!(
                    "Unspendable coin: {}.0: its stored ancestry is incomplete, ancestor(s) not stored: [{}]; \
                     createAction fetches none, and the wallet's other coins do not cover the action",
                    demoted.id(),
                    parent.id()
                )
            );
    assert!(!msg.contains("inputBEEF"), "{msg}");
    assert!(!msg.starts_with("Insufficient funds"), "{msg}");
    assert_eq!(
        the_store(&storage).await,
        before,
        "the action wrote nothing"
    );
    assert!(storage.in_flight_picks().reserved().is_empty());
}
