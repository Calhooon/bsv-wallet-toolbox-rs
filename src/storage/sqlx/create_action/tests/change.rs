use super::*;
use bsv_rs::wallet::CreateActionOutput;

#[test]
fn test_random_derivation() {
    let d1 = random_derivation(16);
    let d2 = random_derivation(16);

    // Should be base64 encoded
    assert!(d1
        .chars()
        .all(|c| c.is_alphanumeric() || c == '+' || c == '/' || c == '='));

    // Should be different each time
    assert_ne!(d1, d2);
}

#[test]
fn test_caller_complete_plan_rule() {
    use CallerCompletePlan::*;
    // Self-sufficient, signatures forbid funding: verbatim whatever the fee.
    assert_eq!(caller_complete_plan(5_000, 4_000, 100, false), Verbatim);
    assert_eq!(caller_complete_plan(4_000, 4_000, 100, false), Verbatim);
    // Shortfall, signatures forbid funding: hard insufficient funds.
    assert_eq!(caller_complete_plan(1, 1_001, 100, false), Unfundable);
    // Shortfall, signatures permit funding: fund it.
    assert_eq!(caller_complete_plan(1, 1_001, 100, true), Fund);
    // Fundable and the surplus covers our fee: verbatim (surplus = caller's fee).
    assert_eq!(caller_complete_plan(5_000, 4_000, 100, true), Verbatim);
    assert_eq!(caller_complete_plan(4_100, 4_000, 100, true), Verbatim);
    // Fundable but the surplus is below our fee: fund rather than 465.
    assert_eq!(caller_complete_plan(4_099, 4_000, 100, true), Fund);
    assert_eq!(caller_complete_plan(4_000, 4_000, 100, true), Fund);
}

/// The Zanaadu V3 publish shape: ONE covenant input signed
/// SIGHASH_SINGLE|ANYONECANPAY|FORKID (0xc3), 1 sat in, 1,001 sats out. The
/// wallet must append a funding input and a change output, and must leave the
/// caller's input at vin 0 and outputs at vout 0/1 untouched.
#[tokio::test]
async fn test_caller_complete_anyonecanpay_shortfall_gets_funding_input_and_change() {
    let (storage, user_id) = caller_complete_storage().await;
    let covenant_lock = vec![0x51u8]; // OP_TRUE stand-in for the covenant script
    let cov_txid = seed_custom_output(&storage, user_id, 1, &covenant_lock, "c0c3").await;
    seed_change_output(&storage, user_id, 100_000).await;

    let unlock = p2pkh_unlock(0xc3);
    let args = bsv_rs::wallet::CreateActionArgs {
        description: "V3 publish".to_string(),
        input_beef: None,
        inputs: Some(vec![covenant_input(&cov_txid, unlock.clone())]),
        outputs: Some(vec![
            CreateActionOutput {
                locking_script: covenant_lock.clone(),
                satoshis: 1,
                output_description: "next covenant state".to_string(),
                basket: None,
                custom_instructions: None,
                tags: None,
            },
            p2pkh_output(1_000, "platform fee"),
        ]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let result = create_action_internal(&storage, None, user_id, args)
        .await
        .expect("an ANYONECANPAY shortfall must be funded, not rejected");

    // The caller's input stays first; the wallet's funding is appended after it.
    assert_eq!(result.inputs[0].vin, 0);
    assert_eq!(result.inputs[0].source_txid, cov_txid);
    assert_eq!(result.inputs[0].source_vout, 0);
    assert_eq!(result.inputs[0].source_satoshis, 1);
    assert_eq!(result.inputs[0].provided_by, StorageProvidedBy::You);
    assert_eq!(
        result.inputs[0].unlocking_script_length as usize,
        unlock.len()
    );
    assert!(
        result.inputs.len() >= 2,
        "a funding input must be appended, got {:?}",
        result.inputs
    );
    for (i, input) in result.inputs.iter().enumerate().skip(1) {
        assert_eq!(input.vin as usize, i);
        assert_eq!(input.provided_by, StorageProvidedBy::Storage);
    }

    // The caller's outputs keep vout 0 and 1 byte-for-byte; change is appended.
    assert_eq!(result.outputs[0].vout, 0);
    assert_eq!(result.outputs[0].satoshis, 1);
    assert_eq!(
        result.outputs[0].locking_script,
        hex::encode(&covenant_lock)
    );
    assert_eq!(result.outputs[1].vout, 1);
    assert_eq!(result.outputs[1].satoshis, 1_000);
    let change: Vec<_> = result
        .outputs
        .iter()
        .filter(|o| o.purpose.as_deref() == Some("change"))
        .collect();
    assert!(!change.is_empty(), "a change output must be appended");
    assert!(change.iter().all(|o| o.vout >= 2));

    // Funded: inputs cover outputs plus a positive miner fee.
    let in_total: u64 = result.inputs.iter().map(|i| i.source_satoshis).sum();
    let out_total: u64 = result.outputs.iter().map(|o| o.satoshis).sum();
    assert!(
        in_total > out_total,
        "in {} must exceed out {}",
        in_total,
        out_total
    );
    assert!(
        in_total - out_total < 1_000,
        "fee {} is implausibly large",
        in_total - out_total
    );
}

/// Same shortfall, but the caller signed SIGHASH_ALL|FORKID (0x41): the
/// signatures commit to every input and output, so funding is impossible and
/// the shortfall is a hard InsufficientFunds even though change is available.
#[tokio::test]
async fn test_caller_complete_sighash_all_shortfall_is_insufficient_funds() {
    let (storage, user_id) = caller_complete_storage().await;
    let cov_txid = seed_custom_output(&storage, user_id, 1, &[0x51], "a041").await;
    seed_change_output(&storage, user_id, 100_000).await;

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "sealed shortfall".to_string(),
        input_beef: None,
        inputs: Some(vec![covenant_input(&cov_txid, p2pkh_unlock(0x41))]),
        outputs: Some(vec![p2pkh_output(1_000, "payment")]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let err = create_action_internal(&storage, None, user_id, args)
        .await
        .expect_err("a SIGHASH_ALL shortfall cannot be funded");
    assert!(
        matches!(
            err,
            Error::InsufficientFunds {
                needed: 1_000,
                available: 1
            }
        ),
        "got {:?}",
        err
    );

    // Nothing was allocated: the change UTXO is still spendable.
    let (spendable, spent_by): (i64, Option<i64>) = sqlx::query_as(
        "SELECT spendable, spent_by FROM outputs WHERE \"change\" = 1 AND satoshis = 100000",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(spendable, 1);
    assert_eq!(spent_by, None);
}

/// The b23a960 regression: a fully SIGHASH_ALL 2-in/3-out transaction that
/// already covers its outputs is broadcast verbatim — no funding input and no
/// change output are injected even though change UTXOs are available.
#[tokio::test]
async fn test_caller_complete_sighash_all_self_sufficient_is_verbatim() {
    let (storage, user_id) = caller_complete_storage().await;
    let in_a = seed_custom_output(&storage, user_id, 30_000, &[0x51], "a1").await;
    let in_b = seed_custom_output(&storage, user_id, 30_000, &[0x51], "b2").await;
    seed_change_output(&storage, user_id, 100_000).await;

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "2-of-2 covenant settle".to_string(),
        input_beef: None,
        inputs: Some(vec![
            covenant_input(&in_a, p2pkh_unlock(0x41)),
            covenant_input(&in_b, p2pkh_unlock(0x41)),
        ]),
        outputs: Some(vec![
            p2pkh_output(20_000, "party a"),
            p2pkh_output(20_000, "party b"),
            p2pkh_output(19_500, "party c"),
        ]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let result = create_action_internal(&storage, None, user_id, args)
        .await
        .expect("self-sufficient caller-complete tx");
    assert_eq!(result.inputs.len(), 2, "no funding input may be injected");
    assert_eq!(result.outputs.len(), 3, "no change output may be injected");
    assert!(result
        .inputs
        .iter()
        .all(|i| i.provided_by == StorageProvidedBy::You));
    assert!(result
        .outputs
        .iter()
        .all(|o| o.purpose.as_deref() != Some("change")));
    assert_eq!(result.inputs[0].source_txid, in_a);
    assert_eq!(result.inputs[1].source_txid, in_b);
}

/// An ANYONECANPAY|SINGLE input whose surplus already covers the wallet's fee
/// is self-sufficient too: the surplus is the caller's chosen miner fee and
/// the bytes go out verbatim.
#[tokio::test]
async fn test_caller_complete_anyonecanpay_self_sufficient_is_verbatim() {
    let (storage, user_id) = caller_complete_storage().await;
    let cov_txid = seed_custom_output(&storage, user_id, 5_000, &[0x51], "c5c3").await;
    seed_change_output(&storage, user_id, 100_000).await;

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "covenant close".to_string(),
        input_beef: None,
        inputs: Some(vec![covenant_input(&cov_txid, p2pkh_unlock(0xc3))]),
        outputs: Some(vec![p2pkh_output(4_000, "payout")]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let result = create_action_internal(&storage, None, user_id, args)
        .await
        .unwrap();
    assert_eq!(result.inputs.len(), 1);
    assert_eq!(result.outputs.len(), 1);
}

/// An ANYONECANPAY|SINGLE input that covers its outputs but not the wallet's
/// miner fee is funded instead of being sent out to a "fee too low" rejection.
#[tokio::test]
async fn test_caller_complete_anyonecanpay_fee_gap_is_funded() {
    let (storage, user_id) = caller_complete_storage().await;
    let cov_txid = seed_custom_output(&storage, user_id, 1_001, &[0x51], "c1c3").await;
    seed_change_output(&storage, user_id, 100_000).await;

    let args = bsv_rs::wallet::CreateActionArgs {
        description: "zero-fee covenant".to_string(),
        input_beef: None,
        inputs: Some(vec![covenant_input(&cov_txid, p2pkh_unlock(0xc3))]),
        outputs: Some(vec![p2pkh_output(1_000, "payment")]),
        lock_time: None,
        version: None,
        labels: None,
        options: None,
    };

    let result = create_action_internal(&storage, None, user_id, args)
        .await
        .unwrap();
    assert!(result.inputs.len() >= 2, "the fee gap must be funded");
    assert_eq!(result.inputs[0].source_txid, cov_txid);
    assert_eq!(result.outputs[0].vout, 0);
    assert_eq!(result.outputs[0].satoshis, 1_000);
    let in_total: u64 = result.inputs.iter().map(|i| i.source_satoshis).sum();
    let out_total: u64 = result.outputs.iter().map(|o| o.satoshis).sum();
    assert!(in_total > out_total);
}
