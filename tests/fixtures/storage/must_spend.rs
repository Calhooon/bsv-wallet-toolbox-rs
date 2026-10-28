//! The offering call's must-spend list (#353, bsv-wallet-cli #313): each
//! named coin is wallet-managed change inside the offer, allocated ahead of
//! the pick and signed as a picked coin, or the call refuses it by name.

use anyhow::{Context, Result};
use bsv_rs::transaction::Transaction;
use bsv_rs::wallet::CreateActionArgs;
use bsv_wallet_toolbox_rs::storage::{StorageCreateActionResult, WalletStorageWriter};
use serde_json::json;

use super::{model::Fixture, rows, seed, Replay};

/// The recorded receipt (an unproven wallet payment of 9999, `receive`) and
/// one proven seed: `funding.0` 600 and `funding.1` 5000 and `funding.3`
/// 8000, managed change; `funding.2` 700, the user's but not change.
async fn seeded() -> Result<Replay> {
    let mut fixture: Fixture = serde_json::from_str(include_str!(
        "../data/storage/create_received_unproven_without_rule.json"
    ))?;
    fixture
        .inputs
        .transactions
        .push(serde_json::from_value(json!({
            "name": "funding",
            "outputs": [
                {"basket": "default", "change": true, "satoshis": 600},
                {"basket": "default", "change": true, "satoshis": 5000},
                {"satoshis": 700, "script": "51"},
                {"basket": "default", "change": true, "satoshis": 8000}
            ]
        }))?);
    let mut replay = seed::setup(&fixture.inputs).await?;
    let accepted = replay.execute(&fixture.calls[0]).await?;
    assert!(accepted.get("ok").is_some(), "{accepted}");
    Ok(replay)
}

async fn id(replay: &Replay, outpoint: &str) -> Result<i64> {
    let (seed, vout) = outpoint.rsplit_once('.').context("name.vout")?;
    let txid = replay.seeds.get(seed).context("seed")?.tx.id();
    Ok(
        sqlx::query_scalar("SELECT output_id FROM outputs WHERE txid = ? AND vout = ?")
            .bind(txid)
            .bind(vout.parse::<i64>()?)
            .fetch_one(replay.storage.pool())
            .await?,
    )
}

async fn ids(replay: &Replay, outpoints: &[&str]) -> Result<Vec<i64>> {
    let mut ids = Vec::new();
    for outpoint in outpoints {
        ids.push(id(replay, outpoint).await?);
    }
    Ok(ids)
}

fn payment(satoshis: u64, rule: bool, sign: bool) -> CreateActionArgs {
    let mut args = json!({
        "description": "Spend the named coins",
        "outputs": [{"lockingScript": "51", "satoshis": satoshis,
            "outputDescription": "Recipient"}],
        "options": {"noSend": true, "randomizeOutputs": false, "signAndProcess": sign}
    });
    if rule {
        args["labels"] = json!(["underReceivedRule"]);
    }
    serde_json::from_value(args).expect("payment")
}

/// The storage door's offering call with the list, as a template.
async fn template(
    replay: &Replay,
    satoshis: u64,
    rule: bool,
    offer: &[&str],
    must_spend: &[&str],
) -> Result<bsv_wallet_toolbox_rs::Result<StorageCreateActionResult>> {
    let offer = ids(replay, offer).await?;
    let must_spend = ids(replay, must_spend).await?;
    Ok(replay
        .storage
        .create_action_offering(
            &replay.auth,
            payment(satoshis, rule, false),
            &offer,
            &must_spend,
        )
        .await)
}

/// A refusal that names the coin and writes nothing.
async fn refused(
    replay: &Replay,
    call: impl std::future::Future<Output = Result<String>>,
) -> Result<String> {
    let before = rows(&replay.storage).await?;
    let error = call.await?;
    assert_eq!(rows(&replay.storage).await?, before, "{error}");
    Ok(error)
}

async fn refusal(
    replay: &Replay,
    satoshis: u64,
    rule: bool,
    offer: &[&str],
    must_spend: &[&str],
) -> Result<String> {
    refused(replay, async {
        match template(replay, satoshis, rule, offer, must_spend).await? {
            Ok(created) => anyhow::bail!("taken: {}", created.reference),
            Err(e) => Ok(e.to_string()),
        }
    })
    .await
}

fn spent(created: &StorageCreateActionResult) -> Vec<(String, u32)> {
    created
        .inputs
        .iter()
        .map(|i| (i.source_txid.clone(), i.source_vout))
        .collect()
}

fn outpoint(replay: &Replay, name: &str) -> (String, u32) {
    let (seed, vout) = name.rsplit_once('.').expect("name.vout");
    (replay.seeds[seed].tx.id(), vout.parse().expect("vout"))
}

/// Inputs minus outputs, and the change returned.
fn fee_and_change(created: &StorageCreateActionResult) -> (u64, u64) {
    let inputs: u64 = created.inputs.iter().map(|i| i.source_satoshis).sum();
    let outputs: u64 = created.outputs.iter().map(|o| o.satoshis).sum();
    let change: u64 = created
        .outputs
        .iter()
        .filter(|o| o.purpose.as_deref() == Some("change"))
        .map(|o| o.satoshis)
        .sum();
    (inputs - outputs, change)
}

#[tokio::test]
async fn a_named_coin_outside_the_offer_is_refused_by_name() -> Result<()> {
    let replay = seeded().await?;
    let coin = id(&replay, "funding.3").await?;
    let error = refusal(&replay, 1000, false, &["funding.1"], &["funding.3"]).await?;
    assert_eq!(
        error,
        format!("Validation error: mustSpend[0]: output {coin} is outside the offer")
    );
    Ok(())
}

#[tokio::test]
async fn a_named_coin_that_is_not_managed_change_is_refused_by_name() -> Result<()> {
    let replay = seeded().await?;
    let (txid, _) = outpoint(&replay, "funding.2");
    let error = refusal(
        &replay,
        1000,
        false,
        &["funding.1", "funding.2"],
        &["funding.2"],
    )
    .await?;
    assert_eq!(
        error,
        format!("Validation error: mustSpend[0]: output {txid}.2 is not wallet-managed change")
    );
    Ok(())
}

#[tokio::test]
async fn a_named_spent_or_reserved_coin_is_refused_by_name() -> Result<()> {
    let replay = seeded().await?;
    let (txid, _) = outpoint(&replay, "funding.1");
    // An earlier template reserves the coin it names.
    let earlier = template(&replay, 1000, false, &["funding.1"], &["funding.1"]).await??;
    assert_eq!(spent(&earlier), [(txid.clone(), 1)]);
    let error = refusal(&replay, 1000, false, &["funding.1"], &["funding.1"]).await?;
    assert_eq!(
        error,
        format!("Validation error: mustSpend[0]: output {txid}.1 is spent or reserved")
    );
    Ok(())
}

#[tokio::test]
async fn a_named_coin_of_another_user_is_refused_by_name() -> Result<()> {
    let replay = seeded().await?;
    let coin = id(&replay, "funding.1").await?;
    let (other, _) = replay.storage.find_or_insert_user("02another_user").await?;
    sqlx::query("UPDATE outputs SET user_id = ? WHERE output_id = ?")
        .bind(other.user_id)
        .bind(coin)
        .execute(replay.storage.writer())
        .await?;
    let error = refusal(&replay, 1000, false, &["funding.1"], &["funding.1"]).await?;
    assert_eq!(
        error,
        format!("Validation error: mustSpend[0]: output {coin} is not this user's")
    );
    Ok(())
}

#[tokio::test]
async fn a_coin_named_twice_is_refused_by_name() -> Result<()> {
    let replay = seeded().await?;
    let coin = id(&replay, "funding.1").await?;
    let error = refusal(
        &replay,
        1000,
        false,
        &["funding.1"],
        &["funding.1", "funding.1"],
    )
    .await?;
    assert_eq!(
        error,
        format!("Validation error: mustSpend[1]: output {coin} is named twice")
    );
    Ok(())
}

/// R1 (d): the list does not bypass the received rule.
#[tokio::test]
async fn a_named_received_unproven_coin_without_the_rule_is_refused_by_name() -> Result<()> {
    let replay = seeded().await?;
    let (txid, _) = outpoint(&replay, "receipt.0");
    let error = refusal(
        &replay,
        1000,
        false,
        &["funding.1", "receipt.0"],
        &["receipt.0"],
    )
    .await?;
    assert_eq!(
        error,
        format!(
            "Validation error: mustSpend[0]: output {txid}.0 is a received coin whose transaction \
             is unproven, not completed; the action does not name underReceivedRule"
        )
    );
    Ok(())
}

/// The consumers' case (bsv-wallet-cli #313): under the rule the named
/// receipt is spent, signed by the wallet as a picked coin, and the proven
/// coin that would have been picked is left unspent.
#[tokio::test]
async fn under_the_rule_the_named_receipt_is_spent_and_the_proven_coin_left() -> Result<()> {
    let replay = seeded().await?;
    let offer = ids(&replay, &["funding.1", "receipt.0"]).await?;
    let must_spend = ids(&replay, &["receipt.0"]).await?;
    let wallet = replay.wallet.as_ref().context("wallet")?;
    // Without the list the pick takes the proven coin (the order stays).
    let plain = template(&replay, 1000, true, &["funding.1", "receipt.0"], &[]).await??;
    assert_eq!(spent(&plain), [outpoint(&replay, "funding.1")]);
    sqlx::query("UPDATE outputs SET spent_by = NULL, spendable = 1")
        .execute(replay.storage.writer())
        .await?;

    let signed = wallet
        .create_action_offering(
            payment(1000, true, true),
            Some("fixture.local"),
            &offer,
            &must_spend,
        )
        .await?;
    let tx = Transaction::from_binary(&signed.tx.context("signed tx")?)?;
    let inputs: Vec<(String, u32)> = tx
        .inputs
        .iter()
        .map(|i| {
            (
                i.source_txid.clone().unwrap_or_default(),
                i.source_output_index,
            )
        })
        .collect();
    assert_eq!(inputs, [outpoint(&replay, "receipt.0")]);
    assert!(tx.inputs[0].unlocking_script.is_some());
    let unspent: Vec<i64> = sqlx::query_scalar(
        "SELECT output_id FROM outputs WHERE spent_by IS NULL AND output_id IN (?, ?) ORDER BY output_id",
    )
    .bind(offer[0])
    .bind(offer[1])
    .fetch_all(replay.storage.pool())
    .await?;
    assert_eq!(unspent, [offer[0]]);
    Ok(())
}

/// The named coins count toward the target; the pick funds the remainder
/// and the fee by the unchanged formula, and change returns as always.
#[tokio::test]
async fn the_fee_and_change_are_the_formulas() -> Result<()> {
    // Both coins needed: the plain pick takes them; the list naming the
    // small one takes the same two, with the same fee and change.
    let plain_replay = seeded().await?;
    let plain = template(&plain_replay, 5300, false, &["funding.0", "funding.1"], &[]).await??;
    let named_replay = seeded().await?;
    let named = template(
        &named_replay,
        5300,
        false,
        &["funding.0", "funding.1"],
        &["funding.0"],
    )
    .await??;
    let mut a = spent(&plain);
    let mut b = spent(&named);
    a.sort();
    b.sort();
    assert_eq!(a, b);
    assert_eq!(fee_and_change(&plain), fee_and_change(&named));

    // The named small coin does not cover: it is input 0 and the pick funds
    // the remainder from the offer.
    let replay = seeded().await?;
    let short = template(
        &replay,
        1000,
        false,
        &["funding.0", "funding.1"],
        &["funding.0"],
    )
    .await??;
    assert_eq!(
        spent(&short),
        [
            outpoint(&replay, "funding.0"),
            outpoint(&replay, "funding.1")
        ]
    );

    // A named coin far above the action: no pick, the excess is change.
    let replay = seeded().await?;
    let rich = template(
        &replay,
        100,
        false,
        &["funding.1", "funding.3"],
        &["funding.3"],
    )
    .await??;
    assert_eq!(spent(&rich), [outpoint(&replay, "funding.3")]);
    let (fee, change) = fee_and_change(&rich);
    assert!(fee > 0 && fee < 100, "fee {fee}");
    assert_eq!(change, 8000 - 100 - fee);
    Ok(())
}

/// A caller passing no list sees the base's behaviour: the recorded door
/// fixtures replay unchanged (`replay_storage_fixtures`), and the list's
/// empty form takes the pick's own coin here.
#[tokio::test]
async fn an_empty_list_is_the_offer_alone() -> Result<()> {
    let replay = seeded().await?;
    let created = template(&replay, 1000, false, &["funding.1", "funding.3"], &[]).await??;
    assert_eq!(spent(&created), [outpoint(&replay, "funding.1")]);
    Ok(())
}
