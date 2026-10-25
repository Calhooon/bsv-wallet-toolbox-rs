use anyhow::{Context, Result};
use bsv_rs::wallet::{CreateActionArgs, CreateActionOptions, CreateActionResult};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::services::mock::{
    error_post_beef_result, success_post_beef_result, MockResponse,
};
use bsv_wallet_toolbox_rs::storage::{StorageProcessActionArgs, StorageProcessActionResults};
use bsv_wallet_toolbox_rs::{Error, StorageSqlx, Wallet, WalletStorageProvider};
use serde_json::{json, Value};

use super::{model::Inputs, rows, seed};

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

async fn wallet() -> Result<TestWallet> {
    let fixture: Value = serde_json::from_str(include_str!("../data/storage/process_nosend.json"))?;
    let mut inputs = fixture["inputs"].clone();
    inputs["transactions"][0]["outputs"][0]["repeat"] = json!(2);
    let inputs: Inputs = serde_json::from_value(inputs)?;
    let seeded = seed::setup(&inputs).await?;
    Ok(Wallet::new(Some(seeded.root), seeded.storage, MockWalletServices::new()).await?)
}

fn payment(no_send: bool) -> CreateActionArgs {
    serde_json::from_value(json!({
        "description": "Recorded wallet door payment",
        "outputs": [{"lockingScript":"51", "satoshis":1000,
            "outputDescription":"Recorded recipient"}],
        "options": {"noSend":no_send, "randomizeOutputs":false}
    }))
    .expect("fixture payment")
}

fn post(handle: String) -> StorageProcessActionArgs {
    StorageProcessActionArgs {
        is_new_tx: false,
        is_send_with: true,
        is_no_send: false,
        is_delayed: false,
        reference: None,
        txid: None,
        raw_tx: None,
        send_with: vec![handle],
    }
}

fn status(result: &StorageProcessActionResults) -> &str {
    &result.send_with_results.as_ref().expect("send results")[0].status
}

async fn build(wallet: &TestWallet, coin: i64) -> Result<CreateActionResult> {
    Ok(wallet
        .create_action_offering(payment(true), "fixture.local", &[coin])
        .await?)
}

#[tokio::test]
async fn two_built_actions_post_concurrently_and_record_each_instance() -> Result<()> {
    let wallet = wallet().await?;
    let a = hex::encode(build(&wallet, 1).await?.txid.context("signed txid")?);
    let b = hex::encode(build(&wallet, 2).await?.txid.context("signed txid")?);
    assert_eq!(wallet.services().call_count("post_beef"), 0);
    let (a_result, b_result) = tokio::join!(
        wallet.process_action(post(a.clone()), "fixture.local"),
        wallet.process_action(post(b.clone()), "fixture.local"),
    );
    assert_eq!(status(&a_result?), "unproven");
    assert_eq!(status(&b_result?), "unproven");
    assert_eq!(wallet.services().call_count("post_beef"), 2);
    for txid in [a, b] {
        let stored: (String, String, String) = sqlx::query_as(
            "SELECT t.status, r.status, r.history FROM transactions t \
             JOIN proven_tx_reqs r ON r.txid = t.txid WHERE t.txid = ?",
        )
        .bind(txid)
        .fetch_one(wallet.storage().pool())
        .await?;
        assert_eq!((&*stored.0, &*stored.1), ("unproven", "unmined"));
        assert!(stored.2.contains("MockProvider"));
    }
    Ok(())
}

#[tokio::test]
async fn post_handles_templates_unknown_references_and_repeats_without_writes() -> Result<()> {
    let wallet = wallet().await?;
    let mut args = payment(true);
    args.options
        .get_or_insert_with(CreateActionOptions::default)
        .sign_and_process = Some(false);
    let template = wallet
        .create_action_offering(args, "fixture.local", &[1])
        .await?;
    let reference =
        String::from_utf8(template.signable_transaction.context("template")?.reference)?;
    let before = rows(wallet.storage()).await?;
    let refused = wallet
        .process_action(post(reference), "fixture.local")
        .await;
    assert!(matches!(refused, Err(Error::InvalidTransactionStatus(_))));
    let unknown = wallet
        .process_action(post("not-built".into()), "fixture.local")
        .await?;
    assert_eq!(status(&unknown), "failed");
    assert_eq!(rows(wallet.storage()).await?, before);
    let txid = hex::encode(build(&wallet, 2).await?.txid.context("signed txid")?);
    let reference: String = sqlx::query_scalar("SELECT reference FROM transactions WHERE txid = ?")
        .bind(&txid)
        .fetch_one(wallet.storage().pool())
        .await?;
    assert_eq!(
        status(
            &wallet
                .process_action(post(reference.clone()), "fixture.local")
                .await?
        ),
        "unproven"
    );
    let before = rows(wallet.storage()).await?;
    assert_eq!(
        status(
            &wallet
                .process_action(post(reference), "fixture.local")
                .await?
        ),
        "unproven"
    );
    assert_eq!(rows(wallet.storage()).await?, before);
    assert_eq!(wallet.services().call_count("post_beef"), 1);
    Ok(())
}

#[tokio::test]
async fn post_preserves_no_send_and_schedules_delayed_without_network() -> Result<()> {
    let wallet = wallet().await?;
    let txid = hex::encode(build(&wallet, 1).await?.txid.context("signed txid")?);
    let before = rows(wallet.storage()).await?;
    let mut args = post(txid.clone());
    args.is_no_send = true;
    args.is_send_with = false;
    let result = wallet.process_action(args, "fixture.local").await?;
    assert!(result
        .send_with_results
        .context("noSend result")?
        .is_empty());
    assert_eq!(rows(wallet.storage()).await?, before);
    let mut args = post(txid.clone());
    args.is_delayed = true;
    assert_eq!(
        status(&wallet.process_action(args, "fixture.local").await?),
        "sending"
    );
    let stored: (String, String) = sqlx::query_as(
        "SELECT t.status, r.status FROM transactions t \
         JOIN proven_tx_reqs r ON r.txid = t.txid WHERE t.txid = ?",
    )
    .bind(txid)
    .fetch_one(wallet.storage().pool())
    .await?;
    assert_eq!((&*stored.0, &*stored.1), ("sending", "unsent"));
    assert_eq!(wallet.services().call_count("post_beef"), 0);
    Ok(())
}

#[tokio::test]
async fn invalid_member_prevents_posting_the_ready_subset() -> Result<()> {
    let wallet = wallet().await?;
    let txid = hex::encode(build(&wallet, 1).await?.txid.context("signed txid")?);
    let before = rows(wallet.storage()).await?;
    let mut args = post(txid);
    args.send_with.push("not-built".into());
    let result = wallet.process_action(args, "fixture.local").await?;
    assert!(result
        .send_with_results
        .context("results")?
        .iter()
        .all(|r| r.status == "failed"));
    assert_eq!(rows(wallet.storage()).await?, before);
    assert_eq!(wallet.services().call_count("post_beef"), 0);
    Ok(())
}

#[tokio::test]
async fn post_rebuilds_trimmed_ancestry_from_storage() -> Result<()> {
    let wallet = wallet().await?;
    let funding: String = sqlx::query_scalar("SELECT txid FROM outputs WHERE output_id = 1")
        .fetch_one(wallet.storage().pool())
        .await?;
    let mut known = [0; 32];
    known.copy_from_slice(&hex::decode(funding)?);
    let mut args = payment(true);
    args.options.as_mut().context("options")?.known_txids = Some(vec![known]);
    let built = wallet
        .create_action_offering(args, "fixture.local", &[1])
        .await?;
    let compact_len = built.beef.context("compact BEEF")?.len();
    let txid = hex::encode(built.txid.context("txid")?);
    assert_eq!(
        status(&wallet.process_action(post(txid), "fixture.local").await?),
        "unproven"
    );
    let calls = wallet.services().call_history();
    let post = calls
        .iter()
        .find(|call| call.method == "post_beef")
        .context("post call")?;
    let full_len: usize = post.args[0]
        .strip_prefix("beef_len=")
        .context("BEEF size")?
        .parse()?;
    assert!(
        full_len > compact_len,
        "the post restores the omitted funding transaction"
    );
    assert_eq!(wallet.services().call_count("get_raw_tx"), 0);
    assert_eq!(wallet.services().call_count("get_merkle_path"), 0);
    Ok(())
}

#[tokio::test]
async fn post_keeps_acceptance_and_refusal_words_per_instance() -> Result<()> {
    let wallet = wallet().await?;
    let txid = hex::encode(build(&wallet, 1).await?.txid.context("signed txid")?);
    let words = vec![
        success_post_beef_result("acceptor", &[&txid]),
        error_post_beef_result("refuser", "recorded refusal"),
        error_post_beef_result("unreachable", "recorded transport fault"),
    ];
    let services = std::sync::Arc::new(
        MockWalletServices::builder()
            .post_beef_response(MockResponse::Success(words.clone()))
            .build(),
    );
    wallet.storage().set_services(services);
    assert_eq!(
        status(
            &wallet
                .process_action(post(txid.clone()), "fixture.local")
                .await?
        ),
        "unproven"
    );
    let history: String = sqlx::query_scalar("SELECT history FROM proven_tx_reqs WHERE txid = ?")
        .bind(txid)
        .fetch_one(wallet.storage().pool())
        .await?;
    let history: Value = serde_json::from_str(&history)?;
    assert_eq!(
        history["notes"]
            .as_array()
            .context("notes")?
            .last()
            .context("post note")?["words"],
        serde_json::to_value(words)?
    );
    Ok(())
}

#[tokio::test]
async fn no_send_outputs_match_the_reference_before_and_after_post() -> Result<()> {
    let wallet = wallet().await?;
    let txid = hex::encode(build(&wallet, 1).await?.txid.context("signed txid")?);
    for posted in [false, true] {
        if posted {
            wallet
                .process_action(post(txid.clone()), "fixture.local")
                .await?;
        }
        let flags: Vec<bool> = sqlx::query_scalar("SELECT spendable FROM outputs WHERE txid = ?")
            .bind(&txid)
            .fetch_all(wallet.storage().pool())
            .await?;
        assert!(!flags.is_empty());
        assert!(
            flags.iter().all(|spendable| *spendable),
            "the reference marks every signed output spendable, posted={posted}"
        );
    }
    let locked: bool = sqlx::query_scalar("SELECT spendable FROM outputs WHERE output_id = 1")
        .fetch_one(wallet.storage().pool())
        .await?;
    assert!(!locked, "the input remains locked to this action");
    Ok(())
}
