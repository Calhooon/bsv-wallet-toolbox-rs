//! The must-spend list's refusals the docs name beside the rules
//! (#353, PR #399's review finding 3): a named coin whose transaction is
//! neither `completed` nor `unproven`, a caller-complete transaction whose
//! signatures forbid adding the named coins, a named coin whose BEEF cannot
//! be built, and the trait's default door, which cannot enforce a list.

use std::time::Duration;

use anyhow::Result;
use serde_json::json;

use super::{id, outpoint, payment, refusal, refused, seeded};

/// A named coin whose transaction is `failed` is refused by name, never
/// taken as the funding it would cover.
#[tokio::test]
async fn a_named_coin_whose_transaction_is_not_spendable_is_refused_by_name() -> Result<()> {
    let replay = seeded().await?;
    let (txid, _) = outpoint(&replay, "funding.1");
    sqlx::query("UPDATE transactions SET status = 'failed' WHERE txid = ?")
        .bind(&txid)
        .execute(replay.storage.writer())
        .await?;
    let error = refusal(&replay, 1000, false, &["funding.1"], &["funding.1"]).await?;
    assert_eq!(
        error,
        format!(
            "Validation error: mustSpend[0]: output {txid}.1 is not spendable: \
             its transaction is failed"
        )
    );
    Ok(())
}

/// A caller input signed `SIGHASH_ALL` commits to the whole transaction:
/// the named coins cannot be added, and the call says so rather than
/// broadcasting the caller's bytes without them.
#[tokio::test]
async fn a_caller_input_signed_over_the_whole_transaction_refuses_the_list() -> Result<()> {
    let replay = seeded().await?;
    let (txid, _) = outpoint(&replay, "funding.2");
    let coin = id(&replay, "funding.1").await?;
    // `<DER sig, SIGHASH_ALL|FORKID> <pubkey>`: structurally a signature.
    let mut sig = vec![0x30, 0x44, 0x02, 0x20];
    sig.extend([0x11; 32]);
    sig.extend([0x02, 0x20]);
    sig.extend([0x22; 32]);
    sig.push(0x41);
    let mut unlock = vec![sig.len() as u8];
    unlock.extend(sig);
    unlock.push(33);
    unlock.extend([0x02; 33]);
    let mut args = serde_json::to_value(payment(100, false, false))?;
    args["inputs"] = json!([{
        "outpoint": format!("{txid}.2"),
        "inputDescription": "Caller signed input",
        "unlockingScript": hex::encode(&unlock),
    }]);
    let error = refused(&replay, async {
        match replay
            .storage
            .create_action_offering(
                &replay.auth,
                serde_json::from_value(args)?,
                &[coin],
                &[coin],
            )
            .await
        {
            Ok(created) => anyhow::bail!("taken: {}", created.reference),
            Err(e) => Ok(e.to_string()),
        }
    })
    .await?;
    assert_eq!(
        error,
        "Validation error: mustSpend: a caller input is signed over the whole transaction; \
         the wallet cannot add the named coins"
    );
    Ok(())
}

/// A named coin whose ancestry the wallet does not store (its transaction
/// demoted to `unproven`, its proof gone, its parent never stored) is the
/// refusal, never picked around: the proven coin beside it in the offer
/// would cover the action.
#[tokio::test]
async fn a_named_coin_whose_beef_cannot_be_built_is_the_refusal() -> Result<()> {
    let replay = seeded().await?;
    let (txid, _) = outpoint(&replay, "funding.3");
    let parent = replay.seeds["funding"].tx.inputs[0]
        .source_txid
        .clone()
        .unwrap_or_default();
    sqlx::query("UPDATE transactions SET status = 'unproven', proven_tx_id = NULL WHERE txid = ?")
        .bind(&txid)
        .execute(replay.storage.writer())
        .await?;
    sqlx::query("DELETE FROM proven_txs WHERE txid = ?")
        .bind(&txid)
        .execute(replay.storage.writer())
        .await?;
    let error = tokio::time::timeout(
        Duration::from_secs(30),
        refusal(
            &replay,
            1000,
            false,
            &["funding.1", "funding.3"],
            &["funding.3"],
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the named coin was picked around without end"))??;
    assert_eq!(
        error,
        format!(
            "Unspendable coin: {txid}.3: its stored ancestry is incomplete, ancestor(s) \
             not stored: [{parent}]; createAction fetches none, and the wallet's other \
             coins do not cover the action"
        )
    );
    Ok(())
}

/// The trait's default door (here the storage client's, which does not
/// override it) refuses a list it cannot enforce, before it sends anything.
#[cfg(feature = "remote")]
#[tokio::test]
async fn the_default_door_refuses_a_list() -> Result<()> {
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::wallet::ProtoWallet;
    use bsv_wallet_toolbox_rs::storage::client::StorageClient;
    use bsv_wallet_toolbox_rs::storage::{CreateActionDoor, DoorRefusal, WalletStorageWriter};

    let replay = seeded().await?;
    let mut server = mockito::Server::new_async().await;
    let nothing_sent = server
        .mock("POST", mockito::Matcher::Any)
        .expect(0)
        .create_async()
        .await;
    let wallet = ProtoWallet::new(Some(PrivateKey::from_bytes(&[7; 32])?));
    let client = StorageClient::new_unauthenticated(wallet, server.url());
    let refused = client
        .create_action_at_door(
            &replay.auth,
            payment(1000, false, false),
            CreateActionDoor {
                offer: None,
                must_spend: &[1],
                sign_and_process: None,
            },
        )
        .await;
    match refused {
        Err(DoorRefusal::Create(e)) => assert_eq!(
            e.to_string(),
            "Invalid operation: this storage cannot allocate a must-spend list"
        ),
        Err(other) => anyhow::bail!("refused by another step: {other:?}"),
        Ok(done) => anyhow::bail!("taken: {}", done.created.reference),
    }
    nothing_sent.assert_async().await;
    Ok(())
}
