//! The grant rule as a corpus scene (#751 step 2a, the owner's word of
//! 2026-11-04): a protocol grant mints the reference's token through the
//! wallet's own door, and a seek-false call for the same key is admitted
//! from that token by a new manager over the same store, whose cache is
//! empty as an expired one is. The seed and the expected token are
//! `data/permissions/protocol_grant_mint.json`. A token's fields are
//! encrypted under a fresh IV, so the scene checks their plaintexts, the
//! script's shape and the stored rows rather than replaying bytes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use bsv_rs::script::templates::PushDrop;
use bsv_rs::script::LockingScript;
use bsv_rs::wallet::{DecryptArgs, GetPublicKeyArgs, Protocol, SecurityLevel, WalletInterface};
use bsv_wallet_toolbox_rs::managers::{WalletPermissionsManager, WalletPermissionsManagerConfig};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet};
use serde::Deserialize;
use serde_json::json;

use super::{model::Inputs, seed};

type SceneWallet = Wallet<StorageSqlx, MockWalletServices>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantScene {
    name: String,
    issue: u64,
    inputs: Inputs,
    admin: String,
    originator: String,
    protocol: (u8, String),
    basket: String,
    description: String,
    fields: Vec<String>,
    tags: Vec<String>,
}

fn scene() -> Result<GrantScene> {
    let source = include_str!("../data/permissions/protocol_grant_mint.json");
    let scene: GrantScene = serde_json::from_str(source)?;
    ensure!(scene.name == "protocol_grant_mint" && scene.issue == 751);
    Ok(scene)
}

fn protocol(scene: &GrantScene) -> Protocol {
    let level = match scene.protocol.0 {
        1 => SecurityLevel::App,
        _ => SecurityLevel::Counterparty,
    };
    Protocol::new(level, &scene.protocol.1)
}

/// A manager over a wallet on `storage` (the underlying, no manager of its
/// own), and the door a host calls: a second wallet on the same store with
/// the manager set. Clones of one store, as one process holds one.
async fn door(
    storage: &StorageSqlx,
    root: &bsv_rs::primitives::PrivateKey,
    admin: &str,
) -> Result<(Arc<SceneWallet>, Arc<WalletPermissionsManager>, SceneWallet)> {
    let services = MockWalletServices::new;
    let underlying = Arc::new(Wallet::new(Some(root.clone()), storage.clone(), services()).await?);
    let config = WalletPermissionsManagerConfig::all_enabled();
    let manager = WalletPermissionsManager::new(underlying.clone(), admin.to_string(), config);
    let manager = Arc::new(manager);
    let mut front = Wallet::new(Some(root.clone()), storage.clone(), services()).await?;
    front.set_permissions_manager(manager.clone());
    Ok((underlying, manager, front))
}

/// A key call under the scene's protocol, `seek` as the request's.
async fn key_call(front: &SceneWallet, scene: &GrantScene, seek: bool) -> bsv_rs::Result<String> {
    let args = GetPublicKeyArgs {
        protocol_id: Some(protocol(scene)),
        key_id: Some("1".to_string()),
        seek_permission: Some(seek),
        ..Default::default()
    };
    let key = front.get_public_key(args, Some(&scene.originator)).await?;
    Ok(key.public_key)
}

#[tokio::test]
async fn a_protocol_grant_mints_the_token_a_restarted_manager_admits_a_seek_false_call_by(
) -> Result<()> {
    let scene = scene()?;
    let seeded = seed::setup(&scene.inputs).await?;
    let (storage, root) = (seeded.storage, seeded.root);
    let (underlying, manager, front) = door(&storage, &root, &scene.admin).await?;
    let prompts = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&prompts);
    manager
        .set_permission_request_handler(Arc::new(move |request| {
            seen.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                let expiry = chrono::Utc::now().timestamp() + 3600;
                let token = json!({
                    "txid": "", "tx": [], "outputIndex": 0, "outputScript": "",
                    "satoshis": 1, "originator": request.originator, "expiry": expiry,
                });
                Ok(serde_json::from_value(token).expect("a token"))
            })
        }))
        .await;

    // Before the grant a seek-false call is refused with the reference's text.
    let refused = key_call(&front, &scene, false).await;
    let absent = "No protocol permission token found (seekPermission=false).";
    ensure!(
        matches!(&refused, Err(e) if e.to_string().contains(absent)),
        "{refused:?}"
    );
    let granted = key_call(&front, &scene, true).await?;
    ensure!(prompts.load(Ordering::SeqCst) == 1, "one prompt");

    // The mint: one 1-satoshi output in the admin basket under the tags,
    // one action described as the reference's, delayed: nothing posted.
    let db = storage.pool();
    let outputs: Vec<(i64, Vec<u8>, i64, i64)> = sqlx::query_as(
        "SELECT o.output_id, o.locking_script, o.satoshis, o.spendable FROM outputs o \
         JOIN output_baskets b ON b.basket_id = o.basket_id WHERE b.name = ?",
    )
    .bind(&scene.basket)
    .fetch_all(db)
    .await?;
    ensure!(outputs.len() == 1, "one token: {}", outputs.len());
    let (output, script, satoshis, spendable) = outputs.into_iter().next().context("token")?;
    ensure!((satoshis, spendable) == (1, 1), "one spendable satoshi");
    let mut tags: Vec<String> = sqlx::query_scalar(
        "SELECT t.tag FROM output_tags t JOIN output_tags_map m \
         ON m.output_tag_id = t.output_tag_id WHERE m.output_id = ?",
    )
    .bind(output)
    .fetch_all(db)
    .await?;
    tags.sort();
    ensure!(tags == scene.tags, "{tags:?}");
    let described: Vec<String> =
        sqlx::query_scalar("SELECT description FROM transactions WHERE description = ?")
            .bind(&scene.description)
            .fetch_all(db)
            .await?;
    ensure!(described.len() == 1, "one grant action");
    ensure!(underlying.services().call_count("post_beef") == 0);

    // The token: a PushDrop of six fields and a signature, each field the
    // wallet's own ciphertext of the scene's plaintext.
    let decoded = PushDrop::decode(&LockingScript::from_binary(&script)?)?;
    ensure!(
        decoded.fields.len() == scene.fields.len() + 1,
        "fields and a signature"
    );
    for (field, expected) in decoded.fields.iter().zip(&scene.fields) {
        let args = DecryptArgs {
            ciphertext: field.clone(),
            protocol_id: Protocol::new(
                SecurityLevel::Counterparty,
                "admin permission token encryption",
            ),
            key_id: "1".to_string(),
            ..Default::default()
        };
        let plain = underlying
            .decrypt(args, Some(&scene.admin))
            .await?
            .plaintext;
        let plain = String::from_utf8(plain)?;
        if expected == "<expiry>" {
            ensure!(plain.parse::<i64>()? > chrono::Utc::now().timestamp());
        } else {
            ensure!(plain == *expected, "{plain} != {expected}");
        }
    }

    // A restart: a new manager over the same store, no handler, no cache.
    let (_, _, restarted) = door(&storage, &root, &scene.admin).await?;
    let admitted = key_call(&restarted, &scene, false).await?;
    ensure!(admitted == granted, "the token admits the same key");
    ensure!(prompts.load(Ordering::SeqCst) == 1, "no further prompt");
    Ok(())
}
