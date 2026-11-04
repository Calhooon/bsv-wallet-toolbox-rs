//! The grant's witnesses (#751 step 2a): a persistent grant mints the
//! token the step 1 lookup reads (ts-stack@edf6e03
//! `WalletPermissionsManager.ts:1104-1149`, `:3349-3392`), a denial and an
//! ephemeral grant mint nothing (`:1189-1205`, `:1179`), and a duplicate
//! grant inside the cover mints once (`:1112-1124`). The manager mints
//! through a full wallet over the in-memory store, funded with one proven
//! coin of its own; `acceptDelayedBroadcast` leaves the mint unsent, so the
//! mock services see no post.

use std::time::Duration;

use bsv_rs::primitives::ec::PublicKey;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::GetPublicKeyArgs;
use tokio::sync::mpsc;

use super::permission_token_lookup_tests::{
    basket_call, certificate_call, live, protocol_call, refusal, scene, strings, verifier,
    witness_protocol, Scene, ADMIN, APP, BASKET, CERT_TYPE,
};
use super::*;

/// The P2PKH lock of the wallet's BRC-29 change key for the derivation
/// `cHJlZml4 c3VmZml4`, so the signer signs the coin.
async fn change_lock(scene: &Scene) -> LockingScript {
    let args = GetPublicKeyArgs {
        protocol_id: Some(Protocol::new(SecurityLevel::Counterparty, "3241645161d8")),
        key_id: Some("cHJlZml4 c3VmZml4".to_string()),
        counterparty: Some(Counterparty::Self_),
        for_self: Some(true),
        ..Default::default()
    };
    let key = scene.counted.inner.get_public_key(args, Some(ADMIN)).await;
    let key = PublicKey::from_hex(&key.unwrap().public_key).unwrap();
    let hash = bsv_rs::primitives::hash160(&key.to_compressed());
    LockingScript::from_binary(&[&[0x76, 0xa9, 0x14][..], &hash, &[0x88, 0xac]].concat()).unwrap()
}

/// One proven 100,000-satoshi coin, change in the default basket, its
/// parent and height its own per `tag`.
async fn fund(scene: &Scene, tag: u8) {
    let storage = scene.counted.inner.storage();
    let db = storage.writer();
    let user: i64 = sqlx::query_scalar("SELECT user_id FROM users")
        .fetch_one(db)
        .await
        .unwrap();
    let basket = storage.find_or_create_default_basket(user).await.unwrap();
    let mut parent = Transaction::new();
    let mut input = TransactionInput::new(format!("{tag:02x}").repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    parent.inputs.push(input);
    let coin = change_lock(scene).await;
    parent
        .outputs
        .push(TransactionOutput::new(100_000, coin.clone()));
    let (txid, raw) = (parent.id(), parent.to_binary());
    let height = bsv_rs::block::ChainParams::mainnet().genesis_height + 500 + u32::from(tag);
    let proof = MerklePath::from_coinbase_txid(&txid, height).to_binary();
    let proven = "INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, \
                  merkle_path, raw_tx) VALUES (?1, ?2, 0, ?3, ?1, ?4, ?5)";
    let (hash, txid_ref) = ("cc".repeat(32), txid.as_str());
    sqlx::query(proven)
        .bind(txid_ref)
        .bind(height)
        .bind(hash)
        .bind(proof)
        .bind(&raw)
        .execute(db)
        .await
        .unwrap();
    let funding = "INSERT INTO transactions (user_id, status, reference, is_outgoing, \
                   satoshis, version, lock_time, description, txid, raw_tx) \
                   VALUES (?1, 'completed', ?4, 0, 100000, 1, 0, 'grant funding', ?2, ?3)";
    let transaction = sqlx::query(funding)
        .bind(user)
        .bind(txid_ref)
        .bind(&raw)
        .bind(format!("grant-funding-{tag}"))
        .execute(db)
        .await
        .unwrap()
        .last_insert_rowid();
    let change = "INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, \
                  locking_script, txid, type, spendable, change, derivation_prefix, \
                  derivation_suffix, provided_by, purpose, output_description) \
                  VALUES (?1, ?2, ?3, 0, 100000, ?4, ?5, 'P2PKH', 1, 1, 'cHJlZml4', 'c3VmZml4', \
                  'storage', 'change', 'grant funding')";
    sqlx::query(change)
        .bind(user)
        .bind(transaction)
        .bind(basket.basket_id)
        .bind(coin.to_binary())
        .bind(txid_ref)
        .execute(db)
        .await
        .unwrap();
}

/// The step 1 scene with two coins: the port's selection does not spend
/// the change of a mint still `sending`, so each mint takes its own.
async fn funded() -> Scene {
    let scene = scene().await;
    fund(&scene, 0xfe).await;
    fund(&scene, 0xfd).await;
    scene
}

/// The tokens in `basket`: each output's locking script and its tags,
/// sorted, read from the store.
async fn tokens(scene: &Scene, basket: &str) -> Vec<(Vec<u8>, Vec<String>)> {
    let db = scene.counted.inner.storage().pool();
    let outputs: Vec<(i64, Vec<u8>, i64)> = sqlx::query_as(
        "SELECT o.output_id, o.locking_script, o.satoshis FROM outputs o \
         JOIN output_baskets b ON b.basket_id = o.basket_id WHERE b.name = ?",
    )
    .bind(basket)
    .fetch_all(db)
    .await
    .unwrap();
    let mut found = Vec::new();
    for (id, script, satoshis) in outputs {
        assert_eq!(satoshis, 1, "a token holds one satoshi");
        let mut tags: Vec<String> = sqlx::query_scalar(
            "SELECT t.tag FROM output_tags t JOIN output_tags_map m \
             ON m.output_tag_id = t.output_tag_id WHERE m.output_id = ?",
        )
        .bind(id)
        .fetch_all(db)
        .await
        .unwrap();
        tags.sort();
        found.push((script, tags));
    }
    found
}

/// Every admin basket's token count.
async fn minted(scene: &Scene) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM outputs o JOIN output_baskets b \
         ON b.basket_id = o.basket_id WHERE b.name LIKE 'admin %'",
    )
    .fetch_one(scene.counted.inner.storage().pool())
    .await
    .unwrap()
}

/// The token's data fields decrypted, the signature checked by the step 1
/// source check.
async fn plaintexts(scene: &Scene, script: &[u8], count: usize) -> Vec<String> {
    let script = LockingScript::from_binary(script).unwrap();
    let decoded = PushDrop::decode(&script).unwrap();
    let fields = scene.manager.signed_token_fields(decoded, count).await;
    let mut texts = Vec::new();
    for field in fields.expect("signed under the token key") {
        texts.push(scene.manager.decrypt_token_text(&field).await.unwrap());
    }
    texts
}

/// The manager's cache emptied and its cover ended, as five minutes do.
async fn age(manager: &WalletPermissionsManager) {
    let ttl = Duration::from_secs(CACHE_TTL_SECS + 1);
    let past = Instant::now().checked_sub(ttl).unwrap();
    for entry in manager.permission_cache.write().await.values_mut() {
        entry.cached_at = past;
    }
    manager.recent_grants.lock().unwrap().clear();
}

/// The witness protocol call on `manager`, not seeking.
async fn seek_false(manager: &WalletPermissionsManager) -> Result<bool> {
    let usage = PermissionUsageType::Generic;
    let protocol = witness_protocol();
    let call = manager.ensure_protocol_permission_seeking(
        APP,
        false,
        false,
        &protocol,
        Some("self"),
        None,
        usage,
    );
    call.await
}

/// A new manager over the same wallet with no handler: a restart.
fn restarted(scene: &Scene) -> WalletPermissionsManager {
    let config = WalletPermissionsManagerConfig::all_enabled();
    WalletPermissionsManager::new(scene.counted.clone(), ADMIN.to_string(), config)
}

const PROTOCOL_TAGS: [&str; 5] = [
    "counterparty self",
    "originator app.example.com",
    "privileged false",
    "protocolname ticket stub",
    "protocolsecuritylevel 2",
];

#[tokio::test]
async fn a_protocol_grant_mints_one_token_the_lookup_reads_back() {
    let scene = funded().await;
    assert!(protocol_call(&scene, true).await.unwrap());
    let minted = tokens(&scene, "admin protocol-permission").await;
    assert_eq!(minted.len(), 1);
    let (script, tags) = &minted[0];
    assert_eq!(*tags, strings(&PROTOCOL_TAGS));
    let texts = plaintexts(&scene, script, 6).await;
    let expiry: u64 = texts[1].parse().unwrap();
    assert!(expiry > Utc::now().timestamp().unsigned_abs());
    let rest = strings(&["false", "2", "ticket stub", "self"]);
    assert_eq!((&texts[0], &texts[2..]), (&APP.to_string(), &rest[..]));
    let query = TokenQuery::Protocol {
        privileged: false,
        protocol: &witness_protocol(),
        counterparty: "self",
    };
    let found = scene.manager.find_stored_token(APP, &query).await.unwrap();
    assert_eq!(found, Some(expiry), "the step 1 lookup reads the mint");
    let services = scene.counted.inner.services();
    assert_eq!(services.call_count("post_beef"), 0, "delayed: not sent");
}

#[tokio::test]
async fn a_seek_false_call_is_admitted_from_the_token_after_the_cache_and_a_restart() {
    let scene = funded().await;
    assert!(protocol_call(&scene, true).await.unwrap());
    age(&scene.manager).await;
    assert!(protocol_call(&scene, false).await.unwrap());
    let manager = restarted(&scene);
    let admitted = seek_false(&manager).await;
    assert!(admitted.unwrap(), "the restarted manager reads the token");
    assert_eq!(scene.prompts.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_basket_grant_mints_its_token_and_admits_after_a_restart() {
    let scene = funded().await;
    assert!(basket_call(&scene).await.unwrap());
    let minted = tokens(&scene, "admin basket-access").await;
    assert_eq!(minted.len(), 1);
    let tags = strings(&["basket tickets", "originator app.example.com"]);
    assert_eq!(minted[0].1, tags);
    let texts = plaintexts(&scene, &minted[0].0, 3).await;
    assert_eq!((&*texts[0], &*texts[2]), (APP, BASKET));
    let manager = restarted(&scene);
    let admitted = manager.ensure_basket_access(APP, BASKET, None, BasketUsageType::Listing);
    assert!(admitted.await.unwrap(), "no handler: the token admits");
}

#[tokio::test]
async fn a_certificate_grant_mints_its_token_and_admits_after_a_restart() {
    let scene = funded().await;
    assert!(certificate_call(&scene).await.unwrap());
    let minted = tokens(&scene, "admin certificate-access").await;
    assert_eq!(minted.len(), 1);
    let mut tags = vec![
        "originator app.example.com".to_string(),
        "privileged false".to_string(),
        format!("type {}", CERT_TYPE.to_lowercase()),
        format!("verifier {}", verifier()),
    ];
    tags.sort();
    assert_eq!(minted[0].1, tags);
    let texts = plaintexts(&scene, &minted[0].0, 6).await;
    let held = [CERT_TYPE.to_string(), r#"["seat"]"#.to_string(), verifier()];
    assert_eq!((&*texts[2], &texts[3..]), ("false", &held[..]));
    let manager = restarted(&scene);
    let fields = strings(&["seat"]);
    let verifier = verifier();
    let disclosure = CertificateUsageType::Disclosure;
    let admitted = manager
        .ensure_certificate_access(APP, false, &verifier, CERT_TYPE, &fields, None, disclosure);
    assert!(admitted.await.unwrap(), "no handler: the token admits");
}

/// A manager over `scene`'s wallet whose handler publishes each request id
/// and leaves the answer to the host.
async fn asking(
    scene: &Scene,
) -> (
    Arc<WalletPermissionsManager>,
    mpsc::UnboundedReceiver<String>,
) {
    let manager = Arc::new(restarted(scene));
    let (ids, asked) = mpsc::unbounded_channel();
    manager
        .set_permission_request_handler(Arc::new(move |request| {
            let _ = ids.send(request.request_id.clone().unwrap());
            Box::pin(std::future::pending())
        }))
        .await;
    (manager, asked)
}

/// The witness protocol call on `manager`, seeking, in its own task.
fn spawn_call(manager: &Arc<WalletPermissionsManager>) -> tokio::task::JoinHandle<Result<bool>> {
    let manager = Arc::clone(manager);
    tokio::spawn(async move {
        let protocol = witness_protocol();
        let usage = PermissionUsageType::Generic;
        let call =
            manager.ensure_protocol_permission(APP, false, &protocol, Some("self"), None, usage);
        call.await
    })
}

#[tokio::test]
async fn a_denial_refuses_the_caller_and_mints_nothing() {
    let scene = funded().await;
    let (manager, mut asked) = asking(&scene).await;
    let call = spawn_call(&manager);
    let id = asked.recv().await.unwrap();
    manager.deny_permission(&id).unwrap();
    assert_eq!(refusal(call.await.unwrap()), "Permission denied.");
    assert_eq!(minted(&scene).await, 0);
    let again = manager.deny_permission(&id);
    assert!(matches!(again, Err(Error::InvalidArgument(m)) if m == "Request ID not found."));
}

#[tokio::test]
async fn an_ephemeral_grant_admits_its_callers_alone_and_mints_nothing() {
    let scene = funded().await;
    let (manager, mut asked) = asking(&scene).await;
    let call = spawn_call(&manager);
    let id = asked.recv().await.unwrap();
    manager
        .grant_permission(&id, None, true, None)
        .await
        .unwrap();
    assert!(call.await.unwrap().unwrap());
    assert_eq!(minted(&scene).await, 0);
    let refused = seek_false(&manager).await;
    let absent = "No protocol permission token found (seekPermission=false).";
    assert_eq!(refusal(refused), absent, "nothing cached, nothing stored");
}

#[tokio::test]
async fn a_host_grant_by_request_id_mints_and_admits_the_caller() {
    let scene = funded().await;
    let (manager, mut asked) = asking(&scene).await;
    let call = spawn_call(&manager);
    let id = asked.recv().await.unwrap();
    let expiry = live().unsigned_abs();
    manager
        .grant_permission(&id, Some(expiry), false, None)
        .await
        .unwrap();
    assert!(call.await.unwrap().unwrap());
    assert_eq!(minted(&scene).await, 1);
}

#[tokio::test]
async fn every_spending_grant_mints_its_amount_and_is_never_covered() {
    let scene = funded().await;
    let request = PermissionRequest {
        spending: Some(SpendingPermissionDetails {
            satoshis: 2_500,
            line_items: None,
        }),
        ..PermissionRequest::bare(PermissionType::Spending, APP)
    };
    let flow = &scene.manager;
    assert!(flow.request_permission_flow(request.clone()).await.unwrap());
    assert!(flow.request_permission_flow(request).await.unwrap());
    let minted = tokens(&scene, "admin spending-authorization").await;
    assert_eq!(minted.len(), 2, "spending skips no mint (:1112-1115)");
    assert_eq!(minted[0].1, strings(&["originator app.example.com"]));
    let texts = plaintexts(&scene, &minted[0].0, 2).await;
    assert_eq!(texts, strings(&[APP, "2500"]));
}

#[tokio::test]
async fn a_duplicate_grant_inside_the_cover_mints_once() {
    let scene = funded().await;
    let request = PermissionRequest {
        privileged: Some(false),
        protocol_id: Some(witness_protocol()),
        counterparty: Some("self".to_string()),
        ..PermissionRequest::bare(PermissionType::Protocol, APP)
    };
    let flow = &scene.manager;
    assert!(flow.request_permission_flow(request.clone()).await.unwrap());
    assert!(flow.request_permission_flow(request).await.unwrap());
    assert_eq!(scene.prompts.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(minted(&scene).await, 1, "the second grant is covered");
}

#[tokio::test]
async fn a_caller_dropped_while_it_waits_leaves_the_next_caller_its_own_prompt() {
    let scene = funded().await;
    let (manager, mut asked) = asking(&scene).await;
    let dropped = spawn_call(&manager);
    asked.recv().await.unwrap();
    dropped.abort();
    assert!(dropped.await.unwrap_err().is_cancelled());
    let call = spawn_call(&manager);
    let prompt = tokio::time::timeout(Duration::from_secs(5), asked.recv()).await;
    let id = prompt.expect("the next caller is asked again").unwrap();
    manager
        .grant_permission(&id, None, true, None)
        .await
        .unwrap();
    assert!(call.await.unwrap().unwrap());
}

#[tokio::test]
async fn a_proto_wallets_create_and_list_refusals_read_as_no_token_each_by_its_own_verb() {
    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::wallet::{ProtoWallet, WalletInterface};
    let proto = ProtoWallet::new(Some(PrivateKey::random()));
    let args = serde_json::json!({ "description": "Grant protocol permission" });
    let created = proto.create_action(serde_json::from_value(args).unwrap(), Some(ADMIN));
    let created = created.await.unwrap_err();
    let text = "createAction requires a full wallet implementation with UTXO management";
    assert!(matches!(&created, bsv_rs::Error::WalletError(m) if m == text));
    assert!(proto_wallet_refuses(&created, "createAction"));
    assert!(!proto_wallet_refuses(&created, "listOutputs"));
    let args = serde_json::json!({ "basket": "admin protocol-permission" });
    let listed = proto.list_outputs(serde_json::from_value(args).unwrap(), Some(ADMIN));
    let listed = listed.await.unwrap_err();
    let text = "listOutputs requires a full wallet implementation with UTXO management";
    assert!(matches!(&listed, bsv_rs::Error::WalletError(m) if m == text));
    assert!(cannot_list(&listed));
    assert!(!proto_wallet_refuses(&listed, "createAction"));
}
