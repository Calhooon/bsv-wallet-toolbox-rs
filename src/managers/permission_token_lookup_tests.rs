//! The stored-token lookup's witnesses (#751 step 1): a protocol, basket and
//! certificate token seeded as the reference's mint writes one
//! (ts-stack@edf6e03 `WalletPermissionsManager.ts:3349-3392`: the fields
//! of `buildPushdropFields` `:3708-3749` encrypted under
//! `[2, 'admin permission token encryption']` keyID `"1"`, locked with
//! `PushDrop.lock(fields, protocol, '1', 'self', true, true)`, one satoshi
//! in the kind's admin basket under `buildTagsForRequest`'s tags
//! `:3755-3787`). The wallet's own `encrypt`, `create_signature` and
//! `get_public_key` make the fields, the signature and the key as the
//! admin originator; the transaction, its proof and the output, basket and
//! tag rows are written to the store directly, so nothing is minted, funded
//! or broadcast. The manager reads through a full wallet over that store,
//! wrapped in a double that counts its reads.

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::*;

use super::*;
use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::{StorageSqlx, Wallet};

const ADMIN: &str = "wallet.admin";
const APP: &str = "app.example.com";
const BASKET: &str = "tickets";
const CERT_TYPE: &str = "dGhlIHdpdG5lc3MgY2VydGlmaWNhdGUgdHlwZSAzMiBi";

type FullWallet = Wallet<StorageSqlx, MockWalletServices>;

/// The full wallet behind the manager, its three token reads counted.
struct Counted {
    inner: FullWallet,
    lists: AtomicUsize,
    decrypts: AtomicUsize,
    keys: AtomicUsize,
}

impl Counted {
    /// The reads so far: `(list_outputs, decrypt, get_public_key)`.
    fn reads(&self) -> (usize, usize, usize) {
        (
            self.lists.load(Ordering::SeqCst),
            self.decrypts.load(Ordering::SeqCst),
            self.keys.load(Ordering::SeqCst),
        )
    }
}

// The uncounted calls, listed alphabetically, delegated as they are.
macro_rules! counted_delegates {
    ($($name:ident($($arg:ident: $arg_type:ty)?) -> $result:ty;)*) => {
        #[async_trait]
        impl WalletInterface for Counted {
            async fn get_public_key(&self, args: GetPublicKeyArgs, by: Option<&str>) -> bsv_rs::Result<GetPublicKeyResult> {
                self.keys.fetch_add(1, Ordering::SeqCst);
                self.inner.get_public_key(args, by).await
            }
            async fn list_outputs(&self, args: ListOutputsArgs, by: Option<&str>) -> bsv_rs::Result<ListOutputsResult> {
                self.lists.fetch_add(1, Ordering::SeqCst);
                self.inner.list_outputs(args, by).await
            }
            async fn decrypt(&self, args: DecryptArgs, by: Option<&str>) -> bsv_rs::Result<DecryptResult> {
                self.decrypts.fetch_add(1, Ordering::SeqCst);
                self.inner.decrypt(args, by).await
            }
            $(async fn $name(&self, $($arg: $arg_type,)? by: Option<&str>) -> bsv_rs::Result<$result> {
                WalletInterface::$name(&self.inner, $($arg,)? by).await
            })*
        }
    };
}

counted_delegates! {
    abort_action(args: AbortActionArgs) -> AbortActionResult;
    acquire_certificate(args: AcquireCertificateArgs) -> WalletCertificate;
    create_action(args: CreateActionArgs) -> CreateActionResult;
    create_hmac(args: CreateHmacArgs) -> CreateHmacResult;
    create_signature(args: CreateSignatureArgs) -> CreateSignatureResult;
    discover_by_attributes(args: DiscoverByAttributesArgs) -> DiscoverCertificatesResult;
    discover_by_identity_key(args: DiscoverByIdentityKeyArgs) -> DiscoverCertificatesResult;
    encrypt(args: EncryptArgs) -> EncryptResult;
    get_header_for_height(args: GetHeaderArgs) -> GetHeaderResult;
    get_height() -> GetHeightResult;
    get_network() -> GetNetworkResult;
    get_version() -> GetVersionResult;
    internalize_action(args: InternalizeActionArgs) -> InternalizeActionResult;
    is_authenticated() -> AuthenticatedResult;
    list_actions(args: ListActionsArgs) -> ListActionsResult;
    list_certificates(args: ListCertificatesArgs) -> ListCertificatesResult;
    prove_certificate(args: ProveCertificateArgs) -> ProveCertificateResult;
    relinquish_certificate(args: RelinquishCertificateArgs) -> RelinquishCertificateResult;
    relinquish_output(args: RelinquishOutputArgs) -> RelinquishOutputResult;
    reveal_counterparty_key_linkage(args: interface::RevealCounterpartyKeyLinkageArgs) -> RevealCounterpartyKeyLinkageResult;
    reveal_specific_key_linkage(args: interface::RevealSpecificKeyLinkageArgs) -> RevealSpecificKeyLinkageResult;
    sign_action(args: SignActionArgs) -> SignActionResult;
    verify_hmac(args: VerifyHmacArgs) -> VerifyHmacResult;
    verify_signature(args: VerifySignatureArgs) -> VerifySignatureResult;
    wait_for_authentication() -> AuthenticatedResult;
}

/// A manager over the counted full wallet, every kind's prompt on, and a
/// handler that counts its prompts and grants each for an hour.
struct Scene {
    counted: Arc<Counted>,
    manager: WalletPermissionsManager,
    prompts: Arc<AtomicUsize>,
}

async fn scene() -> Scene {
    let storage = StorageSqlx::in_memory().await.unwrap();
    let root = PrivateKey::random();
    let identity = root.public_key().to_hex();
    storage
        .migrate("token-lookup-tests", &identity)
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let inner = Wallet::new(Some(root), storage, MockWalletServices::new())
        .await
        .unwrap();
    let counted = Arc::new(Counted {
        inner,
        lists: AtomicUsize::new(0),
        decrypts: AtomicUsize::new(0),
        keys: AtomicUsize::new(0),
    });
    let config = WalletPermissionsManagerConfig::all_enabled();
    let manager = WalletPermissionsManager::new(counted.clone(), ADMIN.to_string(), config);
    let prompts = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&prompts);
    manager
        .set_permission_request_handler(Arc::new(move |request| {
            seen.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                let token = serde_json::json!({
                    "txid": "", "tx": [], "outputIndex": 0, "outputScript": "",
                    "satoshis": 1, "originator": request.originator,
                    "expiry": Utc::now().timestamp() + 3600,
                });
                Ok(serde_json::from_value(token).unwrap())
            })
        }))
        .await;
    Scene {
        counted,
        manager,
        prompts,
    }
}

/// The token protocol, keyID `"1"`, as the mint and the lookup use it.
fn token_protocol() -> Protocol {
    Protocol::new(
        SecurityLevel::Counterparty,
        "admin permission token encryption",
    )
}

/// The token's locking script as `PushDrop.lock(fields, protocol, '1',
/// 'self', true, true)` writes it: each field encrypted by the wallet as
/// the admin originator, a signature over the ciphertexts by the token key,
/// that key's lock ahead of them.
async fn token_script(wallet: &FullWallet, plaintexts: &[String]) -> LockingScript {
    let mut fields = Vec::new();
    for plaintext in plaintexts {
        let args = EncryptArgs {
            plaintext: plaintext.as_bytes().to_vec(),
            protocol_id: token_protocol(),
            key_id: "1".to_string(),
            ..Default::default()
        };
        fields.push(wallet.encrypt(args, Some(ADMIN)).await.unwrap().ciphertext);
    }
    let signed = CreateSignatureArgs {
        data: Some(fields.concat()),
        protocol_id: token_protocol(),
        key_id: "1".to_string(),
        counterparty: Some(Counterparty::Self_),
        ..Default::default()
    };
    let signature = wallet.create_signature(signed, Some(ADMIN)).await.unwrap();
    fields.push(signature.signature);
    let key_args = GetPublicKeyArgs {
        protocol_id: Some(token_protocol()),
        key_id: Some("1".to_string()),
        counterparty: Some(Counterparty::Self_),
        for_self: Some(true),
        ..Default::default()
    };
    let key = wallet.get_public_key(key_args, Some(ADMIN)).await.unwrap();
    let key = PublicKey::from_hex(&key.public_key).unwrap();
    PushDrop::new(key, fields).lock()
}

/// Each seed's parent outpoint, so no two seeds share a txid.
static SEEDS: AtomicU32 = AtomicU32::new(0);

/// Writes `script` as output 0 of a proven one-satoshi transaction (its
/// parent outpoint `SEEDS`' next count) in `basket`
/// under `tags`, straight to the store.
async fn store_token(wallet: &FullWallet, script: LockingScript, basket: &str, tags: &[String]) {
    let tag = SEEDS.fetch_add(1, Ordering::SeqCst);
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(format!("{tag:064x}"), 0xffff_ffff);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput {
        satoshis: Some(1),
        locking_script: script.clone(),
        change: false,
    });
    let (txid, raw) = (tx.id(), tx.to_binary());
    let height = bsv_rs::block::ChainParams::mainnet().genesis_height + 1 + tag;
    let bump = MerklePath::from_coinbase_txid(&txid, height);
    let db = wallet.storage().writer();
    let user: i64 = sqlx::query_scalar("SELECT user_id FROM users")
        .fetch_one(db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) VALUES (?, ?, 0, ?, ?, ?, ?)")
        .bind(&txid).bind(height).bind("bb".repeat(32)).bind(&txid).bind(bump.to_binary()).bind(&raw)
        .execute(db).await.unwrap();
    let transaction = sqlx::query("INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx) VALUES (?, 'completed', ?, 1, 1, 1, 0, 'a permission token', ?, ?)")
        .bind(user).bind(format!("token-{txid}")).bind(&txid).bind(&raw)
        .execute(db).await.unwrap().last_insert_rowid();
    sqlx::query("INSERT OR IGNORE INTO output_baskets (user_id, name) VALUES (?, ?)")
        .bind(user)
        .bind(basket)
        .execute(db)
        .await
        .unwrap();
    let basket_id: i64 =
        sqlx::query_scalar("SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = ?")
            .bind(user)
            .bind(basket)
            .fetch_one(db)
            .await
            .unwrap();
    let output = sqlx::query("INSERT INTO outputs (user_id, transaction_id, basket_id, vout, satoshis, locking_script, txid, type, spendable, change, provided_by, purpose, output_description) VALUES (?, ?, ?, 0, 1, ?, ?, 'custom', 1, 0, 'you', '', 'a permission token')")
        .bind(user).bind(transaction).bind(basket_id).bind(script.to_binary()).bind(&txid)
        .execute(db).await.unwrap().last_insert_rowid();
    for tag in tags {
        sqlx::query("INSERT OR IGNORE INTO output_tags (user_id, tag) VALUES (?, ?)")
            .bind(user)
            .bind(tag)
            .execute(db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO output_tags_map (output_tag_id, output_id) SELECT output_tag_id, ? FROM output_tags WHERE user_id = ? AND tag = ?")
            .bind(output).bind(user).bind(tag).execute(db).await.unwrap();
    }
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(ToString::to_string).collect()
}

/// The witness protocol at level 2 with the counterparty `self`.
fn witness_protocol() -> Protocol {
    Protocol::new(SecurityLevel::Counterparty, "ticket stub")
}

/// A protocol token for `APP` on the witness protocol, everyday, `self`.
async fn seed_protocol(scene: &Scene, expiry: i64) {
    let fields = strings(&[
        APP,
        &expiry.to_string(),
        "false",
        "2",
        "ticket stub",
        "self",
    ]);
    let script = token_script(&scene.counted.inner, &fields).await;
    let tags = strings(&[
        "originator app.example.com",
        "privileged false",
        "protocolName ticket stub",
        "protocolSecurityLevel 2",
        "counterparty self",
    ]);
    store_token(
        &scene.counted.inner,
        script,
        "admin protocol-permission",
        &tags,
    )
    .await;
}

async fn seed_basket(scene: &Scene, expiry: i64) {
    let fields = strings(&[APP, &expiry.to_string(), BASKET]);
    let script = token_script(&scene.counted.inner, &fields).await;
    let tags = strings(&["originator app.example.com", "basket tickets"]);
    store_token(&scene.counted.inner, script, "admin basket-access", &tags).await;
}

/// A certificate token holding the fields `name` and `seat`.
async fn seed_certificate(scene: &Scene, expiry: i64) {
    let verifier = verifier();
    let fields = strings(&[
        APP,
        &expiry.to_string(),
        "false",
        CERT_TYPE,
        r#"["name","seat"]"#,
        &verifier,
    ]);
    let script = token_script(&scene.counted.inner, &fields).await;
    let tags = vec![
        "originator app.example.com".to_string(),
        "privileged false".to_string(),
        format!("type {CERT_TYPE}"),
        format!("verifier {verifier}"),
    ];
    store_token(
        &scene.counted.inner,
        script,
        "admin certificate-access",
        &tags,
    )
    .await;
}

fn verifier() -> String {
    PrivateKey::from_hex(&format!("{:064x}", 41))
        .unwrap()
        .public_key()
        .to_hex()
}

fn live() -> i64 {
    Utc::now().timestamp() + 3600
}

fn expired() -> i64 {
    Utc::now().timestamp() - 3600
}

async fn protocol_call(scene: &Scene, seek: bool) -> Result<bool> {
    scene
        .manager
        .ensure_protocol_permission_seeking(
            APP,
            false,
            seek,
            &witness_protocol(),
            Some("self"),
            None,
            PermissionUsageType::Generic,
        )
        .await
}

async fn basket_call(scene: &Scene) -> Result<bool> {
    scene
        .manager
        .ensure_basket_access(APP, BASKET, None, BasketUsageType::Listing)
        .await
}

async fn certificate_call(scene: &Scene) -> Result<bool> {
    certificate_call_for(scene, &["seat"]).await
}

/// A disclosure of `fields` of the witness certificate type to the verifier.
async fn certificate_call_for(scene: &Scene, fields: &[&str]) -> Result<bool> {
    let verifier = verifier();
    scene
        .manager
        .ensure_certificate_access(
            APP,
            false,
            &verifier,
            CERT_TYPE,
            &strings(fields),
            None,
            CertificateUsageType::Disclosure,
        )
        .await
}

fn prompts(scene: &Scene) -> usize {
    scene.prompts.load(Ordering::SeqCst)
}

fn refusal(result: Result<bool>) -> String {
    match result {
        Err(Error::AccessDenied(message)) => message,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

// =========================================================================
// Protocol (DPACP): `findProtocolToken` ahead of `:1533` and the prompt
// =========================================================================

#[tokio::test]
async fn a_live_protocol_token_admits_a_seek_false_call_with_no_prompt() {
    let scene = scene().await;
    seed_protocol(&scene, live()).await;
    assert!(protocol_call(&scene, false).await.unwrap());
    assert_eq!(prompts(&scene), 0);
    // The cost of one admitted call: the tagged list finds the token on the
    // trimmed originator's pass; the key read once; six fields decrypted.
    assert_eq!(scene.counted.reads(), (1, 6, 1));
    // The grant is cached as a prompt's is (`:1565`): no read again.
    assert!(protocol_call(&scene, false).await.unwrap());
    assert_eq!(scene.counted.reads(), (1, 6, 1));
}

#[tokio::test]
async fn a_live_protocol_token_admits_a_seek_true_call_with_no_prompt() {
    let scene = scene().await;
    seed_protocol(&scene, live()).await;
    assert!(protocol_call(&scene, true).await.unwrap());
    assert_eq!(prompts(&scene), 0);
}

#[tokio::test]
async fn no_protocol_token_refuses_seek_false_and_prompts_seek_true_as_before() {
    let scene = scene().await;
    let refused = refusal(protocol_call(&scene, false).await);
    assert_eq!(
        refused,
        "No protocol permission token found (seekPermission=false)."
    );
    // The cost of an absent token: the tagged pass and the untagged pass,
    // `APP` already normalized; nothing to decrypt.
    assert_eq!(scene.counted.reads(), (2, 0, 0));
    assert_eq!(prompts(&scene), 0);
    assert!(protocol_call(&scene, true).await.unwrap());
    assert_eq!(prompts(&scene), 1);
    assert_eq!(scene.counted.reads(), (4, 0, 0));
}

#[tokio::test]
async fn an_expired_protocol_token_refuses_seek_false_and_prompts_seek_true() {
    let scene = scene().await;
    seed_protocol(&scene, expired()).await;
    let refused = refusal(protocol_call(&scene, false).await);
    assert_eq!(
        refused,
        "Protocol permission expired and no further user consent allowed (seekPermission=false)."
    );
    assert_eq!(prompts(&scene), 0);
    assert!(protocol_call(&scene, true).await.unwrap());
    assert_eq!(prompts(&scene), 1);
}

#[tokio::test]
async fn a_protocol_token_for_another_counterparty_does_not_answer() {
    let scene = scene().await;
    seed_protocol(&scene, live()).await;
    let other = verifier();
    let result = scene
        .manager
        .ensure_protocol_permission_seeking(
            APP,
            false,
            false,
            &witness_protocol(),
            Some(&other),
            None,
            PermissionUsageType::Generic,
        )
        .await;
    assert_eq!(
        refusal(result),
        "No protocol permission token found (seekPermission=false)."
    );
}

// =========================================================================
// Basket (DBAP) and certificate (DCAP): `findBasketToken` and
// `findCertificateToken` ahead of the prompt; the port's two doors take no
// `seekPermission`, so a missing or expired token prompts as before.
// =========================================================================

#[tokio::test]
async fn a_live_basket_token_admits_with_no_prompt() {
    let scene = scene().await;
    seed_basket(&scene, live()).await;
    assert!(basket_call(&scene).await.unwrap());
    assert_eq!(prompts(&scene), 0);
    assert_eq!(scene.counted.reads(), (1, 3, 1));
}

#[tokio::test]
async fn no_basket_token_or_an_expired_one_prompts_as_before() {
    let scene = scene().await;
    assert!(basket_call(&scene).await.unwrap());
    assert_eq!(prompts(&scene), 1);
    assert_eq!(scene.counted.reads(), (2, 0, 0));
    let scene = scene_with_basket(expired()).await;
    assert!(basket_call(&scene).await.unwrap());
    assert_eq!(prompts(&scene), 1);
}

async fn scene_with_basket(expiry: i64) -> Scene {
    let scene = scene().await;
    seed_basket(&scene, expiry).await;
    scene
}

#[tokio::test]
async fn a_live_certificate_token_holding_the_fields_admits_with_no_prompt() {
    let scene = scene().await;
    seed_certificate(&scene, live()).await;
    assert!(certificate_call(&scene).await.unwrap());
    assert_eq!(prompts(&scene), 0);
    assert_eq!(scene.counted.reads(), (1, 6, 1));
}

#[tokio::test]
async fn no_certificate_token_or_an_expired_one_prompts_as_before() {
    let scene = scene().await;
    assert!(certificate_call(&scene).await.unwrap());
    assert_eq!(prompts(&scene), 1);
    assert_eq!(scene.counted.reads(), (2, 0, 0));
    let scene = self::scene().await;
    seed_certificate(&scene, expired()).await;
    assert!(certificate_call(&scene).await.unwrap());
    assert_eq!(prompts(&scene), 1);
}

#[tokio::test]
async fn a_certificate_token_missing_a_requested_field_does_not_answer() {
    let scene = scene().await;
    seed_certificate(&scene, live()).await;
    assert!(certificate_call_for(&scene, &["seat", "email"])
        .await
        .unwrap());
    assert_eq!(prompts(&scene), 1);
}

// =========================================================================
// The source check (`:2798-2828`): a token under another key is not this
// wallet's
// =========================================================================

#[tokio::test]
async fn a_token_signed_under_another_key_does_not_answer() {
    let scene = scene().await;
    let stranger = scene_wallet_script().await;
    let tags = strings(&["originator app.example.com", "basket tickets"]);
    store_token(&scene.counted.inner, stranger, "admin basket-access", &tags).await;
    assert!(basket_call(&scene).await.unwrap());
    assert_eq!(prompts(&scene), 1);
}

/// A basket token for `APP` made by another wallet's keys.
async fn scene_wallet_script() -> LockingScript {
    let other = scene().await;
    let fields = strings(&[APP, &live().to_string(), BASKET]);
    token_script(&other.counted.inner, &fields).await
}
