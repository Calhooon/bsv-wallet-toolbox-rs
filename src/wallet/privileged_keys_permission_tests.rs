//! The permission witnesses of the seven key calls (#186, #576, #581): the
//! grant under the request's `privileged`, the usage type each call passes
//! and the counterparty and reason in the prompt, split from
//! `privileged_keys_tests.rs` (#727); the identity key's own ask (#719) and
//! `get_public_key`'s `seek_permission` (#720); the whitelisted
//! counterparties (#725). `args!` sends
//! `seek_permission: Some(false)`, which `get_public_key` alone passes, so its
//! prompting calls here ask with it on.

use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

use super::busy_door_tests::wallet;
use super::privileged_keys_tests::{
    args, asked, call, protocol, signature_args, verify_signature_args, Asked, PrivilegedDouble,
    TestWallet, CALLS, ORIGINATOR, REASON,
};
use super::*;

// =========================================================================
// The permission check under the request's `privileged` (#186): a
// permissions manager that differentiates privileged operations keys its
// grants on the flag, as the reference passes `requestArgs.privileged` to
// `ensureProtocolPermission` (ts-stack@edf6e03
// `WalletPermissionsManager.ts:5165-5350`, `:1515`).
// =========================================================================

/// The key calls the permission check guards: all seven (#576).
const GUARDED: [&str; 7] = CALLS;

/// The handler's refusal as the wallet answers it.
const PERMISSION_REFUSAL: &str = "Access denied: Protocol permission refused by the test";

/// The prompts the handler saw and the flags it grants; every other prompt
/// it refuses.
#[derive(Default)]
struct Prompts {
    requests: StdMutex<Vec<crate::managers::PermissionRequest>>,
    grants: StdMutex<Vec<bool>>,
}

impl Prompts {
    /// Each prompt's `privileged`.
    fn seen(&self) -> Vec<Option<bool>> {
        let requests = self.requests.lock().unwrap();
        requests.iter().map(|request| request.privileged).collect()
    }
}

type Permissions = Arc<crate::managers::WalletPermissionsManager>;

type Guarded = (
    tempfile::TempDir,
    TestWallet,
    Arc<PrivilegedDouble>,
    Permissions,
    Arc<Prompts>,
);

/// A wallet with the privileged double and a permissions manager that
/// differentiates privileged operations and holds an everyday grant for the
/// witness protocol; its handler refuses every later prompt.
async fn guarded_wallet() -> Guarded {
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    assert!(config.differentiate_privileged_operations);
    let guarded = permitted_wallet(config).await;
    grant(&guarded.3, &guarded.4, false).await;
    guarded
}

/// A wallet with the privileged double and a permissions manager under
/// `config`, holding no grant; its handler refuses every prompt it is not
/// told to grant.
async fn permitted_wallet(config: crate::managers::WalletPermissionsManagerConfig) -> Guarded {
    whitelisted_wallet(config, HashMap::new()).await
}

/// [`permitted_wallet`] with the manager's whitelisted counterparties (#725).
async fn whitelisted_wallet(
    config: crate::managers::WalletPermissionsManagerConfig,
    whitelist: HashMap<String, Vec<String>>,
) -> Guarded {
    let (dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::new();
    wallet.set_privileged_key_manager(double.clone());
    let manager = Arc::new(
        crate::managers::WalletPermissionsManager::new(
            Arc::new(wallet.proto_wallet.clone()),
            "wallet.admin".to_string(),
            config,
        )
        .with_whitelisted_counterparties(whitelist),
    );
    let prompts = Arc::new(Prompts::default());
    let handler_prompts = Arc::clone(&prompts);
    manager
        .set_permission_request_handler(Arc::new(move |request| {
            handler_prompts
                .requests
                .lock()
                .unwrap()
                .push(request.clone());
            let flag = request.privileged.unwrap_or(false);
            let granted = handler_prompts.grants.lock().unwrap().contains(&flag);
            Box::pin(async move {
                if !granted {
                    return Err(crate::Error::AccessDenied(
                        "Protocol permission refused by the test".to_string(),
                    ));
                }
                let token = serde_json::json!({
                    "txid": "",
                    "tx": [],
                    "outputIndex": 0,
                    "outputScript": "",
                    "satoshis": 1,
                    "originator": request.originator,
                    "expiry": chrono::Utc::now().timestamp() as u64 + 3600,
                    "privileged": request.privileged,
                });
                Ok(serde_json::from_value(token).unwrap())
            })
        }))
        .await;
    wallet.set_permissions_manager(manager.clone());
    (dir, wallet, double, manager, prompts)
}

/// The user grants the witness protocol to `ORIGINATOR` once, privileged or
/// everyday, through the manager's own prompt; the prompt is not counted.
async fn grant(manager: &Permissions, prompts: &Prompts, privileged: bool) {
    *prompts.grants.lock().unwrap() = vec![privileged];
    manager
        .ensure_protocol_permission(
            ORIGINATOR,
            privileged,
            &protocol(),
            None,
            None,
            crate::managers::PermissionUsageType::Generic,
        )
        .await
        .unwrap();
    prompts.grants.lock().unwrap().clear();
    prompts.requests.lock().unwrap().clear();
}

/// One of the seven guarded calls; `get_public_key` names the witness
/// protocol so the check runs. `verify_signature`'s signature is made off
/// the wallet, under the key the route answers with (the double's for a
/// privileged call), so the only check and the only ask are the verify's.
async fn guarded_call(
    wallet: &TestWallet,
    double: &PrivilegedDouble,
    name: &str,
    privileged: Option<bool>,
) -> bsv_rs::Result<String> {
    if name == "verify_signature" {
        let key = match privileged {
            Some(true) => &double.key,
            _ => &wallet.proto_wallet,
        };
        let made = key.create_signature(signature_args(privileged))?;
        let args = verify_signature_args(made.signature, privileged);
        let verified = wallet.verify_signature(args, Some(ORIGINATOR)).await?;
        assert!(verified.valid, "{privileged:?}: {verified:?}");
        return Ok(format!("{verified:?}"));
    }
    if name == IDENTITY_KEY {
        let answer = identity_key_call(wallet, privileged, None).await?;
        return Ok(format!("{answer:?}"));
    }
    if name != "get_public_key" {
        return call(wallet, name, privileged).await;
    }
    let answer = public_key_call(wallet, privileged, None).await?;
    Ok(format!("{answer:?}"))
}

/// `get_public_key` naming the witness protocol under the request's
/// `privileged` and `seek_permission`.
async fn public_key_call(
    wallet: &TestWallet,
    privileged: Option<bool>,
    seek_permission: Option<bool>,
) -> bsv_rs::Result<GetPublicKeyResult> {
    let mut args = args!(
        GetPublicKeyArgs {
            identity_key: false,
            protocol_id: Some(protocol()),
            key_id: Some("1".to_string())
        },
        privileged
    );
    args.seek_permission = seek_permission;
    wallet.get_public_key(args, Some(ORIGINATOR)).await
}

/// `get_public_key` naming the identity key and no protocol: its own ask.
const IDENTITY_KEY: &str = "get_public_key(identity_key)";

/// `get_public_key` for the identity key under the request's `privileged`
/// and `seek_permission`.
async fn identity_key_call(
    wallet: &TestWallet,
    privileged: Option<bool>,
    seek_permission: Option<bool>,
) -> bsv_rs::Result<GetPublicKeyResult> {
    let mut args = args!(GetPublicKeyArgs { identity_key: true }, privileged);
    args.seek_permission = seek_permission;
    wallet.get_public_key(args, Some(ORIGINATOR)).await
}

#[tokio::test]
async fn an_everyday_grant_does_not_admit_a_privileged_key_call() {
    let (_dir, wallet, double, _manager, prompts) = guarded_wallet().await;
    for name in GUARDED {
        match guarded_call(&wallet, &double, name, Some(true)).await {
            Err(bsv_rs::Error::WalletRefusal { class, message }) => {
                assert_eq!(class, bsv_rs::WalletRefusalClass::Unauthorized, "{name}");
                assert_eq!(message, PERMISSION_REFUSAL, "{name}");
            }
            other => panic!("{name}: expected the permission refusal, got {other:?}"),
        }
    }
    // Each call prompted once for a privileged grant; the manager heard none.
    assert_eq!(prompts.seen(), vec![Some(true); GUARDED.len()]);
    assert_eq!(double.asked(), Vec::<Asked>::new());
}

#[tokio::test]
async fn a_privileged_grant_admits_a_privileged_key_call() {
    let (_dir, wallet, double, manager, prompts) = guarded_wallet().await;
    grant(&manager, &prompts, true).await;
    for name in GUARDED {
        let before = double.asked().len();
        let answer = guarded_call(&wallet, &double, name, Some(true)).await;
        assert!(answer.is_ok(), "{name}: {answer:?}");
        // decrypt and verify_hmac make their input through the same route.
        let made: Vec<Asked> = double.asked().split_off(before);
        let expected: Vec<Asked> = match name {
            "decrypt" => vec![asked("encrypt"), asked("decrypt")],
            "verify_hmac" => vec![asked("create_hmac"), asked("verify_hmac")],
            // `guarded_call` asks `get_public_key` seeking (#720).
            "get_public_key" => vec![Asked::privileged(name, REASON, None)],
            _ => vec![asked(name)],
        };
        assert_eq!(made, expected, "{name}");
    }
    assert_eq!(prompts.seen(), Vec::<Option<bool>>::new());
}

#[tokio::test]
async fn an_unprivileged_key_call_under_the_everyday_grant_is_unchanged() {
    let (_dir, wallet, double, _manager, prompts) = guarded_wallet().await;
    for privileged in [None, Some(false)] {
        for name in GUARDED {
            let answer = guarded_call(&wallet, &double, name, privileged).await;
            assert!(answer.is_ok(), "{name} {privileged:?}: {answer:?}");
        }
    }
    assert_eq!(prompts.seen(), Vec::<Option<bool>>::new());
    assert_eq!(double.asked(), Vec::<Asked>::new());
}

// =========================================================================
// The usage type each key call passes (#581): with one type's
// `seek_protocol_permissions_for_<type>` off, the calls of that type are
// admitted with no prompt and every call of another type still prompts, as
// the reference's `usageType` per call (ts-stack@edf6e03
// `WalletPermissionsManager.ts:5165-5384`, `:1514`).
// =========================================================================

/// The usage type the reference passes for each of the seven calls.
fn usage_type(name: &str) -> crate::managers::PermissionUsageType {
    use crate::managers::PermissionUsageType::*;
    match name {
        "get_public_key" => PublicKey,
        IDENTITY_KEY => IdentityKey,
        "encrypt" | "decrypt" => Encrypting,
        "create_hmac" | "verify_hmac" => Hmac,
        "create_signature" | "verify_signature" => Signing,
        other => unreachable!("{other}"),
    }
}

/// `name`'s usage type exempt from prompting, every other type prompting:
/// each everyday call of `name`'s type is admitted with no prompt, and each
/// of another type is refused after one prompt.
async fn assert_usage_type(name: &str) {
    use crate::managers::PermissionUsageType::*;
    let mut config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    let exempt = usage_type(name);
    match exempt {
        Signing => config.seek_protocol_permissions_for_signing = false,
        Encrypting => config.seek_protocol_permissions_for_encrypting = false,
        Hmac => config.seek_protocol_permissions_for_hmac = false,
        PublicKey => config.seek_permissions_for_public_key_revelation = false,
        IdentityKey => config.seek_permissions_for_identity_key_revelation = false,
        other => unreachable!("{other:?}"),
    }
    let (_dir, wallet, double, _manager, prompts) = permitted_wallet(config).await;
    let answer = guarded_call(&wallet, &double, name, None).await;
    assert!(answer.is_ok(), "{name}: {answer:?}");
    assert_eq!(prompts.seen(), Vec::<Option<bool>>::new(), "{name}");
    let calls = GUARDED.into_iter().chain([IDENTITY_KEY]);
    for sibling in calls.filter(|s| usage_type(s) != exempt) {
        let before = prompts.seen().len();
        match guarded_call(&wallet, &double, sibling, None).await {
            Err(bsv_rs::Error::WalletRefusal { message, .. }) => {
                assert_eq!(message, PERMISSION_REFUSAL, "{name}: {sibling}");
            }
            other => panic!("{name}: {sibling} expected the permission refusal, got {other:?}"),
        }
        assert_eq!(prompts.seen().len(), before + 1, "{name}: {sibling}");
    }
    assert_eq!(double.asked(), Vec::<Asked>::new());
}

macro_rules! usage_type_witnesses {
    ($($test:ident: $name:expr;)*) => {$(
        #[tokio::test]
        async fn $test() {
            assert_usage_type($name).await;
        }
    )*};
}

usage_type_witnesses! {
    get_public_key_passes_the_public_key_usage_type: "get_public_key";
    encrypt_passes_the_encrypting_usage_type: "encrypt";
    decrypt_passes_the_encrypting_usage_type: "decrypt";
    create_hmac_passes_the_hmac_usage_type: "create_hmac";
    verify_hmac_passes_the_hmac_usage_type: "verify_hmac";
    create_signature_passes_the_signing_usage_type: "create_signature";
    verify_signature_passes_the_signing_usage_type: "verify_signature";
    get_public_key_for_the_identity_key_passes_the_identity_key_usage_type: IDENTITY_KEY;
}

// =========================================================================
// The counterparty and the reason in the prompt (#581): each call passes
// `requestArgs.counterparty || 'self'` and `requestArgs.privilegedReason`
// (ts-stack@edf6e03 `WalletPermissionsManager.ts:5165-5384`); a level-1
// grant names no counterparty (`:1505`).
// =========================================================================

/// One of the seven everyday calls under `protocol` and `counterparty`; the
/// handler refuses it, so its other arguments are never read.
async fn prompted_call(
    wallet: &TestWallet,
    name: &str,
    protocol: &Protocol,
    counterparty: Option<Counterparty>,
) -> bsv_rs::Result<()> {
    let o = Some(ORIGINATOR);
    macro_rules! ask {
        ($method:ident($kind:ident)) => {{
            let args = args!(
                $kind {
                    protocol_id: protocol.clone(),
                    key_id: "1".to_string(),
                    counterparty: counterparty.clone()
                },
                None
            );
            wallet.$method(args, o).await.map(drop)
        }};
    }
    match name {
        "get_public_key" => {
            let mut args = args!(
                GetPublicKeyArgs {
                    protocol_id: Some(protocol.clone()),
                    key_id: Some("1".to_string()),
                    counterparty: counterparty.clone()
                },
                None
            );
            args.seek_permission = None;
            wallet.get_public_key(args, o).await.map(drop)
        }
        "encrypt" => ask!(encrypt(EncryptArgs)),
        "decrypt" => ask!(decrypt(DecryptArgs)),
        "create_hmac" => ask!(create_hmac(CreateHmacArgs)),
        "verify_hmac" => ask!(verify_hmac(VerifyHmacArgs)),
        "create_signature" => ask!(create_signature(CreateSignatureArgs)),
        "verify_signature" => ask!(verify_signature(VerifySignatureArgs)),
        other => unreachable!("{other}"),
    }
}

#[tokio::test]
async fn each_key_call_prompts_with_the_requests_counterparty_and_reason() {
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    let (_dir, wallet, _double, _manager, prompts) = permitted_wallet(config).await;
    let key = PrivateKey::random().public_key();
    let level2 = Protocol::new(SecurityLevel::Counterparty, "privileged witness");
    let (other, hex) = (Some(Counterparty::Other(key.clone())), key.to_hex());
    let scenes = [
        (&level2, other.clone(), Some(hex.as_str())),
        (&level2, Some(Counterparty::Anyone), Some("anyone")),
        (&level2, Some(Counterparty::Self_), Some("self")),
        (&level2, None, Some("self")),
        (&protocol(), other, None),
    ];
    for (protocol, counterparty, expected) in scenes {
        for name in GUARDED {
            let scene = format!("{name} {counterparty:?} at {:?}", protocol.security_level);
            match prompted_call(&wallet, name, protocol, counterparty.clone()).await {
                Err(bsv_rs::Error::WalletRefusal { message, .. }) => {
                    assert_eq!(message, PERMISSION_REFUSAL, "{scene}");
                }
                other => panic!("{scene}: expected the permission refusal, got {other:?}"),
            }
            let request = prompts.requests.lock().unwrap().pop().unwrap();
            assert_eq!(request.counterparty.as_deref(), expected, "{scene}");
            assert_eq!(request.reason.as_deref(), Some(REASON), "{scene}");
            assert_eq!(request.protocol_id.as_ref(), Some(protocol), "{scene}");
        }
    }
    assert!(prompts.requests.lock().unwrap().is_empty());
}

/// The identity key's prompt (#719): the level-1 protocol `identity key
/// retrieval`, no counterparty (the reference's `self` dropped at level 1),
/// the request's `privileged` and reason (ts-stack@edf6e03
/// `WalletPermissionsManager.ts:5174-5185`, `:1506`).
#[tokio::test]
async fn the_identity_key_prompts_for_identity_key_retrieval() {
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    let (_dir, wallet, double, _manager, prompts) = permitted_wallet(config).await;
    let retrieval = Protocol::new(SecurityLevel::App, "identity key retrieval");
    for privileged in [None, Some(true)] {
        match identity_key_call(&wallet, privileged, None).await {
            Err(bsv_rs::Error::WalletRefusal { message, .. }) => {
                assert_eq!(message, PERMISSION_REFUSAL, "{privileged:?}");
            }
            other => panic!("{privileged:?}: expected the permission refusal, got {other:?}"),
        }
        let request = prompts.requests.lock().unwrap().pop().unwrap();
        assert_eq!(request.protocol_id.as_ref(), Some(&retrieval));
        assert_eq!(request.counterparty, None, "{privileged:?}");
        assert_eq!(request.reason.as_deref(), Some(REASON), "{privileged:?}");
        assert_eq!(request.privileged, Some(privileged == Some(true)));
    }
    assert!(prompts.requests.lock().unwrap().is_empty());
    assert_eq!(double.asked(), Vec::<Asked>::new());
}

// =========================================================================
// `get_public_key`'s `seek_permission` (#720): each branch passes
// `requestArgs.seekPermission` (ts-stack@edf6e03
// `WalletPermissionsManager.ts:5169`, `:5182`), absent read as true
// (`:1487`); false with no grant is refused with no prompt (`:1533`).
// =========================================================================

/// The refusal of a request that seeks no permission and holds no grant.
const NO_SEEK_REFUSAL: &str =
    "Access denied: No protocol permission token found (seekPermission=false).";

/// The two branches of `get_public_key` under `seek_permission`.
async fn public_key_branches(
    wallet: &TestWallet,
    seek_permission: Option<bool>,
) -> [(&'static str, bsv_rs::Result<GetPublicKeyResult>); 2] {
    [
        (
            "protocol",
            public_key_call(wallet, None, seek_permission).await,
        ),
        (
            IDENTITY_KEY,
            identity_key_call(wallet, None, seek_permission).await,
        ),
    ]
}

#[tokio::test]
async fn get_public_key_seeking_no_permission_is_refused_without_a_prompt() {
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    let (_dir, wallet, _double, _manager, prompts) = permitted_wallet(config).await;
    for (branch, answer) in public_key_branches(&wallet, Some(false)).await {
        match answer {
            Err(bsv_rs::Error::WalletRefusal { class, message }) => {
                assert_eq!(class, bsv_rs::WalletRefusalClass::Unauthorized, "{branch}");
                assert_eq!(message, NO_SEEK_REFUSAL, "{branch}");
            }
            other => panic!("{branch}: expected the no-seek refusal, got {other:?}"),
        }
    }
    assert_eq!(prompts.seen(), Vec::<Option<bool>>::new());
}

#[tokio::test]
async fn get_public_key_seeking_permission_prompts_and_a_grant_admits_it_unseeking() {
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    let (_dir, wallet, _double, _manager, prompts) = permitted_wallet(config).await;
    // Seeking, refused by the user: one prompt per branch.
    for seek in [None, Some(true)] {
        for (branch, answer) in public_key_branches(&wallet, seek).await {
            match answer {
                Err(bsv_rs::Error::WalletRefusal { message, .. }) => {
                    assert_eq!(message, PERMISSION_REFUSAL, "{branch} {seek:?}");
                }
                other => panic!("{branch} {seek:?}: expected the prompt, got {other:?}"),
            }
        }
    }
    assert_eq!(prompts.seen(), vec![Some(false); 4]);
    // Seeking, granted by the user: the grant then admits the unseeking call.
    *prompts.grants.lock().unwrap() = vec![false];
    for (branch, answer) in public_key_branches(&wallet, None).await {
        assert!(answer.is_ok(), "{branch}: {answer:?}");
    }
    assert_eq!(prompts.seen().len(), 6);
    prompts.grants.lock().unwrap().clear();
    for (branch, answer) in public_key_branches(&wallet, Some(false)).await {
        assert!(answer.is_ok(), "{branch}: {answer:?}");
    }
    assert_eq!(prompts.seen().len(), 6);
}

// =========================================================================
// The whitelisted counterparties (#725): an everyday call whose
// counterparty is whitelisted for the protocol's name is admitted with no
// prompt, after the usage-type exemptions and the privileged flag's
// normalizing, before the cache (ts-stack@edf6e03
// `WalletPermissionsManager.ts:1514-1518`, the helper at `:5731-5742`); a
// level-1 call's counterparty is dropped ahead of it (`:1506`).
// =========================================================================

/// A level-2 witness protocol and a key whitelisted for it, by `name`.
fn whitelisted(name: &str) -> (Protocol, PublicKey, HashMap<String, Vec<String>>) {
    let level2 = Protocol::new(SecurityLevel::Counterparty, "privileged witness");
    let key = PrivateKey::random().public_key();
    let whitelist = HashMap::from([(key.to_hex(), vec![name.to_string()])]);
    (level2, key, whitelist)
}

/// Each of the seven calls under `protocol` naming `key`, and the prompts
/// it raised; an admitted call may still fail past the check.
async fn prompts_per_call(
    wallet: &TestWallet,
    prompts: &Prompts,
    protocol: &Protocol,
    key: &PublicKey,
) -> Vec<(&'static str, usize)> {
    let mut seen = Vec::new();
    for name in GUARDED {
        let before = prompts.seen().len();
        let counterparty = Some(Counterparty::Other(key.clone()));
        let _ = prompted_call(wallet, name, protocol, counterparty).await;
        seen.push((name, prompts.seen().len() - before));
    }
    seen
}

#[tokio::test]
async fn a_whitelisted_counterparty_at_level_2_is_admitted_without_a_prompt() {
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    let (level2, key, whitelist) = whitelisted("PRIVILEGED Witness");
    let (_dir, wallet, _double, _manager, prompts) = whitelisted_wallet(config, whitelist).await;
    let seen = prompts_per_call(&wallet, &prompts, &level2, &key).await;
    assert_eq!(seen, GUARDED.map(|name| (name, 0)).to_vec());
}

#[tokio::test]
async fn a_counterparty_not_whitelisted_for_the_protocol_prompts() {
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    let (level2, key, whitelist) = whitelisted("another protocol");
    let stranger = PrivateKey::random().public_key();
    let (_dir, wallet, _double, _manager, prompts) = whitelisted_wallet(config, whitelist).await;
    for counterparty in [&key, &stranger] {
        let seen = prompts_per_call(&wallet, &prompts, &level2, counterparty).await;
        assert_eq!(seen, GUARDED.map(|name| (name, 1)).to_vec());
    }
}

#[tokio::test]
async fn a_whitelisted_counterparty_on_a_privileged_request_still_prompts() {
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    assert!(config.differentiate_privileged_operations);
    let (level2, key, whitelist) = whitelisted("privileged witness");
    let (_dir, _wallet, _double, manager, prompts) = whitelisted_wallet(config, whitelist).await;
    let hex = key.to_hex();
    let ask = |privileged| {
        manager.ensure_protocol_permission(
            ORIGINATOR,
            privileged,
            &level2,
            Some(&hex),
            None,
            crate::managers::PermissionUsageType::Generic,
        )
    };
    assert!(ask(false).await.unwrap());
    assert_eq!(prompts.seen(), Vec::<Option<bool>>::new());
    let refused = ask(true).await.unwrap_err();
    assert_eq!(refused.to_string(), PERMISSION_REFUSAL);
    assert_eq!(prompts.seen(), vec![Some(true)]);
}

/// Without `differentiate_privileged_operations` a privileged request is an
/// everyday one (`:1515`), so the whitelist admits it.
#[tokio::test]
async fn a_whitelisted_privileged_request_is_admitted_when_privilege_is_not_differentiated() {
    let mut config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    config.differentiate_privileged_operations = false;
    let (level2, key, whitelist) = whitelisted("privileged witness");
    let (_dir, _wallet, _double, manager, prompts) = whitelisted_wallet(config, whitelist).await;
    let admitted = manager
        .ensure_protocol_permission(
            ORIGINATOR,
            true,
            &level2,
            Some(&key.to_hex()),
            None,
            crate::managers::PermissionUsageType::Generic,
        )
        .await;
    assert!(admitted.unwrap());
    assert_eq!(prompts.seen(), Vec::<Option<bool>>::new());
}

/// A level-1 call names no counterparty by the time the whitelist is read
/// (`:1506` ahead of `:1516`), so a whitelisted key still prompts.
#[tokio::test]
async fn a_whitelisted_counterparty_at_level_1_still_prompts() {
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    let (_, key, whitelist) = whitelisted(&protocol().protocol_name);
    let (_dir, wallet, _double, _manager, prompts) = whitelisted_wallet(config, whitelist).await;
    let seen = prompts_per_call(&wallet, &prompts, &protocol(), &key).await;
    assert_eq!(seen, GUARDED.map(|name| (name, 1)).to_vec());
    let requests = prompts.requests.lock().unwrap();
    assert!(requests
        .iter()
        .all(|request| request.counterparty.is_none()));
}
