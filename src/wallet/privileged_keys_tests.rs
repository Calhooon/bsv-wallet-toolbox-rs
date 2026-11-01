//! The seven key calls under `privileged` (#186): a privileged call reaches
//! the wallet's `PrivilegedKeyManager` with the caller's `privileged`,
//! `privileged_reason` and `seek_permission`, as ts-stack@edf6e03
//! `Wallet.ts:455-570` sends it to `this.privilegedKeyManager`; with none
//! configured it is the reference's refusal, text byte for byte; an
//! unprivileged call stays with the everyday key.

use std::sync::Mutex as StdMutex;

use super::busy_door_tests::wallet;
use super::key_route::key_calls;
use super::*;
use crate::services::mock::MockWalletServices;
use crate::StorageSqlx;

pub(super) const ORIGINATOR: &str = "privileged.example.com";
const REASON: &str = "privileged witness";
const REFUSAL: &str =
    "Privileged operations require the Wallet to be configured with a privileged key manager.";
const CALLS: [&str; 7] = [
    "get_public_key",
    "encrypt",
    "decrypt",
    "create_hmac",
    "verify_hmac",
    "create_signature",
    "verify_signature",
];

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

/// What the double was asked: the call, the caller's three fields and the
/// originator.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Asked {
    pub(super) call: &'static str,
    pub(super) privileged: Option<bool>,
    pub(super) privileged_reason: Option<String>,
    pub(super) seek_permission: Option<bool>,
    pub(super) originator: String,
}

impl Asked {
    /// A privileged request from `ORIGINATOR` as the double records it.
    pub(super) fn privileged(
        call: &'static str,
        reason: &str,
        seek_permission: Option<bool>,
    ) -> Self {
        Self {
            call,
            privileged: Some(true),
            privileged_reason: Some(reason.to_string()),
            seek_permission,
            originator: ORIGINATOR.to_string(),
        }
    }
}

/// A privileged key manager that records each request and answers under its
/// own key, which is not the wallet's unless a scene gives it that key.
pub(super) struct PrivilegedDouble {
    pub(super) key: ProtoWallet,
    pub(super) asked: StdMutex<Vec<Asked>>,
}

impl PrivilegedDouble {
    pub(super) fn new() -> Arc<Self> {
        Self::with_key(ProtoWallet::new(Some(PrivateKey::random())))
    }

    pub(super) fn with_key(key: ProtoWallet) -> Arc<Self> {
        Arc::new(Self {
            key,
            asked: StdMutex::new(Vec::new()),
        })
    }

    pub(super) fn asked(&self) -> Vec<Asked> {
        self.asked.lock().unwrap().clone()
    }
}

// Each method records its arguments' three fields, then answers through the
// double's own ProtoWallet.
macro_rules! recorded {
    ($($name:ident($args:ty) -> $result:ty;)*) => {
        #[async_trait]
        impl PrivilegedKeyManager for PrivilegedDouble {
            $(
                async fn $name(
                    &self,
                    args: $args,
                    originator: &str,
                ) -> std::result::Result<$result, bsv_rs::Error> {
                    self.asked.lock().unwrap().push(Asked {
                        call: stringify!($name),
                        privileged: args.privileged,
                        privileged_reason: args.privileged_reason.clone(),
                        seek_permission: args.seek_permission,
                        originator: originator.to_string(),
                    });
                    self.key.$name(args)
                }
            )*
        }
    };
}

key_calls!(recorded);

fn protocol() -> Protocol {
    Protocol::new(SecurityLevel::App, "privileged witness")
}

// The caller's arguments with the three fields set as the scene asks; a
// `keyed` call's with the witness protocol and key id as well.
macro_rules! args {
    (keyed $kind:ident { $($field:ident: $value:expr),* $(,)? }, $privileged:expr) => {
        args!(
            $kind { $($field: $value,)* protocol_id: protocol(), key_id: "1".to_string() },
            $privileged
        )
    };
    ($kind:ident { $($field:ident: $value:expr),* $(,)? }, $privileged:expr) => {
        $kind {
            $($field: $value,)*
            privileged: $privileged,
            privileged_reason: Some(REASON.to_string()),
            seek_permission: Some(false),
            ..Default::default()
        }
    };
}

/// The witness's signing request and the verify of its signature.
fn signature_args(privileged: Option<bool>) -> CreateSignatureArgs {
    args!(keyed CreateSignatureArgs { data: Some(b"data".to_vec()) }, privileged)
}

fn verify_signature_args(signature: Vec<u8>, privileged: Option<bool>) -> VerifySignatureArgs {
    args!(
        keyed VerifySignatureArgs {
            data: Some(b"data".to_vec()),
            signature: signature,
            counterparty: Some(Counterparty::Anyone),
            for_self: Some(true)
        },
        privileged
    )
}

/// One of the seven calls with `privileged` as given, its answer in words.
async fn call(wallet: &TestWallet, name: &str, privileged: Option<bool>) -> bsv_rs::Result<String> {
    let originator = Some(ORIGINATOR);
    // decrypt, verify_hmac and verify_signature make their input ahead of the
    // call, with the same request encrypt, create_hmac and create_signature send.
    let encrypt_args = || args!(keyed EncryptArgs { plaintext: b"secret".to_vec() }, privileged);
    let hmac_args = || args!(keyed CreateHmacArgs { data: b"data".to_vec() }, privileged);
    let answer = match name {
        "get_public_key" => {
            let args = args!(GetPublicKeyArgs { identity_key: true }, privileged);
            format!("{:?}", wallet.get_public_key(args, originator).await?)
        }
        "encrypt" => format!("{:?}", wallet.encrypt(encrypt_args(), originator).await?),
        "decrypt" => {
            // Sealed under the key the call is answered with, so it opens.
            let sealed = wallet.encrypt(encrypt_args(), originator).await?;
            let args = args!(keyed DecryptArgs { ciphertext: sealed.ciphertext }, privileged);
            format!("{:?}", wallet.decrypt(args, originator).await?)
        }
        "create_hmac" => format!("{:?}", wallet.create_hmac(hmac_args(), originator).await?),
        "verify_hmac" => {
            let made = wallet.create_hmac(hmac_args(), originator).await?;
            let args = args!(
                keyed VerifyHmacArgs {
                    data: b"data".to_vec(),
                    hmac: made.hmac,
                },
                privileged
            );
            format!("{:?}", wallet.verify_hmac(args, originator).await?)
        }
        "create_signature" => {
            let made = wallet
                .create_signature(signature_args(privileged), originator)
                .await?;
            format!("{made:?}")
        }
        "verify_signature" => {
            let made = wallet
                .create_signature(signature_args(privileged), originator)
                .await?;
            let args = verify_signature_args(made.signature, privileged);
            format!("{:?}", wallet.verify_signature(args, originator).await?)
        }
        other => unreachable!("{other}"),
    };
    Ok(answer)
}

fn asked(call: &'static str) -> Asked {
    Asked::privileged(call, REASON, Some(false))
}

pub(super) fn assert_privileged_refusal<T: std::fmt::Debug>(
    scene: &str,
    result: bsv_rs::Result<T>,
) {
    match result {
        Err(bsv_rs::Error::WalletRefusal { class, message }) => {
            assert_eq!(class, bsv_rs::WalletRefusalClass::NotImplemented, "{scene}");
            assert_eq!(message, REFUSAL, "{scene}");
        }
        other => panic!("{scene}: expected the reference's refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn each_privileged_key_call_reaches_the_manager_with_the_callers_fields() {
    let (_dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::new();
    wallet.set_privileged_key_manager(double.clone());
    for name in CALLS {
        let before = double.asked().len();
        let answer = call(&wallet, name, Some(true)).await;
        assert!(answer.is_ok(), "{name}: {answer:?}");
        if name.starts_with("verify") {
            assert!(answer.unwrap().contains("valid: true"), "{name}");
        }
        // decrypt, verify_hmac and verify_signature make their input ahead of the
        // call through the same route; every request reached the double.
        let made: Vec<Asked> = double.asked().split_off(before);
        let expected: Vec<Asked> = match name {
            "decrypt" => vec![asked("encrypt"), asked("decrypt")],
            "verify_hmac" => vec![asked("create_hmac"), asked("verify_hmac")],
            "verify_signature" => vec![asked("create_signature"), asked("verify_signature")],
            _ => vec![asked(name)],
        };
        assert_eq!(made, expected, "{name}");
    }
    // The privileged identity key is the manager's, not the wallet's.
    let answer = call(&wallet, "get_public_key", Some(true)).await.unwrap();
    assert!(answer.contains(&double.key.identity_key_hex()), "{answer}");
    assert!(!answer.contains(wallet.identity_key()), "{answer}");
}

#[tokio::test]
async fn each_privileged_key_call_without_a_manager_is_the_references_refusal() {
    let (_dir, wallet) = wallet().await;
    assert!(wallet.privileged_key_manager().is_none());
    for name in CALLS {
        assert_privileged_refusal(name, call(&wallet, name, Some(true)).await);
    }
}

#[tokio::test]
async fn unprivileged_key_calls_stay_with_the_everyday_key() {
    let (_dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::new();
    wallet.set_privileged_key_manager(double.clone());
    for privileged in [None, Some(false)] {
        for name in CALLS {
            let answer = call(&wallet, name, privileged).await;
            assert!(answer.is_ok(), "{name} {privileged:?}: {answer:?}");
        }
        let answer = call(&wallet, "get_public_key", privileged).await.unwrap();
        assert!(answer.contains(wallet.identity_key()), "{answer}");
        for name in ["verify_hmac", "verify_signature"] {
            let answer = call(&wallet, name, privileged).await.unwrap();
            assert!(answer.contains("valid: true"), "{name}: {answer}");
        }
    }
    assert_eq!(double.asked(), Vec::<Asked>::new());
}

/// One of the seven calls with `privileged: true` and no reason, every other
/// argument valid: the everyday ProtoWallet's validators refuse it before the
/// route decides, as the reference's substrates run `validateWalletArgs`
/// ahead of the wallet.
async fn call_without_reason(wallet: &TestWallet, name: &str) -> bsv_rs::Result<()> {
    let originator = Some(ORIGINATOR);
    macro_rules! unreasoned {
        ($kind:ident { $($field:ident: $value:expr),* $(,)? }) => {
            $kind {
                $($field: $value,)*
                privileged: Some(true),
                privileged_reason: None,
                ..Default::default()
            }
        };
    }
    let key_id = || "1".to_string();
    match name {
        "get_public_key" => {
            let args = unreasoned!(GetPublicKeyArgs { identity_key: true });
            wallet.get_public_key(args, originator).await.map(drop)
        }
        "encrypt" => {
            let args = unreasoned!(EncryptArgs {
                plaintext: b"secret".to_vec(),
                protocol_id: protocol(),
                key_id: key_id()
            });
            wallet.encrypt(args, originator).await.map(drop)
        }
        "decrypt" => {
            let args = unreasoned!(DecryptArgs {
                ciphertext: b"sealed".to_vec(),
                protocol_id: protocol(),
                key_id: key_id()
            });
            wallet.decrypt(args, originator).await.map(drop)
        }
        "create_hmac" => {
            let args = unreasoned!(CreateHmacArgs {
                data: b"data".to_vec(),
                protocol_id: protocol(),
                key_id: key_id()
            });
            wallet.create_hmac(args, originator).await.map(drop)
        }
        "verify_hmac" => {
            let args = unreasoned!(VerifyHmacArgs {
                data: b"data".to_vec(),
                hmac: [0; 32],
                protocol_id: protocol(),
                key_id: key_id()
            });
            wallet.verify_hmac(args, originator).await.map(drop)
        }
        "create_signature" => {
            let args = unreasoned!(CreateSignatureArgs {
                data: Some(b"data".to_vec()),
                protocol_id: protocol(),
                key_id: key_id()
            });
            wallet.create_signature(args, originator).await.map(drop)
        }
        "verify_signature" => {
            let args = unreasoned!(VerifySignatureArgs {
                data: Some(b"data".to_vec()),
                signature: vec![1],
                protocol_id: protocol(),
                key_id: key_id()
            });
            wallet.verify_signature(args, originator).await.map(drop)
        }
        other => unreachable!("{other}"),
    }
}

fn assert_argument_refusal<T: std::fmt::Debug>(
    scene: &str,
    result: bsv_rs::Result<T>,
    expected: &str,
) {
    match result {
        Err(bsv_rs::Error::WalletRefusal { class, message }) => {
            assert_eq!(
                class,
                bsv_rs::WalletRefusalClass::InvalidParameter,
                "{scene}"
            );
            assert_eq!(message, expected, "{scene}");
        }
        other => panic!("{scene}: expected the validator's refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn a_privileged_key_call_without_a_reason_is_the_validators_refusal() {
    const MISSING_REASON: &str =
        "The privilegedReason parameter must be a value when 'privileged' is true";
    let (_dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::new();
    for manager in ["without", "with"] {
        if manager == "with" {
            wallet.set_privileged_key_manager(double.clone());
        }
        for name in CALLS {
            let answer = call_without_reason(&wallet, name).await;
            let scene = format!("{name} {manager} a manager");
            assert_argument_refusal(&scene, answer, MISSING_REASON);
        }
    }
    // The manager never hears a request its arguments refuse.
    assert_eq!(double.asked(), Vec::<Asked>::new());
}

#[tokio::test]
async fn a_privileged_argument_refusal_after_the_privilege_keeps_its_place() {
    // A valid privilege and a signature that is no DER: the validator's
    // later refusal, ahead of the no-manager refusal.
    let (_dir, wallet) = wallet().await;
    let args = VerifySignatureArgs {
        data: Some(b"data".to_vec()),
        signature: vec![1],
        protocol_id: protocol(),
        key_id: "1".to_string(),
        privileged: Some(true),
        privileged_reason: Some(REASON.to_string()),
        ..Default::default()
    };
    assert_argument_refusal(
        "verify_signature",
        wallet.verify_signature(args, Some(ORIGINATOR)).await,
        "The signature parameter must be a canonical DER-encoded ECDSA signature",
    );
}

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

/// The prompts the handler saw (each request's `privileged`) and the flags it
/// grants; every other prompt it refuses.
#[derive(Default)]
struct Prompts {
    seen: StdMutex<Vec<Option<bool>>>,
    grants: StdMutex<Vec<bool>>,
}

impl Prompts {
    fn seen(&self) -> Vec<Option<bool>> {
        self.seen.lock().unwrap().clone()
    }
}

type Permissions = Arc<crate::managers::WalletPermissionsManager>;

/// A wallet with the privileged double and a permissions manager that
/// differentiates privileged operations and holds an everyday grant for the
/// witness protocol; its handler refuses every later prompt.
async fn guarded_wallet() -> (
    tempfile::TempDir,
    TestWallet,
    Arc<PrivilegedDouble>,
    Permissions,
    Arc<Prompts>,
) {
    let (dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::new();
    wallet.set_privileged_key_manager(double.clone());
    let config = crate::managers::WalletPermissionsManagerConfig::all_enabled();
    assert!(config.differentiate_privileged_operations);
    let manager = Arc::new(crate::managers::WalletPermissionsManager::new(
        Arc::new(wallet.proto_wallet.clone()),
        "wallet.admin".to_string(),
        config,
    ));
    let prompts = Arc::new(Prompts::default());
    let handler_prompts = Arc::clone(&prompts);
    manager
        .set_permission_request_handler(Arc::new(move |request| {
            handler_prompts
                .seen
                .lock()
                .unwrap()
                .push(request.privileged);
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
    grant(&manager, &prompts, false).await;
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
    prompts.seen.lock().unwrap().clear();
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
    if name != "get_public_key" {
        return call(wallet, name, privileged).await;
    }
    let args = args!(
        GetPublicKeyArgs {
            identity_key: false,
            protocol_id: Some(protocol()),
            key_id: Some("1".to_string())
        },
        privileged
    );
    Ok(format!(
        "{:?}",
        wallet.get_public_key(args, Some(ORIGINATOR)).await?
    ))
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
