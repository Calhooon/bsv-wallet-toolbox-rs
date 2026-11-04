//! The permission modules on the seven key calls (#726, the keeper's
//! 2026-11-03 reading): a `p <scheme>` protocol delegates to the module
//! registered under the scheme, as the reference's
//! `delegateToPModuleIfNeeded` does (ts-stack@edf6e03
//! `WalletPermissionsManager.ts:843-885`, the wrappers `:5142-5385`); the
//! refusals carry the class `InvalidParameter` and the text alone.

use std::sync::Mutex as StdMutex;

use serde_json::{json, Value};

use super::busy_door_tests::wallet;
use super::privileged_keys_tests::{
    Asked, PrivilegedDouble, TestWallet, CALLS, ORIGINATOR, REASON,
};
use super::*;
use crate::managers::{
    ModuleCall, PermissionsModule, WalletPermissionsManager, WalletPermissionsManagerConfig,
};

/// The reason the module's arguments carry to the underlying call.
const MODULE_REASON: &str = "module reason";
const ADMIN: &str = "wallet.admin";
const NOSEND_TEXT: &str =
    "BRC-177 noSend expiry labels are only valid for createAction and listActions";

/// Where the module double refuses, if it does.
#[derive(Clone, Copy, PartialEq)]
enum Refuse {
    Never,
    Request,
    Response,
}

/// A module that records what it is asked, asks the underlying call for the
/// privileged keyring with its own reason, and rewrites every result.
struct ModuleDouble {
    refuse: Refuse,
    requests: StdMutex<Vec<(String, Value, String)>>,
    responses: StdMutex<Vec<(Value, Value)>>,
}

impl ModuleDouble {
    fn new(refuse: Refuse) -> Arc<Self> {
        Arc::new(Self {
            refuse,
            requests: StdMutex::new(Vec::new()),
            responses: StdMutex::new(Vec::new()),
        })
    }

    fn asked(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

/// Each field changed: bytes reversed, a string replaced, a flag negated.
fn rewrite(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().rev().cloned().collect()),
        Value::String(_) => Value::String("module answer".to_string()),
        Value::Bool(flag) => Value::Bool(!flag),
        Value::Object(map) => map.iter().map(|(k, v)| (k.clone(), rewrite(v))).collect(),
        other => other.clone(),
    }
}

#[async_trait]
impl PermissionsModule for ModuleDouble {
    async fn on_request(
        &self,
        method: &str,
        args: Value,
        originator: &str,
    ) -> std::result::Result<Value, String> {
        let entry = (method.to_string(), args.clone(), originator.to_string());
        self.requests.lock().unwrap().push(entry);
        if self.refuse == Refuse::Request {
            return Err(format!("the module refused {method}"));
        }
        let mut args = args;
        args["privileged"] = json!(true);
        args["privilegedReason"] = json!(MODULE_REASON);
        Ok(args)
    }

    async fn on_response(
        &self,
        res: Value,
        method: &str,
        _originator: &str,
    ) -> std::result::Result<Value, String> {
        if self.refuse == Refuse::Response {
            return Err(format!("the module refused the {method} result"));
        }
        let answer = rewrite(&res);
        self.responses.lock().unwrap().push((res, answer.clone()));
        Ok(answer)
    }
}

/// A wallet whose privileged double holds the wallet's own key, under a
/// permissions manager with `modules` registered; every prompt is refused
/// and counted.
async fn module_wallet(
    modules: Vec<(&str, Arc<ModuleDouble>)>,
) -> (
    tempfile::TempDir,
    TestWallet,
    Arc<PrivilegedDouble>,
    Arc<StdMutex<Vec<String>>>,
) {
    let (dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::with_key(wallet.proto_wallet.clone());
    wallet.set_privileged_key_manager(double.clone());
    let mut config = WalletPermissionsManagerConfig::all_enabled();
    for (scheme, module) in modules {
        config
            .permission_modules
            .insert(scheme.to_string(), module as Arc<dyn PermissionsModule>);
    }
    let underlying = Arc::new(wallet.proto_wallet.clone());
    let manager = Arc::new(WalletPermissionsManager::new(
        underlying,
        ADMIN.into(),
        config,
    ));
    let prompts = Arc::new(StdMutex::new(Vec::new()));
    let seen = Arc::clone(&prompts);
    manager
        .set_permission_request_handler(Arc::new(move |request| {
            let name = request.protocol_id.map(|p| p.protocol_name);
            seen.lock().unwrap().push(name.unwrap_or_default());
            Box::pin(async { Err(crate::Error::AccessDenied("refused by the test".into())) })
        }))
        .await;
    wallet.set_permissions_manager(manager);
    (dir, wallet, double, prompts)
}

fn named(name: &str) -> Protocol {
    Protocol::new(SecurityLevel::App, name)
}

/// One of the seven calls under `protocol` from `originator`, its answer as
/// the module reads a result. The inputs decrypt and the two verifies need
/// are made under the wallet's own key, outside the permission path.
async fn key_call(
    wallet: &TestWallet,
    name: &str,
    protocol: &Protocol,
    originator: &str,
) -> bsv_rs::Result<Value> {
    let o = Some(originator);
    let key = &wallet.proto_wallet;
    macro_rules! keyed {
        ($kind:ident { $($field:ident: $value:expr),* $(,)? }) => {
            $kind {
                $($field: $value,)*
                protocol_id: protocol.clone(),
                key_id: "1".to_string(),
                privileged_reason: Some(REASON.to_string()),
                seek_permission: Some(false),
                ..Default::default()
            }
        };
    }
    macro_rules! answer {
        ($kind:ident, $method:ident, $args:expr) => {
            Ok(<$kind as ModuleCall>::output_json(
                &wallet.$method($args, o).await?,
            ))
        };
    }
    let data = || b"data".to_vec();
    match name {
        "get_public_key" => {
            let args = GetPublicKeyArgs {
                protocol_id: Some(protocol.clone()),
                key_id: Some("1".to_string()),
                privileged_reason: Some(REASON.to_string()),
                ..Default::default()
            };
            answer!(GetPublicKeyArgs, get_public_key, args)
        }
        "encrypt" => answer!(
            EncryptArgs,
            encrypt,
            keyed!(EncryptArgs { plaintext: data() })
        ),
        "decrypt" => {
            let sealed = key.encrypt(keyed!(EncryptArgs { plaintext: data() }))?;
            let args = keyed!(DecryptArgs {
                ciphertext: sealed.ciphertext
            });
            answer!(DecryptArgs, decrypt, args)
        }
        "create_hmac" => answer!(
            CreateHmacArgs,
            create_hmac,
            keyed!(CreateHmacArgs { data: data() })
        ),
        "verify_hmac" => {
            let made = key.create_hmac(keyed!(CreateHmacArgs { data: data() }))?;
            let args = keyed!(VerifyHmacArgs {
                data: data(),
                hmac: made.hmac
            });
            answer!(VerifyHmacArgs, verify_hmac, args)
        }
        "create_signature" => {
            let args = keyed!(CreateSignatureArgs { data: Some(data()) });
            answer!(CreateSignatureArgs, create_signature, args)
        }
        "verify_signature" => {
            let made = key.create_signature(keyed!(CreateSignatureArgs { data: Some(data()) }))?;
            let args = keyed!(VerifySignatureArgs {
                data: Some(data()),
                signature: made.signature,
                counterparty: Some(Counterparty::Anyone),
                for_self: Some(true),
            });
            answer!(VerifySignatureArgs, verify_signature, args)
        }
        other => unreachable!("{other}"),
    }
}

/// The reference's method name of each call, in `CALLS`'s order.
const METHODS: [&str; 7] = [
    "getPublicKey",
    "encrypt",
    "decrypt",
    "createHmac",
    "verifyHmac",
    "createSignature",
    "verifySignature",
];

/// The refusal a host sees: `InvalidParameter`, `text` byte for byte, no
/// prefix in the message or its display.
fn assert_module_refusal(answer: bsv_rs::Result<Value>, text: &str, call: &str) {
    let err = answer.expect_err(call);
    let shown = err.to_string();
    match err {
        bsv_rs::Error::WalletRefusal { class, message } => {
            assert_eq!(
                class,
                bsv_rs::WalletRefusalClass::InvalidParameter,
                "{call}"
            );
            assert_eq!(message, text, "{call}");
            assert_eq!(shown, text, "{call}");
        }
        other => panic!("{call}: {other:?}"),
    }
    for prefix in ["wallet error: ", "Internal error: ", "Access denied: "] {
        assert!(!shown.contains(prefix), "{call}: {shown}");
    }
}

// =========================================================================
// A registered module is asked (`:874-884`): the request it sees, its
// arguments reaching the underlying call, its result reaching the caller,
// and no prompt.
// =========================================================================

#[tokio::test]
async fn registered_module_owns_each_key_call() {
    for (call, method) in CALLS.into_iter().zip(METHODS) {
        let module = ModuleDouble::new(Refuse::Never);
        let (_dir, wallet, double, prompts) = module_wallet(vec![("btms", module.clone())]).await;
        let answer = key_call(&wallet, call, &named("p btms witness"), ORIGINATOR)
            .await
            .unwrap_or_else(|e| panic!("{call}: {e:?}"));

        let requests = module.requests.lock().unwrap().clone();
        let [(asked_method, args, originator)] = requests.as_slice() else {
            panic!("{call}: {requests:?}");
        };
        assert_eq!(
            (asked_method.as_str(), originator.as_str()),
            (method, ORIGINATOR)
        );
        assert_eq!(args["protocolID"], json!([1, "p btms witness"]), "{call}");
        assert_eq!(args["keyID"], json!("1"), "{call}");
        assert_eq!(args["privilegedReason"], json!(REASON), "{call}");
        assert!(args.get("privileged").is_none(), "{call}");

        let reached = Asked {
            call,
            privileged: Some(true),
            privileged_reason: Some(MODULE_REASON.to_string()),
            seek_permission: (call != "get_public_key").then_some(false),
            originator: ORIGINATOR.to_string(),
        };
        assert_eq!(double.asked(), vec![reached], "{call}");

        let responses = module.responses.lock().unwrap().clone();
        let [(seen, rewritten)] = responses.as_slice() else {
            panic!("{call}: {responses:?}");
        };
        assert_ne!(seen, rewritten, "{call}");
        assert_eq!(&answer, rewritten, "{call}");
        assert!(prompts.lock().unwrap().is_empty(), "{call}");
    }
}

#[tokio::test]
async fn admin_originator_is_delegated_too() {
    let module = ModuleDouble::new(Refuse::Never);
    let (_dir, wallet, _double, _prompts) = module_wallet(vec![("btms", module.clone())]).await;
    for call in CALLS {
        key_call(&wallet, call, &named("p btms witness"), ADMIN)
            .await
            .unwrap_or_else(|e| panic!("{call}: {e:?}"));
    }
    assert_eq!(module.asked(), CALLS.len());
}

// =========================================================================
// The refusals: an unregistered scheme (`:856-860`), a module's own refusal,
// the reserved `nosend` (`:792-801`).
// =========================================================================

#[tokio::test]
async fn unregistered_scheme_is_refused_before_any_check() {
    let module = ModuleDouble::new(Refuse::Never);
    let (_dir, wallet, double, prompts) = module_wallet(vec![("btms", module.clone())]).await;
    for call in CALLS {
        let answer = key_call(&wallet, call, &named("p foo witness"), ORIGINATOR).await;
        assert_module_refusal(answer, "Unsupported P-module scheme: p foo", call);
    }
    assert_eq!(module.asked(), 0);
    assert!(double.asked().is_empty());
    assert!(prompts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn module_refusal_crosses_with_its_text() {
    for (refuse, text) in [
        (Refuse::Request, "the module refused {}"),
        (Refuse::Response, "the module refused the {} result"),
    ] {
        let module = ModuleDouble::new(refuse);
        let (_dir, wallet, double, _prompts) = module_wallet(vec![("btms", module)]).await;
        for (call, method) in CALLS.into_iter().zip(METHODS) {
            let answer = key_call(&wallet, call, &named("p btms witness"), ORIGINATOR).await;
            assert_module_refusal(answer, &text.replace("{}", method), call);
        }
        let reached = if refuse == Refuse::Request {
            0
        } else {
            CALLS.len()
        };
        assert_eq!(double.asked().len(), reached);
    }
}

#[tokio::test]
async fn host_nosend_module_is_never_asked() {
    let host = ModuleDouble::new(Refuse::Never);
    let (_dir, wallet, double, prompts) = module_wallet(vec![("nosend", host.clone())]).await;
    for call in CALLS {
        let answer = key_call(&wallet, call, &named("p nosend witness"), ORIGINATOR).await;
        assert_module_refusal(answer, NOSEND_TEXT, call);
    }
    assert_eq!(host.asked(), 0);
    assert!(double.asked().is_empty());
    assert!(prompts.lock().unwrap().is_empty());
}

// =========================================================================
// What does not delegate: any other name (the case-sensitive `'p '`,
// `:851`), the identity key alone (`:5147`), the linkage reveals.
// =========================================================================

#[tokio::test]
async fn other_names_take_the_ordinary_check() {
    let module = ModuleDouble::new(Refuse::Never);
    let (_dir, wallet, _double, prompts) = module_wallet(vec![("btms", module.clone())]).await;
    for name in ["btms witness", "P btms witness", "pbtms witness"] {
        for call in CALLS {
            let err = key_call(&wallet, call, &named(name), ORIGINATOR)
                .await
                .expect_err(call);
            let unauthorized = bsv_rs::WalletRefusalClass::Unauthorized;
            assert!(
                matches!(&err, bsv_rs::Error::WalletRefusal { class, .. } if *class == unauthorized),
                "{name} {call}: {err:?}"
            );
        }
    }
    assert_eq!(module.asked(), 0);
    // `key_call`'s get_public_key leaves `seek_permission` absent, which
    // seeks, so all seven prompt under each name.
    assert_eq!(prompts.lock().unwrap().len(), 3 * CALLS.len());
}

#[tokio::test]
async fn identity_key_alone_does_not_delegate() {
    let module = ModuleDouble::new(Refuse::Never);
    let (_dir, wallet, _double, prompts) = module_wallet(vec![("btms", module.clone())]).await;
    let args = GetPublicKeyArgs {
        identity_key: true,
        ..Default::default()
    };
    let err = wallet.get_public_key(args, Some(ORIGINATOR)).await;
    assert!(err.is_err());
    assert_eq!(module.asked(), 0);
    let asked = prompts.lock().unwrap().clone();
    assert_eq!(asked, vec!["identity key retrieval".to_string()]);
}

#[tokio::test]
async fn linkage_reveals_do_not_delegate() {
    let module = ModuleDouble::new(Refuse::Never);
    let (_dir, wallet, _double, prompts) = module_wallet(vec![("btms", module.clone())]).await;
    let verifier = PrivateKey::random().public_key();
    let specific = InterfaceRevealSpecificArgs {
        counterparty: Counterparty::Anyone,
        verifier: verifier.clone(),
        protocol_id: named("p btms witness"),
        key_id: "1".to_string(),
        privileged: None,
        privileged_reason: None,
    };
    assert!(wallet
        .reveal_specific_key_linkage(specific, Some(ORIGINATOR))
        .await
        .is_err());
    let counterparty = InterfaceRevealCounterpartyArgs {
        counterparty: PrivateKey::random().public_key(),
        verifier,
        privileged: None,
        privileged_reason: None,
    };
    assert!(wallet
        .reveal_counterparty_key_linkage(counterparty, Some(ORIGINATOR))
        .await
        .is_err());
    assert_eq!(module.asked(), 0);
    let asked = prompts.lock().unwrap().clone();
    assert_eq!(asked.len(), 2, "{asked:?}");
    assert!(asked[0].starts_with("specific key linkage revelation p btms witness"));
}
