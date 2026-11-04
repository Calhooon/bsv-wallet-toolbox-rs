//! The usage type in the prompt (#763): the request a handler receives
//! carries the usage the manager asks for, as the reference's request
//! carries `usageType` (ts-stack@edf6e03 `WalletPermissionsManager.ts:311`,
//! set at `:1540`, `:1557`, `:1599`, `:1608`, `:1687`, `:1702`); a spending
//! request none. The request id appends it (`:5830-5840`), so two usages of
//! one protocol prompt apart; one usage asked twice prompts once. The seven
//! key calls' witnesses are the wallet's (`privileged_keys_permission_tests`).

use std::sync::Mutex as StdMutex;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::ProtoWallet;
use tokio::sync::mpsc;

use super::*;

const APP: &str = "app.example.com";

/// A manager over a `ProtoWallet` (no stored token) under every flag on.
fn manager() -> Arc<WalletPermissionsManager> {
    let wallet: Arc<dyn WalletInterface + Send + Sync> =
        Arc::new(ProtoWallet::new(Some(PrivateKey::random())));
    let config = WalletPermissionsManagerConfig::all_enabled();
    Arc::new(WalletPermissionsManager::new(
        wallet,
        "admin.wallet".to_string(),
        config,
    ))
}

/// A manager whose handler records each request and refuses it.
async fn recording() -> (
    Arc<WalletPermissionsManager>,
    Arc<StdMutex<Vec<PermissionRequest>>>,
) {
    let manager = manager();
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let record = Arc::clone(&seen);
    manager
        .set_permission_request_handler(Arc::new(move |request| {
            record.lock().unwrap().push(request);
            Box::pin(async { Err(Error::AccessDenied("refused by the test".to_string())) })
        }))
        .await;
    (manager, seen)
}

/// The usage type of the one request `seen` holds, taken.
fn asked(seen: &StdMutex<Vec<PermissionRequest>>) -> Option<RequestUsageType> {
    let mut seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "{seen:?}");
    seen.pop().unwrap().usage_type
}

fn level2() -> Protocol {
    Protocol::new(SecurityLevel::Counterparty, "usage witness")
}

const PROTOCOL_USAGES: [PermissionUsageType; 7] = [
    PermissionUsageType::Signing,
    PermissionUsageType::Encrypting,
    PermissionUsageType::Hmac,
    PermissionUsageType::PublicKey,
    PermissionUsageType::IdentityKey,
    PermissionUsageType::LinkageRevelation,
    PermissionUsageType::Generic,
];

#[tokio::test]
async fn a_protocol_prompt_carries_the_usage_type_it_asks_for() {
    let (manager, seen) = recording().await;
    for usage in PROTOCOL_USAGES {
        let protocol = level2();
        let call = manager.ensure_protocol_permission(APP, false, &protocol, None, None, usage);
        assert!(call.await.is_err(), "{usage:?}");
        assert_eq!(asked(&seen), Some(RequestUsageType::Protocol(usage)));
    }
}

#[tokio::test]
async fn a_basket_prompt_carries_its_basket_usage() {
    let (manager, seen) = recording().await;
    for usage in [
        BasketUsageType::Insertion,
        BasketUsageType::Removal,
        BasketUsageType::Listing,
    ] {
        let call = manager.ensure_basket_access(APP, "usage basket", None, usage);
        assert!(call.await.is_err(), "{usage:?}");
        assert_eq!(asked(&seen), Some(RequestUsageType::Basket(usage)));
    }
}

#[tokio::test]
async fn a_certificate_prompt_carries_disclosure_and_a_spending_prompt_none() {
    let (manager, seen) = recording().await;
    let fields = vec!["name".to_string()];
    let disclosure = CertificateUsageType::Disclosure;
    let verifier = PrivateKey::random().public_key().to_hex();
    let call =
        manager.ensure_certificate_access(APP, false, &verifier, "cert", &fields, None, disclosure);
    assert!(call.await.is_err());
    assert_eq!(
        asked(&seen),
        Some(RequestUsageType::Certificate(disclosure))
    );
    assert!(manager
        .ensure_spending_permission(APP, 10, None)
        .await
        .is_err());
    assert_eq!(
        asked(&seen),
        None,
        "the reference's spending request has none"
    );
}

#[test]
fn the_usage_type_is_on_the_wire_by_the_references_names() {
    let names = [
        "signing",
        "encrypting",
        "hmac",
        "publicKey",
        "identityKey",
        "linkageRevelation",
        "generic",
    ];
    let protocol = PROTOCOL_USAGES.map(RequestUsageType::from);
    let others = [
        (BasketUsageType::Insertion.into(), "insertion"),
        (BasketUsageType::Removal.into(), "removal"),
        (BasketUsageType::Listing.into(), "listing"),
        (CertificateUsageType::Disclosure.into(), "disclosure"),
    ];
    for (usage, name) in protocol.into_iter().zip(names).chain(others) {
        let request = PermissionRequest {
            usage_type: Some(usage),
            ..PermissionRequest::bare(PermissionType::Protocol, APP)
        };
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["usageType"], name);
        assert_eq!(usage.as_str(), name);
        let parsed: PermissionRequest = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.usage_type, Some(usage), "{name}");
    }
    let bare = serde_json::to_value(PermissionRequest::bare(PermissionType::Spending, APP));
    assert!(bare.unwrap().get("usageType").is_none());
}

/// A manager whose handler publishes each request and leaves the answer
/// to the host.
async fn pending() -> (
    Arc<WalletPermissionsManager>,
    mpsc::UnboundedReceiver<PermissionRequest>,
) {
    let manager = manager();
    let (requests, asked) = mpsc::unbounded_channel();
    manager
        .set_permission_request_handler(Arc::new(move |request| {
            let _ = requests.send(request);
            Box::pin(std::future::pending())
        }))
        .await;
    (manager, asked)
}

/// The witness protocol asked under `usage`, in its own task.
fn spawn_ask(
    manager: &Arc<WalletPermissionsManager>,
    usage: PermissionUsageType,
) -> tokio::task::JoinHandle<Result<bool>> {
    let manager = Arc::clone(manager);
    tokio::spawn(async move {
        let protocol = level2();
        let call = manager.ensure_protocol_permission(APP, false, &protocol, None, None, usage);
        call.await
    })
}

/// The next prompt, or a failure within five seconds: a request that joins
/// another's queue prompts nothing.
async fn next_prompt(asked: &mut mpsc::UnboundedReceiver<PermissionRequest>) -> PermissionRequest {
    let next = tokio::time::timeout(std::time::Duration::from_secs(5), asked.recv()).await;
    next.expect("a prompt of its own").unwrap()
}

#[tokio::test]
async fn two_usages_of_one_protocol_prompt_apart_and_one_usage_prompts_once() {
    let (manager, mut asked) = pending().await;
    let signing = spawn_ask(&manager, PermissionUsageType::Signing);
    let signing_request = next_prompt(&mut asked).await;
    let linkage = spawn_ask(&manager, PermissionUsageType::LinkageRevelation);
    let linkage_request = next_prompt(&mut asked).await;
    let signing_id = signing_request.request_id.unwrap();
    let linkage_id = linkage_request.request_id.unwrap();
    assert!(signing_id.ends_with(":signing"), "{signing_id}");
    assert!(linkage_id.ends_with(":linkageRevelation"), "{linkage_id}");
    let base = signing_id.strip_suffix("signing").unwrap();
    assert_eq!(linkage_id.strip_suffix("linkageRevelation"), Some(base));
    // A second signing ask joins the pending one: no prompt of its own.
    // The single-thread test runtime polls it to the queue in a few yields
    // (no I/O on the way); a prompt of its own would leave it pending.
    let joined = spawn_ask(&manager, PermissionUsageType::Signing);
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    manager.deny_permission(&signing_id).unwrap();
    assert!(signing.await.unwrap().is_err());
    let joined = tokio::time::timeout(std::time::Duration::from_secs(5), joined).await;
    assert!(joined.expect("joined the signing queue").unwrap().is_err());
    manager.deny_permission(&linkage_id).unwrap();
    assert!(linkage.await.unwrap().is_err());
    assert!(asked.try_recv().is_err(), "one prompt per usage");
}
