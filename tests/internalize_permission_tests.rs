//! The basket and label permission checks of `Wallet::internalize_action`
//! with a permissions manager configured, as the reference's manager asks
//! them before the underlying call (ts-stack@4d0ea63c8
//! `WalletPermissionsManager.ts:5056-5072`, `:4991-5021`):
//!
//! - each basket insertion's basket that is not a `p ` basket is asked at
//!   insertion usage, the request's description the reason; a basket the
//!   manager reserves (`admin ...`, `default`) is refused as admin-only
//!   (`:1586-1590`);
//! - each label that is not a `p ` label is asked as the level-1 protocol
//!   `action label <label>`; an `admin ...` label is refused as admin-only
//!   (`:1786-1835`, `:5600-5609`);
//! - the admin originator is admitted, and so is an originator the user
//!   grants; with no manager nothing is asked.
//!
//! A `p <scheme>` basket or label goes to its module, as before
//! (`src/wallet/permission_modules_basket_tests.rs`).
//!
//! Real SQLite storage, a mock broadcaster and tracker; nothing touches the
//! network.

#![cfg(feature = "sqlite")]

#[path = "common/internalize_payment.rs"]
mod common;
#[path = "common/insertion.rs"]
mod insertion;

use std::sync::{Arc, Mutex};

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{InternalizeActionArgs, ProtoWallet, WalletInterface};
use bsv_rs::WalletRefusalClass;
use bsv_wallet_toolbox_rs::{
    Error, PermissionRequest, PermissionToken, WalletPermissionsManager,
    WalletPermissionsManagerConfig,
};

use common::{args, beef_paying, other_lock, refusal};
use insertion::*;

const APP: &str = "app.example";
const ADMIN: &str = "wallet.admin";
const DESCRIPTION: &str = "an output to keep";

type Asked = Arc<Mutex<Vec<String>>>;

fn token() -> PermissionToken {
    PermissionToken {
        txid: String::new(),
        tx: vec![],
        output_index: 0,
        output_script: String::new(),
        satoshis: 1,
        originator: String::new(),
        expiry: 4_000_000_000,
        privileged: None,
        protocol: None,
        security_level: None,
        counterparty: None,
        basket_name: None,
        cert_type: None,
        cert_fields: None,
        verifier: None,
        authorized_amount: None,
    }
}

/// A wallet of `root` that admits `txid`'s broadcast, under a manager with
/// every check on whose handler grants what `grant` admits and refuses the
/// rest; each prompt recorded as
/// `<basket or protocol> <reason>`.
async fn guarded(
    root: &PrivateKey,
    txid: &str,
    grant: fn(&PermissionRequest) -> bool,
) -> (TestWallet, Asked) {
    let config = WalletPermissionsManagerConfig::all_enabled();
    guarded_by(root, txid, grant, config).await
}

/// [`guarded`] under `config`.
async fn guarded_by(
    root: &PrivateKey,
    txid: &str,
    grant: fn(&PermissionRequest) -> bool,
    config: WalletPermissionsManagerConfig,
) -> (TestWallet, Asked) {
    let mut wallet = wallet(root, txid).await;
    let manager = Arc::new(WalletPermissionsManager::new(
        Arc::new(ProtoWallet::new(Some(root.clone()))),
        ADMIN.to_string(),
        config,
    ));
    let asked: Asked = Arc::default();
    let seen = Arc::clone(&asked);
    manager
        .set_permission_request_handler(Arc::new(move |r: PermissionRequest| {
            let name = r
                .basket
                .clone()
                .or(r.protocol_id.as_ref().map(|p| p.protocol_name.clone()))
                .unwrap_or_default();
            let reason = r.reason.clone().unwrap_or_default();
            seen.lock().unwrap().push(format!("{name} | {reason}"));
            let granted = grant(&r);
            Box::pin(async move {
                if granted {
                    Ok(token())
                } else {
                    Err(Error::AccessDenied("refused by the user".into()))
                }
            })
        }))
        .await;
    wallet.set_permissions_manager(manager);
    (wallet, asked)
}

/// An insertion of a fresh transaction's one output into `basket`,
/// labelled `labels`.
fn request(basket: &str, labels: &[&str]) -> (InternalizeActionArgs, String) {
    let (bytes, txid) = beef_paying(other_lock());
    let mut a = args(bytes, insertion(0, basket));
    a.description = DESCRIPTION.to_string();
    if !labels.is_empty() {
        a.labels = Some(labels.iter().map(ToString::to_string).collect());
    }
    (a, txid)
}

async fn transactions(wallet: &TestWallet) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
        .fetch_one(wallet.storage().writer())
        .await
        .unwrap()
}

async fn from(
    wallet: &TestWallet,
    a: InternalizeActionArgs,
    originator: &str,
) -> bsv_rs::Result<bool> {
    wallet
        .internalize_action(a, Some(originator))
        .await
        .map(|r| r.accepted)
}

/// A refused request: its class and text, nothing stored, no basket made.
async fn refused(wallet: &TestWallet, a: InternalizeActionArgs) -> (WalletRefusalClass, String) {
    let before = counts(wallet).await;
    let answer = refusal(from(wallet, a, APP).await, "internalize");
    assert_eq!(counts(wallet).await, before, "nothing stored");
    assert_eq!(transactions(wallet).await, 0, "no transaction row");
    answer
}

/// An originator with no grant inserting into a named basket is asked, the
/// description the reason, and refused as the user refuses; the basket is
/// asked as `create_action` asks it, the same refusal.
#[tokio::test]
async fn an_insertion_into_a_basket_with_no_grant_is_refused() {
    let root = PrivateKey::random();
    let (a, txid) = request("tokens", &[]);
    let (wallet, asked) = guarded(&root, &txid, |_| false).await;
    let answer = refused(&wallet, a).await;
    assert_eq!(
        answer,
        (
            WalletRefusalClass::Unauthorized,
            "Access denied: refused by the user".to_string()
        )
    );
    assert_eq!(
        asked.lock().unwrap().clone(),
        vec![format!("tokens | {DESCRIPTION}")]
    );
}

/// An insertion into an `admin ...` basket is refused as admin-only, with
/// no prompt.
#[tokio::test]
async fn an_insertion_into_an_admin_basket_is_refused_as_admin_only() {
    let root = PrivateKey::random();
    let (a, txid) = request("admin secrets", &[]);
    let (wallet, asked) = guarded(&root, &txid, |_| true).await;
    let answer = refused(&wallet, a).await;
    assert_eq!(
        answer,
        (
            WalletRefusalClass::Unauthorized,
            "Access denied: Basket \"admin secrets\" is admin-only.".to_string()
        )
    );
    assert!(asked.lock().unwrap().is_empty());
}

/// A label with no grant is asked as `action label <label>` and refused; an
/// `admin ...` label is refused as admin-only with no prompt.
#[tokio::test]
async fn a_label_with_no_grant_and_an_admin_label_are_refused() {
    let root = PrivateKey::random();
    // The basket granted, the label refused.
    let (a, txid) = request("tokens", &["invoices"]);
    let (wallet, asked) = guarded(&root, &txid, |r| r.basket.is_some()).await;
    let answer = refused(&wallet, a).await;
    assert_eq!(
        answer,
        (
            WalletRefusalClass::Unauthorized,
            "Access denied: refused by the user".to_string()
        )
    );
    assert_eq!(
        asked.lock().unwrap().clone(),
        vec![
            format!("tokens | {DESCRIPTION}"),
            format!("action label invoices | {DESCRIPTION}"),
        ]
    );

    let (a, txid) = request("tokens", &["admin label"]);
    let (wallet, asked) = guarded(&root, &txid, |_| true).await;
    let answer = refused(&wallet, a).await;
    assert_eq!(
        answer,
        (
            WalletRefusalClass::Unauthorized,
            "Access denied: Label \"admin label\" is admin-only.".to_string()
        )
    );
    // The basket was asked ahead of the label, and granted; the label refused unasked.
    assert_eq!(
        asked.lock().unwrap().clone(),
        vec![format!("tokens | {DESCRIPTION}")]
    );
}

/// The admin originator is admitted into an `admin ...` basket with an
/// `admin ...` label, with no prompt.
#[tokio::test]
async fn the_admin_originator_is_admitted() {
    let root = PrivateKey::random();
    let (a, txid) = request("admin secrets", &["admin label", "invoices"]);
    let (wallet, asked) = guarded(&root, &txid, |_| false).await;
    assert!(from(&wallet, a, ADMIN).await.unwrap());
    assert_eq!(
        row(&wallet, &txid, 0).await.0.as_deref(),
        Some("admin secrets")
    );
    assert!(asked.lock().unwrap().is_empty());
}

/// An originator the user grants is admitted: the basket asked, then each
/// label, then the output stored.
#[tokio::test]
async fn an_originator_the_user_grants_is_admitted() {
    let root = PrivateKey::random();
    let (a, txid) = request("tokens", &["invoices"]);
    let (wallet, asked) = guarded(&root, &txid, |_| true).await;
    assert!(from(&wallet, a, APP).await.unwrap());
    assert_eq!(row(&wallet, &txid, 0).await.0.as_deref(), Some("tokens"));
    assert_eq!(
        asked.lock().unwrap().clone(),
        vec![
            format!("tokens | {DESCRIPTION}"),
            format!("action label invoices | {DESCRIPTION}"),
        ]
    );
}

/// With no manager nothing is asked: a named basket and any label are
/// admitted, as before.
#[tokio::test]
async fn with_no_manager_nothing_is_asked() {
    let root = PrivateKey::random();
    let (a, txid) = request("admin secrets", &["admin label"]);
    let wallet = wallet(&root, &txid).await;
    assert!(from(&wallet, a, APP).await.unwrap());
    assert_eq!(
        row(&wallet, &txid, 0).await.0.as_deref(),
        Some("admin secrets")
    );
}

/// An originator with no grant inserting into `tokens` with the label
/// `invoices`, the handler refusing every prompt, under the default config
/// and with each of the two flags on: with both off (`Default`) it is
/// admitted unasked; with `seek_basket_insertion_permissions` on the basket
/// is asked and refused; with `seek_permission_when_applying_action_labels`
/// on the label is asked and refused.
#[tokio::test]
async fn each_flag_decides_whether_its_name_is_asked() {
    let root = PrivateKey::random();
    let basket_on = WalletPermissionsManagerConfig {
        seek_basket_insertion_permissions: true,
        ..Default::default()
    };
    let label_on = WalletPermissionsManagerConfig {
        seek_permission_when_applying_action_labels: true,
        ..Default::default()
    };
    let refusal_text = "Access denied: refused by the user".to_string();
    for (name, config, answer, prompts) in [
        (
            "default",
            WalletPermissionsManagerConfig::default(),
            Ok(true),
            vec![],
        ),
        (
            "basket flag on",
            basket_on,
            Err((WalletRefusalClass::Unauthorized, refusal_text.clone())),
            vec![format!("tokens | {DESCRIPTION}")],
        ),
        (
            "label flag on",
            label_on,
            Err((WalletRefusalClass::Unauthorized, refusal_text.clone())),
            vec![format!("action label invoices | {DESCRIPTION}")],
        ),
    ] {
        let (a, txid) = request("tokens", &["invoices"]);
        let (wallet, asked) = guarded_by(&root, &txid, |_| false, config).await;
        let got = from(&wallet, a, APP)
            .await
            .map_err(|e| refusal(Err(e), name));
        let prompted = asked.lock().unwrap().clone();
        println!("{name}: {got:?}, prompts {prompted:?}");
        assert_eq!((got, prompted), (answer, prompts), "{name}");
    }
}

/// An insertion into `default` under a manager, with the default config,
/// is refused as admin-only by the manager before the validator; with no
/// manager the validator's sentence answers.
#[tokio::test]
async fn an_insertion_into_default_under_a_manager_is_admin_only() {
    let root = PrivateKey::random();
    let (a, txid) = request("default", &[]);
    let config = WalletPermissionsManagerConfig::default();
    let (wallet, asked) = guarded_by(&root, &txid, |_| true, config).await;
    let answer = refused(&wallet, a).await;
    println!("under a manager: {answer:?}");
    assert_eq!(
        answer,
        (
            WalletRefusalClass::Unauthorized,
            "Access denied: Basket \"default\" is admin-only.".to_string()
        )
    );
    assert!(asked.lock().unwrap().is_empty());

    let (a, txid) = request("default", &[]);
    let bare = insertion::wallet(&root, &txid).await;
    let answer = refused(&bare, a).await;
    println!("with no manager: {answer:?}");
    assert_eq!(
        answer,
        (
            WalletRefusalClass::InvalidParameter,
            format!("Validation error: {NON_DEFAULT}")
        )
    );
}
