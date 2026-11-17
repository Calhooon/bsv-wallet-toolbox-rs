//! `Wallet::internalize_action` with a permissions manager asks it before
//! anything is stored: each basket insertion's basket for insertion (as
//! `create_action` asks for its output baskets), and an admin-reserved
//! basket or action label is refused to every originator but the admin
//! originator. With no manager nothing is asked.
//!
//! Real SQLite storage, a mock broadcaster; nothing touches the network.

#![cfg(feature = "sqlite")]

mod common;

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{
    BasketInsertion, CreateActionArgs, CreateActionOptions, CreateActionOutput,
    InternalizeActionArgs, InternalizeOutput, WalletInterface,
};
use bsv_wallet_toolbox_rs::managers::{
    PermissionRequest, PermissionRequestHandler, PermissionToken, WalletPermissionsManager,
    WalletPermissionsManagerConfig,
};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{Error, StorageSqlx, Wallet};

use common::{beef_paying, other_lock, stored_outputs};

type W = Wallet<StorageSqlx, MockWalletServices>;

const ADMIN: &str = "admin.local";
const GRANTED: &str = "granted.example";
const APP: &str = "app.example";

fn token(originator: &str) -> PermissionToken {
    PermissionToken {
        txid: "ab".repeat(32),
        tx: vec![0],
        output_index: 0,
        output_script: "76a914".into(),
        satoshis: 1,
        originator: originator.into(),
        expiry: (chrono::Utc::now().timestamp() + 3600) as u64,
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

/// A wallet paid by `tx`; with `manager`, a permissions manager whose
/// handler grants `GRANTED` and refuses every other originator.
async fn wallet(txid: &str, manager: bool) -> W {
    let root = PrivateKey::random();
    let mut wallet = common::wallet("internalize-permission-tests", &root, txid).await;
    if manager {
        let under = common::wallet("underlying", &PrivateKey::random(), txid).await;
        let under: Arc<dyn WalletInterface + Send + Sync> = Arc::new(under);
        let pm = Arc::new(WalletPermissionsManager::new(
            under,
            ADMIN.into(),
            WalletPermissionsManagerConfig::all_enabled(),
        ));
        let handler: PermissionRequestHandler = Arc::new(move |r: PermissionRequest| {
            let originator = r.originator.clone();
            Box::pin(async move {
                if originator == GRANTED {
                    Ok(token(&originator))
                } else {
                    Err(Error::AccessDenied(format!(
                        "the user refused {originator}"
                    )))
                }
            })
        });
        pm.set_permission_request_handler(handler).await;
        wallet.set_permissions_manager(pm);
    }
    wallet
}

fn insertion_args(tx: Vec<u8>, basket: &str, labels: Option<Vec<String>>) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![InternalizeOutput {
            output_index: 0,
            protocol: "basket insertion".to_string(),
            payment_remittance: None,
            insertion_remittance: Some(BasketInsertion {
                basket: basket.to_string(),
                custom_instructions: None,
                tags: None,
            }),
        }],
        description: "an insertion to check".to_string(),
        labels,
        seek_permission: None,
    }
}

/// The answer to internalizing one output into `basket` with `labels` by
/// `originator`, and the number of outputs stored after it.
async fn internalize(
    manager: bool,
    originator: &str,
    basket: &str,
    labels: Option<Vec<String>>,
) -> (Result<bool, String>, i64) {
    let (tx, txid) = beef_paying(other_lock());
    let wallet = wallet(&txid, manager).await;
    let answer = wallet
        .internalize_action(insertion_args(tx, basket, labels), originator)
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());
    (answer, stored_outputs(&wallet).await)
}

#[tokio::test]
async fn a_named_basket_with_no_grant_is_refused_as_create_action_refuses_it() {
    let (answer, stored) = internalize(true, APP, "tokens", None).await;
    println!("internalize, no grant, basket tokens: {answer:?}");
    assert_eq!(stored, 0, "nothing is stored");

    let (_, txid) = beef_paying(other_lock());
    let create = wallet(&txid, true)
        .await
        .create_action(
            CreateActionArgs {
                description: "an output to check".into(),
                input_beef: None,
                inputs: None,
                outputs: Some(vec![CreateActionOutput {
                    locking_script: other_lock().to_binary(),
                    satoshis: 0,
                    output_description: "an output".into(),
                    basket: Some("tokens".into()),
                    custom_instructions: None,
                    tags: None,
                }]),
                lock_time: None,
                version: None,
                labels: None,
                options: Some(CreateActionOptions {
                    no_send: Some(true),
                    ..Default::default()
                }),
            },
            APP,
        )
        .await
        .map(|_| true)
        .map_err(|e| e.to_string());
    println!("createAction, no grant, basket tokens: {create:?}");
    assert_eq!(answer, create, "the same refusal at both calls");
    assert!(answer.is_err());
}

#[tokio::test]
async fn an_admin_basket_is_refused_to_an_application() {
    let (answer, stored) = internalize(true, GRANTED, "admin vault", None).await;
    println!("internalize, basket admin vault: {answer:?}");
    assert_eq!(
        answer,
        Err("wallet error: Access denied: Basket \"admin vault\" is admin-only.".into())
    );
    assert_eq!(stored, 0);
}

#[tokio::test]
async fn an_admin_label_is_refused_to_an_application() {
    let labels = Some(vec!["admin secret".to_string()]);
    let (answer, stored) = internalize(true, GRANTED, "tokens", labels).await;
    println!("internalize, label admin secret: {answer:?}");
    assert_eq!(
        answer,
        Err("wallet error: Access denied: Label \"admin secret\" is admin-only.".into())
    );
    assert_eq!(stored, 0);
}

#[tokio::test]
async fn the_admin_originator_and_a_granted_originator_are_admitted() {
    let labels = Some(vec!["admin secret".to_string()]);
    let (answer, stored) = internalize(true, ADMIN, "admin vault", labels).await;
    assert_eq!((answer, stored), (Ok(true), 1));

    let labels = Some(vec!["receipts".to_string()]);
    let (answer, stored) = internalize(true, GRANTED, "tokens", labels).await;
    assert_eq!((answer, stored), (Ok(true), 1));
}

#[tokio::test]
async fn with_no_manager_nothing_is_asked() {
    let labels = Some(vec!["admin secret".to_string()]);
    let (answer, stored) = internalize(false, APP, "admin vault", labels).await;
    assert_eq!((answer, stored), (Ok(true), 1));
}

/// A manager with no permission request handler: a named basket with no
/// grant is refused with the manager's own text.
#[tokio::test]
async fn a_named_basket_with_no_handler_is_refused() {
    let root = PrivateKey::random();
    let (tx, txid) = beef_paying(other_lock());
    let mut wallet = common::wallet("internalize-permission-tests", &root, &txid).await;
    let under = common::wallet("underlying", &PrivateKey::random(), &txid).await;
    let under: Arc<dyn WalletInterface + Send + Sync> = Arc::new(under);
    wallet.set_permissions_manager(Arc::new(WalletPermissionsManager::new(
        under,
        ADMIN.into(),
        WalletPermissionsManagerConfig::all_enabled(),
    )));
    let answer = wallet
        .internalize_action(insertion_args(tx, "tokens", None), APP)
        .await
        .map(|r| r.accepted)
        .map_err(|e| e.to_string());
    println!("no handler: {answer:?}");
    assert!(answer.is_err());
    assert_eq!(stored_outputs(&wallet).await, 0);
}
