//! The two key-linkage reveals under `privileged` (#569): a privileged
//! reveal reaches the wallet's `PrivilegedKeyManager` with the caller's
//! `privileged` and `privileged_reason`, as ts-stack@edf6e03
//! `Wallet.ts:467-490` sends it to `this.privilegedKeyManager`; with none
//! configured it is the reference's refusal, text byte for byte; an
//! unprivileged reveal stays with the everyday key. Neither the reference's
//! reveal arguments nor bsv-rs's carry `seekPermission`.

use super::busy_door_tests::wallet;
use super::privileged_keys_tests::{
    assert_argument_refusal, assert_privileged_refusal, Asked, PrivilegedDouble, ORIGINATOR,
};
use super::*;
use crate::services::mock::MockWalletServices;
use crate::StorageSqlx;

const REASON: &str = "privileged witness";
const REVEALS: [&str; 2] = [
    "reveal_counterparty_key_linkage",
    "reveal_specific_key_linkage",
];

type TestWallet = Wallet<StorageSqlx, MockWalletServices>;

/// The key the reveals name as counterparty and verifier.
fn other_key() -> PublicKey {
    PrivateKey::random().public_key()
}

/// One reveal with `privileged` and `privileged_reason` as given; its answer's
/// prover and counterparty, as compressed hex.
async fn reveal(
    wallet: &TestWallet,
    name: &str,
    privileged: Option<bool>,
    privileged_reason: Option<&str>,
) -> bsv_rs::Result<(String, String)> {
    let originator = Some(ORIGINATOR);
    let privileged_reason = privileged_reason.map(str::to_string);
    let counterparty = other_key();
    let linkage = match name {
        "reveal_counterparty_key_linkage" => {
            let args = InterfaceRevealCounterpartyArgs {
                counterparty,
                verifier: other_key(),
                privileged,
                privileged_reason,
            };
            WalletInterface::reveal_counterparty_key_linkage(wallet, args, originator)
                .await?
                .linkage
        }
        "reveal_specific_key_linkage" => {
            let args = InterfaceRevealSpecificArgs {
                counterparty: Counterparty::Other(counterparty),
                verifier: other_key(),
                protocol_id: Protocol::new(SecurityLevel::App, "privileged witness"),
                key_id: "1".to_string(),
                privileged,
                privileged_reason,
            };
            WalletInterface::reveal_specific_key_linkage(wallet, args, originator)
                .await?
                .linkage
        }
        other => unreachable!("{other}"),
    };
    Ok((linkage.prover.to_hex(), linkage.counterparty.to_hex()))
}

#[tokio::test]
async fn each_privileged_reveal_reaches_the_manager_with_the_callers_fields() {
    let (_dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::new();
    wallet.set_privileged_key_manager(double.clone());
    for name in REVEALS {
        let before = double.asked().len();
        let answer = reveal(&wallet, name, Some(true), Some(REASON)).await;
        let (prover, _) = answer.unwrap_or_else(|e| panic!("{name}: {e:?}"));
        // The linkage is revealed under the manager's key, not the wallet's.
        assert_eq!(prover, double.key.identity_key_hex(), "{name}");
        assert_ne!(prover, wallet.identity_key(), "{name}");
        let made: Vec<Asked> = double.asked().split_off(before);
        assert_eq!(made, vec![Asked::privileged(name, REASON, None)], "{name}");
    }
}

#[tokio::test]
async fn each_privileged_reveal_without_a_manager_is_the_references_refusal() {
    let (_dir, wallet) = wallet().await;
    assert!(wallet.privileged_key_manager().is_none());
    for name in REVEALS {
        assert_privileged_refusal(name, reveal(&wallet, name, Some(true), Some(REASON)).await);
    }
}

#[tokio::test]
async fn unprivileged_reveals_stay_with_the_everyday_key() {
    let (_dir, mut wallet) = wallet().await;
    let double = PrivilegedDouble::new();
    wallet.set_privileged_key_manager(double.clone());
    for privileged in [None, Some(false)] {
        for name in REVEALS {
            let answer = reveal(&wallet, name, privileged, None).await;
            let (prover, _) = answer.unwrap_or_else(|e| panic!("{name} {privileged:?}: {e:?}"));
            assert_eq!(prover, wallet.identity_key(), "{name} {privileged:?}");
        }
    }
    assert_eq!(double.asked(), Vec::<Asked>::new());
}

#[tokio::test]
async fn a_privileged_reveal_without_a_reason_is_the_validators_refusal() {
    let double = PrivilegedDouble::new();
    for manager in [None, Some(double.clone())] {
        let (_dir, mut wallet) = wallet().await;
        if let Some(manager) = manager.clone() {
            wallet.set_privileged_key_manager(manager);
        }
        for name in REVEALS {
            let scene = format!("{name}, a manager: {}", manager.is_some());
            let refusal =
                "The privilegedReason parameter must be a value when 'privileged' is true";
            assert_argument_refusal(
                &scene,
                reveal(&wallet, name, Some(true), None).await,
                refusal,
            );
        }
    }
    // The manager never hears a request its arguments refuse.
    assert!(double.asked().is_empty());
}
