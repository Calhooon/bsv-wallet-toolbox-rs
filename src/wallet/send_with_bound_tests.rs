//! signAction's `sendWith` bounds (#324): ts-stack@edf6e03
//! validationHelpers.ts validateSendWith (136-144, `MAXIMUM_SEND_WITH_TRANSACTIONS`
//! 1000 at 69) called by validateSignActionOptions at 823, rendered through
//! WERR_INVALID_PARAMETER.ts:14, ahead of the session lookup (signAction.ts:57).

use super::busy_door_tests::{store_snapshot, wallet};
use super::*;
use bsv_rs::wallet::SignActionOptions;
use bsv_rs::WalletRefusalClass;

const ORIGINATOR: &str = "refusal.test";
const SEND_WITH_LIMIT: usize = 1000;
const REFERENCE: &str = "bWlzc2luZw==";
const BOUND_TEXT: &str =
    "Validation error: The sendWith parameter must be an array of at most 1000 items";

fn txid(i: usize) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[..8].copy_from_slice(&(i as u64).to_le_bytes());
    txid
}

fn sign_request(send_with: Vec<[u8; 32]>) -> SignActionArgs {
    SignActionArgs {
        reference: REFERENCE.to_string(),
        spends: HashMap::new(),
        options: Some(SignActionOptions {
            accept_delayed_broadcast: None,
            return_txid_only: None,
            no_send: None,
            send_with: Some(send_with),
        }),
    }
}

async fn refusal(send_with: Vec<[u8; 32]>) -> (WalletRefusalClass, String) {
    let (_dir, wallet) = wallet().await;
    let before = store_snapshot(&wallet).await;
    let error = wallet
        .sign_action(sign_request(send_with), Some(ORIGINATOR))
        .await
        .unwrap_err();
    assert_eq!(store_snapshot(&wallet).await, before);
    assert_eq!(wallet.services().total_calls(), 0);
    match error {
        bsv_rs::Error::WalletRefusal { class, message } => (class, message),
        other => panic!("expected a WalletRefusal, got {other:?}"),
    }
}

#[tokio::test]
async fn sign_send_with_over_its_bound_is_refused_ahead_of_the_session() {
    // 137: validateArray(value, 'sendWith', 1000) refuses 1001 IDs.
    let (class, message) = refusal((0..=SEND_WITH_LIMIT).map(txid).collect()).await;
    assert_eq!(class, WalletRefusalClass::InvalidParameter);
    assert_eq!(message, BOUND_TEXT);
}

#[tokio::test]
async fn sign_send_with_duplicates_are_refused_ahead_of_the_session() {
    // 140-142: invalid('sendWith', 'unique transaction IDs').
    let (class, message) = refusal(vec![txid(1), txid(2), txid(1)]).await;
    assert_eq!(class, WalletRefusalClass::InvalidParameter);
    assert_eq!(
        message,
        "Validation error: The sendWith parameter must be unique transaction IDs"
    );
}

#[tokio::test]
async fn sign_send_with_bound_runs_ahead_of_the_duplicates() {
    let mut send_with: Vec<_> = (0..=SEND_WITH_LIMIT).map(txid).collect();
    send_with[1] = send_with[0];
    let (_, message) = refusal(send_with).await;
    assert_eq!(message, BOUND_TEXT);
}
