//! #419 item 4: ProtoWallet argument refusals through the toolbox wallet.
#![cfg(feature = "sqlite")]

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::*;
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

async fn wallet() -> Wallet<StorageSqlx, MockWalletServices> {
    let key = PrivateKey::from_hex(&format!("{:064x}", 42)).unwrap();
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("pin-validators", &key.public_key().to_hex()).await.unwrap();
    storage.make_available().await.unwrap();
    Wallet::new(Some(key), storage, MockWalletServices::new()).await.unwrap()
}

fn protocol() -> Protocol {
    Protocol::new(SecurityLevel::Counterparty, "pin validators")
}

fn invalid<T: std::fmt::Debug>(result: bsv_rs::Result<T>, message: &str) {
    let error = result.unwrap_err();
    assert_eq!(error.to_string(), message);
    assert!(matches!(error, bsv_rs::Error::WalletRefusal {
        class: bsv_rs::WalletRefusalClass::InvalidParameter, ..
    }));
}

#[tokio::test]
async fn signature_data_and_der_refusals_use_the_beta3_reference_words() {
    let wallet = wallet().await;
    for (data, hash_to_directly_sign) in [(None, None), (Some(vec![1]), Some([2; 32]))] {
        invalid(wallet.create_signature(CreateSignatureArgs {
            protocol_id: protocol(), key_id: "key".into(), data,
            hash_to_directly_sign, ..Default::default()
        }, None).await,
        "The data, hashToDirectlySign parameter must be exactly one value");
    }
    for (signature, expected) in [
        (vec![1], "The signature parameter must be a canonical DER-encoded ECDSA signature"),
        (vec![1; 73], "The signature parameter must be at most 72 bytes"),
    ] {
        invalid(wallet.verify_signature(VerifySignatureArgs {
            protocol_id: protocol(), key_id: "key".into(), data: Some(vec![1]),
            signature, ..Default::default()
        }, None).await, expected);
    }
    invalid(wallet.verify_signature(VerifySignatureArgs {
        protocol_id: protocol(), key_id: "key".into(), ..Default::default()
    }, None).await, "The data, hashToDirectlyVerify parameter must be exactly one value");
}

#[tokio::test]
async fn hmac_argument_refusals_precede_cryptography_in_beta3() {
    let wallet = wallet().await;
    invalid(wallet.create_hmac(CreateHmacArgs {
        protocol_id: Protocol::new(SecurityLevel::Counterparty, "tiny"),
        key_id: "key".into(), ..Default::default()
    }, None).await, "The protocolID[1] parameter must be at least 5 length.");
    invalid(wallet.verify_hmac(VerifyHmacArgs {
        protocol_id: protocol(), key_id: String::new(), ..Default::default()
    }, None).await, "The keyID parameter must be at least 1 length.");
    invalid(wallet.create_hmac(CreateHmacArgs {
        protocol_id: protocol(), key_id: "key".into(), privileged: Some(true),
        ..Default::default()
    }, None).await, "The privilegedReason parameter must be a value when 'privileged' is true");
}
