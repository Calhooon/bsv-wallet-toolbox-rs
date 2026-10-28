//! #419 item 7: compressed inherent results and the reference's wire frames.
#![cfg(feature = "sqlite")]

use std::sync::{Arc, Mutex};

use bsv_rs::primitives::{PrivateKey, PublicKey};
use bsv_rs::wallet::interface::{RevealCounterpartyKeyLinkageArgs, RevealSpecificKeyLinkageArgs};
use bsv_rs::wallet::wire::{WalletWire, WalletWireTransceiver};
use bsv_rs::wallet::{Counterparty, KeyLinkageResult, Protocol, SecurityLevel, WalletInterface};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};
use serde_json::Value;

fn key(n: u8) -> PrivateKey {
    PrivateKey::from_hex(&format!("{n:064x}")).unwrap()
}

#[tokio::test]
async fn toolbox_specific_linkage_uses_the_returned_counterparty_key() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage
        .migrate("pin-linkage", &key(42).public_key().to_hex())
        .await
        .unwrap();
    storage.make_available().await.unwrap();
    let wallet = Wallet::new(Some(key(42)), storage, MockWalletServices::new())
        .await
        .unwrap();
    for (counterparty, expected) in [
        (Counterparty::Self_, key(42).public_key()),
        (Counterparty::Anyone, key(1).public_key()),
        (
            Counterparty::Other(key(3).public_key()),
            key(3).public_key(),
        ),
    ] {
        let result = wallet
            .reveal_specific_key_linkage(
                RevealSpecificKeyLinkageArgs {
                    counterparty,
                    verifier: key(5).public_key(),
                    protocol_id: Protocol::new(SecurityLevel::Counterparty, "fixture linkage"),
                    key_id: "key-1".into(),
                    privileged: None,
                    privileged_reason: None,
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.linkage.counterparty, expected);
        assert_eq!(result.linkage.prover, key(42).public_key());
        assert!(!result.linkage.encrypted_linkage.is_empty());
        assert!(!result.linkage.encrypted_linkage_proof.is_empty());
    }
}

struct RecordedWire {
    response: Vec<u8>,
    sent: Arc<Mutex<Vec<u8>>>,
}

#[async_trait::async_trait]
impl WalletWire for RecordedWire {
    async fn transmit_to_wallet(&self, message: &[u8]) -> bsv_rs::Result<Vec<u8>> {
        *self.sent.lock().unwrap() = message.to_vec();
        Ok(self.response.clone())
    }
}

fn public(value: &Value) -> PublicKey {
    PublicKey::from_hex(value.as_str().unwrap()).unwrap()
}

fn check_linkage(result: KeyLinkageResult, expected: &Value) {
    assert_eq!(result.prover, public(&expected["prover"]));
    assert_eq!(result.verifier, public(&expected["verifier"]));
    assert_eq!(result.counterparty, public(&expected["counterparty"]));
    assert_eq!(
        serde_json::json!(result.encrypted_linkage),
        expected["encryptedLinkage"]
    );
    assert_eq!(
        serde_json::json!(result.encrypted_linkage_proof),
        expected["encryptedLinkageProof"]
    );
}

#[tokio::test]
async fn wire_linkage_calls_replay_the_reference_frames() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/data/linkage_calls_406.json")).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let client = WalletWireTransceiver::new(RecordedWire {
            response: hex::decode(case["response_hex"].as_str().unwrap()).unwrap(),
            sent: Arc::clone(&sent),
        });
        let args = &case["args"];
        let originator = fixture["originator"].as_str();
        let privileged = args["privileged"].as_bool();
        let privileged_reason = args["privilegedReason"].as_str().map(str::to_owned);
        if case["call"] == "revealCounterpartyKeyLinkage" {
            let result = client
                .reveal_counterparty_key_linkage(
                    RevealCounterpartyKeyLinkageArgs {
                        counterparty: public(&args["counterparty"]),
                        verifier: public(&args["verifier"]),
                        privileged,
                        privileged_reason,
                    },
                    originator,
                )
                .await
                .unwrap();
            assert_eq!(result.revelation_time, case["result"]["revelationTime"]);
            check_linkage(result.linkage, &case["result"]);
        } else {
            let result = client
                .reveal_specific_key_linkage(
                    RevealSpecificKeyLinkageArgs {
                        counterparty: match args["counterparty"].as_str().unwrap() {
                            "self" => Counterparty::Self_,
                            "anyone" => Counterparty::Anyone,
                            key => Counterparty::from_hex(key).unwrap(),
                        },
                        verifier: public(&args["verifier"]),
                        protocol_id: Protocol::new(
                            SecurityLevel::from_u8(args["protocolID"][0].as_u64().unwrap() as u8)
                                .unwrap(),
                            args["protocolID"][1].as_str().unwrap(),
                        ),
                        key_id: args["keyID"].as_str().unwrap().into(),
                        privileged,
                        privileged_reason,
                    },
                    originator,
                )
                .await
                .unwrap();
            assert_eq!(result.key_id, case["result"]["keyID"]);
            assert_eq!(
                result.proof_type,
                case["result"]["proofType"].as_u64().unwrap() as u8
            );
            check_linkage(result.linkage, &case["result"]);
        }
        assert_eq!(
            hex::encode(&*sent.lock().unwrap()),
            case["request_hex"],
            "{}",
            case["name"]
        );
    }
}
