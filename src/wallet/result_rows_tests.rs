//! #488 surface 7 (bsv-rs #487, #432): three more of the wallet's answers
//! read back by bsv-rs's own JSON client, `HttpWalletJson`, which since beta.4
//! applies the reference client's result rows (`validateWalletResult` at
//! ts-stack@edf6e03). Each answer is the wallet's, served unchanged by a
//! loopback mock as a host's JSON daemon would serve it; each expected
//! refusal is bsv-rs's `Invalid <call> result <field>: expected <expected>`.

use super::busy_door_tests::{fund_tagged, own_lock, wallet};
use super::direct_acquire_tests::signed_args;
use super::*;
use bsv_rs::primitives::sha256;
use bsv_rs::wallet::substrates::HttpWalletJson;

const ORIGINATOR: &str = "rows.test";

async fn served(call: &str, body: Vec<u8>) -> mockito::ServerGuard {
    let mut server = mockito::Server::new_async().await;
    server
        .mock("POST", format!("/{call}").as_str())
        .with_header("content-type", "application/json")
        .with_body(body)
        .create_async()
        .await;
    server
}

fn client(server: &mockito::ServerGuard) -> HttpWalletJson {
    HttpWalletJson::new(Some(ORIGINATOR.into()), Some(server.url())).unwrap()
}

/// R86 (bsv-rs `certificates.rs` `proof`): the verifier keyring holds exactly
/// the requested fields. The wallet's `prove_certificate` answers each subset
/// of a two-field certificate, the empty request included, and the client
/// accepts every answer.
#[tokio::test]
async fn the_proof_keyring_is_exactly_the_requested_fields() {
    let (_dir, wallet) = wallet().await;
    let certificate = wallet
        .acquire_certificate(signed_args(&wallet).await, Some(ORIGINATOR))
        .await
        .unwrap();
    let verifier = PrivateKey::from_bytes(&[5; 32])
        .unwrap()
        .public_key()
        .to_hex();
    for reveal in [vec![], vec!["name"], vec!["role"], vec!["name", "role"]] {
        let args = ProveCertificateArgs {
            certificate: certificate.clone(),
            fields_to_reveal: reveal.iter().map(ToString::to_string).collect(),
            verifier: verifier.clone(),
            privileged: None,
            privileged_reason: None,
        };
        let answer = wallet
            .prove_certificate(args.clone(), Some(ORIGINATOR))
            .await
            .unwrap();
        let mut keys: Vec<_> = answer.keyring_for_verifier.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, reveal, "{reveal:?}");
        let server = served("proveCertificate", serde_json::to_vec(&answer).unwrap()).await;
        let read = client(&server).prove_certificate(args, None).await.unwrap();
        assert_eq!(read.keyring_for_verifier, answer.keyring_for_verifier);
    }
}

/// CLI #371, recorded and not moved (nothing moves on it until the stack's
/// word): under `includeInputUnlockingScripts` the wallet answers an unsigned
/// action's input with no `unlockingScript`, the faithful port of
/// `listActionsIdb.ts:77,91`; the reference's own client refuses that answer.
/// These are today's words.
#[tokio::test]
async fn an_unsigned_input_without_its_script_is_refused_by_the_client() {
    let (_dir, wallet) = wallet().await;
    fund_tagged(&wallet, &own_lock(&wallet), 0).await;
    let created = wallet
        .create_action(
            CreateActionArgs {
                description: "an unsigned action".into(),
                input_beef: None,
                inputs: None,
                lock_time: None,
                version: None,
                labels: Some(vec!["rows".into()]),
                outputs: Some(vec![bsv_rs::wallet::CreateActionOutput {
                    locking_script: vec![0x51],
                    satoshis: 1_000,
                    output_description: "a payment to sign".into(),
                    basket: None,
                    custom_instructions: None,
                    tags: None,
                }]),
                options: Some(bsv_rs::wallet::CreateActionOptions {
                    sign_and_process: Some(false),
                    ..Default::default()
                }),
            },
            Some(ORIGINATOR),
        )
        .await
        .unwrap();
    assert!(created.signable_transaction.is_some());
    let args = ListActionsArgs {
        include_input_unlocking_scripts: Some(true),
        include_inputs: Some(true),
        labels: vec!["rows".into()],
        label_query_mode: None,
        include_labels: None,
        include_input_source_locking_scripts: None,
        include_outputs: None,
        include_output_locking_scripts: None,
        limit: None,
        offset: None,
        seek_permission: None,
    };
    let answer = wallet
        .list_actions(args.clone(), Some(ORIGINATOR))
        .await
        .unwrap();
    let input = &answer.actions[0].inputs.as_ref().unwrap()[0];
    assert!(input.unlocking_script.is_none());
    let server = served("listActions", serde_json::to_vec(&answer).unwrap()).await;
    let error = client(&server).list_actions(args, None).await.unwrap_err();
    assert_eq!(
        error,
        bsv_rs::Error::WalletError(
            "Invalid listActions result actions[0].inputs[0].unlockingScript: \
             expected the explicitly requested unlocking script"
                .into()
        )
    );
}

/// The `data` form of createSignature, which the key-value store and
/// revocation paths send as `data: sha256(preimage)`: the wallet signs the
/// data's SHA-256, verifies it in both forms, and the client accepts its
/// signature (the row's canonical DER).
#[tokio::test]
async fn create_signature_takes_the_data_form() {
    let (_dir, wallet) = wallet().await;
    let data = sha256(b"a key-value token preimage").to_vec();
    let protocol_id = Protocol::new(SecurityLevel::Counterparty, "row witness");
    let args = CreateSignatureArgs {
        data: Some(data.clone()),
        protocol_id: protocol_id.clone(),
        key_id: "1".into(),
        counterparty: Some(Counterparty::Anyone),
        ..Default::default()
    };
    let answer = wallet
        .create_signature(args.clone(), Some(ORIGINATOR))
        .await
        .unwrap();
    for (data, hash) in [(Some(data.clone()), None), (None, Some(sha256(&data)))] {
        let verified = wallet
            .verify_signature(
                VerifySignatureArgs {
                    data,
                    hash_to_directly_verify: hash,
                    signature: answer.signature.clone(),
                    protocol_id: protocol_id.clone(),
                    key_id: "1".into(),
                    counterparty: Some(Counterparty::Anyone),
                    for_self: Some(true),
                    ..Default::default()
                },
                Some(ORIGINATOR),
            )
            .await
            .unwrap();
        assert!(verified.valid);
    }
    let body = serde_json::to_vec(&serde_json::json!({ "signature": answer.signature })).unwrap();
    let server = served("createSignature", body).await;
    let read = client(&server).create_signature(args, None).await.unwrap();
    assert_eq!(read.signature, answer.signature);
}
