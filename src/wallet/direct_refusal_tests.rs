//! Direct SDK refusals retain their text and use the pinned reference's class.

use super::busy_door_tests::{store_snapshot, wallet};
use super::*;
use bsv_rs::WalletRefusalClass;

const ORIGINATOR: &str = "refusal.test";

fn assert_refusal(error: &bsv_rs::Error, expected: WalletRefusalClass, text: &str) {
    match error {
        bsv_rs::Error::WalletRefusal { class, message } => {
            assert_eq!(*class, expected);
            assert_eq!(message, text);
        }
        other => panic!("expected {expected:?}, got {other:?}"),
    }
    assert_eq!(error.to_string(), text);
}

fn sign_request(reference: &str) -> SignActionArgs {
    SignActionArgs {
        reference: reference.to_string(),
        spends: HashMap::new(),
        options: None,
    }
}

#[tokio::test]
async fn sign_reference_refusals_keep_class_and_text_without_writes() {
    // validationHelpers.ts:861; signAction.ts:57-60 at ts-stack@edf6e03.
    let (_dir, wallet) = wallet().await;
    let before = store_snapshot(&wallet).await;
    for (reference, class, text) in [
        (
            "",
            WalletRefusalClass::InvalidParameter,
            "Missing reference argument for sign action",
        ),
        (
            "bWlzc2luZw==",
            WalletRefusalClass::NotImplemented,
            "No pending transaction found for reference: bWlzc2luZw==",
        ),
    ] {
        let error = wallet
            .sign_action(sign_request(reference), Some(ORIGINATOR))
            .await
            .unwrap_err();
        assert_refusal(&error, class, text);
        assert_eq!(store_snapshot(&wallet).await, before);
        assert!(wallet.pending_transactions.read().await.is_empty());
    }
    assert_eq!(wallet.services().total_calls(), 0);
}

#[tokio::test]
async fn expired_sign_reference_uses_the_missing_session_class() {
    // The port's TTL expires the session; signAction.ts:57-60 cannot recover it.
    let (_dir, wallet) = wallet().await;
    let reference = "ZXhwaXJlZA==";
    wallet.pending_transactions.write().await.insert(
        reference.to_string(),
        PendingSignAction {
            transaction: PendingTransaction {
                reference: reference.to_string(),
                raw_tx: vec![],
                inputs: vec![],
                input_beef: None,
                is_no_send: true,
                is_delayed: false,
                send_with: vec![],
                created_at: Utc::now()
                    - chrono::Duration::seconds(PENDING_TRANSACTION_TTL_SECS + 1),
            },
            storage_inputs: HashSet::new(),
            unlocking_script_lengths: vec![],
        },
    );
    let before = store_snapshot(&wallet).await;
    let error = wallet
        .sign_action(sign_request(reference), Some(ORIGINATOR))
        .await
        .unwrap_err();
    assert_refusal(
        &error,
        WalletRefusalClass::NotImplemented,
        "Pending transaction has expired",
    );
    assert!(wallet.pending_transactions.read().await.is_empty());
    assert_eq!(store_snapshot(&wallet).await, before);
    assert_eq!(wallet.services().total_calls(), 0);
}

#[tokio::test]
async fn abort_reference_refusals_keep_their_site_class_without_writes() {
    // StorageProvider.ts:716-720 classes outgoing/status reference checks.
    let (_dir, wallet) = wallet().await;
    for (status, outgoing, class, detail) in [
        (
            "unsigned",
            false,
            WalletRefusalClass::InvalidParameter,
            "cannot abort action: must be an outgoing transaction",
        ),
        (
            "unproven",
            true,
            WalletRefusalClass::InvalidParameter,
            "cannot abort action: action with status 'unproven' records an acceptance and cannot be aborted",
        ),
        (
            "completed",
            true,
            WalletRefusalClass::InvalidParameter,
            "cannot abort action: action with status 'completed' cannot be aborted",
        ),
        (
            "failed",
            true,
            WalletRefusalClass::InvalidParameter,
            "cannot abort action: action with status 'failed' cannot be aborted",
        ),
        (
            "unknown",
            true,
            WalletRefusalClass::InvalidOperation,
            "unknown transaction status: unknown",
        ),
    ] {
        let reference = format!("refusal-{status}");
        sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, satoshis, description) \
             VALUES (?, ?, ?, ?, 0, 'reference refusal')",
        )
        .bind(wallet.user_id)
        .bind(status)
        .bind(&reference)
        .bind(outgoing)
        .execute(wallet.storage().writer())
        .await
        .unwrap();
        let before = store_snapshot(&wallet).await;
        let error = wallet
            .abort_action(AbortActionArgs { reference }, Some(ORIGINATOR))
            .await
            .unwrap_err();
        assert_refusal(&error, class, &format!("Invalid transaction status: {detail}"));
        assert_eq!(store_snapshot(&wallet).await, before);
    }
    assert_eq!(wallet.services().total_calls(), 0);
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn prove_lookup_refusals_and_missing_keyring_fault_keep_class_and_text() {
    use crate::services::mock::MockWalletServices;
    use crate::storage::client::StorageClient;
    use crate::storage::WalletStorageWriter;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use bsv_rs::wallet::CertificateResult;
    use mockito::Matcher;
    use serde_json::json;

    // proveCertificate.ts:28-37; MasterCertificate.ts:35-37 at ts-stack@edf6e03.
    let (_dir, local) = wallet().await;
    let (user, _) = local
        .storage()
        .find_or_insert_user(local.identity_key())
        .await
        .unwrap();
    let key = PrivateKey::from_bytes(&[23; 32]).unwrap();
    let public_key = key.public_key().to_hex();
    let certificate = WalletCertificate {
        certificate_type: STANDARD.encode([33; 32]),
        serial_number: STANDARD.encode([34; 32]),
        subject: public_key.clone(),
        certifier: public_key.clone(),
        revocation_outpoint: format!("{}.0", "ab".repeat(32)),
        signature: "300602010a02010b".to_string(),
        fields: HashMap::new(),
    };
    for count in [0, 2, 1] {
        let mut server = mockito::Server::new_async().await;
        let user_call = server
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(json!({"method": "findOrInsertUser"})))
            .with_body(
                json!({"jsonrpc": "2.0", "id": 1,
                "result": {"user": user, "isNew": false}})
                .to_string(),
            )
            .create_async()
            .await;
        let listed = ListCertificatesResult {
            total_certificates: count,
            certificates: (0..count)
                .map(|_| CertificateResult {
                    certificate: certificate.clone(),
                    keyring: None,
                    verifier: None,
                })
                .collect(),
        };
        let lookup = server
            .mock("POST", "/")
            .match_body(Matcher::PartialJson(json!({"method": "listCertificates"})))
            .with_body(json!({"jsonrpc": "2.0", "id": 2, "result": listed}).to_string())
            .create_async()
            .await;
        let client =
            StorageClient::new_unauthenticated(ProtoWallet::new(Some(key.clone())), server.url());
        let wallet = Wallet::new(Some(key.clone()), client, MockWalletServices::new())
            .await
            .unwrap();
        let error = wallet
            .prove_certificate(
                ProveCertificateArgs {
                    certificate: certificate.clone(),
                    fields_to_reveal: vec![],
                    verifier: public_key.clone(),
                    privileged: None,
                    privileged_reason: None,
                },
                Some(ORIGINATOR),
            )
            .await
            .unwrap_err();
        match count {
            0 => assert_refusal(
                &error,
                WalletRefusalClass::InvalidParameter,
                "Certificate not found with the provided arguments",
            ),
            2 => assert_refusal(
                &error,
                WalletRefusalClass::InvalidParameter,
                "Multiple certificates match the provided arguments, expected unique match",
            ),
            _ => assert!(matches!(error, bsv_rs::Error::WalletError(message)
                if message == "Certificate does not have a master keyring stored")),
        }
        user_call.assert_async().await;
        lookup.assert_async().await;
        assert_eq!(wallet.services().total_calls(), 0);
    }
}
