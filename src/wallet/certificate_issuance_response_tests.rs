//! #283: the response guards are exercised through local HTTP and real storage.

use super::*;
use bsv_rs::auth::certificates::Certificate;
use bsv_rs::wallet::*;
use serde_json::{json, Value};

struct ObservedWallet {
    inner: ProtoWallet,
    calls: Mutex<Vec<VerifyHmacArgs>>,
    nonce_result: Option<bool>,
    nonce_error: bool,
    serial_false: bool,
    decrypt_error: bool,
}

impl Default for ObservedWallet {
    fn default() -> Self {
        Self {
            inner: ProtoWallet::new(Some(PrivateKey::from_hex(&format!("{:064x}", 37)).unwrap())),
            calls: Mutex::new(Vec::new()),
            nonce_result: None,
            nonce_error: false,
            serial_false: false,
            decrypt_error: false,
        }
    }
}

#[async_trait::async_trait]
impl WalletInterface for ObservedWallet {
    async fn verify_hmac(
        &self,
        args: VerifyHmacArgs,
        _originator: &str,
    ) -> bsv_rs::Result<VerifyHmacResult> {
        self.calls.lock().unwrap().push(args.clone());
        if args.protocol_id.protocol_name == "server hmac" {
            if self.nonce_error {
                return Err(bsv_rs::Error::WalletError("nonce verifier refused".into()));
            }
            if let Some(valid) = self.nonce_result {
                return Ok(VerifyHmacResult { valid });
            }
        }
        if self.serial_false && args.protocol_id.protocol_name == CERTIFICATE_ISSUANCE_PROTOCOL {
            return Ok(VerifyHmacResult { valid: false });
        }
        self.inner.verify_hmac(args)
    }

    async fn decrypt(&self, args: DecryptArgs, _originator: &str) -> bsv_rs::Result<DecryptResult> {
        if self.decrypt_error {
            return Err(bsv_rs::Error::WalletError("fixture decrypt refused".into()));
        }
        self.inner.decrypt(args)
    }
    async fn get_public_key(
        &self,
        args: GetPublicKeyArgs,
        originator: &str,
    ) -> bsv_rs::Result<GetPublicKeyResult> {
        WalletInterface::get_public_key(&self.inner, args, originator).await
    }
    async fn encrypt(&self, args: EncryptArgs, originator: &str) -> bsv_rs::Result<EncryptResult> {
        WalletInterface::encrypt(&self.inner, args, originator).await
    }
    async fn create_hmac(
        &self,
        args: CreateHmacArgs,
        originator: &str,
    ) -> bsv_rs::Result<CreateHmacResult> {
        WalletInterface::create_hmac(&self.inner, args, originator).await
    }
    async fn create_signature(
        &self,
        args: CreateSignatureArgs,
        originator: &str,
    ) -> bsv_rs::Result<CreateSignatureResult> {
        WalletInterface::create_signature(&self.inner, args, originator).await
    }
    async fn verify_signature(
        &self,
        args: VerifySignatureArgs,
        originator: &str,
    ) -> bsv_rs::Result<VerifySignatureResult> {
        WalletInterface::verify_signature(&self.inner, args, originator).await
    }
    async fn reveal_counterparty_key_linkage(
        &self,
        args: WalletRevealCounterpartyArgs,
        originator: &str,
    ) -> bsv_rs::Result<RevealCounterpartyKeyLinkageResult> {
        WalletInterface::reveal_counterparty_key_linkage(&self.inner, args, originator).await
    }
    async fn reveal_specific_key_linkage(
        &self,
        args: WalletRevealSpecificArgs,
        originator: &str,
    ) -> bsv_rs::Result<RevealSpecificKeyLinkageResult> {
        WalletInterface::reveal_specific_key_linkage(&self.inner, args, originator).await
    }
    async fn create_action(
        &self,
        args: CreateActionArgs,
        originator: &str,
    ) -> bsv_rs::Result<CreateActionResult> {
        WalletInterface::create_action(&self.inner, args, originator).await
    }
    async fn sign_action(
        &self,
        args: SignActionArgs,
        originator: &str,
    ) -> bsv_rs::Result<SignActionResult> {
        WalletInterface::sign_action(&self.inner, args, originator).await
    }
    async fn abort_action(
        &self,
        args: AbortActionArgs,
        originator: &str,
    ) -> bsv_rs::Result<AbortActionResult> {
        WalletInterface::abort_action(&self.inner, args, originator).await
    }
    async fn list_actions(
        &self,
        args: ListActionsArgs,
        originator: &str,
    ) -> bsv_rs::Result<ListActionsResult> {
        WalletInterface::list_actions(&self.inner, args, originator).await
    }
    async fn internalize_action(
        &self,
        args: InternalizeActionArgs,
        originator: &str,
    ) -> bsv_rs::Result<InternalizeActionResult> {
        WalletInterface::internalize_action(&self.inner, args, originator).await
    }
    async fn list_outputs(
        &self,
        args: ListOutputsArgs,
        originator: &str,
    ) -> bsv_rs::Result<ListOutputsResult> {
        WalletInterface::list_outputs(&self.inner, args, originator).await
    }
    async fn relinquish_output(
        &self,
        args: RelinquishOutputArgs,
        originator: &str,
    ) -> bsv_rs::Result<RelinquishOutputResult> {
        WalletInterface::relinquish_output(&self.inner, args, originator).await
    }
    async fn acquire_certificate(
        &self,
        args: AcquireCertificateArgs,
        originator: &str,
    ) -> bsv_rs::Result<WalletCertificate> {
        WalletInterface::acquire_certificate(&self.inner, args, originator).await
    }
    async fn list_certificates(
        &self,
        args: ListCertificatesArgs,
        originator: &str,
    ) -> bsv_rs::Result<ListCertificatesResult> {
        WalletInterface::list_certificates(&self.inner, args, originator).await
    }
    async fn prove_certificate(
        &self,
        args: ProveCertificateArgs,
        originator: &str,
    ) -> bsv_rs::Result<ProveCertificateResult> {
        WalletInterface::prove_certificate(&self.inner, args, originator).await
    }
    async fn relinquish_certificate(
        &self,
        args: RelinquishCertificateArgs,
        originator: &str,
    ) -> bsv_rs::Result<RelinquishCertificateResult> {
        WalletInterface::relinquish_certificate(&self.inner, args, originator).await
    }
    async fn discover_by_identity_key(
        &self,
        args: DiscoverByIdentityKeyArgs,
        originator: &str,
    ) -> bsv_rs::Result<DiscoverCertificatesResult> {
        WalletInterface::discover_by_identity_key(&self.inner, args, originator).await
    }
    async fn discover_by_attributes(
        &self,
        args: DiscoverByAttributesArgs,
        originator: &str,
    ) -> bsv_rs::Result<DiscoverCertificatesResult> {
        WalletInterface::discover_by_attributes(&self.inner, args, originator).await
    }
    async fn is_authenticated(&self, originator: &str) -> bsv_rs::Result<AuthenticatedResult> {
        WalletInterface::is_authenticated(&self.inner, originator).await
    }
    async fn wait_for_authentication(
        &self,
        originator: &str,
    ) -> bsv_rs::Result<AuthenticatedResult> {
        WalletInterface::wait_for_authentication(&self.inner, originator).await
    }
    async fn get_height(&self, originator: &str) -> bsv_rs::Result<GetHeightResult> {
        WalletInterface::get_height(&self.inner, originator).await
    }
    async fn get_header_for_height(
        &self,
        args: GetHeaderArgs,
        originator: &str,
    ) -> bsv_rs::Result<GetHeaderResult> {
        WalletInterface::get_header_for_height(&self.inner, args, originator).await
    }
    async fn get_network(&self, originator: &str) -> bsv_rs::Result<GetNetworkResult> {
        WalletInterface::get_network(&self.inner, originator).await
    }
    async fn get_version(&self, originator: &str) -> bsv_rs::Result<GetVersionResult> {
        WalletInterface::get_version(&self.inner, originator).await
    }
}

async fn response_run(
    observer: &ObservedWallet,
    cert_type: String,
    nonce: Vec<u8>,
    mutate: impl Fn(&mut Value) + Send + Sync + 'static,
) -> (Result<WalletCertificate>, Value) {
    let wallet = wallet().await;
    let mut server = mockito::Server::new_async().await;
    let issued_response = Arc::new(Mutex::new(Value::Null));
    let captured = Arc::clone(&issued_response);
    let subject = PublicKey::from_hex(wallet.identity_key()).unwrap();
    let request = server.mock("POST", "/issue")
        .with_header(HEADER_IDENTITY_KEY, &certifier())
        .with_body_from_request(move |request| {
            let request: ProtocolIssuanceRequest = serde_json::from_slice(request.body().unwrap()).unwrap();
            let private = PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap();
            let signer = ProtoWallet::new(Some(private.clone()));
            let server_nonce = BASE64.encode(&nonce);
            let mut data = BASE64.decode(&request.client_nonce).unwrap();
            data.extend_from_slice(&nonce);
            let serial = signer.create_hmac(CreateHmacArgs {
                data,
                protocol_id: Protocol::new(SecurityLevel::Counterparty, CERTIFICATE_ISSUANCE_PROTOCOL),
                key_id: format!("{server_nonce}{}", request.client_nonce),
                counterparty: Some(Counterparty::Other(subject.clone())),
                ..Default::default()
            }).unwrap().hmac;
            let type_bytes = if request.cert_type.ends_with("B=") { [0; 32] } else { [7; 32] };
            let mut certificate = Certificate::new(type_bytes, serial, subject.clone(), private.public_key());
            certificate.revocation_outpoint = Some(Outpoint::new([0; 32], 0));
            for (name, value) in &request.fields { certificate.set_field(name.clone(), value.as_bytes().to_vec()); }
            certificate.sign(&private).unwrap();
            let mut response = json!({
                "serverNonce": server_nonce,
                "certificate": {
                    "type": request.cert_type, "serialNumber": BASE64.encode(serial),
                    "subject": subject.to_hex(), "certifier": certifier(),
                    "revocationOutpoint": format!("{}.0", "00".repeat(32)),
                    "fields": request.fields, "signature": hex::encode(certificate.signature.unwrap())
                }
            });
            mutate(&mut response);
            *captured.lock().unwrap() = response.clone();
            serde_json::to_vec(&response).unwrap()
        }).expect(1).create_async().await;
    let mut acquisition = args(certifier(), format!("{}/issue", server.url()));
    acquisition.certificate_type = cert_type;
    acquisition.fields.insert("city".into(), "Paris".into());
    let fixture_url = acquisition.certifier_url.take().unwrap();
    acquisition.certifier_url = Some("https://issuer.example/issue".to_string());
    let result = ISSUANCE_TEST_URL
        .scope(
            fixture_url,
            acquire_certificate_issuance(
                observer,
                wallet.storage(),
                &AuthId::with_user_id(wallet.identity_key(), 1),
                acquisition,
                wallet.identity_key(),
                ORIGINATOR,
            ),
        )
        .await;
    request.assert_async().await;
    let response = issued_response.lock().unwrap().clone();
    if let Ok(certificate) = &result {
        use crate::storage::{FindCertificatesArgs, WalletStorageReader};
        let stored = wallet
            .storage()
            .find_certificates(
                &AuthId::with_user_id(wallet.identity_key(), 1),
                FindCertificatesArgs::default(),
            )
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].cert_type, certificate.certificate_type);
        assert_eq!(
            stored[0].revocation_outpoint,
            response["certificate"]["revocationOutpoint"]
                .as_str()
                .unwrap()
        );
    }
    (result, response)
}

async fn response_error(mutate: impl Fn(&mut Value) + Send + Sync + 'static, expected: &str) {
    let (result, _) = response_run(
        &ObservedWallet::default(),
        BASE64.encode([7; 32]),
        vec![5; 32],
        mutate,
    )
    .await;
    assert_eq!(
        result.unwrap_err().to_string(),
        format!("Validation error: {expected}")
    );
}

#[tokio::test]
async fn certificate_presence_precedes_nonce_presence() {
    response_error(
        |r| {
            r["certificate"] = Value::Null;
            r["serverNonce"] = json!("");
        },
        "No certificate received from certifier!",
    )
    .await;
}

#[tokio::test]
async fn missing_nonce_uses_exclamation() {
    response_error(
        |r| {
            r["serverNonce"] = json!("");
        },
        "No serverNonce received from certifier!",
    )
    .await;
}

#[tokio::test]
async fn omitted_nonce_uses_presence_text() {
    response_error(
        |r| {
            r.as_object_mut().unwrap().remove("serverNonce");
        },
        "No serverNonce received from certifier!",
    )
    .await;
}

#[tokio::test]
async fn serial_hmac_precedes_certificate_type() {
    let observer = ObservedWallet {
        serial_false: true,
        ..Default::default()
    };
    let (result, _) = response_run(&observer, BASE64.encode([7; 32]), vec![5; 32], |r| {
        r["certificate"]["type"] = json!(BASE64.encode([8; 32]));
    })
    .await;
    assert_eq!(
        result.unwrap_err().to_string(),
        "Validation error: Invalid serialNumber"
    );
}

#[tokio::test]
async fn invalid_serial_encoding_and_length_use_reference_text() {
    for (serial, expected) in [
        ("!".to_string(), "Invalid base64 character at index 0"),
        ("AA=A".to_string(), "Invalid base64 padding"),
        (
            BASE64.encode([1; 16]),
            "The hmac parameter must be exactly 32 bytes",
        ),
        (
            BASE64.encode([1; 33]),
            "The hmac parameter must be exactly 32 bytes",
        ),
    ] {
        response_error(
            move |r| {
                r["certificate"]["serialNumber"] = json!(serial);
            },
            expected,
        )
        .await;
    }
    let (result, _) = response_run(
        &ObservedWallet::default(),
        BASE64.encode([7; 32]),
        vec![5; 32],
        |r| {
            r["certificate"]["serialNumber"] = json!(BASE64.encode([9; 32]));
        },
    )
    .await;
    assert!(
        matches!(result, Err(Error::SdkError(bsv_rs::Error::WalletError(message))) if message == "HMAC is not valid")
    );
}

#[tokio::test]
async fn type_precedes_subject_and_certifier() {
    response_error(
        |r| {
            r["certificate"]["type"] = json!(BASE64.encode([8; 32]));
            r["certificate"]["subject"] = json!(certifier());
            r["certificate"]["certifier"] = json!(PrivateKey::from_hex(&format!("{:064x}", 41))
                .unwrap()
                .public_key()
                .to_hex());
        },
        &format!(
            "Invalid certificate type! Expected: {}, Received: {}",
            BASE64.encode([7; 32]),
            BASE64.encode([8; 32])
        ),
    )
    .await;
}

#[tokio::test]
async fn subject_precedes_certifier() {
    let identity = PrivateKey::from_hex(&format!("{:064x}", 37))
        .unwrap()
        .public_key()
        .to_hex();
    response_error(
        |r| {
            r["certificate"]["subject"] = json!(certifier());
            r["certificate"]["certifier"] = json!(PrivateKey::from_hex(&format!("{:064x}", 41))
                .unwrap()
                .public_key()
                .to_hex());
        },
        &format!(
            "Invalid certificate subject! Expected: {identity}, Received: {}",
            certifier()
        ),
    )
    .await;
}

#[tokio::test]
async fn certifier_precedes_revocation() {
    let other = PrivateKey::from_hex(&format!("{:064x}", 41))
        .unwrap()
        .public_key()
        .to_hex();
    response_error(
        |r| {
            r["certificate"]["certifier"] = json!(PrivateKey::from_hex(&format!("{:064x}", 41))
                .unwrap()
                .public_key()
                .to_hex());
            r["certificate"]["revocationOutpoint"] = json!("");
        },
        &format!(
            "Invalid certifier! Expected: {}, Received: {other}",
            certifier()
        ),
    )
    .await;
}

#[tokio::test]
async fn revocation_precedes_fields() {
    response_error(
        |r| {
            r["certificate"]["revocationOutpoint"] = json!("");
            r["certificate"]["fields"] = json!({});
        },
        "Invalid revocationOutpoint!",
    )
    .await;
}

#[tokio::test]
async fn field_count_uses_reference_text() {
    response_error(
        |r| {
            r["certificate"]["fields"] = json!({});
        },
        "Fields mismatch! Objects have different numbers of keys.",
    )
    .await;
}

#[tokio::test]
async fn missing_field_uses_reference_text() {
    response_error(
        |r| {
            r["certificate"]["fields"] = json!({"other": "dGVzdA=="});
        },
        "Missing field: city in certificate.fields",
    )
    .await;
}

#[tokio::test]
async fn verification_error_uses_reference_text() {
    response_error(
        |r| {
            r["certificate"]["signature"] = json!("00");
        },
        "Certificate verification failed",
    )
    .await;
}

#[tokio::test]
async fn decrypt_all_fields_uses_reference_text() {
    let observer = ObservedWallet {
        decrypt_error: true,
        ..Default::default()
    };
    let (result, _) = response_run(&observer, BASE64.encode([7; 32]), vec![5; 32], |_| {}).await;
    assert_eq!(
        result.unwrap_err().to_string(),
        "Validation error: Failed to decrypt all master certificate fields."
    );
}

#[tokio::test]
async fn noncanonical_type_survives_round_trip() {
    let cert_type = format!("{}B=", "A".repeat(42));
    let (result, _) = response_run(
        &ObservedWallet::default(),
        cert_type.clone(),
        vec![5; 32],
        |_| {},
    )
    .await;
    assert_eq!(result.unwrap().certificate_type, cert_type);
}

#[tokio::test]
async fn unparsable_truthy_outpoint_reaches_field_checks() {
    response_error(
        |r| {
            r["certificate"]["revocationOutpoint"] = json!("unparsable");
            r["certificate"]["fields"] = json!({});
        },
        "Fields mismatch! Objects have different numbers of keys.",
    )
    .await;
}

#[tokio::test]
async fn server_nonce_verification_precedes_serial_and_uses_reference_derivation() {
    let observer = ObservedWallet {
        nonce_error: true,
        ..Default::default()
    };
    let nonce = [vec![b'a'; 16], vec![3; 32]].concat();
    let (result, _) = response_run(&observer, BASE64.encode([7; 32]), nonce, |_| {}).await;
    assert!(result
        .unwrap_err()
        .to_string()
        .ends_with("nonce verifier refused"));
    let calls = observer.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].data, vec![b'a'; 16]);
    assert_eq!(calls[0].hmac, [3; 32]);
    assert_eq!(
        calls[0].protocol_id,
        Protocol::new(SecurityLevel::Counterparty, "server hmac")
    );
    assert_eq!(calls[0].key_id, "a".repeat(16));
    assert_eq!(
        calls[0].counterparty,
        Some(Counterparty::Other(
            PublicKey::from_hex(&certifier()).unwrap()
        ))
    );
}

#[tokio::test]
async fn false_server_nonce_result_is_ignored_as_in_reference() {
    let observer = ObservedWallet {
        nonce_result: Some(false),
        ..Default::default()
    };
    let (result, _) = response_run(&observer, BASE64.encode([7; 32]), vec![b'a'; 48], |_| {}).await;
    assert!(result.is_ok(), "{result:?}");
    let calls = observer.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].protocol_id.protocol_name, "server hmac");
    assert_eq!(
        calls[1].protocol_id.protocol_name,
        CERTIFICATE_ISSUANCE_PROTOCOL
    );
}

#[tokio::test]
async fn truthy_outpoint_text_is_retained_in_storage() {
    use crate::storage::{FindCertificatesArgs, WalletStorageReader};

    let wallet = wallet().await;
    let auth = AuthId::with_user_id(wallet.identity_key(), 1);
    for (index, outpoint) in ["unparsable".to_string(), format!("{}.07", "AB".repeat(32))]
        .into_iter()
        .enumerate()
    {
        let certificate = CertificateResponse {
            cert_type: BASE64.encode([7; 32]),
            serial_number: BASE64.encode([u8::try_from(index).unwrap(); 32]),
            subject: wallet.identity_key().to_string(),
            certifier: certifier(),
            revocation_outpoint: outpoint.clone(),
            fields: HashMap::new(),
            signature: String::new(),
        };
        let parsed = parse_certificate_response(&ProtocolIssuanceResponse {
            certificate: Some(certificate),
            server_nonce: "fixture".into(),
            protocol: String::new(),
            timestamp: String::new(),
            version: String::new(),
        })
        .unwrap();
        let stored = store_certificate(
            wallet.storage(),
            &auth,
            &parsed,
            &HashMap::new(),
            &PublicKey::from_hex(&certifier()).unwrap(),
            wallet.identity_key(),
            &BASE64.encode([7; 32]),
        )
        .await
        .unwrap();
        assert_eq!(stored.revocation_outpoint, outpoint);
    }
    let rows = wallet
        .storage()
        .find_certificates(&auth, FindCertificatesArgs::default())
        .await
        .unwrap();
    let mut actual: Vec<_> = rows
        .into_iter()
        .map(|row| row.revocation_outpoint)
        .collect();
    actual.sort();
    let mut expected = vec!["unparsable".to_string(), format!("{}.07", "AB".repeat(32))];
    expected.sort();
    assert_eq!(actual, expected);
}
