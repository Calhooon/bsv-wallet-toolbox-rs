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

// Expand the delegation before async_trait transforms the implementation.
macro_rules! observed_wallet_methods {
    ($(fn $name:ident(&self $(, $arg:ident: $arg_type:ty)*) -> $result:ty;)*) => {
        #[async_trait::async_trait]
        impl WalletInterface for ObservedWallet {
    async fn verify_hmac(
        &self,
        args: VerifyHmacArgs,
        _originator: Option<&str>,
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

    async fn decrypt(&self, args: DecryptArgs, _originator: Option<&str>) -> bsv_rs::Result<DecryptResult> {
        if self.decrypt_error {
            return Err(bsv_rs::Error::WalletError("fixture decrypt refused".into()));
        }
        self.inner.decrypt(args)
    }
            $(async fn $name(&self $(, $arg: $arg_type)*) -> $result {
                WalletInterface::$name(&self.inner $(, $arg)*).await
            })*
        }
    };
}

observed_wallet_methods! {
    fn get_public_key(&self, args: GetPublicKeyArgs, originator: Option<&str>) -> bsv_rs::Result<GetPublicKeyResult>;
    fn encrypt(&self, args: EncryptArgs, originator: Option<&str>) -> bsv_rs::Result<EncryptResult>;
    fn create_hmac(&self, args: CreateHmacArgs, originator: Option<&str>) -> bsv_rs::Result<CreateHmacResult>;
    fn create_signature(&self, args: CreateSignatureArgs, originator: Option<&str>) -> bsv_rs::Result<CreateSignatureResult>;
    fn verify_signature(&self, args: VerifySignatureArgs, originator: Option<&str>) -> bsv_rs::Result<VerifySignatureResult>;
    fn reveal_counterparty_key_linkage(&self, args: WalletRevealCounterpartyArgs, originator: Option<&str>) -> bsv_rs::Result<RevealCounterpartyKeyLinkageResult>;
    fn reveal_specific_key_linkage(&self, args: WalletRevealSpecificArgs, originator: Option<&str>) -> bsv_rs::Result<RevealSpecificKeyLinkageResult>;
    fn create_action(&self, args: CreateActionArgs, originator: Option<&str>) -> bsv_rs::Result<CreateActionResult>;
    fn sign_action(&self, args: SignActionArgs, originator: Option<&str>) -> bsv_rs::Result<SignActionResult>;
    fn abort_action(&self, args: AbortActionArgs, originator: Option<&str>) -> bsv_rs::Result<AbortActionResult>;
    fn list_actions(&self, args: ListActionsArgs, originator: Option<&str>) -> bsv_rs::Result<ListActionsResult>;
    fn internalize_action(&self, args: InternalizeActionArgs, originator: Option<&str>) -> bsv_rs::Result<InternalizeActionResult>;
    fn list_outputs(&self, args: ListOutputsArgs, originator: Option<&str>) -> bsv_rs::Result<ListOutputsResult>;
    fn relinquish_output(&self, args: RelinquishOutputArgs, originator: Option<&str>) -> bsv_rs::Result<RelinquishOutputResult>;
    fn acquire_certificate(&self, args: AcquireCertificateArgs, originator: Option<&str>) -> bsv_rs::Result<WalletCertificate>;
    fn list_certificates(&self, args: ListCertificatesArgs, originator: Option<&str>) -> bsv_rs::Result<ListCertificatesResult>;
    fn prove_certificate(&self, args: ProveCertificateArgs, originator: Option<&str>) -> bsv_rs::Result<ProveCertificateResult>;
    fn relinquish_certificate(&self, args: RelinquishCertificateArgs, originator: Option<&str>) -> bsv_rs::Result<RelinquishCertificateResult>;
    fn discover_by_identity_key(&self, args: DiscoverByIdentityKeyArgs, originator: Option<&str>) -> bsv_rs::Result<DiscoverCertificatesResult>;
    fn discover_by_attributes(&self, args: DiscoverByAttributesArgs, originator: Option<&str>) -> bsv_rs::Result<DiscoverCertificatesResult>;
    fn is_authenticated(&self, originator: Option<&str>) -> bsv_rs::Result<AuthenticatedResult>;
    fn wait_for_authentication(&self, originator: Option<&str>) -> bsv_rs::Result<AuthenticatedResult>;
    fn get_height(&self, originator: Option<&str>) -> bsv_rs::Result<GetHeightResult>;
    fn get_header_for_height(&self, args: GetHeaderArgs, originator: Option<&str>) -> bsv_rs::Result<GetHeaderResult>;
    fn get_network(&self, originator: Option<&str>) -> bsv_rs::Result<GetNetworkResult>;
    fn get_version(&self, originator: Option<&str>) -> bsv_rs::Result<GetVersionResult>;
}

async fn response_run(
    observer: &ObservedWallet,
    cert_type: String,
    nonce: Vec<u8>,
    mutate: impl Fn(&mut Value) + Send + Sync + 'static,
) -> (Result<WalletCertificate>, Value) {
    let fields = HashMap::from([("city".to_string(), "Paris".to_string())]);
    let outpoint = Outpoint::new([0; 32], 0);
    response_run_with(observer, cert_type, nonce, fields, outpoint, mutate).await
}

// The certifier signs `signed_outpoint`; the answered text is the mutation's.
async fn response_run_with(
    observer: &ObservedWallet,
    cert_type: String,
    nonce: Vec<u8>,
    fields: HashMap<String, String>,
    signed_outpoint: Outpoint,
    mutate: impl Fn(&mut Value) + Send + Sync + 'static,
) -> (Result<WalletCertificate>, Value) {
    let wallet = wallet().await;
    let mut server = mockito::Server::new_async().await;
    let issued_response = Arc::new(Mutex::new(Value::Null));
    let captured = Arc::clone(&issued_response);
    let subject = PublicKey::from_hex(wallet.identity_key()).unwrap();
    let request = server.mock("POST", "/issue")
        .with_header(HEADER_CERTIFIER_IDENTITY_KEY, &certifier())
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
            certificate.revocation_outpoint = Some(signed_outpoint.clone());
            for (name, value) in &request.fields { certificate.set_field(name.clone(), value.clone()); }
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
    acquisition.fields = fields;
    let fixture_url = acquisition.certifier_url.take().unwrap();
    acquisition.certifier_url = Some("https://issuer.example/issue".to_string());
    let result = ISSUANCE_TEST_URL
        .scope(
            fixture_url,
            acquire_certificate_issuance(
                observer,
                subject_proto(),
                wallet.storage(),
                &AuthId::with_user_id(wallet.identity_key(), 1),
                acquisition,
                wallet.identity_key(),
                Some(ORIGINATOR),
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

// #188 (bsv-rs #299): verify answers Ok(false), never an error, so each of
// the three false verdicts (unsigned, non-DER, wrong signer) reaches
// Wallet.ts:816's refusal.
#[tokio::test]
async fn false_verdicts_are_refused() {
    let stranger = PrivateKey::from_hex(&format!("{:064x}", 41)).unwrap();
    let wrong_signer = hex::encode(stranger.sign(&[9u8; 32]).unwrap().to_der());
    let non_der = "30440220".to_string() + &"ab".repeat(32);
    for signature in [String::new(), non_der, wrong_signer] {
        response_error(
            move |r| r["certificate"]["signature"] = json!(signature.clone()),
            "Certificate verification failed",
        )
        .await;
    }
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

// Certificate.ts:233 and :237 decode strictly inside verify (425-441); the
// caught throw answers Wallet.ts:816's text. The serial's HMAC at :808 is lenient.
#[tokio::test]
async fn noncanonical_type_is_refused_at_verify() {
    let cert_type = format!("{}B=", "A".repeat(42));
    let (result, _) =
        response_run(&ObservedWallet::default(), cert_type, vec![5; 32], |_| {}).await;
    assert_eq!(
        result.unwrap_err().to_string(),
        "Validation error: Certificate verification failed"
    );
}

#[tokio::test]
async fn noncanonical_serial_passes_hmac_and_is_refused_at_verify() {
    let observer = ObservedWallet::default();
    let (result, _) = response_run(&observer, BASE64.encode([7; 32]), vec![5; 32], |r| {
        let serial = r["certificate"]["serialNumber"]
            .as_str()
            .unwrap()
            .to_string();
        let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let index = alphabet.find(&serial[42..43]).unwrap() | 1;
        r["certificate"]["serialNumber"] =
            json!(format!("{}{}=", &serial[..42], &alphabet[index..=index]));
    })
    .await;
    assert_eq!(
        result.unwrap_err().to_string(),
        "Validation error: Certificate verification failed"
    );
    // The 32-byte fixture nonce fails verifyNonce's shape; the one call is :807.
    let calls = observer.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].protocol_id.protocol_name,
        CERTIFICATE_ISSUANCE_PROTOCOL
    );
}

// MasterCertificate.ts:522-524 throws outside the decrypt try at Wallet.ts:817.
#[tokio::test]
async fn empty_master_keyring_is_refused_before_storage() {
    let (result, _) = response_run_with(
        &ObservedWallet::default(),
        BASE64.encode([7; 32]),
        vec![5; 32],
        HashMap::new(),
        Outpoint::new([0; 32], 0),
        |_| {},
    )
    .await;
    assert_eq!(
        result.unwrap_err().to_string(),
        "Validation error: A MasterCertificate must have a valid masterKeyring!"
    );
}

// Wallet.ts:809 decodes the nonces before verifyHmac's
// validateByteArray(hmac, 32) at validationHelpers.ts:1774.
#[tokio::test]
async fn nonce_decode_precedes_hmac_length() {
    response_error(
        |r| {
            r["serverNonce"] = json!("AA=A");
            r["certificate"]["serialNumber"] = json!(BASE64.encode([1; 16]));
        },
        "Invalid base64 padding",
    )
    .await;
}

// #300: Wallet.ts:809 decodes `clientNonce + serverNonce` as one string, so a
// bad character in the server nonce is named past the client's 64 characters.
#[tokio::test]
async fn nonces_decode_as_one_string() {
    response_error(
        |r| {
            r["serverNonce"] = json!("AA!A");
        },
        "Invalid base64 character at index 66",
    )
    .await;
}

// #300: verifyHmac checks the keyID (`serverNonce + clientNonce`, 1 to 800
// bytes, validationHelpers.ts:227 via :1772) before the hmac's length at :1774.
#[tokio::test]
async fn key_id_length_precedes_hmac_length() {
    let long = BASE64.encode([5; 552]);
    for (server_nonce, expected) in [
        (
            format!("{long}AAAA"),
            "The keyID parameter must be no more than 800 length.",
        ),
        (
            format!("{long} "),
            "The keyID parameter must be no more than 800 length.",
        ),
        (long, "The hmac parameter must be exactly 32 bytes"),
    ] {
        response_error(
            move |r| {
                r["serverNonce"] = json!(server_nonce);
                r["certificate"]["serialNumber"] = json!(BASE64.encode([1; 16]));
            },
            expected,
        )
        .await;
    }
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
    let nonce = [vec![b'a'; 16], vec![3; 32]].concat();
    let calls = refused_nonce(nonce).await;
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

// Certificate.ts:260-270: a txid-only outpoint is vout 0 over the real txid;
// an unparsable text and a `.07` index throw, and verify answers false.
#[tokio::test]
async fn outpoint_grammar_is_to_binary_grammar() {
    let txid = [0xab; 32];
    for (signed, text) in [
        (Outpoint::new([0; 32], 0), "unparsable".to_string()),
        (Outpoint::new(txid, 7), format!("{}.07", "AB".repeat(32))),
        (Outpoint::new(txid, 0), format!("{}.", "ab".repeat(32))),
        (Outpoint::new(txid, 0), format!("{}.0.0", "ab".repeat(32))),
    ] {
        let (result, _) = response_run_with(
            &ObservedWallet::default(),
            BASE64.encode([7; 32]),
            vec![5; 32],
            HashMap::from([("city".to_string(), "Paris".to_string())]),
            signed,
            move |r| r["certificate"]["revocationOutpoint"] = json!(text),
        )
        .await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "Validation error: Certificate verification failed"
        );
    }
    for text in ["AB".repeat(32), format!("{}.7", "AB".repeat(32))] {
        let vout = if text.contains('.') { 7 } else { 0 };
        let (result, _) = response_run_with(
            &ObservedWallet::default(),
            BASE64.encode([7; 32]),
            vec![5; 32],
            HashMap::from([("city".to_string(), "Paris".to_string())]),
            Outpoint::new(txid, vout),
            {
                let text = text.clone();
                move |r| r["certificate"]["revocationOutpoint"] = json!(text)
            },
        )
        .await;
        // response_run_with also checks the stored row keeps the answered text.
        assert_eq!(result.unwrap().revocation_outpoint, text);
    }
    // #300: the accept branch's txid and vout, asserted where they are parsed.
    let hex: String = hex::encode((0u8..32).collect::<Vec<u8>>());
    for (text, vout) in [(hex.clone(), 0), (format!("{hex}.7"), 7)] {
        let outpoint = parse_revocation_outpoint(&text).unwrap();
        assert_eq!(hex::encode(outpoint.txid), hex);
        assert_eq!(outpoint.vout, vout);
    }
}

#[tokio::test]
async fn server_nonce_uses_the_requested_certifier_key() {
    let observer = ObservedWallet::default();
    let signer = ProtoWallet::new(Some(PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap()));
    let data = vec![b'a'; 16];
    let hmac = signer
        .create_hmac(CreateHmacArgs {
            data: data.clone(),
            protocol_id: Protocol::new(SecurityLevel::Counterparty, "server hmac"),
            key_id: "a".repeat(16),
            counterparty: Some(Counterparty::Other(
                PrivateKey::from_hex(&format!("{:064x}", 37))
                    .unwrap()
                    .public_key(),
            )),
            ..Default::default()
        })
        .unwrap()
        .hmac;
    let nonce = [data, hmac.to_vec()].concat();
    let (result, _) = response_run(&observer, BASE64.encode([7; 32]), nonce.clone(), |_| {}).await;
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(observer.calls.lock().unwrap().len(), 2);

    let observer = ObservedWallet::default();
    let mut tampered = nonce;
    tampered[16] ^= 1;
    let (result, _) = response_run(&observer, BASE64.encode([7; 32]), tampered, |_| {}).await;
    assert!(
        matches!(result, Err(Error::SdkError(bsv_rs::Error::WalletError(message))) if message == "HMAC is not valid")
    );
    assert_eq!(observer.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn server_nonce_key_id_matches_textdecoder_bom_handling() {
    let mut nonce = vec![0xef, 0xbb, 0xbf];
    nonce.extend([b'a'; 13]);
    nonce.extend([3; 32]);
    let calls = refused_nonce(nonce).await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].data.len(), 16);
    assert_eq!(calls[0].key_id, "a".repeat(13));
}

async fn refused_nonce(nonce: Vec<u8>) -> Vec<VerifyHmacArgs> {
    let observer = ObservedWallet {
        nonce_error: true,
        ..Default::default()
    };
    let (result, _) = response_run(&observer, BASE64.encode([7; 32]), nonce, |_| {}).await;
    assert!(result
        .unwrap_err()
        .to_string()
        .ends_with("nonce verifier refused"));
    observer.calls.into_inner().unwrap()
}
