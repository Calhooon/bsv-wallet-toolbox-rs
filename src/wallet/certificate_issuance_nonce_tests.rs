//! #300: the client nonce is ts-stack@edf6e03 createNonce.ts:21-44 (16 random
//! bytes, their `[2, 'server hmac']` hmac with the certifier, unpadded base64)
//! and passes verifyNonce.ts:26-57 as a certifier written against it reads it.

use super::*;

// verifyNonce.ts:28: ^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$
fn matches_verify_nonce_regex(nonce: &str) -> bool {
    let body = nonce.trim_end_matches('=');
    nonce.len().is_multiple_of(4)
        && nonce.len() - body.len() <= 2
        && body
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
}

// utils.ts:211-213: toUTF8 is TextDecoder().decode, which drops one leading BOM.
fn text_decoder(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    text.strip_prefix('\u{feff}').unwrap_or(&text).to_string()
}

async fn client_nonce(wallet: &TestWallet) -> String {
    let certifier_key = PublicKey::from_hex(&certifier()).unwrap();
    let subject = PublicKey::from_hex(wallet.identity_key()).unwrap();
    let acquisition = args(certifier(), "https://issuer.example/issue".to_string());
    prepare_issuance_action_data(wallet, &acquisition, &certifier_key, &subject, ORIGINATOR)
        .await
        .unwrap()
        .client_nonce
}

#[tokio::test]
async fn client_nonce_is_create_nonce_and_passes_verify_nonce() {
    let wallet = wallet().await;
    let certifier_key = PublicKey::from_hex(&certifier()).unwrap();
    let subject = PublicKey::from_hex(wallet.identity_key()).unwrap();
    let certifier_wallet =
        ProtoWallet::new(Some(PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap()));
    // Random bytes are malformed UTF-8 almost always, so the lossy keyID is
    // exercised on nearly every run; a few runs widen the sample.
    for _ in 0..8 {
        let nonce = client_nonce(&wallet).await;
        assert_eq!(nonce.len(), 64, "{nonce}");
        assert!(!nonce.contains('='), "{nonce}");
        assert!(matches_verify_nonce_regex(&nonce), "{nonce}");
        let bytes = decode_issuance_base64(&nonce).unwrap();
        assert_eq!(bytes.len(), 48);
        assert_eq!(BASE64.encode(&bytes), nonce);
        let (data, hmac) = bytes.split_at(16);
        let args = |counterparty: &PublicKey| VerifyHmacArgs {
            data: data.to_vec(),
            hmac: hmac.try_into().unwrap(),
            protocol_id: Protocol::new(SecurityLevel::Counterparty, "server hmac"),
            key_id: text_decoder(data),
            counterparty: Some(Counterparty::Other(counterparty.clone())),
            ..Default::default()
        };
        // The subject's own reading (counterparty the certifier) and the
        // certifier's (verifyNonce(clientNonce, wallet, subject)).
        let own = wallet.verify_hmac(args(&certifier_key), ORIGINATOR).await;
        assert!(own.unwrap().valid);
        assert!(certifier_wallet.verify_hmac(args(&subject)).unwrap().valid);
    }
}

// Wallet.ts:809 decodes `clientNonce + serverNonce` as one string, and
// utils.ts:150-156 refuses a `=` with anything after it.
#[tokio::test]
async fn client_nonce_concatenates_as_the_reference_decodes() {
    let wallet = wallet().await;
    let nonce = client_nonce(&wallet).await;
    let server_nonce = BASE64.encode([5; 32]);
    let data = decode_issuance_base64(&format!("{nonce}{server_nonce}")).unwrap();
    assert_eq!(data.len(), 48 + 32);
    assert_eq!(data[..48], decode_issuance_base64(&nonce).unwrap());
}

// TextDecoder substitutes one U+FFFD per maximal subpart. Node 20.19.5 gives
// fffd 41 fffd*11 42 for this vector: F0 90 80 (1), E0 80 (2), ED A0 80 (3),
// C0 AF (2), FF (1), F4 90 80 (3).
#[tokio::test]
async fn nonce_key_id_is_textdecoder_lossy_utf8() {
    let data = [
        0xf0, 0x90, 0x80, 0x41, 0xe0, 0x80, 0xed, 0xa0, 0x80, 0xc0, 0xaf, 0xff, 0xf4, 0x90, 0x80,
        0x42,
    ];
    let expected = format!("\u{fffd}A{}B", "\u{fffd}".repeat(11));
    assert_eq!(String::from_utf8_lossy(&data), expected);
    assert_eq!(text_decoder(&data), expected);
    let subject = PrivateKey::from_hex(&format!("{:064x}", 37)).unwrap();
    let certifier_key = PrivateKey::from_hex(&format!("{:064x}", 31)).unwrap();
    let hmac = ProtoWallet::new(Some(certifier_key.clone()))
        .create_hmac(CreateHmacArgs {
            data: data.to_vec(),
            protocol_id: Protocol::new(SecurityLevel::Counterparty, "server hmac"),
            key_id: expected,
            counterparty: Some(Counterparty::Other(subject.public_key())),
            ..Default::default()
        })
        .unwrap()
        .hmac;
    let nonce = BASE64.encode([data.as_slice(), &hmac].concat());
    let reader = ProtoWallet::new(Some(subject));
    let certifier_key = certifier_key.public_key();
    let valid = verify_server_nonce(&reader, &nonce, &certifier_key, ORIGINATOR);
    assert!(valid.await.unwrap());
}
