//! #283: representable acquisition guards, ordered as validationHelpers.ts.

use super::*;
use bsv_rs::wallet::KeyringRevealer;

fn acquisition() -> AcquireCertificateArgs {
    args(certifier(), "https://issuer.example/issue".to_string())
}

async fn refuses(acquisition: AcquireCertificateArgs, name: &str, rule: &str) {
    let wallet = wallet().await;
    assert_wallet_error(
        acquire_as_test_wallet(&wallet, acquisition, "invalid identity").await,
        &format!("Validation error: The {name} parameter must be {rule}"),
    );
}

#[tokio::test]
async fn privilege_reason_required_before_direct_fields() {
    let mut a = acquisition();
    a.privileged = Some(true);
    a.serial_number = Some("bad later argument".into());
    refuses(a, "privilegedReason", "a value when 'privileged' is true").await;
}

#[tokio::test]
async fn privilege_reason_minimum_before_protocol() {
    let wallet = wallet().await;
    let mut a = acquisition();
    a.privileged_reason = Some("éé".into());
    a.acquisition_protocol = AcquisitionProtocol::Direct;
    let result = acquire_as_test_wallet(&wallet, a, wallet.identity_key()).await;
    assert_wallet_error(
        result,
        "Validation error: The privilegedReason parameter must be at least 5 length.",
    );
}

#[tokio::test]
async fn privilege_reason_maximum_before_direct_fields() {
    let mut a = acquisition();
    a.privileged_reason = Some("é".repeat(26));
    a.serial_number = Some("bad later argument".into());
    refuses(a, "privilegedReason", "no more than 50 length.").await;
}

#[tokio::test]
async fn issuance_protocol_before_serial_number() {
    let wallet = wallet().await;
    let mut a = acquisition();
    a.acquisition_protocol = AcquisitionProtocol::Direct;
    a.serial_number = Some("bad later argument".into());
    let result = acquire_as_test_wallet(&wallet, a, wallet.identity_key()).await;
    assert_wallet_error(
        result,
        "Validation error: Only acquire certificate via issuance requests allowed here.",
    );
}

#[tokio::test]
async fn serial_number_before_signature() {
    let mut a = acquisition();
    a.serial_number = Some("bad".into());
    a.signature = Some("bad later argument".into());
    refuses(
        a,
        "serialNumber",
        "valid when acquisitionProtocol is \"direct\"",
    )
    .await;
}

#[tokio::test]
async fn signature_before_revocation_outpoint() {
    let mut a = acquisition();
    a.signature = Some("bad".into());
    a.revocation_outpoint = Some("bad later argument".into());
    refuses(
        a,
        "signature",
        "valid when acquisitionProtocol is \"direct\"",
    )
    .await;
}

#[tokio::test]
async fn revocation_outpoint_before_keyring_revealer() {
    let mut a = acquisition();
    a.revocation_outpoint = Some("bad".into());
    a.keyring_revealer = Some(KeyringRevealer::Certifier);
    refuses(
        a,
        "revocationOutpoint",
        "valid when acquisitionProtocol is \"direct\"",
    )
    .await;
}

#[tokio::test]
async fn keyring_revealer_before_subject_keyring() {
    let mut a = acquisition();
    a.keyring_revealer = Some(KeyringRevealer::Certifier);
    a.keyring_for_subject = Some(HashMap::new());
    refuses(
        a,
        "keyringRevealer",
        "valid when acquisitionProtocol is \"direct\"",
    )
    .await;
}

#[tokio::test]
async fn empty_subject_keyring_before_missing_url() {
    let mut a = acquisition();
    a.keyring_for_subject = Some(HashMap::new());
    a.certifier_url = None;
    refuses(
        a,
        "keyringForSubject",
        "valid when acquisitionProtocol is \"direct\"",
    )
    .await;
}

#[tokio::test]
async fn missing_or_empty_url_before_type() {
    for url in [None, Some(String::new())] {
        let mut a = acquisition();
        a.certifier_url = url;
        a.certificate_type = "!".into();
        refuses(
            a,
            "certifierUrl",
            "valid when acquisitionProtocol is \"issuance\"",
        )
        .await;
    }
}

#[tokio::test]
async fn type_before_url_syntax() {
    let mut a = acquisition();
    a.certificate_type = "!".into();
    a.certifier_url = Some("bad URL".into());
    refuses(a, "type", "valid base64 string").await;
}

#[tokio::test]
async fn certifier_before_url_syntax() {
    let mut a = acquisition();
    a.certifier = "gg".into();
    a.certifier_url = Some("bad URL".into());
    refuses(a, "certifier", "hexadecimal string.").await;
}

async fn url_refuses(value: String, rule: &str) {
    let mut a = acquisition();
    a.certifier_url = Some(value);
    a.fields.insert(String::new(), String::new());
    refuses(a, "certifierUrl", rule).await;
}

#[tokio::test]
async fn url_byte_limit_before_whitespace() {
    url_refuses(
        format!(" {}", "é".repeat(1024)),
        "no more than 2048 length.",
    )
    .await;
}

#[tokio::test]
async fn url_exact_whitespace_before_parse() {
    for url in [" bad URL", "https://issuer.example/\u{feff}"] {
        url_refuses(url.into(), "an exact HTTPS URL without whitespace").await;
    }
}

#[tokio::test]
async fn url_absolute_before_scheme() {
    url_refuses("/relative".into(), "an absolute HTTPS URL").await;
}

#[tokio::test]
async fn url_https_before_credentials() {
    url_refuses("http://user@issuer.example/?query".into(), "an HTTPS URL").await;
}

#[tokio::test]
async fn url_credentials_before_query() {
    url_refuses(
        "https://user@issuer.example/?query".into(),
        "a URL without credentials",
    )
    .await;
}

#[tokio::test]
async fn url_query_or_fragment_before_hostname() {
    for url in ["https://localhost/?query", "https://localhost/#fragment"] {
        url_refuses(url.into(), "a URL without a query or fragment").await;
    }
}

#[tokio::test]
async fn url_public_dns_before_fields() {
    for url in [
        "https://localhost",
        "https://a.localhost",
        "https://127.0.0.1",
        "https://[::1]",
        "https://single",
        "https://bad_.example",
        "https://-bad.example",
    ] {
        url_refuses(url.into(), "a public DNS hostname").await;
    }
}

#[tokio::test]
async fn fields_count_before_unsafe_key() {
    let mut a = acquisition();
    a.fields = (0..100_000)
        .map(|i| (i.to_string(), String::new()))
        .collect();
    a.fields.insert("__proto__".into(), String::new());
    refuses(a, "fields", "an object with at most 100000 properties").await;
}

#[tokio::test]
async fn fields_safe_keys_before_field_length() {
    for name in ["__proto__", "constructor", "prototype"] {
        let mut a = acquisition();
        a.fields.insert(name.into(), String::new());
        a.fields.insert(String::new(), String::new());
        refuses(a, "fields", "an object with safe string keys").await;
    }
}

#[tokio::test]
async fn field_name_minimum_bytes() {
    let mut a = acquisition();
    a.fields.insert(String::new(), "value".into());
    refuses(a, "field name", "at least 1 length.").await;
}

#[tokio::test]
async fn field_name_maximum_bytes() {
    let mut a = acquisition();
    a.fields.insert("é".repeat(26), String::new());
    refuses(a, "field name", "no more than 50 length.").await;
}

async fn acquire_as_test_wallet(
    wallet: &TestWallet,
    acquisition: AcquireCertificateArgs,
    identity: &str,
) -> bsv_rs::Result<WalletCertificate> {
    acquire_certificate_issuance(
        wallet,
        wallet.storage(),
        &AuthId::with_user_id(wallet.identity_key(), 1),
        acquisition,
        identity,
        ORIGINATOR,
    )
    .await
    .map_err(Into::into)
}
