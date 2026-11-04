//! #332: the issuance endpoint against the reference's constructor. Each
//! expected value is node v20's
//! ``new URL('signCertificate', `${validateCertificateIssuerUrl(raw)}/`)``
//! (ts-stack@edf6e03 `Wallet.ts:772`, `validationHelpers.ts:1354-1391`).

use super::{sign_certificate_endpoint, validate_issuance_url};

#[test]
fn the_endpoint_is_the_reference_constructors_for_every_base_shape() {
    for (raw, endpoint) in [
        ("https://c.example", "https://c.example/signCertificate"),
        ("https://c.example/", "https://c.example/signCertificate"),
        (
            "https://c.example/api",
            "https://c.example/api/signCertificate",
        ),
        (
            "https://c.example/api/",
            "https://c.example/api/signCertificate",
        ),
        (
            "https://c.example/api//",
            "https://c.example/api/signCertificate",
        ),
        (
            "https://c.example/a/b",
            "https://c.example/a/b/signCertificate",
        ),
        // An empty query or fragment passes validation; the added slash
        // lands in it, so the segment replaces `api` and both are dropped.
        (
            "https://c.example/api?",
            "https://c.example/signCertificate",
        ),
        (
            "https://c.example/api#",
            "https://c.example/signCertificate",
        ),
        (
            "https://c.example/api?#",
            "https://c.example/signCertificate",
        ),
        (
            "https://c.example:8443/x",
            "https://c.example:8443/x/signCertificate",
        ),
        (
            "https://C.Example/A%20b",
            "https://c.example/A%20b/signCertificate",
        ),
    ] {
        let base = validate_issuance_url(raw).unwrap();
        assert_eq!(sign_certificate_endpoint(&base).unwrap(), endpoint, "{raw}");
    }
}
