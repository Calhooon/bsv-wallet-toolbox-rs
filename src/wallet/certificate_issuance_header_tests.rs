//! #269: the certifier's identity header is read as ts-stack@edf6e03 Wallet.ts:789-791
//! reads it: one name, required, compared before the body is read.

use super::*;

// The reference's name, as AuthFetch.ts:479 sets it on the response it returns.
const REFERENCE_HEADER: &str = "x-bsv-auth-identity-key";
// A body the JSON parse refuses, so a parse error would name it.
const UNPARSABLE: &str = "not json";

async fn header_run(
    headers: &[(&'static str, String)],
    body: &str,
    certifier: String,
) -> bsv_rs::Result<WalletCertificate> {
    let wallet = wallet().await;
    let mut server = mockito::Server::new_async().await;
    let mut mock = server.mock("POST", "/issue");
    for (name, value) in headers {
        mock = mock.with_header(*name, value);
    }
    let request = mock.with_body(body).expect(1).create_async().await;
    let result =
        acquire_from_fixture(&wallet, args(certifier, format!("{}/issue", server.url()))).await;
    request.assert_async().await;
    result
}

fn refused_as(received: &str) -> String {
    format!(
        "Validation error: Invalid certifier! Expected: {}, Received: {received}",
        certifier()
    )
}

#[tokio::test]
async fn reference_header_with_the_certifier_passes_to_the_body() {
    let result = header_run(
        &[(REFERENCE_HEADER, certifier())],
        r#"{"serverNonce":"fixture","certificate":null}"#,
        certifier(),
    )
    .await;
    assert_wallet_error(
        result,
        "Validation error: No certificate received from certifier!",
    );
}

#[tokio::test]
async fn absent_header_is_refused_as_null_before_the_body() {
    // A padded certifier argument: Expected names the normalized key.
    let result = header_run(&[], UNPARSABLE, format!(" \t{}\n", certifier())).await;
    assert_wallet_error(result, &refused_as("null"));
}

#[tokio::test]
async fn old_header_name_alone_is_refused_as_absent() {
    let result = header_run(
        &[("x-bsv-identity-key", certifier())],
        UNPARSABLE,
        certifier(),
    )
    .await;
    assert_wallet_error(result, &refused_as("null"));
}

#[tokio::test]
async fn non_ascii_header_is_refused_as_a_mismatch() {
    // Fetch's Headers decode each byte as one code point (isomorphic decode), so
    // the UTF-8 bytes of "é" read back as "Ã©" and differ from the certifier.
    let result = header_run(
        &[(REFERENCE_HEADER, "é".to_string())],
        UNPARSABLE,
        certifier(),
    )
    .await;
    assert_wallet_error(result, &refused_as("Ã©"));
}

#[tokio::test]
async fn repeated_header_is_compared_as_the_joined_value() {
    // Headers.get joins repeated values with ", ", so two copies never equal one key.
    let result = header_run(
        &[
            (REFERENCE_HEADER, certifier()),
            (REFERENCE_HEADER, certifier()),
        ],
        UNPARSABLE,
        certifier(),
    )
    .await;
    assert_wallet_error(result, &refused_as(&format!("{0}, {0}", certifier())));
}
