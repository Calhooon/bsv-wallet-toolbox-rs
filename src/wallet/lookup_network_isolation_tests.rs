//! #248: every resolver constructor uses the isolated transport.

use super::{HttpLookupResolver, NetworkPreset, OverlayLookupResolver};
use crate::services::providers::http_client::test_support::assert_isolated;

fn resolvers(endpoint: &str) -> [HttpLookupResolver; 3] {
    let mut preset = HttpLookupResolver::for_network(NetworkPreset::Local);
    preset.slap_trackers = vec![endpoint.to_string()];
    [
        HttpLookupResolver::new(endpoint),
        HttpLookupResolver::with_endpoints(vec![endpoint.to_string()]),
        preset,
    ]
}

#[tokio::test]
async fn lookup_clients_refuse_non_loopback_before_transport() {
    // The resolver intentionally swallows network failures during failover.
    // Inspect its actual client to distinguish isolation from a DNS/IO error.
    for resolver in resolvers("https://provider.invalid") {
        for scheme in ["http", "https"] {
            let error = resolver
                .client
                .post(format!("{scheme}://provider.invalid/lookup"))
                .json(&serde_json::json!({"service": "ls_slap"}))
                .send()
                .await
                .unwrap_err();
            assert_isolated(&error.into());
        }
    }
}

#[tokio::test]
async fn lookup_clients_accept_loopback_fixtures() {
    let mut server = mockito::Server::new_async().await;
    let request = server
        .mock("POST", "/lookup")
        .match_header("content-type", "application/json")
        .match_body(mockito::Matcher::Json(serde_json::json!({
            "service": "ls_slap",
            "query": {"service": "ls_identity"}
        })))
        .with_header("content-type", "application/json")
        .with_body(r#"{"type":"output-list","outputs":[]}"#)
        .expect(3)
        .create_async()
        .await;

    for resolver in resolvers(&server.url()) {
        let certificates = resolver.lookup_by_identity_key("fixture").await.unwrap();
        assert!(certificates.is_empty());
    }
    request.assert_async().await;
}

#[tokio::test]
async fn attribute_lookup_sends_the_supplied_map_order() {
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction, TransactionOutput};

    let mut server = mockito::Server::new_async().await;
    let host = server.url();
    assert!(host.len() < 76);
    let public = PrivateKey::from_hex(&format!("{:064x}", 42))
        .unwrap()
        .public_key();
    let mut tx = Transaction::new();
    tx.outputs.push(TransactionOutput {
        satoshis: Some(1),
        locking_script: LockingScript::from_hex(&format!(
            "21{}ac{:02x}{}75",
            public.to_hex(),
            host.len(),
            hex::encode(host.as_bytes())
        ))
        .unwrap(),
        change: false,
    });
    let slap = server
        .mock("POST", "/lookup")
        .match_body(mockito::Matcher::PartialJson(serde_json::json!({
            "service": "ls_slap"
        })))
        .with_header("content-type", "application/json")
        .with_body(
            serde_json::json!({
                "type": "output-list",
                "outputs": [{"beef": tx.to_beef(true).unwrap(), "outputIndex": 0}]
            })
            .to_string(),
        )
        .expect(1)
        .create_async()
        .await;
    let lookup = server
        .mock("POST", "/lookup")
        .match_body(r#"{"query":{"attributes":{"zeta":"z","alpha":"a"}},"service":"ls_identity"}"#)
        .with_header("content-type", "application/json")
        .with_body(r#"{"type":"output-list","outputs":[]}"#)
        .expect(1)
        .create_async()
        .await;
    let attributes = [
        ("zeta".to_string(), "z".to_string()),
        ("alpha".to_string(), "a".to_string()),
    ]
    .into_iter()
    .collect();
    let resolver = HttpLookupResolver::new(&host);
    assert!(resolver
        .lookup_by_attributes(&attributes)
        .await
        .unwrap()
        .is_empty());
    slap.assert_async().await;
    lookup.assert_async().await;
}

#[tokio::test]
async fn lookup_clients_refuse_non_loopback_redirects() {
    let mut server = mockito::Server::new_async().await;
    let redirect = server
        .mock("POST", "/lookup")
        .with_status(307)
        .with_header("location", "https://provider.invalid/lookup")
        .expect(3)
        .create_async()
        .await;

    for resolver in resolvers(&server.url()) {
        let error = resolver
            .client
            .post(format!("{}/lookup", server.url()))
            .body("fixture")
            .send()
            .await
            .unwrap_err();
        assert_isolated(&error.into());
    }
    redirect.assert_async().await;
}
