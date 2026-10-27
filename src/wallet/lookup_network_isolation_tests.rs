//! #248: every resolver constructor uses the isolated transport.

use super::{HttpLookupResolver, NetworkPreset, OverlayLookupResolver};
use crate::Error;

#[path = "../../tests/common/transport_isolation.rs"]
mod transport_isolation;
use transport_isolation::assert_isolated;

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
            assert_isolated(error.into());
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
        assert_isolated(error.into());
    }
    redirect.assert_async().await;
}
