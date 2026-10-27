//! #182: capture a default post destination without sending a request.

use super::{http_client::check_url, Chain, Services};

#[test]
fn only_loopback_request_addresses_are_allowed() {
    for host in ["127.0.0.1", "localhost", "[::1]"] {
        for scheme in ["http", "https"] {
            let url = reqwest::Url::parse(&format!("{scheme}://{host}:1234/path")).unwrap();
            assert!(check_url(&url).is_ok(), "{url}");
        }
    }
    for address in [
        "http://provider.invalid/path",
        "https://127.0.0.1.provider.invalid/path",
        "http://localhost./path",
        "http://localhost@provider.invalid/path",
        "http://192.0.2.1/path",
        "https://[2001:db8::1]/path",
        "http://0.0.0.0/path",
        "file:///fixture",
    ] {
        let url = reqwest::Url::parse(address).unwrap();
        let error = check_url(&url).unwrap_err();
        assert!(matches!(error, crate::Error::NetworkError(_)));
        let message = error.to_string();
        assert!(message.contains(url.host_str().unwrap_or("<missing>")));
        assert!(message.contains("#182") && message.contains("rule 28"));
    }
}

#[tokio::test]
async fn the_client_refuses_remote_requests_before_execution() {
    let client = super::http_client::Client::default();
    let error = client
        .post("http://provider.invalid/tx")
        .body("synthetic request")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(error, crate::Error::NetworkError(_)));
    let message = error.to_string();
    assert!(message.contains("provider.invalid") && message.contains("#182"));
}

#[tokio::test]
async fn loopback_redirects_stay_local() {
    let mut server = super::mock_server().await;
    let port = reqwest::Url::parse(&server.url()).unwrap().port().unwrap();
    let redirect = server
        .mock("POST", "/redirect")
        .with_status(307)
        .with_header("location", &format!("http://localhost:{port}/target"))
        .expect(1)
        .create_async()
        .await;
    let target = server
        .mock("POST", "/target")
        .match_body("fixture")
        .with_status(200)
        .expect(1)
        .create_async()
        .await;
    let client = super::http_client::Client::default();
    let response = client
        .post(format!("{}/redirect", server.url()))
        .body("fixture")
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    redirect.assert_async().await;
    target.assert_async().await;
}

#[tokio::test]
async fn a_loopback_redirect_cannot_leave_loopback() {
    let mut server = super::mock_server().await;
    let redirect = server
        .mock("POST", "/redirect")
        .with_status(307)
        .with_header("location", "https://provider.invalid/tx")
        .expect(1)
        .create_async()
        .await;
    let client = super::http_client::Client::default();
    let error = client
        .post(format!("{}/redirect", server.url()))
        .body("fixture")
        .send()
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("provider.invalid") && message.contains("#182"),
        "{message}"
    );
    redirect.assert_async().await;
}

#[test]
fn default_provider_request_is_refused_before_transport() {
    for chain in [Chain::Main, Chain::Test] {
        let services = Services::new(chain).unwrap();
        assert_eq!(
            services.post_beef_mode,
            super::services::PostBeefMode::default()
        );
        let request = reqwest::Client::new()
            .post(&services.options.arc_url)
            .build()
            .unwrap();
        assert!(!matches!(
            request.url().host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        ));
        let result = check_url(request.url());
        assert!(
            matches!(result, Err(crate::Error::NetworkError(ref message)) if message.contains("#182") && message.contains("rule 28")),
            "#182 rule 28: the captured default post destination must be refused before transport"
        );
    }
}
