//! #334: the production issuance client's refusals, each with the
//! reference's text (ts-stack@edf6e03 `PublicHTTPSFetch.ts`), the hostname's
//! answer from a stub resolver. The vectors are the reference's own
//! (`__tests__/PublicHTTPSFetch.test.ts:143-186`).

use std::net::IpAddr;
use std::time::Duration;

use super::public::{
    approved_addresses, is_public_network_address, pinned_client, public_client, Resolver,
    CREDENTIAL_FREE, DNS_TIMED_OUT, LITERAL_NON_PUBLIC, RESOLVED_NON_PUBLIC,
};
use crate::services::providers::http_client::Url;

fn ips(texts: &[&str]) -> Vec<IpAddr> {
    texts.iter().map(|t| t.parse().unwrap()).collect()
}

const PRIVATE: Resolver = |_| Box::pin(async { Ok(ips(&["10.0.0.1"])) });
const LOOPBACK: Resolver = |_| Box::pin(async { Ok(ips(&["127.0.0.1"])) });
const LINK_LOCAL: Resolver = |_| Box::pin(async { Ok(ips(&["169.254.169.254"])) });
const LINK_LOCAL_V6: Resolver = |_| Box::pin(async { Ok(ips(&["fe80::1"])) });
const MIXED: Resolver = |_| Box::pin(async { Ok(ips(&["8.8.8.8", "10.0.0.1"])) });
const NONE: Resolver = |_| Box::pin(async { Ok(Vec::new()) });
const TOO_MANY: Resolver = |_| Box::pin(async { Ok(ips(&["93.184.216.34"; 65])) });
const PUBLIC: Resolver =
    |_| Box::pin(async { Ok(ips(&["93.184.216.34", "2606:4700:4700::1111"])) });
const NEVER: Resolver = |_| Box::pin(std::future::pending());
const UNASKED: Resolver = |host| panic!("a literal address is its own answer, asked {host}");

async fn refusal(url: &str, resolver: Resolver) -> String {
    let target = Url::parse(url).unwrap();
    public_client(&target, resolver)
        .await
        .err()
        .unwrap()
        .to_string()
}

fn network(text: &str) -> String {
    format!("Network error: {text}")
}

#[test]
fn the_reference_vectors_classify_as_is_public_network_address() {
    for address in [
        "0.0.0.0",
        "10.0.0.1",
        "127.0.0.1",
        "169.254.169.254",
        "172.16.0.1",
        "192.168.0.1",
        "::1",
        "fc00::1",
        "fe80::1",
        "::ffff:127.0.0.1",
        "64:ff9b::7f00:1",
        "2002:7f00:1::",
        "100::1",
        "2001:db8::1",
        "3fff::1",
    ] {
        assert!(!is_public_network_address(address), "non-public {address}");
    }
    for address in [
        "999.1.1.1",
        "1.two.3.4",
        "fe80::1%lo0",
        "1::2::3",
        "1.2.3.4:1::",
        "gggg::1",
        "1:2:3:4:5:6:7:8::",
        "2001:4860:4860:0:0:0:8888",
        "::8.8.8.8",
        "64:ff9b::808:808",
    ] {
        assert!(!is_public_network_address(address), "malformed {address}");
    }
    for address in [
        "8.8.8.8",
        "2606:4700:4700::1111",
        "::ffff:8.8.8.8",
        "2002:0808:0808::",
        "2606:4700:4700:0:0:0:0:1111",
        "[2606:4700:4700::1111]",
    ] {
        assert!(is_public_network_address(address), "public {address}");
    }
}

/// The IPv4 ranges at :117-129, each at its edges.
#[test]
fn the_ipv4_ranges_hold_at_their_edges() {
    for (address, public) in [
        ("100.63.255.255", true),
        ("100.64.0.0", false),
        ("100.127.255.255", false),
        ("100.128.0.0", true),
        ("172.15.255.255", true),
        ("172.31.255.255", false),
        ("172.32.0.0", true),
        ("192.0.2.1", false),
        ("192.1.0.0", true),
        ("198.17.255.255", true),
        ("198.19.255.255", false),
        ("198.51.100.1", false),
        ("203.0.113.1", false),
        ("223.255.255.255", true),
        ("224.0.0.1", false),
    ] {
        assert_eq!(is_public_network_address(address), public, "{address}");
    }
}

#[tokio::test]
async fn a_private_address_from_the_resolver_is_refused() {
    let url = "https://certifier.example/signCertificate";
    assert_eq!(refusal(url, PRIVATE).await, network(RESOLVED_NON_PUBLIC));
}

#[tokio::test]
async fn a_loopback_address_from_the_resolver_is_refused() {
    let url = "https://certifier.example/signCertificate";
    assert_eq!(refusal(url, LOOPBACK).await, network(RESOLVED_NON_PUBLIC));
}

#[tokio::test]
async fn a_link_local_address_from_the_resolver_is_refused() {
    let url = "https://certifier.example/signCertificate";
    assert_eq!(refusal(url, LINK_LOCAL).await, network(RESOLVED_NON_PUBLIC));
    assert_eq!(
        refusal(url, LINK_LOCAL_V6).await,
        network(RESOLVED_NON_PUBLIC)
    );
}

/// :263-270: one non-public address among public ones, no address, or more
/// than 64 refuse the request.
#[tokio::test]
async fn a_mixed_empty_or_oversized_answer_is_refused() {
    let url = "https://certifier.example/signCertificate";
    for resolver in [MIXED, NONE, TOO_MANY] {
        assert_eq!(refusal(url, resolver).await, network(RESOLVED_NON_PUBLIC));
    }
}

#[tokio::test]
async fn a_resolution_past_its_deadline_is_refused() {
    let deadline = Duration::from_millis(20);
    let error = approved_addresses(NEVER, "certifier.example", deadline).await;
    assert_eq!(error.err().unwrap().to_string(), network(DNS_TIMED_OUT));
}

#[tokio::test]
async fn a_credential_url_is_refused() {
    for url in [
        "https://user@certifier.example/signCertificate",
        "https://user:pass@certifier.example/signCertificate",
        "https://:pass@certifier.example/signCertificate",
        "https://certifier.example/signCertificate#part",
        "http://certifier.example/signCertificate",
    ] {
        assert_eq!(
            refusal(url, PUBLIC).await,
            network(CREDENTIAL_FREE),
            "{url}"
        );
    }
}

#[tokio::test]
async fn a_non_public_literal_is_refused_before_any_resolution() {
    for url in [
        "https://127.0.0.1/signCertificate",
        "https://10.0.0.1/signCertificate",
        "https://[::1]/signCertificate",
        "https://[fe80::1]/signCertificate",
        "https://[::ffff:7f00:1]/signCertificate",
    ] {
        assert_eq!(
            refusal(url, UNASKED).await,
            network(LITERAL_NON_PUBLIC),
            "{url}"
        );
    }
}

#[tokio::test]
async fn a_public_address_passes_to_the_post() {
    let url = Url::parse("https://certifier.example/signCertificate").unwrap();
    let approved = approved_addresses(PUBLIC, "certifier.example", Duration::from_secs(1));
    assert_eq!(
        approved.await.unwrap(),
        ips(&["93.184.216.34", "2606:4700:4700::1111"])
    );
    assert!(public_client(&url, PUBLIC).await.is_ok());
    let literal = Url::parse("https://8.8.8.8/signCertificate").unwrap();
    assert!(public_client(&literal, UNASKED).await.is_ok());
}

/// The client reaches the approved address and nowhere else, and answers a
/// redirect as it came (:260's `redirect: 'error'`): the target is not asked.
#[tokio::test]
async fn a_redirect_from_the_certifier_is_not_followed() {
    let mut server = mockito::Server::new_async().await;
    let redirect = server
        .mock("POST", "/.well-known/auth")
        .with_status(307)
        .with_header("location", "/elsewhere")
        .create_async()
        .await;
    let elsewhere = server
        .mock("POST", "/elsewhere")
        .expect(0)
        .create_async()
        .await;
    let port = server.socket_address().port();
    let client = pinned_client("certifier.example", &ips(&["127.0.0.1"])).unwrap();
    let url = format!("http://certifier.example:{port}/.well-known/auth");
    let response = client.post(url).body("{}").send().await.unwrap();
    assert_eq!(response.status().as_u16(), 307);
    redirect.assert_async().await;
    elsewhere.assert_async().await;
}
