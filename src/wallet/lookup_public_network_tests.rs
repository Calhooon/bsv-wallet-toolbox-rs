//! Witnesses of the public-network rule on the lookup client.

use super::*;
use crate::wallet::lookup::{HttpLookupResolver, OverlayLookupResolver};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The reference's address list, address by address.
#[test]
fn the_public_address_rule() {
    let public = |s: &str| is_public_network_address(s.parse().unwrap());
    for private in [
        "0.0.0.0",
        "10.1.2.3",
        "127.0.0.1",
        "100.64.0.1",
        "169.254.1.1",
        "172.16.0.1",
        "192.0.0.1",
        "192.168.1.1",
        "198.18.0.1",
        "198.51.100.1",
        "203.0.113.1",
        "224.0.0.1",
        "::",
        "::1",
        "::ffff:127.0.0.1",
        "::ffff:10.0.0.1",
        "64:ff9b::7f00:1",
        "2002:7f00:1::",
        "2001:db8::1",
        "2001:10::1",
        "fc00::1",
        "fe80::1",
        "fec0::1",
        "ff02::1",
        "3fff::1",
    ] {
        assert!(!public(private), "{private} is not public");
    }
    for open in [
        "8.8.8.8",
        "1.1.1.1",
        "::ffff:8.8.8.8",
        "2606:4700::1111",
        "2002:808:808::",
    ] {
        assert!(public(open), "{open} is public");
    }
}

/// A listener on 127.0.0.1 that counts the connections it takes and
/// closes each.
async fn counting_listener() -> (u16, Arc<AtomicUsize>) {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = socket.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
            drop(stream);
        }
    });
    (port, hits)
}

/// Under https trackers, a tracker at a loopback address, and one whose
/// name resolves to loopback, is never connected to.
#[tokio::test]
async fn a_non_public_address_is_not_connected_to_without_http() {
    let (port, hits) = counting_listener().await;
    for tracker in [
        format!("https://127.0.0.1:{port}"),
        format!("https://localhost:{port}"),
    ] {
        let answered = HttpLookupResolver::with_endpoints(vec![tracker.clone()])
            .lookup_by_identity_key("02aa")
            .await
            .unwrap();
        assert!(answered.is_empty());
        assert_eq!(hits.load(Ordering::SeqCst), 0, "{tracker} was connected to");
    }
}
