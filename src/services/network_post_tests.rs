//! Shared unit and bench-feature integration witnesses for #182.

use super::{services::PostBeefMode, Chain, Services, ServicesOptions, WalletServices};
use bsv_rs::transaction::{Beef, MerklePath, Transaction};

fn sample() -> (Vec<u8>, Vec<String>) {
    // Unsigned synthetic parent and child with a fixture proof. No keys.
    let raw = format!(
        "0100000001{}00000000{}ffffffff01{}015100000000",
        "00".repeat(32),
        "00",
        "00".repeat(8)
    );
    let parent = Transaction::from_hex(&raw).unwrap();
    let mut source = hex::decode(parent.id()).unwrap();
    source.reverse();
    let child = format!(
        "0100000001{}0000000000ffffffff010000000000000000015100000000",
        hex::encode(source)
    );
    let mut beef = Beef::new();
    // Height unjudged: the post path judges no script; `check_url` refuses its requests.
    let proof = beef.merge_bump(MerklePath::from_coinbase_txid(&parent.id(), 100_000));
    beef.merge_raw_tx(parent.to_binary(), Some(proof));
    let txid = beef.merge_raw_tx(hex::decode(child).unwrap(), None).txid();
    (beef.to_binary(), vec![txid])
}

/// Options whose broadcaster is a non-loopback placeholder host: Arcade, the
/// default mode, or classic ARC behind the deprecated knob (#317). The
/// defaults name no broadcaster, so they no longer construct.
#[allow(deprecated)]
fn off_loopback(chain: Chain, classic: bool) -> Services {
    let options = if classic {
        ServicesOptions::mainnet().with_classic_arc("https://arc.invalid", None)
    } else {
        ServicesOptions::mainnet().with_arcade("https://arcade.invalid", None)
    };
    Services::with_options(chain, options).unwrap()
}

async fn assert_posts_refused(mut services: Services, mode: PostBeefMode) {
    services.set_post_beef_mode(mode);
    let (beef, txids) = sample();
    let count = services.post_beef_count().unwrap();
    let results = services.post_beef(&beef, &txids).await.unwrap();
    assert_eq!(results.len(), count);
    for result in results {
        assert!(!result.is_success(), "{}: {mode:?}", result.name);
        let error = serde_json::to_string(&result).unwrap();
        assert!(
            error.contains("#182") && error.contains("rule 28"),
            "{error}"
        );
    }
    eprintln!("{mode:?}: {count} provider requests refused before transport");
}

#[tokio::test]
async fn default_post_modes_never_leave_loopback() {
    for chain in [Chain::Main, Chain::Test] {
        for mode in [PostBeefMode::PromiseAll, PostBeefMode::UntilSuccess] {
            for classic in [false, true] {
                assert_posts_refused(off_loopback(chain, classic), mode).await;
            }
        }
    }
}

#[tokio::test]
async fn configured_provider_urls_cannot_bypass_isolation() {
    let options = ServicesOptions::mainnet()
        .with_woc_api_key("isolation-fixture")
        .with_bitails_api_key("isolation-fixture")
        .with_arcade("https://arcade.invalid", None)
        .with_gorillapool("https://arc.invalid", None)
        .with_bananablocks_base("https://explorer.invalid")
        .with_bhs_url("https://bhs.invalid")
        .with_chaintracks_url("https://headers.invalid")
        .with_break_glass_explorer_headers(true);
    let services = Services::with_options(Chain::Main, options).unwrap();
    for error in [
        services
            .arcade
            .as_ref()
            .unwrap()
            .health()
            .await
            .unwrap_err(),
        services
            .bananablocks
            .as_ref()
            .unwrap()
            .get_raw_tx(&"11".repeat(32))
            .await
            .unwrap_err(),
        services
            .bhs
            .as_ref()
            .unwrap()
            .current_height()
            .await
            .unwrap_err(),
        services
            .chaintracks
            .as_ref()
            .unwrap()
            .primary()
            .get_present_height()
            .await
            .unwrap_err(),
    ] {
        let message = error.to_string();
        assert!(
            message.contains("#182") && message.contains("rule 28"),
            "{message}"
        );
    }
    // Arcade's implicit classic ARC fallback must also be refused.
    assert_posts_refused(services, PostBeefMode::default()).await;
}

#[test]
fn environment_variables_cannot_bypass_isolation() {
    let mut local = mockito::Server::new();
    let mut proxy = mockito::Server::new();
    let local_post = local
        .mock("POST", "/v1/tx")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"txStatus":"STORED","txid":"fixture"}"#)
        .expect(2)
        .create();
    let proxy_post = proxy.mock("POST", mockito::Matcher::Any).expect(0).create();
    let proxy_connect = proxy
        .mock("CONNECT", mockito::Matcher::Any)
        .expect(0)
        .create();
    for proxy_enabled in [false, true] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "isolated_environment_worker",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("BSV_TEST_ISOLATION_WORKER", "1")
            .env("BSV_TEST_LOOPBACK_URL", local.url());
        for key in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            if proxy_enabled {
                command.env(key, proxy.url());
            } else {
                command.env_remove(key);
            }
        }
        for key in ["NO_PROXY", "no_proxy"] {
            command.env(key, if proxy_enabled { "" } else { "*" });
        }
        let output = command.output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        eprintln!("hostile environment (proxy={proxy_enabled}): {stdout}{stderr}");
    }
    local_post.assert();
    proxy_post.assert();
    proxy_connect.assert();
}

#[tokio::test]
async fn isolated_environment_worker() {
    if std::env::var_os("BSV_TEST_ISOLATION_WORKER").is_none() {
        return;
    }
    for mode in [PostBeefMode::PromiseAll, PostBeefMode::UntilSuccess] {
        assert_posts_refused(off_loopback(Chain::Main, false), mode).await;
    }
    let local =
        super::Arc::new(std::env::var("BSV_TEST_LOOPBACK_URL").unwrap(), None, None).unwrap();
    let result = local.post_raw_tx("00", None).await.unwrap();
    assert_eq!(result.status, "success");
}
