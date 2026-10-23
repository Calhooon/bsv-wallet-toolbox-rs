//! The host of the ladder fixtures (`tests/fixtures/ladders.rs`,
//! bsv-wallet-toolbox-rs#53). The replay is written against the public
//! API and two names its parent gives it: `toolbox` (this crate) and
//! `services_at`. Only `services_at` needs the crate's inside: WhatsOnChain
//! and Bitails reach another API base only through test-only constructors,
//! so the replay runs here, under `cargo test --lib ladder_fixtures`. A
//! `tests/fixtures/main.rs` hosts the same file once a public way to point
//! the explorers at a local server exists (the owner's word: it is API).

use super::*;

use crate as toolbox;

/// `Services` with WhatsOnChain, Bitails and (when given) BananaBlocks at
/// local fixture bases, each with a gate of its own and a client that
/// gives up after `timeout_secs`; the rest from `options`.
fn services_at(
    options: ServicesOptions,
    woc: &str,
    bitails: &str,
    bananablocks: Option<&str>,
    timeout_secs: u64,
) -> Services {
    Services::with_explorers(
        Chain::Main,
        options,
        StdArc::new(WhatsOnChain::with_base_url_and_config(
            Chain::Main,
            woc,
            WhatsOnChainConfig {
                api_key: None,
                timeout_secs: Some(timeout_secs),
            },
        )),
        StdArc::new(Bitails::with_base_url_and_config(
            Chain::Main,
            bitails,
            BitailsConfig {
                api_key: None,
                timeout_secs: Some(timeout_secs),
            },
        )),
        bananablocks.map(|base| {
            StdArc::new(BananaBlocks::with_base_unshared_timeout(
                base,
                Some(timeout_secs),
            ))
        }),
    )
    .unwrap()
}

#[path = "../../tests/fixtures/ladders.rs"]
mod ladders;
