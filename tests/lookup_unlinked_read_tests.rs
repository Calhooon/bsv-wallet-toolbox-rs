//! The certificate lookups read one output of each answered BEEF unlinked:
//! a BEEF bsv-rs's linking door refuses (a sweep of 587 P2PKH outputs of
//! one parent, `common::p2pkh_sweep_beef_of`) is read at the SLAP tracker's
//! answer and at the overlay host's answer alike. The linking door's own
//! onset stays pinned in `tests/beef_link_budget_tests.rs`.

mod common;

use bsv_rs::overlay::{LookupAnswer, OutputListItem};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::Transaction;
use bsv_wallet_toolbox_rs::{HttpLookupResolver, OverlayLookupResolver};
use common::lookup_identity::{certificate_record, identity_lock, serial_of, slap_lock};
use common::{
    beef_paying, beef_paying_one_satoshi, p2pkh_sweep_beef_of, p2pkh_sweep_beef_of_one_satoshi,
};

/// The sweep the linking door refuses (586 link, 587 do not).
const REFUSED_SWEEP: u32 = 587;

/// The subject the lookups ask for.
fn subject() -> PrivateKey {
    PrivateKey::from_hex(&"11".repeat(32)).unwrap()
}

/// The identity output of the subject's certificate tagged `tag`.
fn cert_lock(tag: &str) -> LockingScript {
    let certifier = PrivateKey::from_hex(&"22".repeat(32)).unwrap();
    identity_lock(
        &subject(),
        &certificate_record(&subject(), &certifier, tag, &[("tag", tag)]),
    )
}

/// The answer of one output: `beef`'s subject's output 0.
fn answer_of(beef: Vec<u8>) -> String {
    serde_json::to_string(&LookupAnswer::OutputList {
        outputs: vec![OutputListItem {
            beef,
            output_index: 0,
            context: None,
        }],
    })
    .unwrap()
}

/// A BEEF the linking door refuses, its subject's output 0 locked by `lock`
/// (of one satoshi when `one_satoshi`, as an advertisement's).
fn refused_beef(lock: LockingScript, one_satoshi: bool) -> Vec<u8> {
    let (beef, _) = if one_satoshi {
        p2pkh_sweep_beef_of_one_satoshi(REFUSED_SWEEP, lock)
    } else {
        p2pkh_sweep_beef_of(REFUSED_SWEEP, lock)
    };
    assert!(
        Transaction::from_beef(&beef, None).is_err(),
        "the linking door must refuse the sweep"
    );
    beef
}

/// The serial numbers a lookup answers when the tracker's output is
/// `slap_beef(host)` and the host's output is `cert_beef`.
async fn lookup_with(slap_beef: impl Fn(&str) -> Vec<u8>, cert_beef: Vec<u8>) -> Vec<String> {
    let mut host = mockito::Server::new_async().await;
    host.mock("POST", "/lookup")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(answer_of(cert_beef))
        .create_async()
        .await;
    let mut tracker = mockito::Server::new_async().await;
    tracker
        .mock("POST", "/lookup")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(answer_of(slap_beef(&host.url())))
        .create_async()
        .await;
    HttpLookupResolver::new(&tracker.url())
        .lookup_by_identity_key(&subject().public_key().to_hex())
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.serial_number)
        .collect()
}

/// A SLAP tracker's output in a BEEF the linking door refuses names its
/// host, and the host is called.
#[tokio::test]
async fn a_slap_output_in_a_beef_the_linking_door_refuses_is_read() {
    let serials = lookup_with(
        |host| refused_beef(slap_lock(host), true),
        beef_paying(cert_lock("S")).0,
    )
    .await;
    assert_eq!(serials, vec![serial_of("S")]);
}

/// An overlay host's certificate output in a BEEF the linking door refuses
/// is read into the answer.
#[tokio::test]
async fn a_certificate_output_in_a_beef_the_linking_door_refuses_is_read() {
    let serials = lookup_with(
        |host| beef_paying_one_satoshi(slap_lock(host)).0,
        refused_beef(cert_lock("S"), false),
    )
    .await;
    assert_eq!(serials, vec![serial_of("S")]);
}
