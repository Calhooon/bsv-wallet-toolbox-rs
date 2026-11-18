//! The certificate lookups read one output of each answered BEEF unlinked:
//! a BEEF bsv-rs's linking door refuses (a sweep of 587 P2PKH outputs of
//! one parent, `common::p2pkh_sweep_beef_of`) is read at the SLAP tracker's
//! answer and at the overlay host's answer alike. The linking door's own
//! onset stays pinned in `tests/beef_link_budget_tests.rs`.

mod common;

use bsv_rs::overlay::{LookupAnswer, OutputListItem};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::Transaction;
use bsv_wallet_toolbox_rs::{HttpLookupResolver, OverlayLookupResolver};
use common::{beef_paying, p2pkh_sweep_beef_of};

/// The sweep the linking door refuses (586 link, 587 do not).
const REFUSED_SWEEP: u32 = 587;

fn pushdrop(fields: Vec<Vec<u8>>) -> LockingScript {
    PushDrop::new(PrivateKey::random().public_key(), fields).lock()
}

fn slap_lock(host: &str) -> LockingScript {
    pushdrop(vec![
        b"SLAP".to_vec(),
        vec![2; 33],
        host.as_bytes().to_vec(),
        b"ls_identity".to_vec(),
    ])
}

fn cert_lock(serial: &str) -> LockingScript {
    let cert = serde_json::json!({
        "type": "dGVzdA==",
        "serialNumber": serial,
        "subject": "02aa",
        "certifier": "03bb",
    });
    pushdrop(vec![serde_json::to_vec(&cert).unwrap()])
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

/// A BEEF the linking door refuses, its subject's output 0 locked by `lock`.
fn refused_beef(lock: LockingScript) -> Vec<u8> {
    let (beef, _) = p2pkh_sweep_beef_of(REFUSED_SWEEP, lock);
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
        .lookup_by_identity_key("02aa")
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
        |host| refused_beef(slap_lock(host)),
        beef_paying(cert_lock("S")).0,
    )
    .await;
    assert_eq!(serials, vec!["S".to_string()]);
}

/// An overlay host's certificate output in a BEEF the linking door refuses
/// is read into the answer.
#[tokio::test]
async fn a_certificate_output_in_a_beef_the_linking_door_refuses_is_read() {
    let serials = lookup_with(
        |host| beef_paying(slap_lock(host)).0,
        refused_beef(cert_lock("S")),
    )
    .await;
    assert_eq!(serials, vec!["S".to_string()]);
}
