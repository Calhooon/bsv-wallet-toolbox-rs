//! Two readings of an overlay answer that bsv-rs 0.9.0-beta.8 moved:
//!
//! - #735: `OutputListItem` carries the output's `txid` hint as a string, so an
//!   answer whose hint is not a string no longer parses: that host's answer is
//!   skipped as any unparseable answer is (a SLAP tracker's, an overlay host's).
//! - #749 (GHSA-gvmh-f5gf-5vjj): a SLAP advertisement is read from its BEEF's
//!   subject alone (`Transaction::from_beef_subject`), so an advertisement whose
//!   transaction spends many outputs of one parent is read as any other; the
//!   default link budget of `Transaction::from_beef` refused it at the pin.
//!
//! Every transaction and BEEF is written by hand, so the same source runs at
//! either pin.

use super::HttpLookupResolver;
use super::OverlayLookupResolver;
use bsv_rs::overlay::OutputListItem;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;

fn varint(n: usize) -> String {
    if n < 0xfd {
        format!("{n:02x}")
    } else {
        format!("fd{:02x}{:02x}", n & 0xff, n >> 8)
    }
}

/// A version-1 transaction spending `inputs` (each unlocked by `OP_0`) to
/// `outputs`, lock time 0.
fn raw(inputs: &[(String, u32)], outputs: &[(u64, String)]) -> Vec<u8> {
    let mut tx = String::from("01000000");
    tx += &varint(inputs.len());
    for (txid, vout) in inputs {
        let mut le = hex::decode(txid).unwrap();
        le.reverse();
        tx += &hex::encode(le);
        tx += &hex::encode(vout.to_le_bytes());
        tx += "0100ffffffff";
    }
    tx += &varint(outputs.len());
    for (satoshis, script) in outputs {
        tx += &hex::encode(satoshis.to_le_bytes());
        tx += &varint(script.len() / 2);
        tx += script;
    }
    tx += "00000000";
    hex::decode(tx).unwrap()
}

/// A V2 BEEF of raw entries, each parent ahead of its children, no BUMP.
fn beef(txs: &[&[u8]]) -> Vec<u8> {
    let mut bytes = hex::decode("0200beef00").unwrap();
    bytes.extend(hex::decode(varint(txs.len())).unwrap());
    for tx in txs {
        bytes.push(0);
        bytes.extend_from_slice(tx);
    }
    bytes
}

/// A SLAP advertisement of `host` for the identity service, as a `PushDrop`.
fn slap(host: &str) -> String {
    let key = PrivateKey::from_hex(&format!("{:064x}", 9))
        .unwrap()
        .public_key();
    let fields = vec![
        b"SLAP".to_vec(),
        host.as_bytes().to_vec(),
        b"ls_identity".to_vec(),
        vec![0x30; 8],
    ];
    hex::encode(PushDrop::new(key, fields).lock().to_binary())
}

/// The advertisement in a transaction spending `fan` outputs of one parent.
fn advertisement(host: &str, fan: u32) -> Vec<u8> {
    let p2pkh = format!("76a914{}88ac", "22".repeat(20));
    let parent = raw(&[("11".repeat(32), 0)], &vec![(1_000, p2pkh); fan as usize]);
    let parent_txid = Transaction::from_binary(&parent).unwrap().id();
    let inputs: Vec<_> = (0..fan).map(|vout| (parent_txid.clone(), vout)).collect();
    let child = raw(&inputs, &[(1, slap(host))]);
    beef(&[&parent, &child])
}

fn output_list(outputs: &serde_json::Value) -> String {
    serde_json::json!({"type": "output-list", "outputs": outputs}).to_string()
}

/// A tracker and host on one loopback server: the tracker's SLAP answer, the
/// host's identity answer; the identity lookup's outputs and how many
/// identity questions the host was asked.
async fn identity_outputs(
    slap_answer: impl FnOnce(&str) -> String,
    identity_answer: String,
) -> (Vec<OutputListItem>, usize) {
    let mut server = mockito::Server::new_async().await;
    let slap_mock = server
        .mock("POST", "/lookup")
        .match_body(mockito::Matcher::PartialJson(
            serde_json::json!({"service": "ls_slap"}),
        ))
        .with_header("content-type", "application/json")
        .with_body(slap_answer(&server.url()))
        .create_async()
        .await;
    let identity = server
        .mock("POST", "/lookup")
        .match_body(mockito::Matcher::PartialJson(
            serde_json::json!({"service": "ls_identity"}),
        ))
        .with_header("content-type", "application/json")
        .with_body(identity_answer)
        .create_async()
        .await;
    let resolver = HttpLookupResolver::new(&server.url());
    let outputs = resolver
        .lookup_by_identity_key("subject", &[])
        .await
        .unwrap();
    drop(slap_mock);
    let asked = usize::from(identity.matched_async().await);
    (outputs, asked)
}

/// [`identity_outputs`], the outputs counted.
async fn identity_lookup(
    slap_answer: impl FnOnce(&str) -> String,
    identity_answer: String,
) -> (usize, usize) {
    let (outputs, asked) = identity_outputs(slap_answer, identity_answer).await;
    (outputs.len(), asked)
}

fn slap_answer(fan: u32, hint: Option<serde_json::Value>) -> impl FnOnce(&str) -> String {
    move |host| slap_answer_at(host, fan, hint)
}

fn slap_answer_at(host: &str, fan: u32, hint: Option<serde_json::Value>) -> String {
    let mut output = serde_json::json!({
        "beef": advertisement(host, fan),
        "outputIndex": 0,
    });
    if let Some(hint) = hint {
        output["txid"] = hint;
    }
    output_list(&serde_json::json!([output]))
}

fn identity_answer(hint: Option<serde_json::Value>) -> String {
    let mut output = serde_json::json!({"beef": [1, 2, 3], "outputIndex": 0});
    if let Some(hint) = hint {
        output["txid"] = hint;
    }
    output_list(&serde_json::json!([output]))
}

#[tokio::test]
async fn an_answer_with_a_string_hint_carries_the_hint_735() {
    let txid = "ab".repeat(32);
    let hint = Some(serde_json::json!(txid));
    let (outputs, asked) =
        identity_outputs(slap_answer(1, hint.clone()), identity_answer(hint)).await;
    assert_eq!((outputs.len(), asked), (1, 1));
    assert_eq!(
        outputs[0].txid,
        Some(txid),
        "the host's hint is on the output as the host sent it"
    );
}

#[tokio::test]
async fn a_host_answer_whose_hint_is_no_string_is_skipped_735() {
    let numeric = Some(serde_json::json!(5));
    assert_eq!(
        identity_lookup(slap_answer(1, None), identity_answer(numeric)).await,
        (0, 1),
        "the host is asked and its answer read as unparseable"
    );
}

#[tokio::test]
async fn a_tracker_answer_whose_hint_is_no_string_names_no_host_735() {
    let numeric = Some(serde_json::json!(5));
    assert_eq!(
        identity_lookup(slap_answer(1, numeric), identity_answer(None)).await,
        (0, 0),
        "no host is discovered, so none is asked"
    );
}

/// An advertisement in a transaction spending `fan` outputs of one parent
/// names its host, and the host is asked.
async fn assert_advertisement_is_read(fan: u32) {
    assert_eq!(
        identity_lookup(slap_answer(fan, None), identity_answer(None)).await,
        (1, 1),
        "an advertisement in a transaction spending {fan} outputs of one parent"
    );
}

/// The control: a fan of 2 is within the default link budget, so this passes
/// at either pin and with either reader.
#[tokio::test]
async fn control_an_advertisement_within_the_link_budget_is_read_749() {
    assert_advertisement_is_read(2).await;
}

/// The witness: a fan of 800 re-materializes its parent past the default link
/// budget, so a linking reader refuses this advertisement at the pin.
#[tokio::test]
async fn witness_an_advertisement_past_the_link_budget_is_read_749() {
    assert_advertisement_is_read(800).await;
}
