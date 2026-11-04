//! The permission modules on the five basket and label calls (#757, the
//! keeper's 2026-11-03 22:0xZ reading): `list_outputs` and
//! `relinquish_output` delegate a `p <scheme>` basket through #726's helper
//! (ts-stack@edf6e03 `WalletPermissionsManager.ts:5080-5140`);
//! `create_action`, `list_actions` and `internalize_action` chain every
//! module their `p ` baskets and labels name (`:4294-4338`, `:4500-4520`,
//! `:4913-4961`, `:5059-5078`). The refusals carry `InvalidParameter` and
//! the text alone.

use std::sync::Mutex as StdMutex;

use serde_json::{json, Value};

use super::busy_door_tests::{above_genesis, fund_tagged, own_lock};
use super::nosend_label_tests::atomic_beef_paying_at;
use super::permission_modules_tests::{assert_module_refusal, module_wallet, ADMIN, NOSEND_TEXT};
use super::privileged_keys_tests::{TestWallet, ORIGINATOR};
use super::*;
use crate::managers::PermissionsModule;

/// A module that records what it is asked, sets `request`'s keys on the
/// arguments and `response`'s on the result.
struct Rewriter {
    request: StdMutex<Value>,
    response: Value,
    requests: StdMutex<Vec<(String, Value, String)>>,
    responses: StdMutex<Vec<(String, Value)>>,
}

impl Rewriter {
    fn new(request: Value, response: Value) -> Arc<Self> {
        Arc::new(Self {
            request: StdMutex::new(request),
            response,
            requests: StdMutex::new(Vec::new()),
            responses: StdMutex::new(Vec::new()),
        })
    }

    /// A module that changes nothing.
    fn quiet() -> Arc<Self> {
        Self::new(json!({}), json!({}))
    }

    fn asked(&self) -> Vec<(String, Value, String)> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl PermissionsModule for Rewriter {
    async fn on_request(
        &self,
        method: &str,
        mut args: Value,
        originator: &str,
    ) -> std::result::Result<Value, String> {
        let entry = (method.to_string(), args.clone(), originator.to_string());
        self.requests.lock().unwrap().push(entry);
        merge(&mut args, &self.request.lock().unwrap());
        Ok(args)
    }

    async fn on_response(
        &self,
        mut res: Value,
        method: &str,
        _originator: &str,
    ) -> std::result::Result<Value, String> {
        self.responses
            .lock()
            .unwrap()
            .push((method.to_string(), res.clone()));
        merge(&mut res, &self.response);
        Ok(res)
    }
}

/// `keys`' entries set on `object`.
fn merge(object: &mut Value, keys: &Value) {
    for (key, value) in keys.as_object().into_iter().flatten() {
        object[key] = value.clone();
    }
}

const LIMIT_TEXT: &str = "Validation error: The limit parameter must be no more than 10000 length.";
const DESCRIPTION_TEXT: &str =
    "Validation error: The description parameter must be at least 5 length.";

fn listing(basket: &str) -> ListOutputsArgs {
    serde_json::from_value(json!({ "basket": basket })).unwrap()
}

fn relinquishing(basket: &str) -> RelinquishOutputArgs {
    let output = format!("{}.0", "ab".repeat(32));
    serde_json::from_value(json!({ "basket": basket, "output": output })).unwrap()
}

/// A zero-satoshi `OP_FALSE OP_RETURN` output into `basket`, labelled
/// `label`, not sent: no spending check, the basket's and the label's alone.
fn creating(basket: &str, label: &str) -> CreateActionArgs {
    serde_json::from_value(json!({
        "description": "a module witness", "labels": [label],
        "outputs": [{ "lockingScript": "006a", "satoshis": 0,
            "outputDescription": "a module witness", "basket": basket }],
        "options": { "noSend": true },
    }))
    .unwrap()
}

fn listing_actions(label: &str) -> ListActionsArgs {
    serde_json::from_value(json!({ "labels": [label] })).unwrap()
}

/// A basket insertion of an `OP_DROP OP_1` output into `basket`, its
/// parent proven past Genesis.
fn internalizing(wallet: &TestWallet, basket: &str, label: &str) -> InternalizeActionArgs {
    let tx = atomic_beef_paying_at("7551", above_genesis(wallet, 1));
    serde_json::from_value(json!({
        "tx": tx, "description": "a module witness", "labels": [label],
        "outputs": [{ "outputIndex": 0, "protocol": "basket insertion",
            "insertionRemittance": { "basket": basket } }],
    }))
    .unwrap()
}

/// One of the five calls from `originator` with `basket` and `label`, its
/// answer as the module reads a result.
async fn call(
    wallet: &TestWallet,
    name: &str,
    basket: &str,
    label: &str,
    originator: &str,
) -> bsv_rs::Result<Value> {
    let o = Some(originator);
    match name {
        "list_outputs" => Ok(shown(&wallet.list_outputs(listing(basket), o).await?)),
        "relinquish_output" => Ok(shown(
            &wallet.relinquish_output(relinquishing(basket), o).await?,
        )),
        "create_action" => Ok(shown(
            &wallet.create_action(creating(basket, label), o).await?,
        )),
        "list_actions" => Ok(shown(
            &wallet.list_actions(listing_actions(label), o).await?,
        )),
        "internalize_action" => Ok(shown(
            &wallet
                .internalize_action(internalizing(wallet, basket, label), o)
                .await?,
        )),
        other => unreachable!("{other}"),
    }
}

/// A result as its JSON object.
fn shown<T: serde::Serialize>(result: &T) -> Value {
    serde_json::to_value(result).unwrap()
}

/// The five calls and their reference method names.
const CALLS: [(&str, &str); 5] = [
    ("list_outputs", "listOutputs"),
    ("relinquish_output", "relinquishOutput"),
    ("create_action", "createAction"),
    ("list_actions", "listActions"),
    ("internalize_action", "internalizeAction"),
];

/// A module wallet funded for `create_action`'s fee.
async fn funded(
    modules: Vec<(&str, Arc<Rewriter>)>,
) -> (tempfile::TempDir, TestWallet, Arc<StdMutex<Vec<String>>>) {
    let (dir, wallet, _double, prompts) = module_wallet(modules).await;
    fund_tagged(&wallet, &own_lock(&wallet), 0).await;
    (dir, wallet, prompts)
}

// =========================================================================
// A registered module is asked: the request it sees, its arguments reaching
// the underlying call, its result reaching the caller, no prompt.
// =========================================================================

#[tokio::test]
async fn registered_module_is_asked_on_each_call() {
    for (name, method) in CALLS {
        let module = Rewriter::quiet();
        let (_dir, wallet, prompts) = funded(vec![("btms", module.clone())]).await;
        call(&wallet, name, "p btms coins", "p btms token", ORIGINATOR)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let asked = module.asked();
        // One scheme on a basket and a label is one module, asked once.
        let [(asked_method, args, originator)] = asked.as_slice() else {
            panic!("{name}: {asked:?}");
        };
        assert_eq!(
            (asked_method.as_str(), originator.as_str()),
            (method, ORIGINATOR)
        );
        let named = match name {
            "list_outputs" | "relinquish_output" => &args["basket"],
            "create_action" => &args["outputs"][0]["basket"],
            "internalize_action" => &args["outputs"][0]["insertionRemittance"]["basket"],
            _ => &args["labels"][0],
        };
        let expected = if name == "list_actions" {
            "p btms token"
        } else {
            "p btms coins"
        };
        assert_eq!(named, &json!(expected), "{name}");
        assert!(prompts.lock().unwrap().is_empty(), "{name}");
    }
}

#[tokio::test]
async fn module_arguments_reach_the_underlying_call() {
    for (name, _) in CALLS {
        let (broken, expected) = match name {
            "list_outputs" | "list_actions" => (json!({ "limit": 10_001 }), LIMIT_TEXT),
            "relinquish_output" => continue,
            _ => (json!({ "description": "x" }), DESCRIPTION_TEXT),
        };
        let module = Rewriter::new(broken, json!({}));
        let (_dir, wallet, _prompts) = funded(vec![("btms", module.clone())]).await;
        let err = call(&wallet, name, "p btms coins", "p btms token", ORIGINATOR)
            .await
            .expect_err(name);
        assert_eq!(err.to_string(), expected, "{name}");
    }
}

/// The relinquish answers `true` for an outpoint the store does not hold,
/// so its module names the funded coin: the coin leaves its basket.
#[tokio::test]
async fn module_arguments_relinquish_the_coin_they_name() {
    let module = Rewriter::quiet();
    let (_dir, wallet, _double, prompts) = module_wallet(vec![("btms", module.clone())]).await;
    let coin = fund_tagged(&wallet, &own_lock(&wallet), 0).await;
    let (txid, vout): (String, i64) =
        sqlx::query_as("SELECT txid, vout FROM outputs WHERE output_id = ?")
            .bind(coin)
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    let named = json!({ "basket": "default", "output": format!("{txid}.{vout}") });
    *module.request.lock().unwrap() = named;
    let basket = || async {
        let row: (Option<i64>,) =
            sqlx::query_as("SELECT basket_id FROM outputs WHERE output_id = ?")
                .bind(coin)
                .fetch_one(wallet.storage().pool())
                .await
                .unwrap();
        row.0
    };
    assert!(basket().await.is_some());
    let answer = call(&wallet, "relinquish_output", "p btms coins", "", ORIGINATOR).await;
    assert_eq!(answer.unwrap(), json!({ "relinquished": true }));
    assert_eq!(basket().await, None);
    assert!(prompts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn module_result_reaches_the_caller() {
    for (name, method) in CALLS {
        let (key, value) = match name {
            "list_outputs" => ("totalOutputs", json!(7)),
            "relinquish_output" => ("relinquished", json!(false)),
            "create_action" => ("txid", json!("cd".repeat(32))),
            "list_actions" => ("totalActions", json!(7)),
            _ => ("accepted", json!(false)),
        };
        let module = Rewriter::new(json!({}), json!({ key: value }));
        let (_dir, wallet, prompts) = funded(vec![("btms", module.clone())]).await;
        let answer = call(&wallet, name, "p btms coins", "p btms token", ORIGINATOR)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(answer[key], value, "{name}");
        let responses = module.responses.lock().unwrap().clone();
        let [(seen_method, seen)] = responses.as_slice() else {
            panic!("{name}: {responses:?}");
        };
        assert_eq!(seen_method, method);
        assert_ne!(seen[key], value, "{name}");
        assert!(prompts.lock().unwrap().is_empty(), "{name}");
    }
}

#[tokio::test]
async fn chain_runs_requests_in_order_and_responses_in_reverse() {
    let basket = Rewriter::new(
        json!({ "description": "the basket's word" }),
        json!({ "txid": "aa".repeat(32) }),
    );
    let label = Rewriter::new(
        json!({ "description": "the label's word" }),
        json!({ "txid": "bb".repeat(32) }),
    );
    let (_dir, wallet, _prompts) =
        funded(vec![("coins", basket.clone()), ("tokens", label.clone())]).await;
    let answer = call(
        &wallet,
        "create_action",
        "p coins x",
        "p tokens y",
        ORIGINATOR,
    )
    .await
    .unwrap();
    // Requests in order, baskets then labels: the label's module saw the
    // basket's arguments, and the store the label's.
    assert_eq!(
        label.asked()[0].1["description"],
        json!("the basket's word")
    );
    let (stored,): (String,) =
        sqlx::query_as("SELECT description FROM transactions WHERE is_outgoing = 1")
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    assert_eq!(stored, "the label's word");
    // Responses in reverse: the label's module saw the wallet's txid, the
    // basket's module the label's rewrite, the caller the basket's.
    assert_ne!(
        label.responses.lock().unwrap()[0].1["txid"],
        json!("bb".repeat(32))
    );
    assert_eq!(
        basket.responses.lock().unwrap()[0].1["txid"],
        json!("bb".repeat(32))
    );
    assert_eq!(answer["txid"], json!("aa".repeat(32)));
}

#[tokio::test]
async fn admin_originator_is_delegated_too() {
    let module = Rewriter::quiet();
    let (_dir, wallet, _prompts) = funded(vec![("btms", module.clone())]).await;
    for (name, _) in CALLS {
        let _ = call(&wallet, name, "p btms coins", "p btms token", ADMIN).await;
    }
    assert_eq!(module.asked().len(), CALLS.len());
}

// =========================================================================
// The refusals: an unregistered scheme, the label grammar, `nosend`.
// =========================================================================

#[tokio::test]
async fn unregistered_scheme_is_refused_before_any_check() {
    let module = Rewriter::quiet();
    let (_dir, wallet, prompts) = funded(vec![("btms", module.clone())]).await;
    for (name, basket, label, text) in [
        (
            "list_outputs",
            "p foo x",
            "",
            "Unsupported P-module scheme: p foo",
        ),
        (
            "relinquish_output",
            "p foo x",
            "",
            "Unsupported P-module scheme: p foo",
        ),
        (
            "create_action",
            "p foo x",
            "p btms y",
            "Unsupported P-basket scheme: p foo",
        ),
        (
            "create_action",
            "p btms x",
            "p foo y",
            "Unsupported P-label scheme: p foo",
        ),
        (
            "list_actions",
            "",
            "p foo y",
            "Unsupported P-label scheme: p foo",
        ),
        (
            "internalize_action",
            "p foo x",
            "p btms y",
            "Unsupported P-basket scheme: p foo",
        ),
        (
            "internalize_action",
            "p btms x",
            "p foo y",
            "Unsupported P-label scheme: p foo",
        ),
    ] {
        let answer = call(&wallet, name, basket, label, ORIGINATOR).await;
        assert_module_refusal(answer, text, name);
    }
    assert!(module.asked().is_empty());
    assert!(prompts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn label_outside_the_grammar_is_refused() {
    let module = Rewriter::quiet();
    let (_dir, wallet, _prompts) = funded(vec![("btms", module.clone())]).await;
    for name in ["create_action", "list_actions", "internalize_action"] {
        let answer = call(&wallet, name, "p btms x", "p btms", ORIGINATOR).await;
        assert_module_refusal(answer, "Invalid P-label format: p btms", name);
    }
    assert!(module.asked().is_empty());
}

#[tokio::test]
async fn host_nosend_module_is_never_asked() {
    let host = Rewriter::quiet();
    let (_dir, wallet, _prompts) = funded(vec![("nosend", host.clone())]).await;
    for name in [
        "list_outputs",
        "relinquish_output",
        "create_action",
        "internalize_action",
    ] {
        let answer = call(&wallet, name, "p nosend x", "plain", ORIGINATOR).await;
        assert_module_refusal(answer, NOSEND_TEXT, name);
    }
    assert!(host.asked().is_empty());
}

/// The reserved scheme on the three chained calls: one class and one text,
/// a label's (`list_actions` takes no basket) and a basket's, the BRC-177
/// expiry label included (the keeper's 2026-11-04 reading).
#[tokio::test]
async fn nosend_is_refused_alike_on_every_chained_call() {
    let module = Rewriter::quiet();
    let (_dir, wallet, prompts) = funded(vec![("btms", module.clone())]).await;
    for name in ["create_action", "list_actions", "internalize_action"] {
        for label in ["p nosend x", "p nosend expiry seconds 300"] {
            let answer = call(&wallet, name, "p btms coins", label, ORIGINATOR).await;
            assert_module_refusal(answer, NOSEND_TEXT, &format!("{name} {label}"));
        }
    }
    for name in ["create_action", "internalize_action"] {
        let answer = call(&wallet, name, "p nosend x", "p btms token", ORIGINATOR).await;
        assert_module_refusal(answer, NOSEND_TEXT, name);
    }
    assert!(module.asked().is_empty());
    assert!(prompts.lock().unwrap().is_empty());
}

/// A BRC-177 label the chain does not read as `p nosend`, which the door
/// normalizes to one, still meets the door's capability refusal, ahead of
/// the chain.
#[tokio::test]
async fn brc177_label_outside_the_chain_meets_the_door() {
    let module = Rewriter::quiet();
    let (_dir, wallet, _prompts) = funded(vec![("btms", module.clone())]).await;
    let label = "P NoSend expiry seconds 300";
    let err = call(&wallet, "create_action", "p btms coins", label, ORIGINATOR)
        .await
        .expect_err("refused");
    let class = bsv_rs::WalletRefusalClass::InvalidOperation;
    assert!(
        matches!(&err, bsv_rs::Error::WalletRefusal { class: c, .. } if *c == class),
        "{err:?}"
    );
    let text = "Invalid operation: Active storage does not support BRC-177 noSend expiry";
    assert_eq!(err.to_string(), text);
    assert!(module.asked().is_empty());
}

// =========================================================================
// Any other name takes the ordinary check, as before.
// =========================================================================

#[tokio::test]
async fn other_names_take_the_ordinary_check() {
    let module = Rewriter::quiet();
    let (_dir, wallet, prompts) = funded(vec![("btms", module.clone())]).await;
    for basket in ["btms coins", "P btms coins", "pbtms coins"] {
        for name in ["list_outputs", "relinquish_output", "create_action"] {
            let err = call(&wallet, name, basket, "plain", ORIGINATOR)
                .await
                .expect_err(name);
            let unauthorized = bsv_rs::WalletRefusalClass::Unauthorized;
            assert!(
                matches!(&err, bsv_rs::Error::WalletRefusal { class, .. } if *class == unauthorized),
                "{basket} {name}: {err:?}"
            );
        }
    }
    assert_eq!(prompts.lock().unwrap().len(), 9);
    // The port checks no label: these list, and the insertion stores.
    call(&wallet, "list_actions", "", "btms token", ORIGINATOR)
        .await
        .unwrap();
    call(
        &wallet,
        "internalize_action",
        "btms coins",
        "btms token",
        ORIGINATOR,
    )
    .await
    .unwrap();
    assert!(module.asked().is_empty());
}

// =========================================================================
// The spending check judges the action the modules answered (`:4516`).
// =========================================================================

/// A module answering `satoshis` on the one output of the action.
fn spending(satoshis: u64) -> Arc<Rewriter> {
    let output = json!({ "lockingScript": "006a", "satoshis": satoshis,
        "outputDescription": "a module witness", "basket": "p btms coins" });
    Rewriter::new(json!({ "outputs": [output] }), json!({}))
}

/// A wallet whose module is `module`, its user granted a spend of 600.
async fn granted(
    module: Arc<Rewriter>,
) -> (tempfile::TempDir, TestWallet, Arc<StdMutex<Vec<String>>>) {
    let (dir, wallet, prompts) = funded(vec![("btms", module)]).await;
    // `build_cache_key`'s spending key, `spend:<originator>:<satoshis>`.
    let key = format!("spend:{ORIGINATOR}:600");
    let manager = wallet.permissions_manager().unwrap();
    manager.cache_permission(key, 0).await;
    (dir, wallet, prompts)
}

#[tokio::test]
async fn module_cannot_lift_the_spending_ceiling() {
    // The refusal a caller asking 1000 past the grant is given.
    let (_dir, wallet, _prompts) = granted(Rewriter::quiet()).await;
    let mut direct = creating("p btms coins", "plain");
    direct.outputs.as_mut().unwrap()[0].satoshis = 1000;
    let expected = wallet
        .create_action(direct, Some(ORIGINATOR))
        .await
        .expect_err("direct");
    assert!(matches!(expected, bsv_rs::Error::WalletRefusal { .. }));
    // A module raising the zero-satoshi output to 1000 meets it too.
    let (_dir, wallet, prompts) = granted(spending(1000)).await;
    let answer = call(
        &wallet,
        "create_action",
        "p btms coins",
        "plain",
        ORIGINATOR,
    )
    .await;
    let err = answer.expect_err("past the ceiling");
    assert_eq!(format!("{err:?}"), format!("{expected:?}"));
    assert_eq!(err.to_string(), expected.to_string());
    assert_eq!(prompts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn module_answering_within_the_ceiling_is_admitted() {
    let (_dir, wallet, prompts) = granted(spending(600)).await;
    call(
        &wallet,
        "create_action",
        "p btms coins",
        "plain",
        ORIGINATOR,
    )
    .await
    .unwrap();
    let (satoshis,): (i64,) =
        sqlx::query_as("SELECT satoshis FROM outputs WHERE locking_script = X'006a'")
            .fetch_one(wallet.storage().pool())
            .await
            .unwrap();
    assert_eq!(satoshis, 600);
    assert!(prompts.lock().unwrap().is_empty());
}
