//! Storage-only wire names. Local entities and embedded bsv-rs types keep
//! their own serde contracts. See docs/STORAGE-WIRE.md at the repository root.

use serde::de::DeserializeOwned;
use serde_json::Value;

const ENTITY_GROUPS: &[&str] = &[
    "provenTxs",
    "provenTxReqs",
    "outputBaskets",
    "txLabels",
    "outputTags",
    "transactions",
    "txLabelMaps",
    "commissions",
    "outputs",
    "outputTagMaps",
    "certificates",
    "certificateFields",
];

fn rename(value: &mut Value, old: &str, new: &str) {
    if let Some(object) = value.as_object_mut() {
        if let Some(field) = object.remove(old) {
            object.insert(new.to_owned(), field);
        }
    }
}

fn alias(value: &mut Value, reference: &str, local: &str) {
    if value.get(local).is_none() {
        rename(value, reference, local);
    }
}

fn transaction_status(value: &mut Value) {
    match value.as_str() {
        Some("noSend") => *value = Value::String("nosend".into()),
        Some("nonFinal") => *value = Value::String("nonfinal".into()),
        _ => {}
    }
}

fn entity_to_wire(value: &mut Value, kind: &str) {
    rename(value, "createdAt", "created_at");
    rename(value, "updatedAt", "updated_at");
    let fields: &[(&str, &str)] = match kind {
        "certificates" => &[("certType", "type")],
        "transactions" | "provenTxReqs" => &[("inputBeef", "inputBEEF")],
        "outputs" => &[("outputType", "type")],
        "outputBaskets" => &[
            ("numberOfDesiredUtxos", "numberOfDesiredUTXOs"),
            ("minimumDesiredUtxoValue", "minimumDesiredUTXOValue"),
        ],
        "txLabels" | "txLabelMaps" => &[("labelId", "txLabelId")],
        "outputTags" | "outputTagMaps" => &[("tagId", "outputTagId")],
        "commissions" => &[("payerLockingScript", "lockingScript")],
        _ => &[],
    };
    for (old, new) in fields {
        rename(value, old, new);
    }
    if kind == "transactions" {
        if let Some(status) = value.get_mut("status") {
            transaction_status(status);
        }
    }
}

fn entity_from_wire(value: &mut Value, kind: &str) {
    // Other entity aliases already live on the shared Rust types.
    match kind {
        "transactions" | "provenTxReqs" => alias(value, "inputBEEF", "inputBeef"),
        "outputs" => alias(value, "type", "outputType"),
        _ => {}
    }
}

fn each_entity(value: &mut Value, kind: &str, convert: fn(&mut Value, &str)) {
    if let Some(rows) = value.as_array_mut() {
        for row in rows {
            convert(row, kind);
        }
    }
}

fn sync_entities(chunk: &mut Value, convert: fn(&mut Value, &str)) {
    if let Some(user) = chunk.get_mut("user") {
        convert(user, "user");
    }
    for kind in ENTITY_GROUPS {
        if let Some(rows) = chunk.get_mut(kind) {
            each_entity(rows, kind, convert);
        }
    }
}

fn partial_filter(args: &mut Value, fields: &[&str]) {
    let mut partial = serde_json::Map::new();
    if let Some(object) = args.as_object_mut() {
        for field in fields {
            if let Some(value) = object.remove(*field) {
                if !value.is_null() {
                    partial.insert((*field).to_owned(), value);
                }
            }
        }
        object.insert("partial".into(), Value::Object(partial));
    }
}

pub(super) fn request_params(method: &str, params: &mut [Value]) {
    match method {
        "findCertificatesAuth" | "findOutputBaskets" => {
            if let Some(args) = params.get_mut(1) {
                let fields: &[&str] = if method == "findCertificatesAuth" {
                    &["userId"]
                } else {
                    &["userId", "name"]
                };
                partial_filter(args, fields);
            }
        }
        "findOutputsAuth" | "findTransactions" => {
            let (index, field) = if method == "findOutputsAuth" {
                (1, "txStatus")
            } else {
                (0, "status")
            };
            if let Some(statuses) = params.get_mut(index).and_then(|v| v.get_mut(field)) {
                if let Some(statuses) = statuses.as_array_mut() {
                    for status in statuses {
                        transaction_status(status);
                    }
                }
            }
        }
        "insertCertificateAuth" => {
            if let Some(certificate) = params.get_mut(1) {
                entity_to_wire(certificate, "certificates");
                if let Some(fields) = certificate.get_mut("fields") {
                    each_entity(fields, "certificateFields", entity_to_wire);
                }
            }
        }
        "insertCertificateFieldAuth" => {
            if let Some(field) = params.get_mut(1) {
                entity_to_wire(field, "certificateFields");
            }
        }
        "processSyncChunk" => {
            if let Some(chunk) = params.get_mut(1) {
                sync_entities(chunk, entity_to_wire);
            }
        }
        "updateProvenTxReqWithNewProvenTx" => {
            if let Some(proven) = params.get_mut(0).and_then(|v| v.get_mut("provenTx")) {
                entity_to_wire(proven, "provenTxs");
            }
        }
        _ => {}
    }
}

pub(super) fn result<T: DeserializeOwned>(method: &str, mut value: Value) -> serde_json::Result<T> {
    match method {
        "createAction" => {
            if let Some(inputs) = value.get_mut("inputs").and_then(Value::as_array_mut) {
                for input in inputs {
                    alias(input, "type", "inputType");
                }
            }
        }
        "findOutputsAuth" => each_entity(&mut value, "outputs", entity_from_wire),
        "findTransactions" => each_entity(&mut value, "transactions", entity_from_wire),
        "findProvenTxReqs" => each_entity(&mut value, "provenTxReqs", entity_from_wire),
        "getSyncChunk" => sync_entities(&mut value, entity_from_wire),
        "findOrInsertSyncStateAuth" => {
            if let Some(state) = value.get_mut("syncState") {
                alias(state, "when", "whenLastSyncStarted");
            }
        }
        "processSyncChunk" => alias(&mut value, "maxUpdated_at", "maxUpdatedAt"),
        _ => {}
    }
    serde_json::from_value(value)
}
