use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::transaction::{Beef, MerklePath, Transaction};
use serde_json::{json, Value};

use super::seed;

#[derive(Default)]
pub(super) struct Normalizer {
    ids: BTreeMap<String, Vec<i64>>,
    tokens: BTreeMap<String, Vec<String>>,
    scripts: BTreeMap<String, String>,
    public_keys: BTreeMap<String, String>,
    offsets: BTreeMap<(String, u64), i64>,
    beef_sizes: BTreeMap<usize, usize>,
}

impl Normalizer {
    pub(super) fn prepare(&mut self, rows: &Value, root: &PrivateKey) -> Result<()> {
        for records in rows.as_object().context("row tables")?.values() {
            for record in records.as_array().context("table rows")? {
                for (key, value) in record.as_object().context("row")? {
                    if let (Some(kind), Some(id)) = (id_kind(key), value.as_i64()) {
                        let ids = self.ids.entry(kind.to_string()).or_default();
                        if !ids.contains(&id) {
                            ids.push(id);
                            ids.sort_unstable();
                        }
                    }
                }
                if let Some(reference) = record["reference"].as_str() {
                    self.token("reference", reference);
                }
            }
        }
        for output in rows["outputs"].as_array().context("outputs")? {
            if let (Some(prefix), Some(suffix)) = (
                output["derivation_prefix"].as_str(),
                output["derivation_suffix"].as_str(),
            ) {
                if prefix != seed::PREFIX || suffix != seed::SUFFIX {
                    let (script, public) = seed::derived_lock(
                        root,
                        prefix,
                        suffix,
                        output["sender_identity_key"].as_str(),
                    )?;
                    let next = self.scripts.len() + 1;
                    self.scripts
                        .entry(hex::encode(script))
                        .or_insert_with(|| format!("<change-script:{next}>"));
                    self.public_keys
                        .entry(hex::encode(public))
                        .or_insert_with(|| format!("<change-key:{next}>"));
                }
            }
        }
        for table in ["transactions", "proven_txs", "proven_tx_reqs"] {
            for record in rows[table].as_array().context("transaction table")? {
                if let (Some(txid), Some(raw)) =
                    (record["txid"].as_str(), record["raw_tx"].as_str())
                {
                    if !raw.is_empty() {
                        for (vout, offset) in
                            output_offsets(&hex::decode(raw)?)?.into_iter().enumerate()
                        {
                            self.offsets.insert((txid.to_string(), vout as u64), offset);
                        }
                    }
                }
                if let (Some(raw), Some(input)) =
                    (record["raw_tx"].as_str(), record["input_beef"].as_str())
                {
                    let raw = hex::decode(raw)?;
                    let delta = signature_size_delta(&raw)?;
                    let mut beef = Beef::from_binary(&hex::decode(input)?)?;
                    beef.merge_raw_tx(raw, None);
                    let length = beef.to_binary().len();
                    self.beef_sizes.insert(
                        length,
                        length
                            .checked_add_signed(delta)
                            .context("canonical BEEF size")?,
                    );
                    if beef.txs.iter().all(|tx| !tx.is_txid_only()) {
                        beef.version = bsv_rs::transaction::BEEF_V1;
                        let length = beef.to_binary().len();
                        self.beef_sizes.insert(
                            length,
                            length
                                .checked_add_signed(delta)
                                .context("canonical V1 BEEF size")?,
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn value(&mut self, value: Value) -> Result<Value> {
        self.walk("", value)
    }

    fn token(&mut self, kind: &str, value: &str) -> String {
        let values = self.tokens.entry(kind.to_string()).or_default();
        let index = match values.iter().position(|v| v == value) {
            Some(index) => index,
            None => {
                values.push(value.to_string());
                values.len() - 1
            }
        };
        format!("<{kind}:{}>", index + 1)
    }

    fn walk(&mut self, key: &str, value: Value) -> Result<Value> {
        let key = snake(key);
        if !value.is_null() {
            match key.as_str() {
                "tx" => {
                    let raw = bytes(value)?;
                    let decoded = if Beef::from_binary(&raw).is_ok() {
                        beef(&raw)?
                    } else {
                        transaction(&raw)?
                    };
                    return self.walk("decoded", decoded);
                }
                "txid" if value.is_array() => {
                    let raw = bytes(value)?;
                    anyhow::ensure!(raw.len() == 32, "transaction ID byte length");
                    return self.string("txid", hex::encode(raw));
                }
                "raw_tx" | "source_transaction" => {
                    return self.walk("decoded", transaction(&bytes(value)?)?);
                }
                "input_beef" | "beef" => return self.walk("decoded", beef(&bytes(value)?)?),
                "merkle_path" => {
                    return self.walk("decoded", merkle(&MerklePath::from_binary(&bytes(value)?)?))
                }
                "unlocking_script" => return Ok(self.unlocking(&bytes(value)?)),
                _ => {}
            }
        }
        Ok(match value {
            Value::Object(mut fields) => {
                // The verifier's fields are evidence, including the exact txid.
                if fields.contains_key("spendRefused") {
                    return Ok(Value::Object(fields));
                }
                if let (Some(txid), Some(vout), Some(offset)) = (
                    fields.get("txid").and_then(Value::as_str),
                    fields.get("vout").and_then(Value::as_u64),
                    fields.get("script_offset").and_then(Value::as_i64),
                ) {
                    if self.offsets.get(&(txid.to_string(), vout)) == Some(&offset) {
                        fields.insert(
                            "script_offset".into(),
                            json!(format!("<output-offset:{vout}>")),
                        );
                    }
                }
                let mut result = serde_json::Map::new();
                for (field, value) in fields {
                    if !timestamp(&field) {
                        result.insert(field.clone(), self.walk(&field, value)?);
                    }
                }
                Value::Object(result)
            }
            Value::Array(values) => Value::Array(
                values
                    .into_iter()
                    .map(|v| self.walk(&key, v))
                    .collect::<Result<_>>()?,
            ),
            Value::Number(number) => {
                if let (Some(kind), Some(id)) = (id_kind(&key), number.as_i64()) {
                    let ids = self.ids.entry(kind.to_string()).or_default();
                    let index = match ids.iter().position(|v| *v == id) {
                        Some(index) => index,
                        None => {
                            ids.push(id);
                            ids.len() - 1
                        }
                    };
                    json!(index + 1)
                } else {
                    Value::Number(number)
                }
            }
            Value::String(value) => self.string(&key, value)?,
            other => other,
        })
    }

    fn string(&mut self, key: &str, value: String) -> Result<Value> {
        if value.is_empty() || value.starts_with('<') {
            return Ok(json!(value));
        }
        if matches!(
            key,
            "custom_instructions"
                | "description"
                | "output_description"
                | "spending_description"
                | "label"
                | "labels"
                | "tag"
                | "tags"
        ) {
            return Ok(json!(value));
        }
        if let Some(length) = value
            .strip_prefix("beef_len=")
            .and_then(|n| n.parse::<usize>().ok())
        {
            if let Some(canonical) = self.beef_sizes.get(&length) {
                return Ok(json!(format!("beef_len={canonical}")));
            }
        }
        if matches!(key, "history" | "notify" | "sync_map" | "text_value") {
            if let Ok(parsed) = serde_json::from_str::<Value>(&value) {
                return self.walk("decoded", parsed);
            }
        }
        if key == "reference" {
            // The result's reference is the storage's twelve bytes (#285): the
            // token is their base64 text, the same value the stored row carries.
            let decoded = hex::decode(&value)
                .ok()
                .map(|raw| bsv_rs::primitives::to_base64(&raw));
            return Ok(json!(
                self.token("reference", decoded.as_deref().unwrap_or(&value))
            ));
        }
        if matches!(key, "batch" | "ref_num") {
            return Ok(json!(self.token(
                if key == "reference" { "reference" } else { key },
                &value
            )));
        }
        if matches!(key, "derivation_prefix" | "derivation_suffix")
            && value != seed::PREFIX
            && value != seed::SUFFIX
        {
            return Ok(json!(self.token("derivation", &value)));
        }
        if matches!(key, "locking_script" | "source_locking_script") {
            return Ok(json!(self.scripts.get(&value).cloned().unwrap_or(value)));
        }
        if is_hash(&value) {
            return Ok(json!(self.token("txid", &value)));
        }
        // Outpoints and error/call strings can contain hashes and references.
        let mut normalized = value.clone();
        for word in value
            .split(|c: char| !c.is_ascii_hexdigit())
            .filter(|s| is_hash(s))
        {
            normalized = normalized.replace(word, &self.token("txid", word));
        }
        if let Some(references) = self.tokens.get("reference") {
            for (i, reference) in references.iter().enumerate() {
                normalized = normalized.replace(reference, &format!("<reference:{}>", i + 1));
            }
        }
        Ok(json!(normalized))
    }

    fn unlocking(&mut self, raw: &[u8]) -> Value {
        // Preserve arbitrary caller scripts. Only a DER-signature/P2PKH pair
        // has volatile bytes; keep the sighash and its actual public key.
        if let [len, ..] = raw {
            let len = usize::from(*len);
            if len > 8
                && raw.len() == len + 35
                && raw[1] == 0x30
                && usize::from(raw[2]) + 3 == len
                && raw[len + 1] == 33
            {
                let signature = self.token("signature", &hex::encode(&raw[1..len]));
                let public = hex::encode(&raw[len + 2..]);
                return json!({"signature": signature, "sighash": raw[len],
                    "publicKey": self.public_keys.get(&public).cloned().unwrap_or(public)});
            }
        }
        json!(hex::encode(raw))
    }
}

fn bytes(value: Value) -> Result<Vec<u8>> {
    match value {
        Value::String(s) => Ok(hex::decode(s)?),
        Value::Array(_) => Ok(serde_json::from_value(value)?),
        _ => anyhow::bail!("expected hex or bytes"),
    }
}

fn transaction(raw: &[u8]) -> Result<Value> {
    let tx = Transaction::from_binary(raw)?;
    anyhow::ensure!(tx.to_binary() == raw, "raw transaction does not round-trip");
    let inputs: Vec<_> = tx.inputs.iter().map(|input| json!({
        "sourceTxid": input.source_txid, "vout": input.source_output_index,
        "sequence": input.sequence,
        "unlockingScript": input.unlocking_script.as_ref().map(|s| hex::encode(s.to_binary())).unwrap_or_default(),
    })).collect();
    let outputs: Vec<_> = tx.outputs.iter().map(|output| json!({
        "satoshis": output.satoshis, "lockingScript": hex::encode(output.locking_script.to_binary()),
    })).collect();
    Ok(
        json!({"txid": tx.id(), "version": tx.version, "lockTime": tx.lock_time, "inputs": inputs, "outputs": outputs}),
    )
}

fn beef(raw: &[u8]) -> Result<Value> {
    let beef = Beef::from_binary(raw)?;
    let txs = beef
        .txs
        .iter()
        .map(|tx| {
            let body = if let Some(body) = tx.tx() {
                Some(transaction(&body.to_binary())?)
            } else {
                tx.raw_tx().map(transaction).transpose()?
            };
            Ok(json!({"txid": tx.txid(), "bumpIndex": tx.bump_index(), "transaction": body}))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"version": beef.version, "atomicTxid": beef.atomic_txid,
        "bumps": beef.bumps.iter().map(merkle).collect::<Vec<_>>(), "transactions": txs}),
    )
}

fn merkle(path: &MerklePath) -> Value {
    let levels: Vec<Vec<_>> = path
        .path
        .iter()
        .map(|level| {
            level.iter().map(|leaf| json!({
        "offset": leaf.offset, "hash": leaf.hash, "txid": leaf.txid, "duplicate": leaf.duplicate,
    })).collect()
        })
        .collect();
    json!({"blockHeight": path.block_height, "path": levels})
}

fn snake(s: &str) -> String {
    s.chars()
        .flat_map(|c| {
            if c.is_ascii_uppercase() {
                vec!['_', c.to_ascii_lowercase()]
            } else {
                vec![c]
            }
        })
        .collect()
}

fn is_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn id_kind(key: &str) -> Option<&'static str> {
    match key {
        "user_id" => Some("user"),
        "basket_id" => Some("basket"),
        "transaction_id" | "spent_by" | "transaction_ids" => Some("transaction"),
        "output_id" => Some("output"),
        "proven_tx_id" => Some("proven_tx"),
        "proven_tx_req_id" => Some("proven_tx_req"),
        "tx_label_id" => Some("tx_label"),
        "tx_label_map_id" => Some("tx_label_map"),
        "output_tag_id" => Some("output_tag"),
        "output_tag_map_id" => Some("output_tag_map"),
        "commission_id" => Some("commission"),
        "sync_state_id" => Some("sync_state"),
        _ => None,
    }
}

fn timestamp(key: &str) -> bool {
    matches!(
        snake(key).as_str(),
        "created_at"
            | "updated_at"
            | "seen_at"
            | "checked_at"
            | "when"
            | "timestamp"
            | "last_checked_at"
            | "next_check_at"
            | "when_last_sync_started"
    )
}

fn output_offsets(raw: &[u8]) -> Result<Vec<i64>> {
    fn varint(raw: &[u8], pos: &mut usize) -> Result<usize> {
        let byte = *raw.get(*pos).context("varint")?;
        *pos += 1;
        let count = match byte {
            0xfd => 2,
            0xfe => 4,
            0xff => 8,
            _ => return Ok(byte as usize),
        };
        let mut bytes = [0; 8];
        bytes[..count].copy_from_slice(raw.get(*pos..*pos + count).context("varint bytes")?);
        *pos += count;
        Ok(usize::try_from(u64::from_le_bytes(bytes))?)
    }
    let mut pos = 4;
    for _ in 0..varint(raw, &mut pos)? {
        pos += 36;
        let length = varint(raw, &mut pos)?;
        pos += length + 4;
    }
    let mut offsets = Vec::new();
    for _ in 0..varint(raw, &mut pos)? {
        pos += 8;
        let length = varint(raw, &mut pos)?;
        offsets.push(pos as i64);
        pos += length;
    }
    Ok(offsets)
}

fn signature_size_delta(raw: &[u8]) -> Result<isize> {
    let tx = Transaction::from_binary(raw)?;
    Ok(tx
        .inputs
        .iter()
        .filter_map(|input| input.unlocking_script.as_ref())
        .map(|script| {
            let raw = script.to_binary();
            let len = match raw.as_slice() {
                [len, ..] => usize::from(*len),
                [] => 0,
            };
            if len > 8
                && raw.len() == len + 35
                && raw[1] == 0x30
                && usize::from(raw[2]) + 3 == len
                && raw[len + 1] == 33
            {
                107 - raw.len() as isize
            } else {
                0
            }
        })
        .sum())
}

#[test]
fn a_spend_refusal_keeps_every_verifier_field_verbatim() -> Result<()> {
    let refusal = json!({
        "error": format!("spend refused at offset 156, input 0 of {}: The top stack element must be truthy after script evaluation.", "ab".repeat(32)),
        "spendRefused": { "offset": 156, "input": 0, "txid": "ab".repeat(32) }
    });
    assert_eq!(Normalizer::default().value(refusal.clone())?, refusal);
    Ok(())
}

#[test]
fn normalization_preserves_wrong_offsets_and_arbitrary_scripts() -> Result<()> {
    let mut normalizer = Normalizer::default();
    normalizer.offsets.insert(("ab".repeat(32), 2), 100);
    let correct =
        normalizer.value(json!({"txid": "ab".repeat(32), "vout": 2, "script_offset": 100}))?;
    let corrupt =
        normalizer.value(json!({"txid": "ab".repeat(32), "vout": 2, "script_offset": 101}))?;
    assert_eq!(correct["script_offset"], "<output-offset:2>");
    assert_eq!(corrupt["script_offset"], 101);
    assert_eq!(
        normalizer.value(json!({"unlockingScript": "5100"}))?["unlockingScript"],
        "5100"
    );
    assert_ne!(
        normalizer.value(json!({"lockingScript": "51"}))?,
        normalizer.value(json!({"lockingScript": "52"}))?
    );
    let instructions = format!("{{\"txid\":\"{}\",\"when\":7}}", "ab".repeat(32));
    assert_eq!(
        normalizer.value(json!({"customInstructions": instructions}))?["customInstructions"],
        instructions
    );
    Ok(())
}
