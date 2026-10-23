use std::time::Duration;

use anyhow::{Context, Result};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{AbortActionArgs, CreateActionArgs, ProtoWallet};
use bsv_wallet_toolbox_rs::storage::{
    CreateActionDoor, MonitorStorage, SignAndProcess, StorageCreateActionResult,
    StorageProcessActionArgs, WalletStorageReader, WalletStorageWriter,
};
use bsv_wallet_toolbox_rs::{services::WalletServices, SignerInput, WalletSigner};
use serde::Serialize;
use serde_json::{json, Value};

use super::{model::Call, seed, Replay};

fn recorded<T: Serialize>(result: bsv_wallet_toolbox_rs::Result<T>) -> Result<Value> {
    match result {
        Ok(value) => Ok(json!({ "ok": serde_json::to_value(value)? })),
        Err(error) => Ok(json!({ "error": error.to_string() })),
    }
}

fn flags(no_send: bool) -> StorageProcessActionArgs {
    StorageProcessActionArgs {
        is_new_tx: true,
        is_send_with: false,
        is_no_send: no_send,
        is_delayed: false,
        reference: None,
        txid: None,
        raw_tx: None,
        send_with: vec![],
    }
}

// The callback uses the public signer and deterministic test keys. It does no I/O.
fn sign(
    root: &PrivateKey,
    created: &StorageCreateActionResult,
    args: &CreateActionArgs,
) -> Result<Vec<u8>> {
    let caller = args.inputs.as_deref().unwrap_or_default();
    let inputs = created
        .inputs
        .iter()
        .map(|input| {
            let provided = caller.iter().find(|i| {
                i.outpoint.to_string() == format!("{}.{}", input.source_txid, input.source_vout)
            });
            Ok(TransactionInput {
                source_transaction: None,
                source_txid: Some(input.source_txid.clone()),
                source_output_index: input.source_vout,
                unlocking_script: Some(UnlockingScript::from_binary(&[])?),
                unlocking_script_template: None,
                sequence: provided.and_then(|i| i.sequence_number).unwrap_or(u32::MAX),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let outputs = created
        .outputs
        .iter()
        .map(|output| {
            let script = if output.locking_script.is_empty() {
                seed::derived_lock(
                    root,
                    &created.derivation_prefix,
                    output
                        .derivation_suffix
                        .as_deref()
                        .context("change suffix")?,
                    None,
                )?
                .0
            } else {
                hex::decode(&output.locking_script)?
            };
            Ok(TransactionOutput {
                satoshis: Some(output.satoshis),
                locking_script: LockingScript::from_binary(&script)?,
                change: false,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let tx = Transaction::with_params(created.version, inputs, outputs, created.lock_time);
    let signer_inputs = created
        .inputs
        .iter()
        .map(|input| {
            Ok(SignerInput {
                vin: input.vin,
                source_txid: input.source_txid.clone(),
                source_vout: input.source_vout,
                satoshis: input.source_satoshis,
                source_locking_script: Some(hex::decode(&input.source_locking_script)?),
                unlocking_script: caller
                    .iter()
                    .find(|i| {
                        i.outpoint.to_string()
                            == format!("{}.{}", input.source_txid, input.source_vout)
                    })
                    .and_then(|i| i.unlocking_script.clone()),
                derivation_prefix: input.derivation_prefix.clone(),
                derivation_suffix: input.derivation_suffix.clone(),
                sender_identity_key: input.sender_identity_key.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(WalletSigner::new(Some(root.clone())).sign_transaction(
        &tx.to_binary(),
        &signer_inputs,
        &ProtoWallet::new(Some(root.clone())),
    )?)
}

impl Replay {
    pub(super) async fn execute(&mut self, call: &Call) -> Result<Value> {
        match call {
            Call::CreateAction {
                name,
                args,
                sign: signed,
            } => {
                let args: CreateActionArgs = serde_json::from_value(self.resolve(args.clone())?)?;
                if *signed {
                    let signer = |created: &StorageCreateActionResult| {
                        sign(&self.root, created, &args)
                            .map_err(|e| bsv_wallet_toolbox_rs::Error::Internal(e.to_string()))
                    };
                    let no_send = args
                        .options
                        .as_ref()
                        .and_then(|o| o.no_send)
                        .unwrap_or(false);
                    let result = self
                        .storage
                        .create_action_at_door(
                            &self.auth,
                            args.clone(),
                            CreateActionDoor {
                                offer: None,
                                sign_and_process: Some(SignAndProcess {
                                    sign: &signer,
                                    process: flags(no_send),
                                }),
                            },
                        )
                        .await
                        .map_err(bsv_wallet_toolbox_rs::storage::DoorRefusal::into_error);
                    match result {
                        Ok(done) => {
                            self.drafts
                                .insert(name.clone(), (done.created.clone(), args));
                            let processed = done.processed.map(|p| {
                                json!({
                                    "txid": p.txid, "rawTx": p.raw_tx, "results": p.results
                                })
                            });
                            Ok(json!({"ok": {"created": done.created, "processed": processed}}))
                        }
                        Err(e) => recorded::<Value>(Err(e)),
                    }
                } else {
                    let result = self.storage.create_action(&self.auth, args.clone()).await;
                    if let Ok(created) = &result {
                        self.drafts.insert(name.clone(), (created.clone(), args));
                    }
                    recorded(result)
                }
            }
            Call::CreateActionOffering { name, args, offer } => {
                let args: CreateActionArgs = serde_json::from_value(self.resolve(args.clone())?)?;
                let mut ids = Vec::new();
                for outpoint in offer {
                    let (seed, vout) = outpoint.rsplit_once('.').context("offer name.vout")?;
                    let txid = self.seeds.get(seed).context("offer seed")?.tx.id();
                    let id: i64 = sqlx::query_scalar(
                        "SELECT output_id FROM outputs WHERE txid = ? AND vout = ?",
                    )
                    .bind(txid)
                    .bind(vout.parse::<i64>()?)
                    .fetch_one(self.storage.pool())
                    .await?;
                    ids.push(id);
                }
                let result = self
                    .storage
                    .create_action_offering(&self.auth, args.clone(), &ids)
                    .await;
                if let Ok(created) = &result {
                    self.drafts.insert(name.clone(), (created.clone(), args));
                }
                recorded(result)
            }
            Call::ProcessAction { from, no_send } => {
                let (created, args) = self.drafts.get(from).context("process draft")?;
                let raw = sign(&self.root, created, args)?;
                recorded(
                    self.storage
                        .process_action(
                            &self.auth,
                            StorageProcessActionArgs {
                                reference: Some(created.reference.clone()),
                                txid: Some(Transaction::from_binary(&raw)?.id()),
                                raw_tx: Some(raw),
                                ..flags(*no_send)
                            },
                        )
                        .await,
                )
            }
            Call::AbortAction { from } => {
                let reference = self
                    .drafts
                    .get(from)
                    .context("abort draft")?
                    .0
                    .reference
                    .clone();
                recorded(
                    self.storage
                        .abort_action(&self.auth, AbortActionArgs { reference })
                        .await,
                )
            }
            Call::InternalizeAction { from, args } => {
                let mut args = self.resolve(args.clone())?;
                args["tx"] = json!(hex::encode(self.beef(from)?));
                recorded(
                    self.storage
                        .internalize_action(&self.auth, serde_json::from_value(args)?)
                        .await,
                )
            }
            Call::ListOutputs { args } => recorded(
                self.storage
                    .list_outputs(&self.auth, serde_json::from_value(args.clone())?)
                    .await,
            ),
            Call::ListActions { args } => recorded(
                self.storage
                    .list_actions(&self.auth, serde_json::from_value(args.clone())?)
                    .await,
            ),
            Call::GetUtxoLeads { script, format } => {
                use bsv_wallet_toolbox_rs::services::GetUtxoStatusOutputFormat;
                let format = match format.as_deref() {
                    None => None,
                    Some("script") => Some(GetUtxoStatusOutputFormat::Script),
                    Some("hash_le") => Some(GetUtxoStatusOutputFormat::HashLE),
                    Some("hash_be") => Some(GetUtxoStatusOutputFormat::HashBE),
                    Some(other) => anyhow::bail!("unknown script format {other}"),
                };
                recorded(self.services.get_utxo_leads(script, format).await)
            }
            Call::SendWaiting => {
                recorded(self.storage.send_waiting_transactions(Duration::ZERO).await)
            }
            Call::AgeRequests { seconds } => {
                anyhow::ensure!(*seconds > 0, "request age must be positive");
                let age = chrono::Utc::now() - chrono::Duration::seconds(*seconds);
                let result =
                    sqlx::query("UPDATE proven_tx_reqs SET created_at = ?, updated_at = ?")
                        .bind(age)
                        .bind(age)
                        .execute(self.storage.writer())
                        .await?;
                Ok(json!({"ok": {"agedRequests": result.rows_affected()}}))
            }
        }
    }
}
