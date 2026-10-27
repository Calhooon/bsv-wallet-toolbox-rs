//! Every output reader returns the full u32 `sequence_number` the writers store (#301).
//!
//! The schema column is an INTEGER and the sync INSERT binds the u32, so a
//! stored value runs up to 0xFFFFFFFF (the reference's column is
//! `integer('sequenceNumber').unsigned()`, KnexMigrations.ts:766).

use super::sync::{entity_names, get_sync_chunk_internal};
use crate::storage::{
    AuthId, FindOutputsArgs, RequestSyncChunkArgs, StorageSqlx, SyncOffset, WalletStorageReader,
    WalletStorageWriter,
};
use crate::Result;
use bsv_rs::wallet::ListActionsArgs;

/// The values at and above 2^31 that an i32 decode refuses.
const WIDE: [u32; 2] = [u32::MAX, 1 << 31];

struct Stored {
    storage: StorageSqlx,
    auth: AuthId,
    txid: String,
}

/// A store holding one output with `sequence_number`, spent by a second action.
async fn stored(sequence_number: u32) -> Stored {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("sequence", "sequence").await.unwrap();
    storage.make_available().await.unwrap();
    let identity = "b".repeat(66);
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
    let mut ids = Vec::new();
    for (txid, reference) in [("1".repeat(64), "funding"), ("2".repeat(64), "spender")] {
        let id = sqlx::query(
            "INSERT INTO transactions (user_id, status, reference, is_outgoing, description, txid)
             VALUES (?, 'completed', ?, 1, 'sequence action', ?)",
        )
        .bind(user.user_id)
        .bind(reference)
        .bind(txid)
        .execute(storage.writer())
        .await
        .unwrap()
        .last_insert_rowid();
        ids.push(id);
    }
    let txid = "1".repeat(64);
    sqlx::query(
        "INSERT INTO outputs (user_id, transaction_id, vout, satoshis, provided_by, purpose,
                              type, spendable, txid, script_length, script_offset,
                              locking_script, spent_by, sequence_number,
                              derivation_prefix, derivation_suffix)
         VALUES (?, ?, 0, 42, 'storage', 'change', 'P2PKH', 0, ?, 1, 0, X'51', ?, ?, 'cHJlZml4', 'c3VmZml4')",
    )
    .bind(user.user_id)
    .bind(ids[0])
    .bind(&txid)
    .bind(ids[1])
    .bind(sequence_number)
    .execute(storage.writer())
    .await
    .unwrap();
    Stored {
        storage,
        auth: AuthId::with_user_id(identity, user.user_id),
        txid,
    }
}

impl Stored {
    async fn set(&self, stored: i64) {
        sqlx::query("UPDATE outputs SET sequence_number = ?")
            .bind(stored)
            .execute(self.storage.writer())
            .await
            .unwrap();
    }

    /// `find_outputs`, `find_output_by_outpoint`, the `list_actions` inputs,
    /// the sync export and the allocator, in that order.
    async fn read_all(&self) -> Vec<Result<Option<u32>>> {
        let mut reads = Vec::new();
        let found = self
            .storage
            .find_outputs(&self.auth, FindOutputsArgs::default())
            .await;
        reads.push(found.map(|outputs| outputs[0].sequence_number));
        let by_outpoint = self
            .storage
            .find_output_by_outpoint(self.auth.user_id.unwrap(), &self.txid, 0)
            .await;
        reads.push(by_outpoint.map(|output| output.unwrap().sequence_number));
        reads.push(self.list_action_input().await);
        reads.push(self.sync_export().await);
        reads.push(self.allocate().await);
        reads
    }

    async fn list_action_input(&self) -> Result<Option<u32>> {
        let result = self
            .storage
            .list_actions(
                &self.auth,
                ListActionsArgs {
                    labels: vec![],
                    label_query_mode: None,
                    include_labels: None,
                    include_inputs: Some(true),
                    include_input_source_locking_scripts: None,
                    include_input_unlocking_scripts: None,
                    include_outputs: None,
                    include_output_locking_scripts: None,
                    limit: Some(10),
                    offset: Some(0),
                    seek_permission: None,
                },
            )
            .await?;
        let inputs: Vec<u32> = result
            .actions
            .iter()
            .flat_map(|action| action.inputs.iter().flatten())
            .map(|input| input.sequence_number)
            .collect();
        assert_eq!(inputs.len(), 1);
        Ok(Some(inputs[0]))
    }

    async fn sync_export(&self) -> Result<Option<u32>> {
        let chunk = get_sync_chunk_internal(
            &self.storage,
            RequestSyncChunkArgs {
                from_storage_identity_key: "sequence".into(),
                to_storage_identity_key: "target".into(),
                identity_key: self.auth.identity_key.clone(),
                since: None,
                max_rough_size: 100_000,
                max_items: 100,
                offsets: vec![SyncOffset {
                    name: entity_names::OUTPUT.into(),
                    offset: 0,
                }],
            },
        )
        .await?;
        let outputs = chunk.outputs.unwrap();
        assert_eq!(outputs.len(), 1);
        Ok(outputs[0].sequence_number)
    }

    /// The allocator claims a free change output, so this read frees the row
    /// and restores the spent state after it.
    async fn allocate(&self) -> Result<Option<u32>> {
        let user_id = self.auth.user_id.unwrap();
        let basket = self
            .storage
            .find_or_create_default_basket(user_id)
            .await
            .unwrap();
        let spender: i64 = sqlx::query_scalar("SELECT spent_by FROM outputs")
            .fetch_one(self.storage.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE outputs SET change = 1, spendable = 1, spent_by = NULL, basket_id = ?")
            .bind(basket.basket_id)
            .execute(self.storage.writer())
            .await
            .unwrap();
        let allocated = self
            .storage
            .allocate_change_input(user_id, basket.basket_id, 1, false, spender)
            .await;
        sqlx::query("UPDATE outputs SET spendable = 0, spent_by = ?")
            .bind(spender)
            .execute(self.storage.writer())
            .await
            .unwrap();
        allocated.map(|output| output.unwrap().sequence_number)
    }
}

#[tokio::test]
async fn every_reader_returns_a_wide_sequence_number() {
    for value in WIDE {
        let s = stored(value).await;
        for (reader, read) in s.read_all().await.into_iter().enumerate() {
            assert_eq!(read.unwrap(), Some(value), "reader {reader}, value {value}");
        }
    }
}

/// A row an i32 writer left (-1 for 0xFFFFFFFF) reads as the same u32 in
/// every reader.
#[tokio::test]
async fn every_reader_returns_a_legacy_negative_as_its_u32() {
    let s = stored(0).await;
    s.set(-1).await;
    for (reader, read) in s.read_all().await.into_iter().enumerate() {
        assert_eq!(read.unwrap(), Some(u32::MAX), "reader {reader}");
    }
}

/// A value outside both the u32 and the i32 range is a decode error, not a
/// panic; the allocator reads it leniently, as it reads every optional column.
#[tokio::test]
async fn every_reader_refuses_an_out_of_range_sequence_number() {
    let s = stored(0).await;
    for stored in [i64::from(u32::MAX) + 1, i64::from(i32::MIN) - 1] {
        s.set(stored).await;
        let reads = s.read_all().await;
        let (strict, lenient) = reads.split_at(reads.len() - 1);
        for (reader, read) in strict.iter().enumerate() {
            assert!(read.is_err(), "reader {reader}, stored {stored}: {read:?}");
        }
        assert!(matches!(lenient, [Ok(None)]), "allocator: {lenient:?}");
    }
}
