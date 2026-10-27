//! Output column witnesses through the sync exporter, JSON, and importer (#276).

use super::{entity_names, get_sync_chunk_internal, process_sync_chunk_internal};
use crate::storage::entities::TableOutput;
use crate::storage::{
    RequestSyncChunkArgs, StorageSqlx, SyncChunk, SyncMap, SyncOffset, WalletStorageWriter,
};
use chrono::{DateTime, Duration, Utc};
use sqlx::Row;

struct OutputSync {
    source: StorageSqlx,
    target: StorageSqlx,
    args: RequestSyncChunkArgs,
    expected: TableOutput,
    map: SyncMap,
}

async fn store(name: &str) -> StorageSqlx {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate(name, name).await.unwrap();
    storage.make_available().await.unwrap();
    storage
}

fn assert_column<T>(row: &sqlx::sqlite::SqliteRow, name: &str, expected: &T)
where
    T: std::fmt::Debug + PartialEq + sqlx::Type<sqlx::Sqlite>,
    for<'r> T: sqlx::Decode<'r, sqlx::Sqlite>,
{
    assert_eq!(&row.get::<T, _>(name), expected, "{name}");
}

impl OutputSync {
    async fn new() -> Self {
        let source = store("source").await;
        let target = store("target").await;
        let identity = "output-sync-user";
        let (user, _) = source.find_or_insert_user(identity).await.unwrap();
        // Force user and basket IDs to differ as well as transaction/output IDs.
        target.find_or_insert_user("unrelated-user").await.unwrap();
        target.find_or_insert_user(identity).await.unwrap();
        let basket_id = sqlx::query_scalar(
            "SELECT basket_id FROM output_baskets WHERE user_id = ? AND name = 'default'",
        )
        .bind(user.user_id)
        .fetch_one(source.pool())
        .await
        .unwrap();
        let now: DateTime<Utc> = "2026-01-02T03:04:05Z".parse().unwrap();
        for id in [101, 202, 203] {
            sqlx::query(
                "INSERT INTO transactions (transaction_id, user_id, txid, status, reference,
                    description, satoshis, version, lock_time, is_outgoing, created_at, updated_at)
                 VALUES (?, ?, ?, 'completed', ?, 'sync transaction', 12345, 1, 0, 1, ?, ?)",
            )
            .bind(id)
            .bind(user.user_id)
            .bind(format!("{id:064x}"))
            .bind(format!("sync-{id}"))
            .bind(now)
            .bind(now)
            .execute(source.writer())
            .await
            .unwrap();
        }
        let expected = TableOutput {
            output_id: 303,
            user_id: user.user_id,
            transaction_id: 101,
            basket_id: Some(basket_id),
            txid: format!("{:064x}", 101),
            vout: 3,
            satoshis: 12345,
            locking_script: Some(vec![0x51, 0x75]),
            script_length: 2,
            script_offset: 42,
            output_type: "custom".into(),
            provided_by: "you-and-storage".into(),
            purpose: Some("savings".into()),
            output_description: Some("funded output".into()),
            spent_by: Some(202),
            sequence_number: Some(u32::MAX),
            spending_description: Some("spent on supplies".into()),
            spendable: false,
            change: true,
            derivation_prefix: Some("cHJlZml4".into()),
            derivation_suffix: Some("c3VmZml4".into()),
            sender_identity_key: Some(format!("02{}", "11".repeat(32))),
            custom_instructions: Some("keep this metadata".into()),
            created_at: now,
            updated_at: now,
        };
        sqlx::query(
            "INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, txid,
                vout, satoshis, locking_script, script_length, script_offset, type,
                provided_by, purpose, output_description, spent_by, sequence_number,
                spending_description, spendable, change, derivation_prefix,
                derivation_suffix, sender_identity_key, custom_instructions, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(expected.output_id)
        .bind(expected.user_id)
        .bind(expected.transaction_id)
        .bind(expected.basket_id)
        .bind(&expected.txid)
        .bind(expected.vout)
        .bind(expected.satoshis)
        .bind(&expected.locking_script)
        .bind(expected.script_length)
        .bind(expected.script_offset)
        .bind(&expected.output_type)
        .bind(&expected.provided_by)
        .bind(&expected.purpose)
        .bind(&expected.output_description)
        .bind(expected.spent_by)
        .bind(expected.sequence_number.map(i64::from))
        .bind(&expected.spending_description)
        .bind(expected.spendable)
        .bind(expected.change)
        .bind(&expected.derivation_prefix)
        .bind(&expected.derivation_suffix)
        .bind(&expected.sender_identity_key)
        .bind(&expected.custom_instructions)
        .bind(expected.created_at)
        .bind(expected.updated_at)
        .execute(source.writer())
        .await
        .unwrap();
        Self {
            source,
            target,
            args: RequestSyncChunkArgs {
                from_storage_identity_key: "source".into(),
                to_storage_identity_key: "target".into(),
                identity_key: identity.into(),
                since: None,
                max_rough_size: 100_000,
                max_items: 100,
                offsets: [
                    entity_names::OUTPUT_BASKET,
                    entity_names::TRANSACTION,
                    entity_names::OUTPUT,
                ]
                .map(|name| SyncOffset {
                    name: name.into(),
                    offset: 0,
                })
                .to_vec(),
            },
            expected,
            map: SyncMap::default(),
        }
    }

    async fn export(&self) -> SyncChunk {
        let chunk = get_sync_chunk_internal(&self.source, self.args.clone())
            .await
            .unwrap();
        // Exercise the same TableOutput serialization used by the remote client.
        serde_json::from_slice(&serde_json::to_vec(&chunk).unwrap()).unwrap()
    }

    async fn apply(&mut self, chunk: SyncChunk) {
        process_sync_chunk_internal(&self.target, self.args.clone(), chunk, &mut self.map)
            .await
            .unwrap();
    }

    async fn update_source_metadata(&self) {
        sqlx::query(
            "UPDATE outputs SET output_description = ?, spent_by = ?, sequence_number = ?,
                spending_description = ?, provided_by = ?, purpose = ?, updated_at = ?
             WHERE output_id = ?",
        )
        .bind(&self.expected.output_description)
        .bind(self.expected.spent_by)
        .bind(self.expected.sequence_number.map(i64::from))
        .bind(&self.expected.spending_description)
        .bind(&self.expected.provided_by)
        .bind(&self.expected.purpose)
        .bind(self.expected.updated_at)
        .bind(self.expected.output_id)
        .execute(self.source.writer())
        .await
        .unwrap();
    }

    async fn assert_stored(&self) {
        let rows = sqlx::query("SELECT * FROM outputs")
            .fetch_all(self.target.pool())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        let e = &self.expected;
        macro_rules! column {
            ($name:literal, $ty:ty, $value:expr) => {
                assert_column::<$ty>(row, $name, &$value);
            };
        }
        column!("output_description", Option<String>, e.output_description);
        column!(
            "sequence_number",
            Option<i64>,
            e.sequence_number.map(i64::from)
        );
        column!(
            "spending_description",
            Option<String>,
            e.spending_description
        );
        column!(
            "spent_by",
            Option<i64>,
            e.spent_by.map(|id| self.map.transaction[&id])
        );
        column!("output_id", i64, self.map.output[&e.output_id]);
        column!(
            "transaction_id",
            i64,
            self.map.transaction[&e.transaction_id]
        );
        column!(
            "basket_id",
            Option<i64>,
            e.basket_id.map(|id| self.map.basket[&id])
        );
        let user = self
            .target
            .find_user(&self.args.identity_key)
            .await
            .unwrap()
            .unwrap();
        column!("user_id", i64, user.user_id);
        column!("txid", String, e.txid);
        column!("vout", i32, e.vout);
        column!("satoshis", i64, e.satoshis);
        column!("locking_script", Option<Vec<u8>>, e.locking_script);
        column!("script_length", i32, e.script_length);
        column!("script_offset", i32, e.script_offset);
        column!("type", String, e.output_type);
        column!("provided_by", String, e.provided_by);
        column!("purpose", Option<String>, e.purpose);
        column!("spendable", bool, e.spendable);
        column!("change", bool, e.change);
        column!("derivation_prefix", Option<String>, e.derivation_prefix);
        column!("derivation_suffix", Option<String>, e.derivation_suffix);
        column!("sender_identity_key", Option<String>, e.sender_identity_key);
        column!("custom_instructions", Option<String>, e.custom_instructions);
        column!("created_at", DateTime<Utc>, e.created_at);
        column!("updated_at", DateTime<Utc>, e.updated_at);
        assert_ne!(self.map.transaction[&101], 101);
        assert_ne!(self.map.transaction[&202], 202);
        assert_ne!(self.map.output[&303], 303);
        assert_ne!(user.user_id, e.user_id);
        assert_ne!(e.basket_id.map(|id| self.map.basket[&id]), e.basket_id);
    }
}

#[tokio::test]
async fn output_columns_survive_sync_round_trip() {
    let mut sync = OutputSync::new().await;
    let chunk = sync.export().await;
    sync.apply(chunk.clone()).await;
    sync.assert_stored().await;
    assert_eq!(
        serde_json::to_value(&chunk.outputs.as_ref().unwrap()[0]).unwrap(),
        serde_json::to_value(&sync.expected).unwrap(),
        "all 25 source columns must be exported"
    );
}

#[tokio::test]
async fn output_metadata_updates_clear_and_ignore_older_chunks() {
    let mut sync = OutputSync::new().await;
    let original = sync.export().await;
    sync.apply(original.clone()).await;
    sync.assert_stored().await;

    sync.expected.output_description = Some("updated output".into());
    sync.expected.spent_by = Some(203);
    sync.expected.spending_description = Some("updated spending".into());
    sync.expected.provided_by = "you".into();
    sync.expected.purpose = Some("custom".into());
    sync.expected.updated_at += Duration::seconds(1);
    sync.update_source_metadata().await;
    let updated = sync.export().await;
    sync.apply(updated).await;
    sync.assert_stored().await;

    sync.expected.output_description = None;
    sync.expected.spent_by = None;
    sync.expected.spending_description = None;
    sync.expected.updated_at += Duration::seconds(1);
    sync.update_source_metadata().await;
    let cleared = sync.export().await;
    sync.apply(cleared.clone()).await;
    sync.assert_stored().await;

    sync.apply(original).await;
    sync.assert_stored().await;
    let mut equal_time = cleared;
    equal_time.outputs.as_mut().unwrap()[0].output_description = Some("must not win".into());
    sync.apply(equal_time).await;
    sync.assert_stored().await;
}

#[tokio::test]
async fn output_newer_row_update_leaves_sequence_number() {
    // EntityOutput.mergeExisting (ts-stack edf6e03, :340-357) does not assign
    // sequenceNumber; only mergeNew writes it, with the whole row.
    let mut sync = OutputSync::new().await;
    let original = sync.export().await;
    sync.apply(original).await;
    sync.assert_stored().await;

    let stored = sync.expected.sequence_number;
    sync.expected.output_description = Some("updated output".into());
    sync.expected.spent_by = Some(203);
    sync.expected.sequence_number = Some(42);
    sync.expected.spending_description = Some("updated spending".into());
    sync.expected.provided_by = "you".into();
    sync.expected.purpose = Some("custom".into());
    sync.expected.updated_at += Duration::seconds(1);
    sync.update_source_metadata().await;
    let updated = sync.export().await;
    assert_eq!(
        updated.outputs.as_ref().unwrap()[0].sequence_number,
        Some(42)
    );
    sync.apply(updated).await;
    sync.expected.sequence_number = stored;
    sync.assert_stored().await;
}

#[tokio::test]
async fn output_spent_by_uses_transaction_map_from_an_earlier_chunk() {
    let mut sync = OutputSync::new().await;
    let mut parents = sync.export().await;
    let outputs = parents.outputs.take();
    sync.apply(parents).await;
    sync.apply(SyncChunk {
        from_storage_identity_key: sync.args.from_storage_identity_key.clone(),
        to_storage_identity_key: sync.args.to_storage_identity_key.clone(),
        user_identity_key: sync.args.identity_key.clone(),
        outputs,
        ..Default::default()
    })
    .await;
    sync.assert_stored().await;
}

#[tokio::test]
async fn output_newer_row_update_writes_only_the_reference_columns() {
    // EntityOutput.mergeExisting (ts-stack edf6e03, :340-357) assigns basketId,
    // spentBy, spendable, change, type, providedBy, purpose, outputDescription,
    // spendingDescription, senderIdentityKey, customInstructions, scriptLength,
    // scriptOffset, lockingScript and updated_at. It leaves satoshis,
    // derivationPrefix and derivationSuffix as stored. transactionId is the
    // match key (mergeFind, :306-311): a remote ID that resolves to the stored
    // transaction updates this row and leaves its transaction_id.
    let mut sync = OutputSync::new().await;
    let original = sync.export().await;
    sync.apply(original).await;
    sync.assert_stored().await;

    let mut newer = sync.export().await;
    let incoming = &mut newer.outputs.as_mut().unwrap()[0];
    // Unmapped remote ID with the stored txid: resolves to the stored transaction.
    incoming.transaction_id = 999;
    incoming.satoshis = 777;
    incoming.derivation_prefix = Some("b3RoZXI=".into());
    incoming.derivation_suffix = None;
    incoming.spent_by = Some(203);
    incoming.spendable = true;
    incoming.change = false;
    incoming.output_type = "P2PKH".into();
    incoming.provided_by = "you".into();
    incoming.purpose = Some("custom".into());
    incoming.output_description = Some("updated output".into());
    incoming.spending_description = None;
    incoming.sender_identity_key = None;
    incoming.custom_instructions = Some("new instructions".into());
    incoming.locking_script = Some(vec![0x76, 0xa9, 0x88]);
    incoming.script_length = 3;
    incoming.script_offset = 7;
    incoming.updated_at += Duration::seconds(1);
    let incoming = incoming.clone();
    sync.apply(newer).await;

    let e = &mut sync.expected;
    e.spent_by = incoming.spent_by;
    e.spendable = incoming.spendable;
    e.change = incoming.change;
    e.output_type = incoming.output_type;
    e.provided_by = incoming.provided_by;
    e.purpose = incoming.purpose;
    e.output_description = incoming.output_description;
    e.spending_description = incoming.spending_description;
    e.sender_identity_key = incoming.sender_identity_key;
    e.custom_instructions = incoming.custom_instructions;
    e.locking_script = incoming.locking_script;
    e.script_length = incoming.script_length;
    e.script_offset = incoming.script_offset;
    e.updated_at = incoming.updated_at;
    // assert_stored also holds transaction_id, satoshis, derivation_prefix,
    // derivation_suffix and sequence_number at their stored values.
    sync.assert_stored().await;
}

#[tokio::test]
async fn output_incoming_row_on_another_transaction_is_another_output() {
    // mergeFind keys on (userId, mapped transactionId, vout) (ts-stack edf6e03,
    // EntityOutput.ts:306-311; MergeEntity.ts:55-62): a row whose transaction
    // maps elsewhere is not found and goes to mergeNew, never over the stored row.
    let mut sync = OutputSync::new().await;
    let original = sync.export().await;
    sync.apply(original).await;
    sync.assert_stored().await;
    let stored_id = sync.map.output[&303];

    let mut other = sync.export().await;
    let incoming = &mut other.outputs.as_mut().unwrap()[0];
    incoming.output_id = 304;
    incoming.transaction_id = 202;
    incoming.satoshis = 777;
    incoming.updated_at += Duration::seconds(1);
    sync.apply(other).await;

    let rows = sqlx::query("SELECT output_id, transaction_id, satoshis FROM outputs")
        .fetch_all(sync.target.pool())
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let new_id = sync.map.output[&304];
    assert_ne!(new_id, stored_id);
    for row in &rows {
        let id: i64 = row.get("output_id");
        let (tx, sats) = if id == stored_id {
            (sync.map.transaction[&101], sync.expected.satoshis)
        } else {
            assert_eq!(id, new_id);
            (sync.map.transaction[&202], 777)
        };
        assert_column::<i64>(row, "transaction_id", &tx);
        assert_column::<i64>(row, "satoshis", &sats);
    }
}

// #313: a chunk output's purpose is written as given. The reference's
// mergeNew inserts the chunk row (EntityOutput.ts:320-330) and mergeExisting
// assigns `this.purpose = ei.purpose` (:347); neither defaults a missing one
// to `change`, which the allocators' managed-change filter would offer.
impl OutputSync {
    /// Shape the exported row as managed change on a completed parent.
    fn managed_change(chunk: &mut SyncChunk, purpose: Option<&str>) {
        let output = &mut chunk.outputs.as_mut().unwrap()[0];
        output.output_type = "P2PKH".into();
        output.provided_by = "storage".into();
        output.purpose = purpose.map(Into::into);
        output.change = true;
        output.spendable = true;
        output.spent_by = None;
    }

    async fn stored_purpose(&self) -> String {
        sqlx::query_scalar("SELECT purpose FROM outputs")
            .fetch_one(self.target.pool())
            .await
            .unwrap()
    }

    /// The managed-change count and both allocators on the target.
    async fn change_offered(&self) -> (i64, bool, bool) {
        let user = self
            .target
            .find_user(&self.args.identity_key)
            .await
            .unwrap()
            .unwrap();
        let basket = self.map.basket[&self.expected.basket_id.unwrap()];
        let spender = self.map.transaction[&202];
        let count = self
            .target
            .count_change_inputs(user.user_id, basket, false)
            .await
            .unwrap();
        let mut conn = self.target.writer().acquire().await.unwrap();
        let door = super::super::create_action::allocate_change_input(
            &mut conn,
            user.user_id,
            basket,
            spender,
            1,
            true,
        )
        .await
        .unwrap()
        .is_some();
        // Free the coin the door claimed so the public allocator sees it too.
        sqlx::query("UPDATE outputs SET spendable = 1, spent_by = NULL")
            .execute(&mut *conn)
            .await
            .unwrap();
        drop(conn);
        let public = self
            .target
            .allocate_change_input(user.user_id, basket, 1, false, spender)
            .await
            .unwrap()
            .is_some();
        (count, door, public)
    }
}

#[tokio::test]
async fn output_without_purpose_is_not_offered_as_change() {
    let mut sync = OutputSync::new().await;
    let mut chunk = sync.export().await;
    OutputSync::managed_change(&mut chunk, None);
    sync.apply(chunk).await;
    assert_eq!(sync.change_offered().await, (0, false, false));
    assert_eq!(sync.stored_purpose().await, "");
}

#[tokio::test]
async fn output_newer_row_without_purpose_is_not_offered_as_change() {
    let mut sync = OutputSync::new().await;
    let mut chunk = sync.export().await;
    OutputSync::managed_change(&mut chunk, Some("change"));
    sync.apply(chunk.clone()).await;
    assert_eq!(sync.stored_purpose().await, "change");

    OutputSync::managed_change(&mut chunk, None);
    chunk.outputs.as_mut().unwrap()[0].updated_at += Duration::seconds(1);
    sync.apply(chunk).await;
    assert_eq!(sync.change_offered().await, (0, false, false));
    assert_eq!(sync.stored_purpose().await, "");
}

#[tokio::test]
async fn output_change_and_receive_purposes_land_as_given() {
    for purpose in ["change", "receive"] {
        // Inserted as given.
        let mut sync = OutputSync::new().await;
        let mut chunk = sync.export().await;
        OutputSync::managed_change(&mut chunk, Some(purpose));
        sync.apply(chunk).await;
        assert_eq!(sync.stored_purpose().await, purpose);
        assert_eq!(sync.change_offered().await, (1, true, true), "{purpose}");

        // Updated as given by a newer row.
        let mut sync = OutputSync::new().await;
        let mut chunk = sync.export().await;
        OutputSync::managed_change(&mut chunk, Some("savings"));
        sync.apply(chunk.clone()).await;
        OutputSync::managed_change(&mut chunk, Some(purpose));
        chunk.outputs.as_mut().unwrap()[0].updated_at += Duration::seconds(1);
        sync.apply(chunk).await;
        assert_eq!(sync.stored_purpose().await, purpose);
        assert_eq!(sync.change_offered().await, (1, true, true), "{purpose}");
    }
}
