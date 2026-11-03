//! The sync-chunk finders' bounds past `i32::MAX` (#703).

use super::{entity_names, get_sync_chunk_internal};
use crate::storage::{RequestSyncChunkArgs, StorageSqlx, SyncOffset, WalletStorageWriter};

// The finders bind their `u32` limit and offset as `i64` (#703). Bound as
// `i32`, 2^31 and `u32::MAX` wrapped negative, SQLite read the OFFSET as 0,
// and a chunk asked past the end answered the head of the table. One user,
// its default basket the one row; every entity's offset is set, so all
// twelve finders bind the value and only the baskets have a row to skip.
const NAMES: [&str; 12] = [
    entity_names::OUTPUT_BASKET,
    entity_names::PROVEN_TX,
    entity_names::PROVEN_TX_REQ,
    entity_names::TX_LABEL,
    entity_names::OUTPUT_TAG,
    entity_names::TRANSACTION,
    entity_names::OUTPUT,
    entity_names::TX_LABEL_MAP,
    entity_names::OUTPUT_TAG_MAP,
    entity_names::CERTIFICATE,
    entity_names::CERTIFICATE_FIELD,
    entity_names::COMMISSION,
];

#[tokio::test]
async fn sync_chunk_offset_past_i32_max_is_an_empty_chunk() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("bounds", &"a".repeat(66)).await.unwrap();
    storage.make_available().await.unwrap();
    let identity_key = "b".repeat(66);
    storage.find_or_insert_user(&identity_key).await.unwrap();

    for offset in [i32::MAX as u32 + 1, u32::MAX] {
        let offsets = NAMES
            .iter()
            .map(|name| SyncOffset {
                name: name.to_string(),
                offset,
            })
            .collect();
        let args = RequestSyncChunkArgs {
            offsets,
            max_items: u32::MAX,
            max_rough_size: 100_000,
            since: None,
            identity_key: identity_key.clone(),
            from_storage_identity_key: "a".repeat(66),
            to_storage_identity_key: "c".repeat(66),
        };
        let chunk = get_sync_chunk_internal(&storage, args).await.unwrap();
        assert!(chunk.output_baskets.is_none(), "offset {offset}");
        assert!(chunk.transactions.is_none(), "offset {offset}");
    }
}
