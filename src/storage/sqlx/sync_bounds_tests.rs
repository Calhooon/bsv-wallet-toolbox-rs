//! The sync-chunk finders' bounds past `i32::MAX` (#703) and the chunk's
//! sums and the lengths into them at `u32::MAX` (#711, #735).

use super::{
    entity_names, get_sync_chunk_internal, len_u32, process_sync_chunk_internal, ChunkingState,
};
use crate::storage::{
    RequestSyncChunkArgs, StorageSqlx, SyncChunk, SyncOffset, WalletStorageWriter,
};

// The finders bind their `u32` limit and offset as `i64` (#703). Bound as
// `i32`, 2^31 and `u32::MAX` wrapped negative, SQLite read the OFFSET as 0,
// and a chunk asked past the end answered the head of the table. The store
// holds a row in each of the twelve tables (#715), so a wrong bind on any
// finder answers that table's head row and the witness reads red.
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

const AT: &str = "2026-11-03T00:00:00Z";

/// One row of each synced table, the parents' source ids the children name.
fn seed_chunk(identity_key: &str) -> SyncChunk {
    let txid = "d".repeat(64);
    serde_json::from_value(serde_json::json!({
        "fromStorageIdentityKey": "c".repeat(66),
        "toStorageIdentityKey": "a".repeat(66),
        "userIdentityKey": identity_key,
        "outputBaskets": [{"basketId": 7, "userId": 1, "name": "payments",
            "numberOfDesiredUtxos": 5, "minimumDesiredUtxoValue": 1000,
            "createdAt": AT, "updatedAt": AT}],
        "provenTxs": [{"provenTxId": 7, "txid": "e".repeat(64), "height": 1, "index": 0,
            "blockHash": "f".repeat(64), "merkleRoot": "f".repeat(64),
            "merklePath": [0], "rawTx": [0], "createdAt": AT, "updatedAt": AT}],
        "provenTxReqs": [{"provenTxReqId": 7, "txid": txid, "status": "unsent",
            "history": "{}", "notify": "{}", "rawTx": [0], "createdAt": AT, "updatedAt": AT}],
        "txLabels": [{"labelId": 7, "userId": 1, "label": "bounds",
            "createdAt": AT, "updatedAt": AT}],
        "outputTags": [{"tagId": 7, "userId": 1, "tag": "bounds",
            "createdAt": AT, "updatedAt": AT}],
        "transactions": [{"transactionId": 7, "userId": 1, "txid": txid,
            "status": "completed", "reference": "bounds", "description": "bounds",
            "satoshis": 1000, "version": 1, "isOutgoing": false,
            "createdAt": AT, "updatedAt": AT}],
        "outputs": [{"outputId": 7, "userId": 1, "transactionId": 7, "basketId": 7,
            "txid": txid, "vout": 0, "satoshis": 1000, "lockingScript": [0x51],
            "scriptLength": 1, "outputType": "custom", "providedBy": "you",
            "spendable": true, "createdAt": AT, "updatedAt": AT}],
        "txLabelMaps": [{"transactionId": 7, "labelId": 7, "createdAt": AT, "updatedAt": AT}],
        "outputTagMaps": [{"outputId": 7, "tagId": 7, "createdAt": AT, "updatedAt": AT}],
        "certificates": [{"certificateId": 7, "userId": 1, "certType": "dHlwZQ==",
            "serialNumber": "c2VyaWFs", "certifier": "02".repeat(33),
            "subject": identity_key, "revocationOutpoint": format!("{txid}.0"),
            "signature": "00", "createdAt": AT, "updatedAt": AT}],
        "certificateFields": [{"certificateId": 7, "userId": 1, "fieldName": "name",
            "fieldValue": "value", "masterKey": "key", "createdAt": AT, "updatedAt": AT}],
        "commissions": [{"commissionId": 7, "userId": 1, "transactionId": 7,
            "satoshis": 10, "payerLockingScript": [0x51], "keyOffset": "offset",
            "isRedeemed": false, "createdAt": AT, "updatedAt": AT}],
    }))
    .unwrap()
}

fn args(identity_key: &str, offset: u32) -> RequestSyncChunkArgs {
    RequestSyncChunkArgs {
        offsets: NAMES
            .iter()
            .map(|name| SyncOffset {
                name: name.to_string(),
                offset,
            })
            .collect(),
        max_items: u32::MAX,
        max_rough_size: 100_000,
        since: None,
        identity_key: identity_key.to_string(),
        from_storage_identity_key: "a".repeat(66),
        to_storage_identity_key: "c".repeat(66),
    }
}

/// The rows each table answered, in `NAMES` order.
fn counts(chunk: &SyncChunk) -> [usize; 12] {
    fn n<T>(rows: &Option<Vec<T>>) -> usize {
        rows.as_ref().map_or(0, Vec::len)
    }
    [
        n(&chunk.output_baskets),
        n(&chunk.proven_txs),
        n(&chunk.proven_tx_reqs),
        n(&chunk.tx_labels),
        n(&chunk.output_tags),
        n(&chunk.transactions),
        n(&chunk.outputs),
        n(&chunk.tx_label_maps),
        n(&chunk.output_tag_maps),
        n(&chunk.certificates),
        n(&chunk.certificate_fields),
        n(&chunk.commissions),
    ]
}

#[tokio::test]
async fn sync_chunk_offset_past_i32_max_is_an_empty_chunk() {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("bounds", &"a".repeat(66)).await.unwrap();
    storage.make_available().await.unwrap();
    let identity_key = "b".repeat(66);
    let mut seed = args(&identity_key, 0);
    (seed.from_storage_identity_key, seed.to_storage_identity_key) =
        (seed.to_storage_identity_key, seed.from_storage_identity_key);
    let chunk = seed_chunk(&identity_key);
    process_sync_chunk_internal(&storage, seed, chunk, &mut Default::default())
        .await
        .unwrap();

    // Every table has a head row to answer at offset 0.
    let head = get_sync_chunk_internal(&storage, args(&identity_key, 0))
        .await
        .unwrap();
    for (name, rows) in NAMES.iter().zip(counts(&head)) {
        assert!(rows > 0, "{name}: no seeded row");
    }

    for offset in [i32::MAX as u32 + 1, u32::MAX] {
        let chunk = get_sync_chunk_internal(&storage, args(&identity_key, offset))
            .await
            .unwrap();
        for (name, rows) in NAMES.iter().zip(counts(&chunk)) {
            assert_eq!(rows, 0, "{name} at offset {offset}");
        }
    }
}

// `add_items` saturates both sums (#711). A plain add past `u32::MAX`
// panicked in a debug build and wrapped small in release, so a huge chunk
// read as having room. Bounds at `u32::MAX`: a saturated sum equals the
// bound, `can_add` is false, and a later add keeps it false.
#[test]
fn chunking_sums_saturate_at_u32_max() {
    let mut size = ChunkingState::new(u32::MAX, u32::MAX);
    size.add_items(1, u32::MAX - 1);
    assert!(size.can_add());
    size.add_items(1, 10);
    assert_eq!(size.rough_size, u32::MAX);
    assert!(!size.can_add());
    size.add_items(1, 1);
    assert_eq!(size.rough_size, u32::MAX);
    assert!(!size.can_add());

    let mut items = ChunkingState::new(u32::MAX, u32::MAX);
    items.add_items(u32::MAX - 1, 1);
    assert!(items.can_add());
    items.add_items(10, 1);
    assert_eq!(items.items_count, u32::MAX);
    assert_eq!(items.remaining_items(), 0);
    assert!(!items.can_add());
    items.add_items(1, 1);
    assert_eq!(items.items_count, u32::MAX);
    assert!(!items.can_add());
}

// A length into the sums saturates (#735). `as u32` wrapped a length past
// `u32::MAX` small (2^32 to 0), so a chunk that large read as having room.
// The seam takes the `usize` directly; no multi-gigabyte value is built.
#[cfg(target_pointer_width = "64")]
#[test]
fn chunk_lengths_saturate_at_u32_max() {
    assert_eq!(len_u32(0), 0);
    assert_eq!(len_u32(u32::MAX as usize), u32::MAX);
    for len in [u32::MAX as usize + 1, usize::MAX] {
        assert_eq!(len_u32(len), u32::MAX);
        let mut state = ChunkingState::new(u32::MAX, u32::MAX);
        state.add_items(1, len_u32(len));
        assert!(!state.can_add());
        let mut state = ChunkingState::new(u32::MAX, u32::MAX);
        state.add_items(len_u32(len), 1);
        assert!(!state.can_add());
    }
}
