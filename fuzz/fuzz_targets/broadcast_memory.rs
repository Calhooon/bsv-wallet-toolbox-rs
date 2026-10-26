//! The broadcast-memory decoder: `broadcast_seen` rows read back as text
//! (one `txid,provider,status,seconds` line each) through the status
//! parser, the ladder, the seen set and the stale check, and the ancestor
//! walk the reduced send runs over a BEEF. No panic is the only assertion.
#![no_main]

use bsv_wallet_toolbox_rs::{
    oldest_stale_ancestor, seen_set_from_records, unproven_ancestors_in_beef, BroadcastSeenRecord,
    InMemoryBroadcastMemory, BROADCAST_SEEN_STALE_SECS,
};
use chrono::{DateTime, Utc};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let memory = InMemoryBroadcastMemory::new();
    let mut records = Vec::new();
    for line in text.lines().take(64) {
        let mut f = line.splitn(4, ',');
        let (txid, provider, status) = (f.next(), f.next(), f.next());
        let (Some(txid), Some(provider), Some(status)) = (txid, provider, status) else {
            continue;
        };
        let secs = f.next().and_then(|s| s.trim().parse::<i64>().ok());
        let seen_at = DateTime::<Utc>::from_timestamp(secs.unwrap_or(0), 0).unwrap_or_default();
        let _ = memory.record_broadcast_status_at(txid, provider, status, seen_at);
        let record = BroadcastSeenRecord {
            txid: txid.to_string(),
            provider: provider.to_string(),
            status: status.to_string(),
            seen_at,
        };
        let _ = record.ladder_status();
        records.push(record);
    }
    let txids: Vec<String> = records.iter().map(|r| r.txid.clone()).collect();
    for provider in records.iter().map(|r| r.provider.as_str()).take(4) {
        let _ = seen_set_from_records(provider, &txids, &records);
        let now = DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap_or_default();
        let _ = oldest_stale_ancestor(provider, &txids, &records, now, BROADCAST_SEEN_STALE_SECS);
    }
    if let Some(subject) = txids.first() {
        let _ = unproven_ancestors_in_beef(data, subject);
    }
});
