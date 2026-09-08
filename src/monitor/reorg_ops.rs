//! Reorg resilience (M19 R1, 2026-09-08): the pure decisions and the one
//! re-prove operation the monitor's header, reorg and review tasks and the
//! CLI's `reproof` verb all share.
//!
//! The reference (ts-stack `wallet-toolbox`): `TaskNewHeader` names a
//! same-height hash change ("reorg header") and processes a header only after
//! it has remained the chain tip for a full cycle; `TaskCheckForProofs`
//! accepts proofs only up to that processed height; `TaskReorg` reproves
//! every `proven_txs` row that cites a deactivated header
//! (`reproveHeader`); `TaskReviewProvenTxs` audits recent heights by merkle
//! root as the backup. What this module adds on top of the reference is only
//! the shape: one `HeaderTracker` (pure, pinned) and one `reprove_anchor`
//! (the storage's replace-or-demote path), so every producer of "this proof
//! is stale" lands in the same place.
//!
//! Why it exists: 2026-09-07 22:39:32Z a 34 MB block at 965771 was orphaned
//! by a 58-tx block seconds after Arcade had pushed inline proofs against
//! it; 28 fleet seats stored those proofs, nothing ever re-validated them,
//! and every spend touching one was refused with "Invalid merkle root".

use std::collections::HashMap;
use std::sync::Arc;

use bsv_rs::primitives::sha256d;
use bsv_rs::transaction::MerklePath;

use crate::services::WalletServices;
use crate::storage::{MonitorStorage, ProofIngestOutcome, ProvenTxAnchor};

/// A block header the chain deactivated (the hash a stored proof may still
/// cite). Produced by the header tracker, consumed by the reorg task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeactivatedHeader {
    /// Block hash that was deactivated.
    pub hash: String,
    /// Block height.
    pub height: u32,
    /// When this was deactivated.
    pub deactivated_at: chrono::DateTime<chrono::Utc>,
    /// Number of retry attempts.
    pub retry_count: u32,
}

/// The queue between the header tracker (producer) and the reorg task
/// (consumer). Shared by `Arc`; the daemon creates one per monitor.
pub type ReorgQueue = Arc<tokio::sync::Mutex<Vec<DeactivatedHeader>>>;

/// A fresh, empty queue.
pub fn new_reorg_queue() -> ReorgQueue {
    Arc::new(tokio::sync::Mutex::new(Vec::new()))
}

/// The display-order (reversed) hex of `sha256d(header)` for an 80-byte
/// serialized block header; `None` for anything else.
pub fn block_hash_of_header(header: &[u8]) -> Option<String> {
    if header.len() != 80 {
        return None;
    }
    let mut digest = sha256d(header);
    digest.reverse();
    Some(hex::encode(digest))
}

/// The display-order (reversed) hex of the merkle root inside an 80-byte
/// serialized block header (bytes 36..68); `None` for anything else.
pub fn merkle_root_of_header(header: &[u8]) -> Option<String> {
    if header.len() != 80 {
        return None;
    }
    let mut root = [0u8; 32];
    root.copy_from_slice(&header[36..68]);
    root.reverse();
    Some(hex::encode(root))
}

/// What one header observation decided (all pure).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderEvents {
    /// Headers the chain deactivated: a same-height hash change deactivates
    /// the old hash; a tip decrease deactivates the old tip.
    pub deactivated: Vec<(u32, String)>,
    /// A header that stayed the tip for a full cycle and is now PROCESSED:
    /// proofs up to this height may be stored, and the proof check runs.
    pub processed: Option<(u32, String)>,
    /// A header newly queued (it must survive one cycle to be processed).
    pub queued: Option<(u32, String)>,
    /// Whether this observation was a reorg (a hash change at the same
    /// height, or a tip decrease).
    pub reorg: bool,
}

/// The reference's `TaskNewHeader` state machine, pure and pinned.
///
/// `observe` is called once per cycle with the chain tip's height and, when
/// the header could be read, its hash. A header is processed only after it
/// is observed unchanged on the NEXT cycle (the one-cycle proof lag). A hash
/// change at the same height, or a tip decrease, is a reorg: the old header
/// is deactivated and the processed height drops below it so the affected
/// heights are re-checked. An unknown hash never produces a reorg event (an
/// unknown never reads as anything).
#[derive(Debug, Clone, Default)]
pub struct HeaderTracker {
    last: Option<(u32, Option<String>)>,
    queued: Option<(u32, Option<String>)>,
    /// The highest height whose proofs may be stored (0 = none yet).
    pub processed_height: u32,
}

impl HeaderTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, height: u32, hash: Option<String>) -> HeaderEvents {
        let mut ev = HeaderEvents::default();
        let hash_lc = hash.map(|h| h.to_ascii_lowercase());
        match self.last.clone() {
            None => {
                // The first observation: everything BELOW the tip is aged
                // by definition; the tip itself must survive one cycle.
                self.processed_height = height.saturating_sub(1);
                self.last = Some((height, hash_lc.clone()));
                self.queued = Some((height, hash_lc.clone()));
                ev.queued = Some((height, hash_lc.unwrap_or_default()));
            }
            Some((last_height, _last_hash)) if height > last_height => {
                self.last = Some((height, hash_lc.clone()));
                self.queued = Some((height, hash_lc.clone()));
                ev.queued = Some((height, hash_lc.unwrap_or_default()));
            }
            Some((last_height, last_hash)) if height < last_height => {
                // The tip went backwards: the old tip is off the chain.
                ev.reorg = true;
                if let Some(h) = last_hash {
                    ev.deactivated.push((last_height, h));
                }
                self.processed_height = self.processed_height.min(height.saturating_sub(1));
                self.last = Some((height, hash_lc.clone()));
                self.queued = Some((height, hash_lc.clone()));
                ev.queued = Some((height, hash_lc.unwrap_or_default()));
            }
            Some((last_height, last_hash)) => {
                // Same height as last cycle.
                let changed = match (&last_hash, &hash_lc) {
                    (Some(a), Some(b)) => a != b,
                    _ => false,
                };
                if changed {
                    ev.reorg = true;
                    if let Some(h) = last_hash {
                        ev.deactivated.push((last_height, h));
                    }
                    self.processed_height =
                        self.processed_height.min(height.saturating_sub(1));
                    self.last = Some((height, hash_lc.clone()));
                    self.queued = Some((height, hash_lc.clone()));
                    ev.queued = Some((height, hash_lc.unwrap_or_default()));
                } else {
                    // A hash learned late (the header was unreadable when the
                    // height was first seen) is adopted, so a later change at
                    // this height is still a reorg.
                    if last_hash.is_none() && hash_lc.is_some() {
                        self.last = Some((height, hash_lc.clone()));
                    }
                    if let Some((qh, qhash)) = self.queued.clone() {
                        if qh == height {
                            // Survived a full cycle as the tip: PROCESSED.
                            self.processed_height = self.processed_height.max(height);
                            self.queued = None;
                            ev.processed =
                                Some((height, qhash.or(hash_lc).unwrap_or_default()));
                        }
                    }
                }
            }
        }
        ev
    }

    /// The last observed tip height (0 before the first observation).
    pub fn last_height(&self) -> u32 {
        self.last.as_ref().map(|(h, _)| *h).unwrap_or(0)
    }
}

/// Anchors whose stored merkle root disagrees with the canonical root at
/// their height. A height with no canonical root known is skipped (an
/// unknown never reads as stale).
pub fn stale_anchors_by_root<'a>(
    anchors: &'a [ProvenTxAnchor],
    canonical_roots: &HashMap<u32, String>,
) -> Vec<&'a ProvenTxAnchor> {
    anchors
        .iter()
        .filter(|a| match canonical_roots.get(&a.height) {
            Some(root) => !root.eq_ignore_ascii_case(&a.merkle_root),
            None => false,
        })
        .collect()
}

/// What `reprove_anchor` did to one stored proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReproveOutcome {
    /// A validated proof for a different block replaced the stored one.
    Replaced { height: u32, block_hash: String },
    /// The providers still answer the SAME block (the "stale" reading was a
    /// transient of the tracker or the provider); nothing changed.
    Unchanged,
    /// The header for the provider's answer has not aged one cycle yet;
    /// left alone for the next pass.
    Deferred { height: u32 },
    /// No valid replacement was available; the stored proof was demoted so
    /// the transaction is unmined again and will be re-proved.
    Demoted,
    /// A network or service fault; try again later.
    TransientError(String),
}

/// Re-prove one stored anchor against the chain: ask the providers for the
/// transaction's current merkle path (validated against the chain tracker
/// by the services layer), replace the stored proof when it names a
/// different block, and demote the stored proof when no valid replacement
/// exists (the reference's `reproveHeader` with demotion instead of
/// retention, because a retained stale proof refuses every spend that
/// touches it).
pub async fn reprove_anchor<S, V>(
    storage: &S,
    services: &V,
    anchor: &ProvenTxAnchor,
) -> ReproveOutcome
where
    S: MonitorStorage + ?Sized,
    V: WalletServices + ?Sized,
{
    let result = match services.get_merkle_path(&anchor.txid, false).await {
        Ok(r) => r,
        Err(e) => return ReproveOutcome::TransientError(e.to_string()),
    };
    let (path_hex, header) = match (result.merkle_path, result.header) {
        (Some(p), Some(h)) => (p, h),
        _ => {
            // The providers hold no proof for it: not mined on the canonical
            // chain right now. The stored proof is wrong by construction.
            return match storage.demote_stale_proof(&anchor.txid).await {
                Ok(_) => ReproveOutcome::Demoted,
                Err(e) => ReproveOutcome::TransientError(e.to_string()),
            };
        }
    };
    let bytes = match hex::decode(&path_hex) {
        Ok(b) => b,
        Err(_) => {
            return match storage.demote_stale_proof(&anchor.txid).await {
                Ok(_) => ReproveOutcome::Demoted,
                Err(e) => ReproveOutcome::TransientError(e.to_string()),
            }
        }
    };
    // The provider's answer names the block; the bump's own height is the
    // authority on the height it proves.
    let height = MerklePath::from_binary(&bytes)
        .map(|mp| mp.block_height)
        .unwrap_or(header.height);
    match storage
        .ingest_push_proof(&anchor.txid, &bytes, height, &header.hash)
        .await
    {
        Ok(Some(ProofIngestOutcome::Ingested(_))) => {
            let same_block = if !anchor.block_hash.is_empty() && !header.hash.is_empty() {
                anchor.block_hash.eq_ignore_ascii_case(&header.hash) && anchor.height == height
            } else {
                anchor.height == height && anchor.merkle_root.eq_ignore_ascii_case(&header.merkle_root)
            };
            if same_block {
                ReproveOutcome::Unchanged
            } else {
                ReproveOutcome::Replaced {
                    height,
                    block_hash: header.hash.to_ascii_lowercase(),
                }
            }
        }
        Ok(Some(ProofIngestOutcome::DeferredAboveProcessedHeight { block_height, .. })) => {
            ReproveOutcome::Deferred { height: block_height }
        }
        Ok(Some(_)) | Ok(None) => match storage.demote_stale_proof(&anchor.txid).await {
            Ok(_) => ReproveOutcome::Demoted,
            Err(e) => ReproveOutcome::TransientError(e.to_string()),
        },
        Err(e) => ReproveOutcome::TransientError(e.to_string()),
    }
}

/// The tally of one re-prove pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReproveTally {
    pub replaced: u32,
    pub unchanged: u32,
    pub deferred: u32,
    pub demoted: u32,
    pub errors: Vec<String>,
}

impl ReproveTally {
    pub fn record(&mut self, txid: &str, outcome: &ReproveOutcome) {
        match outcome {
            ReproveOutcome::Replaced { height, block_hash } => {
                self.replaced += 1;
                tracing::info!(
                    txid = %txid,
                    height,
                    block_hash = %block_hash,
                    marker = "reorg_proof_replaced",
                    "reprove: the stored proof was replaced by the canonical block's"
                );
            }
            ReproveOutcome::Unchanged => self.unchanged += 1,
            ReproveOutcome::Deferred { .. } => self.deferred += 1,
            ReproveOutcome::Demoted => {
                self.demoted += 1;
                tracing::warn!(
                    txid = %txid,
                    marker = "reorg_proof_demoted",
                    "reprove: no valid replacement; the proof was demoted and the transaction is unmined again"
                );
            }
            ReproveOutcome::TransientError(e) => self.errors.push(format!("{txid}: {e}")),
        }
    }
}

/// Re-prove every stored anchor citing `block_hash` (the reorg task's unit
/// of work for one deactivated header).
pub async fn reprove_block_hash<S, V>(storage: &S, services: &V, block_hash: &str) -> ReproveTally
where
    S: MonitorStorage + ?Sized,
    V: WalletServices + ?Sized,
{
    let mut tally = ReproveTally::default();
    let anchors = match storage.find_proven_txs_by_block_hash(block_hash).await {
        Ok(a) => a,
        Err(e) => {
            tally.errors.push(format!("find_proven_txs_by_block_hash: {e}"));
            return tally;
        }
    };
    for anchor in &anchors {
        let outcome = reprove_anchor(storage, services, anchor).await;
        tally.record(&anchor.txid, &outcome);
    }
    tally
}

#[cfg(test)]
mod tests {
    use super::*;

    const GENESIS_HEADER_HEX: &str = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";

    #[test]
    fn genesis_header_hashes_and_roots() {
        let bytes = hex::decode(GENESIS_HEADER_HEX).unwrap();
        assert_eq!(
            block_hash_of_header(&bytes).unwrap(),
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
        assert_eq!(
            merkle_root_of_header(&bytes).unwrap(),
            "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
        );
        assert!(block_hash_of_header(&bytes[..79]).is_none());
        assert!(merkle_root_of_header(&[]).is_none());
    }

    fn h(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    #[test]
    fn first_observation_queues_and_ages_everything_below() {
        let mut t = HeaderTracker::new();
        let ev = t.observe(965771, h("AA"));
        assert_eq!(ev.queued, Some((965771, "aa".to_string())));
        assert_eq!(ev.processed, None);
        assert!(!ev.reorg);
        assert_eq!(t.processed_height, 965770);
    }

    #[test]
    fn a_header_is_processed_only_after_a_full_cycle_as_tip() {
        let mut t = HeaderTracker::new();
        t.observe(965771, h("aa"));
        let ev = t.observe(965771, h("aa"));
        assert_eq!(ev.processed, Some((965771, "aa".to_string())));
        assert_eq!(t.processed_height, 965771);
        // A third identical cycle processes nothing new.
        let ev = t.observe(965771, h("aa"));
        assert_eq!(ev.processed, None);
        assert!(ev.deactivated.is_empty());
    }

    #[test]
    fn a_new_height_replaces_the_queue_and_waits_its_own_cycle() {
        let mut t = HeaderTracker::new();
        t.observe(965771, h("aa"));
        let ev = t.observe(965772, h("bb"));
        assert_eq!(ev.queued, Some((965772, "bb".to_string())));
        assert_eq!(ev.processed, None);
        // 965771 was never processed: the queue holds only the newest tip;
        // proofs at 965771 wait for 965772 to age (one cycle later).
        assert_eq!(t.processed_height, 965770);
        let ev = t.observe(965772, h("bb"));
        assert_eq!(ev.processed, Some((965772, "bb".to_string())));
        assert_eq!(t.processed_height, 965772);
    }

    #[test]
    fn a_same_height_hash_change_is_a_reorg_that_deactivates_the_old_hash() {
        // The 2026-09-07 event: the 34 MB block at 965771 was the tip for a
        // few seconds, then a different block at the SAME height won.
        let mut t = HeaderTracker::new();
        t.observe(965771, h("153e10f4"));
        t.observe(965771, h("153e10f4")); // processed
        assert_eq!(t.processed_height, 965771);
        let ev = t.observe(965771, h("1de5aa96"));
        assert!(ev.reorg);
        assert_eq!(ev.deactivated, vec![(965771, "153e10f4".to_string())]);
        assert_eq!(ev.queued, Some((965771, "1de5aa96".to_string())));
        // The processed height drops below the reorged height.
        assert_eq!(t.processed_height, 965770);
        // The replacement must itself survive a cycle.
        let ev = t.observe(965771, h("1de5aa96"));
        assert_eq!(ev.processed, Some((965771, "1de5aa96".to_string())));
        assert_eq!(t.processed_height, 965771);
    }

    #[test]
    fn a_tip_decrease_is_a_reorg_that_deactivates_the_old_tip() {
        let mut t = HeaderTracker::new();
        t.observe(965773, h("0851a554"));
        t.observe(965773, h("0851a554"));
        let ev = t.observe(965772, h("cc"));
        assert!(ev.reorg);
        assert_eq!(ev.deactivated, vec![(965773, "0851a554".to_string())]);
        assert_eq!(t.processed_height, 965771);
    }

    #[test]
    fn an_unknown_hash_never_produces_a_reorg() {
        let mut t = HeaderTracker::new();
        t.observe(965771, h("aa"));
        let ev = t.observe(965771, None);
        assert!(!ev.reorg);
        assert!(ev.deactivated.is_empty());
        // It still counts as a survived cycle (the height did not move).
        assert_eq!(ev.processed, Some((965771, "aa".to_string())));
        let ev = t.observe(965771, h("zz"));
        // The tracker learned "aa" for the queued header; a later different
        // hash at the same height IS a reorg.
        assert!(ev.reorg);
    }

    #[test]
    fn a_hash_learned_late_is_adopted_so_a_later_change_is_still_a_reorg() {
        let mut t = HeaderTracker::new();
        t.observe(965771, None); // header unreadable at first sight
        let ev = t.observe(965771, h("aa")); // survived a cycle; hash learned
        assert_eq!(ev.processed, Some((965771, "aa".to_string())));
        let ev = t.observe(965771, h("bb"));
        assert!(ev.reorg);
        assert_eq!(ev.deactivated, vec![(965771, "aa".to_string())]);
    }

    fn anchor(txid: &str, height: u32, hash: &str, root: &str) -> ProvenTxAnchor {
        ProvenTxAnchor {
            txid: txid.into(),
            height,
            block_hash: hash.into(),
            merkle_root: root.into(),
        }
    }

    #[test]
    fn stale_anchors_disagree_with_the_canonical_root_and_unknown_heights_are_skipped() {
        let anchors = vec![
            anchor("t1", 965771, "153e", "709df569"),
            anchor("t2", 965771, "1de5", "A785B053"),
            anchor("t3", 965772, "", "deadbeef"),
        ];
        let mut canonical = HashMap::new();
        canonical.insert(965771u32, "a785b053".to_string());
        let stale = stale_anchors_by_root(&anchors, &canonical);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].txid, "t1");
    }

    #[test]
    fn the_tally_counts_every_outcome() {
        let mut t = ReproveTally::default();
        t.record("a", &ReproveOutcome::Replaced { height: 1, block_hash: "x".into() });
        t.record("b", &ReproveOutcome::Unchanged);
        t.record("c", &ReproveOutcome::Deferred { height: 2 });
        t.record("d", &ReproveOutcome::Demoted);
        t.record("e", &ReproveOutcome::TransientError("boom".into()));
        assert_eq!((t.replaced, t.unchanged, t.deferred, t.demoted), (1, 1, 1, 1));
        assert_eq!(t.errors, vec!["e: boom".to_string()]);
    }
}
