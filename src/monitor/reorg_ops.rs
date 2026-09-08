//! Reorg resilience: the pure decisions and the one re-prove operation the
//! monitor's header, reorg and review tasks and the CLI's `reproof` verb all
//! share.
//!
//! The reference (ts-stack `wallet-toolbox`): `TaskNewHeader` queues a new
//! header (a higher height, or a different hash at the same height) and
//! processes it only after it has remained the chain tip for a full cycle; a
//! LOWER height is an "old header", reverted and ignored, never a reorg;
//! `TaskCheckForProofs` accepts proofs only up to the processed height (the
//! proof LAG gate); `TaskReorg` reproves every `proven_txs` row citing a
//! deactivated header (`reproveHeader`), aged ten minutes per try, three
//! tries, then the original is RETAINED; `TaskReviewProvenTxs` audits
//! heights by merkle root as the backup; `reproveProven` UPDATES a row in
//! place from a validated provider proof and retains it otherwise.
//!
//! Ours conforms to that shape. The stated divergences (see
//! `docs/REORG-DIVERGENCES.md`): a stored proof is DEMOTED (reverted to the
//! pre-proof state, bytes preserved) when the chain POSITIVELY refutes it
//! (the tracker answers a definite false for the stored root, at least two
//! providers answer cleanly "not mined", and no provider serves a path),
//! because a retained stale proof refuses every spend that touches it; the
//! deactivated headers come from a ring of recently observed tips compared
//! with the header service, because our chaintracks pushes no reorg event;
//! and the audit is a fresh 12-height window every 10 minutes.
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
use crate::storage::{HeaderTrackerState, MonitorStorage, ProofIngestOutcome, ProvenTxAnchor};

/// A block header the chain deactivated (the hash a stored proof may still
/// cite). Produced by the header task, consumed by the reorg task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeactivatedHeader {
    /// Block hash that was deactivated.
    pub hash: String,
    /// Block height.
    pub height: u32,
    /// When this was deactivated (or last retried).
    pub deactivated_at: chrono::DateTime<chrono::Utc>,
    /// Number of times the reorg task has processed it.
    pub retry_count: u32,
}

/// The queue between the header task (producer) and the reorg task
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

/// How many recent `(height, hash)` tips the header tracker remembers, so a
/// fork deeper than one block is still named (the previous tip replaced
/// under a new tip: A@H then B'@H+1 with A'@H).
pub const HEADER_RING_LEN: usize = 12;

/// What one header observation decided (all pure).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderEvents {
    /// A header newly queued: it must remain the tip for one full cycle
    /// before it is processed.
    pub queued: Option<(u32, String)>,
    /// A header that stayed the tip for a full cycle and is now PROCESSED:
    /// proofs up to this height may be stored, and the proof check runs.
    pub processed: Option<(u32, String)>,
    /// The observed tip was LOWER than the last one: an "old header",
    /// ignored (the reference reverts to the higher header). Never a reorg,
    /// never a deactivation, never a gate change.
    pub old_header: Option<(u32, String)>,
    /// A different hash at the same height as the last tip (the reference's
    /// "reorg header").
    pub reorg_header: bool,
    /// Headers deactivated by this observation alone: the old hash of a
    /// same-height change. The ring walk may add more.
    pub deactivated: Vec<(u32, String)>,
    /// The tip moved (a new height or a reorg header): the ring of earlier
    /// tips should be reconciled against the header service.
    pub check_ring: bool,
}

/// The reference's `TaskNewHeader` state machine, pure and pinned, plus the
/// ring of recent tips.
///
/// `observe` is called once per cycle with the chain tip's height and hash
/// (both from the header service). A header is processed only after it is
/// observed unchanged on the NEXT cycle (the one-cycle proof lag). A hash
/// change at the same height is a reorg header: the old hash is deactivated
/// and the new one queued. A lower height is an old header and changes
/// nothing. The gate itself is not held here: the task raises the storage's
/// persisted gate to a processed header's height.
#[derive(Debug, Clone, Default)]
pub struct HeaderTracker {
    state: HeaderTrackerState,
}

impl HeaderTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resume from a persisted state (hashes normalized to lowercase).
    pub fn from_state(state: HeaderTrackerState) -> Self {
        let lower = |p: (u32, String)| (p.0, p.1.to_ascii_lowercase());
        Self {
            state: HeaderTrackerState {
                last: state.last.map(lower),
                queued: state.queued.map(lower),
                ring: state.ring.into_iter().map(lower).collect(),
            },
        }
    }

    /// The state to persist.
    pub fn state(&self) -> &HeaderTrackerState {
        &self.state
    }

    /// The last observed tip, if any.
    pub fn last(&self) -> Option<&(u32, String)> {
        self.state.last.as_ref()
    }

    /// The last observed tip height (0 before the first observation).
    pub fn last_height(&self) -> u32 {
        self.state.last.as_ref().map(|(h, _)| *h).unwrap_or(0)
    }

    /// The header waiting to survive one cycle, if any.
    pub fn queued(&self) -> Option<&(u32, String)> {
        self.state.queued.as_ref()
    }

    /// The remembered tips below the current one, newest first.
    pub fn ring_below_tip_newest_first(&self) -> Vec<(u32, String)> {
        let tip = self.state.last.as_ref();
        let mut below: Vec<(u32, String)> = self
            .state
            .ring
            .iter()
            .filter(|e| Some(*e) != tip)
            .cloned()
            .collect();
        below.sort_by_key(|entry| std::cmp::Reverse(entry.0));
        below
    }

    fn remember(&mut self, height: u32, hash: &str) {
        self.state.ring.retain(|(h, _)| *h != height);
        self.state.ring.push((height, hash.to_string()));
        while self.state.ring.len() > HEADER_RING_LEN {
            self.state.ring.remove(0);
        }
    }

    fn forget(&mut self, height: u32, hash: &str) {
        self.state
            .ring
            .retain(|(h, x)| !(*h == height && x.eq_ignore_ascii_case(hash)));
    }

    pub fn observe(&mut self, height: u32, hash: &str) -> HeaderEvents {
        let hash = hash.to_ascii_lowercase();
        let mut ev = HeaderEvents::default();
        match self.state.last.clone() {
            None => {
                // The first header: queued, processed one cycle later.
                self.state.last = Some((height, hash.clone()));
                self.state.queued = Some((height, hash.clone()));
                self.remember(height, &hash);
                ev.queued = Some((height, hash));
            }
            Some((last_height, _)) if height < last_height => {
                // An old header: the reference reverts to the higher one.
                ev.old_header = Some((height, hash));
            }
            Some((last_height, _)) if height > last_height => {
                self.state.last = Some((height, hash.clone()));
                self.state.queued = Some((height, hash.clone()));
                self.remember(height, &hash);
                ev.queued = Some((height, hash));
                ev.check_ring = true;
            }
            Some((last_height, last_hash)) if last_hash != hash => {
                // A reorg header: the same height, a different block.
                ev.reorg_header = true;
                ev.deactivated.push((last_height, last_hash.clone()));
                self.forget(last_height, &last_hash);
                self.state.last = Some((height, hash.clone()));
                self.state.queued = Some((height, hash.clone()));
                self.remember(height, &hash);
                ev.queued = Some((height, hash));
                ev.check_ring = true;
            }
            Some(_) => {
                // The same tip as last cycle: a queued header survived.
                if let Some(q) = self.state.queued.take() {
                    ev.processed = Some(q);
                }
            }
        }
        ev
    }

    /// Reconcile the ring against the header service's `canonical` hashes
    /// (`None` when a height could not be read): walk the remembered tips
    /// below the current one from the newest down, deactivate every one whose
    /// hash no longer matches, and stop at the first match or the first
    /// unknown (an unknown never reads as a reorg). A deactivated height is
    /// re-remembered with its canonical hash. Returns the deactivated
    /// `(height, hash)` pairs, newest first.
    pub fn deactivate_mismatched(
        &mut self,
        canonical: &[(u32, Option<String>)],
    ) -> Vec<(u32, String)> {
        let mut out = Vec::new();
        for (height, remembered) in self.ring_below_tip_newest_first() {
            let known = canonical
                .iter()
                .find(|(h, _)| *h == height)
                .map(|(_, c)| c.as_deref());
            match known {
                Some(Some(c)) if c.eq_ignore_ascii_case(&remembered) => break,
                Some(Some(c)) => {
                    let c = c.to_ascii_lowercase();
                    self.forget(height, &remembered);
                    self.remember(height, &c);
                    out.push((height, remembered));
                }
                _ => break,
            }
        }
        out
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

/// The fewest providers that must answer cleanly "not mined" before a
/// stored proof the tracker refutes may be demoted.
pub const DEMOTION_WITNESSES: usize = 2;

/// What `reprove_anchor` did to one stored proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReproveOutcome {
    /// A validated proof for the canonical block replaced the stored one
    /// (stored through the ingest funnel).
    Replaced { height: u32, block_hash: String },
    /// Retained, retry later: the providers still name the stored block, the
    /// tracker refutes the provider's path, or the tracker still confirms
    /// the stored root.
    Unchanged,
    /// The replacement's block is above the proof LAG gate; retry once the
    /// header has aged.
    Deferred { height: u32 },
    /// Positive evidence: the tracker refutes the stored root, at least
    /// [`DEMOTION_WITNESSES`] providers answered cleanly "not mined", and no
    /// provider served a path. The proof was demoted (the transaction is
    /// unproven again, its bytes kept, re-proved later).
    Demoted,
    /// Anything else (a network fault, a tracker fault, too few witnesses):
    /// retained, retry later.
    TransientError(String),
}

fn same_anchor(anchor: &ProvenTxAnchor, height: u32, block_hash: &str, merkle_root: &str) -> bool {
    if anchor.height != height {
        return false;
    }
    if !anchor.block_hash.is_empty() && !block_hash.is_empty() {
        return anchor.block_hash.eq_ignore_ascii_case(block_hash);
    }
    anchor.merkle_root.eq_ignore_ascii_case(merkle_root)
}

/// Re-prove one stored anchor against the chain: ask the providers for the
/// transaction's current merkle path (validated against the chain tracker
/// by the services layer), replace the stored proof through the ingest
/// funnel when it names a different block, retain it when the providers
/// still name the stored block or fault, and demote it ONLY on positive
/// evidence (see [`ReproveOutcome::Demoted`]). The reference's
/// `reproveProven`, plus the stated demotion.
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
    if let (Some(path_hex), Some(header)) = (&result.merkle_path, &result.header) {
        let bytes = match hex::decode(path_hex) {
            Ok(b) => b,
            Err(e) => {
                return ReproveOutcome::TransientError(format!("provider path is not hex: {e}"))
            }
        };
        // The bump's own height is the authority on the height it proves.
        let height = MerklePath::from_binary(&bytes)
            .map(|mp| mp.block_height)
            .unwrap_or(header.height);
        if same_anchor(anchor, height, &header.hash, &header.merkle_root) {
            // The providers still name the stored block: retained, retried.
            return ReproveOutcome::Unchanged;
        }
        return match storage
            .ingest_push_proof(&anchor.txid, &bytes, height, &header.hash)
            .await
        {
            Ok(Some(ProofIngestOutcome::Ingested(_))) => ReproveOutcome::Replaced {
                height,
                block_hash: header.hash.to_ascii_lowercase(),
            },
            Ok(Some(ProofIngestOutcome::DeferredAboveProcessedHeight { block_height, .. })) => {
                ReproveOutcome::Deferred {
                    height: block_height,
                }
            }
            // The storage's tracker refutes the provider's path: retained.
            Ok(Some(ProofIngestOutcome::InvalidMerkleRoot { .. })) => ReproveOutcome::Unchanged,
            Ok(Some(ProofIngestOutcome::TrackerError(e)))
            | Ok(Some(ProofIngestOutcome::InvalidProof(e))) => ReproveOutcome::TransientError(e),
            Ok(None) => {
                ReproveOutcome::TransientError("storage does not ingest proofs".to_string())
            }
            Err(e) => ReproveOutcome::TransientError(e.to_string()),
        };
    }

    // No provider served a path. Demote only on positive evidence: the
    // tracker must DEFINITELY refute the stored root (a fault is never a
    // verdict), and at least DEMOTION_WITNESSES providers must have answered
    // cleanly "not mined"; a provider whose path the tracker refuted means
    // "unchanged, retry".
    let tracker = match services.get_chain_tracker().await {
        Ok(t) => t,
        Err(e) => {
            return ReproveOutcome::TransientError(format!(
                "no chain tracker to refute the stored proof: {e}"
            ))
        }
    };
    match tracker
        .is_valid_root_for_height(&anchor.merkle_root, anchor.height)
        .await
    {
        Ok(false) => {}
        Ok(true) => return ReproveOutcome::Unchanged,
        Err(e) => return ReproveOutcome::TransientError(format!("chain tracker: {e}")),
    }
    let refuted = result.refuted_witnesses();
    if !refuted.is_empty() {
        return ReproveOutcome::Unchanged;
    }
    let witnesses = result.not_mined_witnesses();
    let faults = result.faults();
    if witnesses.len() >= DEMOTION_WITNESSES {
        match storage.demote_stale_proof(&anchor.txid).await {
            Ok(_) => ReproveOutcome::Demoted,
            Err(e) => ReproveOutcome::TransientError(e.to_string()),
        }
    } else {
        ReproveOutcome::TransientError(format!(
            "no positive evidence to demote: {} not-mined witness(es) {:?}, {} fault(s) {:?}",
            witnesses.len(),
            witnesses,
            faults.len(),
            faults
        ))
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
                    "reprove: the chain positively refutes the stored proof; demoted, the transaction is unproven again and will be re-proved"
                );
            }
            ReproveOutcome::TransientError(e) => self.errors.push(format!("{txid}: {e}")),
        }
    }

    /// Whether anything in the pass asks for another try (the reference
    /// retries on `unchanged` or `unavailable`).
    pub fn wants_retry(&self) -> bool {
        self.unchanged > 0 || self.deferred > 0 || !self.errors.is_empty()
    }
}

/// Re-prove every stored anchor citing the deactivated block `block_hash` at
/// `height`, plus every hash-less anchor at that height whose root is not
/// the canonical root there (rows written without a header in hand). The
/// reorg task's unit of work for one deactivated header.
pub async fn reprove_block_hash<S, V>(
    storage: &S,
    services: &V,
    block_hash: &str,
    height: u32,
) -> ReproveTally
where
    S: MonitorStorage + ?Sized,
    V: WalletServices + ?Sized,
{
    let mut tally = ReproveTally::default();
    let mut anchors = match storage.find_proven_txs_by_block_hash(block_hash).await {
        Ok(a) => a,
        Err(e) => {
            tally
                .errors
                .push(format!("find_proven_txs_by_block_hash: {e}"));
            return tally;
        }
    };
    if let Ok(bytes) = services.get_header_for_height(height).await {
        if let Some(canonical_root) = merkle_root_of_header(&bytes) {
            match storage.find_proven_txs_in_heights(height, height).await {
                Ok(at_height) => {
                    for a in at_height {
                        let already = anchors.iter().any(|x| x.txid == a.txid);
                        if !already
                            && a.block_hash.is_empty()
                            && !canonical_root.eq_ignore_ascii_case(&a.merkle_root)
                        {
                            anchors.push(a);
                        }
                    }
                }
                Err(e) => tally
                    .errors
                    .push(format!("find_proven_txs_in_heights: {e}")),
            }
        }
    }
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

    #[test]
    fn the_first_observation_queues_and_processes_nothing() {
        let mut t = HeaderTracker::new();
        let ev = t.observe(965771, "AA");
        assert_eq!(ev.queued, Some((965771, "aa".to_string())));
        assert_eq!(ev.processed, None);
        assert!(!ev.reorg_header);
        assert!(!ev.check_ring);
        assert_eq!(t.queued(), Some(&(965771, "aa".to_string())));
    }

    #[test]
    fn a_header_is_processed_only_after_a_full_cycle_as_tip() {
        let mut t = HeaderTracker::new();
        t.observe(965771, "aa");
        let ev = t.observe(965771, "aa");
        assert_eq!(ev.processed, Some((965771, "aa".to_string())));
        assert!(t.queued().is_none());
        // A third identical cycle processes nothing new.
        let ev = t.observe(965771, "aa");
        assert_eq!(ev.processed, None);
        assert!(ev.deactivated.is_empty());
    }

    #[test]
    fn a_new_height_replaces_the_queue_and_waits_its_own_cycle() {
        let mut t = HeaderTracker::new();
        t.observe(965771, "aa");
        let ev = t.observe(965772, "bb");
        assert_eq!(ev.queued, Some((965772, "bb".to_string())));
        assert_eq!(ev.processed, None);
        assert!(ev.check_ring, "a new height reconciles the ring");
        let ev = t.observe(965772, "bb");
        assert_eq!(ev.processed, Some((965772, "bb".to_string())));
    }

    #[test]
    fn a_same_height_hash_change_is_a_reorg_header_that_deactivates_the_old_hash() {
        // The 2026-09-07 event: the 34 MB block at 965771 was the tip for a
        // few seconds, then a different block at the SAME height won.
        let mut t = HeaderTracker::new();
        t.observe(965771, "153e10f4");
        t.observe(965771, "153e10f4"); // processed
        let ev = t.observe(965771, "1de5aa96");
        assert!(ev.reorg_header);
        assert_eq!(ev.deactivated, vec![(965771, "153e10f4".to_string())]);
        assert_eq!(ev.queued, Some((965771, "1de5aa96".to_string())));
        assert!(ev.check_ring);
        // The replacement must itself survive a cycle.
        let ev = t.observe(965771, "1de5aa96");
        assert_eq!(ev.processed, Some((965771, "1de5aa96".to_string())));
        // The ring holds the winner only.
        assert_eq!(t.state().ring, vec![(965771, "1de5aa96".to_string())]);
    }

    #[test]
    fn a_tip_decrease_is_an_old_header_and_changes_nothing() {
        let mut t = HeaderTracker::new();
        t.observe(965773, "0851a554");
        t.observe(965773, "0851a554");
        let before = t.state().clone();
        let ev = t.observe(965772, "cc");
        assert_eq!(ev.old_header, Some((965772, "cc".to_string())));
        assert!(!ev.reorg_header);
        assert!(ev.deactivated.is_empty());
        assert!(!ev.check_ring);
        assert_eq!(ev.processed, None);
        assert_eq!(ev.queued, None);
        assert_eq!(t.state(), &before, "reverted: the higher header stays");
    }

    #[test]
    fn the_ring_walk_deactivates_a_replaced_previous_tip_and_stops_at_the_first_match() {
        // A@H, then B'@H+1 on top of A'@H: the previous tip was replaced
        // under the new one. The header service now says A' at H and the
        // remembered G at H-1 still matches.
        let mut t = HeaderTracker::new();
        t.observe(100, "gg");
        t.observe(100, "gg");
        t.observe(101, "aa");
        t.observe(101, "aa");
        let ev = t.observe(102, "b1");
        assert!(ev.check_ring);
        assert!(ev.deactivated.is_empty(), "nothing known yet");
        let canonical = vec![(101, Some("a1".to_string())), (100, Some("gg".to_string()))];
        let deactivated = t.deactivate_mismatched(&canonical);
        assert_eq!(deactivated, vec![(101, "aa".to_string())]);
        assert!(
            t.state().ring.contains(&(101, "a1".to_string())),
            "the canonical hash replaces the deactivated one in the ring"
        );
        assert!(t.state().ring.contains(&(100, "gg".to_string())));
        // A second walk with the same answers deactivates nothing.
        assert!(t.deactivate_mismatched(&canonical).is_empty());
    }

    #[test]
    fn the_ring_walk_stops_at_an_unknown_height() {
        let mut t = HeaderTracker::new();
        t.observe(100, "gg");
        t.observe(101, "aa");
        t.observe(102, "bb");
        // Height 101 could not be read: the walk stops there, 100 is not
        // judged even though it would mismatch.
        let canonical = vec![(101, None), (100, Some("zz".to_string()))];
        assert!(t.deactivate_mismatched(&canonical).is_empty());
        assert_eq!(t.state().ring.len(), 3);
    }

    #[test]
    fn the_ring_keeps_the_last_twelve_tips() {
        let mut t = HeaderTracker::new();
        for h in 0..20u32 {
            t.observe(h, &format!("h{h}"));
        }
        assert_eq!(t.state().ring.len(), HEADER_RING_LEN);
        assert_eq!(t.state().ring.first(), Some(&(8, "h8".to_string())));
        assert_eq!(
            t.ring_below_tip_newest_first().first(),
            Some(&(18, "h18".to_string()))
        );
    }

    #[test]
    fn the_tracker_state_round_trips_lowercased() {
        let mut t = HeaderTracker::new();
        t.observe(5, "AA");
        let state = t.state().clone();
        let again = HeaderTracker::from_state(HeaderTrackerState {
            last: Some((5, "AA".into())),
            queued: Some((5, "AA".into())),
            ring: vec![(5, "AA".into())],
        });
        assert_eq!(again.state(), &state);
        let json = serde_json::to_string(&state).unwrap();
        let back: HeaderTrackerState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, state);
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
    fn the_tally_counts_every_outcome_and_asks_to_retry_on_the_right_ones() {
        let mut t = ReproveTally::default();
        t.record(
            "a",
            &ReproveOutcome::Replaced {
                height: 1,
                block_hash: "x".into(),
            },
        );
        assert!(!t.wants_retry(), "a replacement never requeues");
        t.record("d", &ReproveOutcome::Demoted);
        assert!(!t.wants_retry(), "a demotion never requeues");
        t.record("b", &ReproveOutcome::Unchanged);
        assert!(t.wants_retry());
        let mut u = ReproveTally::default();
        u.record("c", &ReproveOutcome::Deferred { height: 2 });
        assert!(u.wants_retry());
        let mut v = ReproveTally::default();
        v.record("e", &ReproveOutcome::TransientError("boom".into()));
        assert!(v.wants_retry());
        assert_eq!(v.errors, vec!["e: boom".to_string()]);
        assert_eq!((t.replaced, t.unchanged, t.demoted), (1, 1, 1));
    }
}
