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
//! Ours keeps that shape under the stack's reorg rule (rulings 5 and 7 of
//! 2026-10-20; the stated divergences are in `docs/REORG-DIVERGENCES.md`). A
//! stored proof is stale when, and only when, its root is not the active
//! header's root at its height in the wallet's own headers. On a deactivated
//! header the monitor rechecks the proofs it touches with one local header
//! read per height and re-proves only those whose root moved, a few a pass
//! ([`REPROVES_PER_PASS`]); a reorg that keeps the root moves nothing. A proof
//! whose root moved and has no replacement now is demoted: the action is
//! `unproven` again ("confirming"), its inputs stay allocated to it, its
//! outputs stay spendable as an unproven creator's are, nothing is released
//! and nothing is failed. The courier is then asked once for the transaction
//! ([`reask_lost_proof`]) and told once only when no broadcaster knows it.
//! This crate does not host bsv-tracker, so the rule is written here. The
//! deactivated headers come from a ring of recently observed tips compared
//! with the header service, because our chaintracks pushes no reorg event,
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

use crate::services::{BroadcastStatus, WalletServices};
use crate::storage::broadcast::BroadcastOutcome;
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

/// How many stored proofs whose root moved one pass re-proves (ruling 5 of
/// 2026-10-20: a few a pass, never every row at or above the fork). The rest
/// wait for the next pass of the same header.
pub const REPROVES_PER_PASS: usize = 4;

/// What the local header read says of one stored proof (ruling 5: a stored
/// proof is stale when, and only when, its root is not the active header's
/// root at its height in the wallet's own headers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootRead {
    /// The active header at the proof's height carries the stored root:
    /// nothing moves, whatever block hash the proof cites.
    Kept,
    /// The active header at the proof's height carries another root: the
    /// proof is stale.
    Moved,
    /// No header could be read at that height: an unknown never reads as
    /// stale.
    Unknown,
}

/// Read one stored anchor against the active root at its height (`None`
/// when the header could not be read).
pub fn read_root(anchor: &ProvenTxAnchor, active_root: Option<&str>) -> RootRead {
    match active_root {
        Some(root) if root.eq_ignore_ascii_case(&anchor.merkle_root) => RootRead::Kept,
        Some(_) => RootRead::Moved,
        None => RootRead::Unknown,
    }
}

/// The active root at `height` in the wallet's own headers (one header
/// read), or `None` when the header service cannot answer.
pub async fn active_root_at<V>(services: &V, height: u32) -> Option<String>
where
    V: WalletServices + ?Sized,
{
    let bytes = services.get_header_for_height(height).await.ok()?;
    merkle_root_of_header(&bytes)
}

/// What the courier's status read decided for a transaction whose proof was
/// just lost (ruling 7d of 2026-10-20: the re-announce is evidence-driven).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CourierWord {
    /// A broadcaster holds the transaction (accepted, seen or mined): the
    /// matter ends, nothing is announced.
    Known,
    /// Every broadcaster that answered refused it: the refusal is written,
    /// nothing is announced.
    Refused(Vec<String>),
    /// Every broadcaster that answered does not know it: one announce.
    Unknown,
    /// No broadcaster answered: nothing is announced; the proof pass's
    /// status read goes on asking.
    NoAnswer,
}

/// Decide from the broadcasters' status answers (pure). A broadcaster that
/// knows the transaction outranks one that refuses it, and a refusal
/// outranks one that does not know it.
pub fn courier_word(statuses: &[(String, BroadcastStatus)]) -> CourierWord {
    if statuses.is_empty() {
        return CourierWord::NoAnswer;
    }
    if statuses.iter().any(|(_, s)| {
        matches!(
            s,
            BroadcastStatus::Accepted | BroadcastStatus::Seen | BroadcastStatus::Mined
        )
    }) {
        return CourierWord::Known;
    }
    let refusing: Vec<String> = statuses
        .iter()
        .filter(|(_, s)| *s == BroadcastStatus::Rejected)
        .map(|(p, _)| p.clone())
        .collect();
    if !refusing.is_empty() {
        return CourierWord::Refused(refusing);
    }
    CourierWord::Unknown
}

/// What the re-ask of a lost proof did (ruling 7d). It runs once per stale
/// episode: right after the demotion that starts the episode, never again
/// for the same proof row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LostProofReask {
    /// A broadcaster knows the transaction: no announce.
    Known,
    /// One announce, which a broadcaster took.
    Announced,
    /// A broadcaster refused it, at the status read or at the one announce:
    /// the refusal is written on the request's history and a competitor it
    /// names is queued for the proof ask. No status moves: the action is
    /// failed only on a competitor's checked proof.
    Refused,
    /// No broadcaster answered the status read: nothing announced.
    NoAnswer,
    /// The one announce could not be built or posted; it is not retried
    /// (the proof pass's status read goes on).
    AnnounceFailed(String),
}

/// The re-ask of a lost proof: the status read at the broadcasters, then at
/// most one announce, only when no broadcaster knows the transaction.
pub async fn reask_lost_proof<S, V>(storage: &S, services: &V, txid: &str) -> LostProofReask
where
    S: MonitorStorage + ?Sized,
    V: WalletServices + ?Sized,
{
    let statuses = services.get_broadcaster_statuses(txid).await;
    match courier_word(&statuses) {
        CourierWord::Known => LostProofReask::Known,
        CourierWord::NoAnswer => LostProofReask::NoAnswer,
        CourierWord::Refused(refusing) => {
            let verdict = format!("status read: refused by {}", refusing.join(", "));
            if let Err(e) = storage.note_lost_proof_refusal(txid, &verdict).await {
                tracing::warn!(txid = %txid, error = %e, "lost proof: the refusal could not be written");
            }
            LostProofReask::Refused
        }
        CourierWord::Unknown => match storage.announce_lost_proof(txid).await {
            Ok(outcome) if outcome.is_success() => LostProofReask::Announced,
            Ok(BroadcastOutcome::OrphanMempool { .. }) => LostProofReask::Announced,
            Ok(BroadcastOutcome::ServiceError { details }) => {
                LostProofReask::AnnounceFailed(details.join("; "))
            }
            Ok(_) => LostProofReask::Refused,
            Err(e) => LostProofReask::AnnounceFailed(e.to_string()),
        },
    }
}

/// What `reprove_anchor` did to one stored proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReproveOutcome {
    /// A validated proof for the active block replaced the stored one
    /// (stored through the ingest funnel).
    Replaced { height: u32, block_hash: String },
    /// The active header at the proof's height still carries the stored
    /// root: nothing moves (a reorg that keeps the root changes nothing).
    Unchanged,
    /// The stored root is gone from the wallet's headers and no replacement
    /// could be stored now: the proof was demoted (the transaction is
    /// `unproven` again, its bytes kept, its inputs still allocated to it,
    /// its outputs spendable as an unproven creator's are), and the courier
    /// was re-asked once.
    Demoted { reask: LostProofReask },
    /// The header could not be read, or the demotion could not be written:
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

/// Ask the providers once for the transaction's current path and store it
/// through the ingest funnel when it names another block. The stored
/// height and block hash when a replacement was stored now.
async fn store_replacement<S, V>(
    storage: &S,
    services: &V,
    anchor: &ProvenTxAnchor,
) -> Option<(u32, String)>
where
    S: MonitorStorage + ?Sized,
    V: WalletServices + ?Sized,
{
    let result = match services.get_merkle_path(&anchor.txid, false).await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(txid = %anchor.txid, error = %e, "reprove: no replacement path");
            return None;
        }
    };
    let (Some(path_hex), Some(header)) = (&result.merkle_path, &result.header) else {
        return None;
    };
    let bytes = hex::decode(path_hex).ok()?;
    // The bump's own height is the authority on the height it proves.
    let height = MerklePath::from_binary(&bytes)
        .map(|mp| mp.block_height)
        .unwrap_or(header.height);
    if same_anchor(anchor, height, &header.hash, &header.merkle_root) {
        // The providers still name the block our headers no longer carry.
        return None;
    }
    match storage
        .ingest_push_proof(&anchor.txid, &bytes, height, &header.hash)
        .await
    {
        Ok(Some(ProofIngestOutcome::Ingested(_))) => {
            Some((height, header.hash.to_ascii_lowercase()))
        }
        other => {
            tracing::debug!(
                txid = %anchor.txid,
                outcome = ?other.map(|o| o.map(|o| format!("{o:?}"))),
                "reprove: the replacement was not stored now; the proof pass will take it"
            );
            None
        }
    }
}

/// Settle one stored anchor whose root the local header read found gone
/// (ruling 5): store a replacement when a provider serves one for the
/// active block, else demote the proof (rulings 7a and 7b: the action back
/// at `unproven`, nothing released, nothing failed) and re-ask the courier
/// once (7d). The header read alone decides the proof is stale; no
/// provider's word is needed to demote it.
pub async fn reprove_moved_anchor<S, V>(
    storage: &S,
    services: &V,
    anchor: &ProvenTxAnchor,
) -> ReproveOutcome
where
    S: MonitorStorage + ?Sized,
    V: WalletServices + ?Sized,
{
    if let Some((height, block_hash)) = store_replacement(storage, services, anchor).await {
        return ReproveOutcome::Replaced { height, block_hash };
    }
    match storage.demote_stale_proof(&anchor.txid).await {
        Ok(true) => {
            let reask = reask_lost_proof(storage, services, &anchor.txid).await;
            ReproveOutcome::Demoted { reask }
        }
        // Another pass or process already demoted it: the episode is theirs.
        Ok(false) => ReproveOutcome::Unchanged,
        Err(e) => ReproveOutcome::TransientError(e.to_string()),
    }
}

/// Re-prove one stored anchor: one local header read at its height (ruling
/// 5), nothing more when the active header still carries its root, and
/// [`reprove_moved_anchor`] when it does not. The CLI's `reproof` verb and
/// the monitor's tasks share it.
pub async fn reprove_anchor<S, V>(
    storage: &S,
    services: &V,
    anchor: &ProvenTxAnchor,
) -> ReproveOutcome
where
    S: MonitorStorage + ?Sized,
    V: WalletServices + ?Sized,
{
    let active = active_root_at(services, anchor.height).await;
    match read_root(anchor, active.as_deref()) {
        RootRead::Kept => ReproveOutcome::Unchanged,
        RootRead::Unknown => ReproveOutcome::TransientError(format!(
            "no header readable at height {}",
            anchor.height
        )),
        RootRead::Moved => reprove_moved_anchor(storage, services, anchor).await,
    }
}

/// The tally of one re-prove pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReproveTally {
    pub replaced: u32,
    pub unchanged: u32,
    pub demoted: u32,
    /// Stale anchors left for the next pass ([`REPROVES_PER_PASS`]).
    pub postponed: u32,
    /// Demotions after which the one announce was made (ruling 7d).
    pub announced: u32,
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
                    "reprove: the stored proof was replaced by the active block's"
                );
            }
            ReproveOutcome::Unchanged => self.unchanged += 1,
            ReproveOutcome::Demoted { reask } => {
                self.demoted += 1;
                if *reask == LostProofReask::Announced {
                    self.announced += 1;
                }
                tracing::info!(
                    txid = %txid,
                    reask = ?reask,
                    marker = "reorg_proof_demoted",
                    "reprove: the stored root is gone from our headers; the transaction is unproven again (confirming), its inputs kept, and will be re-proved"
                );
            }
            ReproveOutcome::TransientError(e) => self.errors.push(format!("{txid}: {e}")),
        }
    }

    /// Whether anything in the pass asks for another try: a stale anchor
    /// left for the next pass, or a fault. A kept root never asks again.
    pub fn wants_retry(&self) -> bool {
        self.postponed > 0 || !self.errors.is_empty()
    }
}

/// Settle the anchors whose roots the local header read found gone, at most
/// [`REPROVES_PER_PASS`] of them; the rest are counted as postponed.
pub async fn reprove_moved_anchors<S, V>(
    storage: &S,
    services: &V,
    moved: &[ProvenTxAnchor],
    tally: &mut ReproveTally,
) where
    S: MonitorStorage + ?Sized,
    V: WalletServices + ?Sized,
{
    for (i, anchor) in moved.iter().enumerate() {
        if i >= REPROVES_PER_PASS {
            tally.postponed += 1;
            continue;
        }
        let outcome = reprove_moved_anchor(storage, services, anchor).await;
        tally.record(&anchor.txid, &outcome);
    }
}

/// Recheck every stored anchor citing the deactivated block `block_hash` at
/// `height`, plus every hash-less anchor at that height, against the active
/// root there (one local header read for the height). An anchor whose root
/// the active header still carries moves nothing; one whose root moved is
/// settled by [`reprove_moved_anchor`], a few a pass. The reorg task's unit
/// of work for one deactivated header.
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
    match storage.find_proven_txs_in_heights(height, height).await {
        Ok(at_height) => {
            for a in at_height {
                if a.block_hash.is_empty() && !anchors.iter().any(|x| x.txid == a.txid) {
                    anchors.push(a);
                }
            }
        }
        Err(e) => tally
            .errors
            .push(format!("find_proven_txs_in_heights: {e}")),
    }
    let mut active: HashMap<u32, Option<String>> = HashMap::new();
    let mut moved = Vec::new();
    for anchor in anchors {
        let root = match active.get(&anchor.height) {
            Some(root) => root.clone(),
            None => {
                let root = active_root_at(services, anchor.height).await;
                active.insert(anchor.height, root.clone());
                root
            }
        };
        match read_root(&anchor, root.as_deref()) {
            RootRead::Kept => tally.record(&anchor.txid, &ReproveOutcome::Unchanged),
            RootRead::Unknown => tally.errors.push(format!(
                "{}: no header readable at height {}",
                anchor.txid, anchor.height
            )),
            RootRead::Moved => moved.push(anchor),
        }
    }
    reprove_moved_anchors(storage, services, &moved, &mut tally).await;
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
        t.record(
            "d",
            &ReproveOutcome::Demoted {
                reask: LostProofReask::Announced,
            },
        );
        assert!(!t.wants_retry(), "a demotion never requeues");
        // Ruling 5: a root the active header still carries moves nothing and
        // is never asked again.
        t.record("b", &ReproveOutcome::Unchanged);
        assert!(!t.wants_retry());
        let u = ReproveTally {
            postponed: 1,
            ..Default::default()
        };
        assert!(u.wants_retry(), "a stale proof left for the next pass");
        let mut v = ReproveTally::default();
        v.record("e", &ReproveOutcome::TransientError("boom".into()));
        assert!(v.wants_retry());
        assert_eq!(v.errors, vec!["e: boom".to_string()]);
        assert_eq!(
            (t.replaced, t.unchanged, t.demoted, t.announced),
            (1, 1, 1, 1)
        );
    }

    /// Ruling 5 (2026-10-20): stale when, and only when, the stored root is
    /// not the active root at its height; an unreadable header is never
    /// stale.
    #[test]
    fn ruling_5_the_root_read_decides_stale_and_an_unknown_never_does() {
        let a = anchor("t", 7, "old-hash", "AB");
        assert_eq!(read_root(&a, Some("ab")), RootRead::Kept);
        assert_eq!(read_root(&a, Some("cd")), RootRead::Moved);
        assert_eq!(read_root(&a, None), RootRead::Unknown);
    }

    /// Ruling 7d (2026-10-20), draft step 4: a broadcaster that knows the
    /// transaction ends the matter, a refusal is written and never answered
    /// with an announce, only "nobody knows it" is announced, and no answer
    /// announces nothing.
    #[test]
    fn ruling_7d_step_4_the_courier_word_announces_only_on_unknown() {
        let p = |s: BroadcastStatus| ("ARC".to_string(), s);
        assert_eq!(courier_word(&[]), CourierWord::NoAnswer);
        assert_eq!(
            courier_word(&[p(BroadcastStatus::Seen)]),
            CourierWord::Known
        );
        assert_eq!(
            courier_word(&[p(BroadcastStatus::Unknown), p(BroadcastStatus::Accepted)]),
            CourierWord::Known
        );
        assert_eq!(
            courier_word(&[p(BroadcastStatus::Unknown)]),
            CourierWord::Unknown
        );
        assert_eq!(
            courier_word(&[p(BroadcastStatus::Unknown), p(BroadcastStatus::Rejected)]),
            CourierWord::Refused(vec!["ARC".to_string()])
        );
        assert_eq!(
            courier_word(&[p(BroadcastStatus::Rejected), p(BroadcastStatus::Mined)]),
            CourierWord::Known
        );
    }
}
