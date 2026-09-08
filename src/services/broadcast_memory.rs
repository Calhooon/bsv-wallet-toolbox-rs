//! Broadcast acceptance memory ("admit-fast" broadcasting).
//!
//! Every new transaction used to be broadcast as a full BEEF carrying the
//! whole unproven change chain (measured on mainnet 2026-09-01: 29 unproven
//! ancestors, ~205 KB per action). Classic ARC providers re-validate that
//! package on every submit: TAAL errors in under a second, GorillaPool spends
//! 7 to 20 s verifying merkle roots and then gives up (`469 ... BEEF
//! verification timed out`), and a later provider accepts the very same tx in
//! ~0.3 s. The network already has every ancestor: each one was accepted
//! seconds or minutes earlier as the subject of its own broadcast. Re-sending
//! and re-verifying 200 KB per action is the whole cost.
//!
//! [`BroadcastMemory`] remembers, per broadcast provider, which txids that
//! provider has already accepted or seen, so [`Services::post_beef`] can
//!
//! 1. send only the transactions a provider has NOT seen (EF of the subject
//!    alone when every unproven ancestor is known, an EF batch of the unseen
//!    ancestors plus the subject otherwise), with a one-shot full-package
//!    fallback when a reduced send is refused for what reads as a missing
//!    parent;
//! 2. try the provider that accepted the previous broadcast first (sticky
//!    provider order, persisted across restarts).
//!
//! # What counts as "seen" (0.3.58)
//!
//! 0.3.56 let a provider's ACCEPTANCE (HTTP 202 / `RECEIVED`) stand in for
//! network presence. On 2026-09-02 (beta, real sats) four wallets sent EF
//! children alone after Arcade had 202'd their parents; the parents had never
//! propagated, so the children were orphans forever. An EF child validates on
//! its own (its parent's output is inline), which is exactly why the
//! broadcaster's acceptance says nothing about the parent being on the
//! network. Acceptance is not network evidence; only `SEEN_ON_NETWORK`,
//! `SEEN_MULTIPLE_NODES`, `MINED` or a chain proof are.
//!
//! So the memory keeps five statuses on a ladder ([`BroadcastStatus`]):
//! `accepted` is remembered but NEVER skips an ancestor; only `seen` and
//! `mined` do ([`BroadcastStatus::is_network_evidence`]). A provider-specific
//! `rejected` / `unknown` row vetoes the network's row for that provider. A
//! `seen` record older than [`BROADCAST_SEEN_STALE_SECS`] is re-checked
//! against Arcade (`GET /tx/{txid}`) before it is trusted for a reduced send
//! ([`oldest_stale_ancestor`]).
//!
//! Nothing changes for a `Services` instance without an attached memory: the
//! full package goes out in the static provider order, exactly as before.
//! [`StorageSqlx`](crate::storage::StorageSqlx) provides the persisted
//! implementation (`broadcast_seen` / `broadcast_prefs` tables, migration
//! `002_broadcast_seen`); [`InMemoryBroadcastMemory`] is the process-local
//! variant used by tests and by callers without a database.
//!
//! [`Services::post_beef`]: crate::services::Services

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{Error, Result};

/// A poisoned mutex is reported, never unwrapped.
fn poisoned<T>(_: std::sync::PoisonError<T>) -> Error {
    Error::Internal("broadcast memory mutex poisoned".to_string())
}

/// `broadcast_seen.status`: the provider accepted the txid on a submit
/// (any non-error ARC `txStatus`, an Arcade `202`, an Arcade `/txs`
/// duplicate). Remembered for diagnostics and the sticky order; NEVER a
/// reason to skip an ancestor.
pub const BROADCAST_STATUS_ACCEPTED: &str = "accepted";

/// `broadcast_seen.status`: a push channel (SSE / webhook), a status probe or
/// a presence probe reported the txid `SEEN_ON_NETWORK` (or further along).
/// Network evidence: qualifies for a reduced send.
pub const BROADCAST_STATUS_SEEN: &str = "seen";

/// `broadcast_seen.status`: the txid is proven (mined). Terminal: never
/// downgraded by a later record.
pub const BROADCAST_STATUS_MINED: &str = "mined";

/// `broadcast_seen.status`: a definitive negative verdict (`REJECTED`,
/// `DOUBLE_SPEND_ATTEMPTED`, or the wallet retired the transaction as a
/// phantom). Overrides `accepted` and `seen`; only `mined` survives it.
pub const BROADCAST_STATUS_REJECTED: &str = "rejected";

/// `broadcast_seen.status`: a probe found the txid absent (404) or not seen
/// where the memory said it was. The memory was stale: the ancestor goes
/// back into the package. `seen_at` of an `unknown` row is the FIRST time
/// the absence was observed (the absence clock a reconciler reads).
pub const BROADCAST_STATUS_UNKNOWN: &str = "unknown";

/// A `seen` record older than this (seconds) is re-checked against the
/// provider before an ancestor is skipped on its strength.
pub const BROADCAST_SEEN_STALE_SECS: i64 = 600;

/// The pseudo-provider for facts about the network as a whole (a seen report
/// from a peer node or an unspecified plane). Rows recorded under it count
/// toward EVERY provider's seen set unless that provider has its own
/// negative row.
pub const BROADCAST_PROVIDER_NETWORK: &str = "network";

/// The pseudo-provider for facts a CHAIN INDEX (or a validated merkle proof)
/// established: WhatsOnChain / Bitails holding the transaction, a proof
/// ingested. Rows recorded under it count toward every provider like the
/// network rows, and they are the only evidence a reconciler trusts for an
/// unproven transaction older than its absence threshold: a broadcaster
/// reporting `SEEN_MULTIPLE_NODES` two hours after a transaction the chain
/// index never saw is not chain evidence (2026-09-02, w0).
pub const BROADCAST_PROVIDER_CHAIN: &str = "chain";

/// Whether `provider` is one of the pseudo-providers whose rows count for
/// every real provider ([`BROADCAST_PROVIDER_NETWORK`],
/// [`BROADCAST_PROVIDER_CHAIN`]).
pub fn is_global_provider(provider: &str) -> bool {
    provider == BROADCAST_PROVIDER_NETWORK || provider == BROADCAST_PROVIDER_CHAIN
}

/// `broadcast_prefs.key` holding the name of the provider that accepted the
/// most recent broadcast.
pub const PREF_LAST_ACCEPTED_PROVIDER: &str = "last_accepted_provider";

/// postBeef provider name: Arcade V2 (registered first when configured).
pub const PROVIDER_ARCADE_V2: &str = "ArcadeV2";
/// postBeef provider name: TAAL classic ARC.
pub const PROVIDER_TAAL_ARC: &str = "TaalArcBeef";
/// postBeef provider name: GorillaPool classic ARC.
pub const PROVIDER_GORILLAPOOL_ARC: &str = "GorillaPoolArcBeef";
/// postBeef provider name: Bitails.
pub const PROVIDER_BITAILS: &str = "Bitails";
/// postBeef provider name: WhatsOnChain.
pub const PROVIDER_WHATSONCHAIN: &str = "WhatsOnChain";

/// The status ladder of one `(txid, provider)` row.
///
/// Ordered by how much the row proves: `Unknown < Rejected < Accepted < Seen
/// < Mined`. Only `Seen` and `Mined` are network evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BroadcastStatus {
    /// Absent on a probe where the memory expected presence.
    Unknown,
    /// Definitively rejected / retired.
    Rejected,
    /// The provider took it on a submit. Not network evidence.
    Accepted,
    /// Seen on the network by a node.
    Seen,
    /// Proven (mined). Terminal.
    Mined,
}

impl BroadcastStatus {
    /// Parse a `broadcast_seen.status` value.
    pub fn parse(status: &str) -> Option<Self> {
        match status {
            BROADCAST_STATUS_UNKNOWN => Some(Self::Unknown),
            BROADCAST_STATUS_REJECTED => Some(Self::Rejected),
            BROADCAST_STATUS_ACCEPTED => Some(Self::Accepted),
            BROADCAST_STATUS_SEEN => Some(Self::Seen),
            BROADCAST_STATUS_MINED => Some(Self::Mined),
            _ => None,
        }
    }

    /// The `broadcast_seen.status` value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => BROADCAST_STATUS_UNKNOWN,
            Self::Rejected => BROADCAST_STATUS_REJECTED,
            Self::Accepted => BROADCAST_STATUS_ACCEPTED,
            Self::Seen => BROADCAST_STATUS_SEEN,
            Self::Mined => BROADCAST_STATUS_MINED,
        }
    }

    /// Whether a row at this status proves the network holds the tx (and so
    /// its parents connected): `seen` or `mined`. The only statuses that
    /// let a provider skip an ancestor.
    pub fn is_network_evidence(self) -> bool {
        matches!(self, Self::Seen | Self::Mined)
    }

    /// A negative verdict (`rejected` / `unknown`): for the provider that
    /// recorded it, the ancestor goes back into the package whatever the
    /// network row says.
    pub fn is_negative(self) -> bool {
        matches!(self, Self::Unknown | Self::Rejected)
    }

    /// The ladder status an Arcade V2 `txStatus` maps to: `SEEN_ON_NETWORK` /
    /// `SEEN_MULTIPLE_NODES` are `Seen`, `MINED` is `Mined`, `REJECTED` /
    /// `DOUBLE_SPEND_ATTEMPTED` are `Rejected`, everything else (`RECEIVED`,
    /// `SENT_TO_NETWORK`, `ACCEPTED_BY_NETWORK`, ...) is `Accepted`: the
    /// broadcaster holds it, the network has not vouched for it.
    pub fn from_arcade_status(tx_status: &str) -> Self {
        match tx_status {
            "SEEN_ON_NETWORK" | "SEEN_MULTIPLE_NODES" => Self::Seen,
            "MINED" | "IMMUTABLE" => Self::Mined,
            "REJECTED" | "DOUBLE_SPEND_ATTEMPTED" => Self::Rejected,
            _ => Self::Accepted,
        }
    }

    /// The ladder status a classic ARC `txStatus` maps to. `SEEN_ON_NETWORK`
    /// is `Seen`; `MINED` / `CONFIRMED` are `Mined`; `REJECTED` /
    /// `DOUBLE_SPEND_ATTEMPTED` are `Rejected`. `SEEN_IN_ORPHAN_MEMPOOL` is
    /// deliberately `Accepted`: the node holds the bytes but NOT the parent,
    /// which is the very failure the memory must never mistake for presence.
    pub fn from_arc_status(tx_status: &str) -> Self {
        match tx_status {
            "SEEN_ON_NETWORK" => Self::Seen,
            "MINED" | "CONFIRMED" => Self::Mined,
            "REJECTED" | "DOUBLE_SPEND_ATTEMPTED" => Self::Rejected,
            _ => Self::Accepted,
        }
    }
}

/// What recording `incoming` over `existing` does to a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LadderStep {
    /// The row stays exactly as it is (status and `seen_at`).
    Keep,
    /// Same status, fresh evidence: `seen_at` moves to now.
    Refresh,
    /// The row takes this status with `seen_at` = now.
    Set(BroadcastStatus),
}

/// THE LADDER. Monotone on the positive side (`accepted < seen < mined`, a
/// later weaker report never downgrades), `mined` terminal, negative verdicts
/// (`rejected`, `unknown`) overriding the positives below `mined`, and only
/// NETWORK evidence (`seen`, `mined`) superseding a negative verdict: a
/// provider accepting a re-sent package says nothing about a transaction
/// it rejected or lost, and must not reset the absence clock.
///
/// Repeating `unknown` keeps the row untouched so `seen_at` stays the FIRST
/// observed absence (the absence clock). Repeating `rejected` likewise.
/// Repeating `accepted` or `seen` refreshes `seen_at` (fresh evidence).
///
/// THE ONE SANCTIONED DOWNGRADE lives outside the ladder: the stale-proof
/// demotion (`StorageSqlx::demote_stale_proof`) forgets a `mined` row
/// through `broadcast_seen::forget_mined_on` when the chain positively
/// refutes the proof that produced it. A `mined` row was network evidence
/// of a block; when the block leaves the chain the evidence goes with it,
/// and a row left behind would make reduced sends omit the transaction as
/// txid-only while it is unproven again. No other path lowers a status.
pub fn ladder_step(existing: Option<BroadcastStatus>, incoming: BroadcastStatus) -> LadderStep {
    use BroadcastStatus::*;
    let Some(existing) = existing else {
        return LadderStep::Set(incoming);
    };
    if existing == Mined {
        return LadderStep::Keep;
    }
    match incoming {
        Mined => LadderStep::Set(Mined),
        Rejected => {
            if existing == Rejected {
                LadderStep::Keep
            } else {
                LadderStep::Set(Rejected)
            }
        }
        Unknown => {
            if existing.is_negative() {
                LadderStep::Keep
            } else {
                LadderStep::Set(Unknown)
            }
        }
        Seen => {
            if existing == Seen {
                LadderStep::Refresh
            } else {
                LadderStep::Set(Seen)
            }
        }
        Accepted => match existing {
            Accepted => LadderStep::Refresh,
            Unknown | Rejected | Seen | Mined => LadderStep::Keep,
        },
    }
}

/// One `broadcast_seen` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastSeenRecord {
    /// Transaction id.
    pub txid: String,
    /// Provider name, or [`BROADCAST_PROVIDER_NETWORK`].
    pub provider: String,
    /// The status string (see [`BroadcastStatus::parse`]).
    pub status: String,
    /// When the status was last set / refreshed (for `unknown`: first
    /// observed absence).
    pub seen_at: DateTime<Utc>,
}

impl BroadcastSeenRecord {
    /// The parsed status, if it is one the ladder knows.
    pub fn ladder_status(&self) -> Option<BroadcastStatus> {
        BroadcastStatus::parse(&self.status)
    }
}

/// The freshest network-evidence row that counts for `provider` on `txid`
/// (its own `seen`/`mined` row or the network's, unless its own row is a
/// negative verdict), from `records`.
fn qualifying_record<'a>(
    provider: &str,
    txid: &str,
    records: &'a [BroadcastSeenRecord],
) -> Option<&'a BroadcastSeenRecord> {
    let own = records
        .iter()
        .find(|r| r.txid == txid && r.provider == provider);
    let own_status = own.and_then(|r| r.ladder_status());
    if own_status.is_some_and(|s| s.is_negative()) {
        return None;
    }
    let global = records
        .iter()
        .filter(|r| r.txid == txid && is_global_provider(&r.provider));
    let mut best: Option<&BroadcastSeenRecord> = None;
    for candidate in own.into_iter().chain(global) {
        if candidate
            .ladder_status()
            .is_some_and(|s| s.is_network_evidence())
            && best.is_none_or(|b| candidate.seen_at > b.seen_at)
        {
            best = Some(candidate);
        }
    }
    best
}

/// The subset of `txids` that `provider` may skip, from the rows in
/// `records`: a `seen` / `mined` row of its own or of the network, unless
/// the provider's own row is `rejected` / `unknown`. An `accepted` row alone
/// never qualifies.
pub fn seen_set_from_records(
    provider: &str,
    txids: &[String],
    records: &[BroadcastSeenRecord],
) -> HashSet<String> {
    txids
        .iter()
        .filter(|txid| qualifying_record(provider, txid, records).is_some())
        .cloned()
        .collect()
}

/// Among `skipped` (the ancestors a reduced send would leave out, in BEEF
/// order), the one whose qualifying evidence is the OLDEST, when that
/// evidence is older than `stale_secs`. `None` when every skipped ancestor
/// has fresh evidence (or none qualifies at all). Ties keep BEEF order.
pub fn oldest_stale_ancestor(
    provider: &str,
    skipped: &[String],
    records: &[BroadcastSeenRecord],
    now: DateTime<Utc>,
    stale_secs: i64,
) -> Option<String> {
    let mut oldest: Option<(&String, DateTime<Utc>)> = None;
    for txid in skipped {
        let Some(record) = qualifying_record(provider, txid, records) else {
            continue;
        };
        if oldest.is_none_or(|(_, at)| record.seen_at < at) {
            oldest = Some((txid, record.seen_at));
        }
    }
    let (txid, seen_at) = oldest?;
    if (now - seen_at).num_seconds() > stale_secs {
        Some(txid.clone())
    } else {
        None
    }
}

/// Persisted memory of which transactions each broadcast provider has
/// already accepted or seen, plus small broadcast preferences (the sticky
/// provider choice).
///
/// Keys are the postBeef provider names used by
/// [`Services`](crate::services::Services)' provider collection
/// ([`PROVIDER_TAAL_ARC`], [`PROVIDER_ARCADE_V2`], ...) and the
/// [`BROADCAST_PROVIDER_NETWORK`] pseudo-provider.
///
/// Implementations must be cheap to read: the broadcast path issues one
/// record query per provider it actually tries (usually one) and one
/// preference read per broadcast.
#[async_trait]
pub trait BroadcastMemory: Send + Sync {
    /// Every row for `txids` recorded under `provider` or under the network
    /// pseudo-provider; every row of every provider when `provider` is
    /// `None`.
    async fn broadcast_records(
        &self,
        provider: Option<&str>,
        txids: &[String],
    ) -> Result<Vec<BroadcastSeenRecord>>;

    /// Record `status` for `(txid, provider)` through [`ladder_step`]:
    /// idempotent, never downgrades a positive, `mined` terminal.
    async fn record_broadcast_status(&self, txid: &str, provider: &str, status: &str)
        -> Result<()>;

    /// Record several txids for one provider and status.
    async fn record_broadcast_status_many(
        &self,
        provider: &str,
        status: &str,
        txids: &[String],
    ) -> Result<()> {
        for txid in txids {
            self.record_broadcast_status(txid, provider, status).await?;
        }
        Ok(())
    }

    /// The subset of `txids` that `provider` may skip: network evidence of
    /// its own or of the network, not vetoed by its own negative row
    /// ([`seen_set_from_records`]).
    async fn broadcast_seen_for(
        &self,
        provider: &str,
        txids: &[String],
    ) -> Result<HashSet<String>> {
        let records = self.broadcast_records(Some(provider), txids).await?;
        Ok(seen_set_from_records(provider, txids, &records))
    }

    /// The subset of `txids` with network evidence from ANY provider (or
    /// the network).
    async fn broadcast_seen_any(&self, txids: &[String]) -> Result<HashSet<String>> {
        let records = self.broadcast_records(None, txids).await?;
        Ok(records
            .iter()
            .filter(|r| r.ladder_status().is_some_and(|s| s.is_network_evidence()))
            .map(|r| r.txid.clone())
            .collect())
    }

    /// The row for `(txid, provider)`, if any.
    async fn broadcast_status_of(
        &self,
        txid: &str,
        provider: &str,
    ) -> Result<Option<BroadcastSeenRecord>> {
        let records = self
            .broadcast_records(Some(provider), std::slice::from_ref(&txid.to_string()))
            .await?;
        Ok(records.into_iter().find(|r| r.provider == provider))
    }

    /// Alias of [`BroadcastMemory::record_broadcast_status`] (0.3.56 name).
    async fn record_broadcast_seen(&self, txid: &str, provider: &str, status: &str) -> Result<()> {
        self.record_broadcast_status(txid, provider, status).await
    }

    /// Alias of [`BroadcastMemory::record_broadcast_status_many`] (0.3.56
    /// name).
    async fn record_broadcast_seen_many(
        &self,
        provider: &str,
        status: &str,
        txids: &[String],
    ) -> Result<()> {
        self.record_broadcast_status_many(provider, status, txids)
            .await
    }

    /// Read a broadcast preference (e.g. [`PREF_LAST_ACCEPTED_PROVIDER`]).
    async fn get_broadcast_pref(&self, key: &str) -> Result<Option<String>>;

    /// Write a broadcast preference (upsert).
    async fn set_broadcast_pref(&self, key: &str, value: &str) -> Result<()>;
}

/// `(txid, provider)` to `(status, seen_at)`.
type SeenRows = HashMap<(String, String), (String, DateTime<Utc>)>;

/// Process-local [`BroadcastMemory`]: a `HashMap` behind a mutex. Loses its
/// contents with the process; use the storage-backed implementation for a
/// served wallet.
#[derive(Debug, Default)]
pub struct InMemoryBroadcastMemory {
    seen: Mutex<SeenRows>,
    prefs: Mutex<HashMap<String, String>>,
}

impl InMemoryBroadcastMemory {
    /// Create an empty memory.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of (txid, provider) rows recorded. For tests and diagnostics.
    pub fn len(&self) -> usize {
        self.seen.lock().map(|m| m.len()).unwrap_or(0)
    }

    /// Whether nothing has been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The status recorded for `(txid, provider)`, if any.
    pub fn status_of(&self, txid: &str, provider: &str) -> Option<String> {
        self.seen.lock().ok().and_then(|m| {
            m.get(&(txid.to_string(), provider.to_string()))
                .map(|(s, _)| s.clone())
        })
    }

    /// The `seen_at` recorded for `(txid, provider)`, if any.
    pub fn seen_at_of(&self, txid: &str, provider: &str) -> Option<DateTime<Utc>> {
        self.seen.lock().ok().and_then(|m| {
            m.get(&(txid.to_string(), provider.to_string()))
                .map(|(_, at)| *at)
        })
    }

    /// [`BroadcastMemory::record_broadcast_status`] with an explicit clock
    /// (tests backdate evidence with it). The ladder applies exactly as for
    /// a live record.
    pub fn record_broadcast_status_at(
        &self,
        txid: &str,
        provider: &str,
        status: &str,
        at: DateTime<Utc>,
    ) -> Result<()> {
        let incoming = BroadcastStatus::parse(status).ok_or_else(|| {
            Error::InvalidArgument(format!("unknown broadcast status '{}'", status))
        })?;
        let mut seen = self.seen.lock().map_err(poisoned)?;
        let key = (txid.to_string(), provider.to_string());
        let existing = seen.get(&key).and_then(|(s, _)| BroadcastStatus::parse(s));
        match ladder_step(existing, incoming) {
            LadderStep::Keep => {}
            LadderStep::Refresh => {
                if let Some(entry) = seen.get_mut(&key) {
                    entry.1 = at;
                }
            }
            LadderStep::Set(status) => {
                seen.insert(key, (status.as_str().to_string(), at));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BroadcastMemory for InMemoryBroadcastMemory {
    async fn broadcast_records(
        &self,
        provider: Option<&str>,
        txids: &[String],
    ) -> Result<Vec<BroadcastSeenRecord>> {
        let seen = self.seen.lock().map_err(poisoned)?;
        let wanted: HashSet<&String> = txids.iter().collect();
        Ok(seen
            .iter()
            .filter(|((txid, prov), _)| {
                wanted.contains(txid)
                    && provider.is_none_or(|p| prov == p || is_global_provider(prov))
            })
            .map(|((txid, prov), (status, at))| BroadcastSeenRecord {
                txid: txid.clone(),
                provider: prov.clone(),
                status: status.clone(),
                seen_at: *at,
            })
            .collect())
    }

    async fn record_broadcast_status(
        &self,
        txid: &str,
        provider: &str,
        status: &str,
    ) -> Result<()> {
        self.record_broadcast_status_at(txid, provider, status, Utc::now())
    }

    async fn get_broadcast_pref(&self, key: &str) -> Result<Option<String>> {
        let prefs = self.prefs.lock().map_err(poisoned)?;
        Ok(prefs.get(key).cloned())
    }

    async fn set_broadcast_pref(&self, key: &str, value: &str) -> Result<()> {
        let mut prefs = self.prefs.lock().map_err(poisoned)?;
        prefs.insert(key.to_string(), value.to_string());
        Ok(())
    }
}

/// The unproven transactions (no BUMP, not txid-only) carried by `beef`
/// other than `subject`, in BEEF order. These are the txids whose seen state
/// decides whether a provider can take a reduced send. An unparseable BEEF
/// yields an empty list (the providers then behave exactly as before).
pub fn unproven_ancestors_in_beef(beef: &[u8], subject: &str) -> Vec<String> {
    match bsv_rs::transaction::Beef::from_binary(beef) {
        Ok(parsed) => parsed
            .txs
            .iter()
            .filter(|btx| btx.bump_index().is_none() && !btx.is_txid_only())
            .map(|btx| btx.txid())
            .filter(|txid| txid != subject)
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Reorder an owned provider list `(service_name, provider_name, service)`
/// so that `sticky` is tried first.
///
/// The static order is otherwise preserved. When `guard_first` (Arcade, the
/// explicitly configured primary) currently heads the list, a sticky choice
/// other than `guard_first` slots in right BEHIND it: the sticky choice never
/// moves a provider ahead of Arcade unless it IS Arcade. An unknown `sticky`
/// name is ignored.
pub fn apply_sticky_provider_order<S>(
    services: &mut Vec<(String, String, S)>,
    sticky: &str,
    guard_first: &str,
) {
    let Some(pos) = services.iter().position(|(_, name, _)| name == sticky) else {
        return;
    };
    let target = if sticky != guard_first
        && services
            .first()
            .map(|(_, name, _)| name == guard_first)
            .unwrap_or(false)
    {
        1
    } else {
        0
    };
    if pos <= target {
        return;
    }
    let entry = services.remove(pos);
    services.insert(target, entry);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn names(v: &[(String, String, ())]) -> Vec<&str> {
        v.iter().map(|(_, n, _)| n.as_str()).collect()
    }

    fn list(names: &[&str]) -> Vec<(String, String, ())> {
        names
            .iter()
            .map(|n| ("postBeef".to_string(), n.to_string(), ()))
            .collect()
    }

    fn rec(txid: &str, provider: &str, status: &str, age_secs: i64) -> BroadcastSeenRecord {
        BroadcastSeenRecord {
            txid: txid.to_string(),
            provider: provider.to_string(),
            status: status.to_string(),
            seen_at: Utc::now() - Duration::seconds(age_secs),
        }
    }

    #[test]
    fn sticky_moves_the_accepting_provider_first() {
        let mut v = list(&[
            PROVIDER_TAAL_ARC,
            PROVIDER_GORILLAPOOL_ARC,
            PROVIDER_BITAILS,
            PROVIDER_WHATSONCHAIN,
        ]);
        apply_sticky_provider_order(&mut v, PROVIDER_BITAILS, PROVIDER_ARCADE_V2);
        assert_eq!(
            names(&v),
            vec![
                PROVIDER_BITAILS,
                PROVIDER_TAAL_ARC,
                PROVIDER_GORILLAPOOL_ARC,
                PROVIDER_WHATSONCHAIN
            ]
        );
    }

    #[test]
    fn sticky_never_moves_ahead_of_a_configured_arcade() {
        let mut v = list(&[
            PROVIDER_ARCADE_V2,
            PROVIDER_TAAL_ARC,
            PROVIDER_GORILLAPOOL_ARC,
            PROVIDER_BITAILS,
        ]);
        apply_sticky_provider_order(&mut v, PROVIDER_GORILLAPOOL_ARC, PROVIDER_ARCADE_V2);
        assert_eq!(
            names(&v),
            vec![
                PROVIDER_ARCADE_V2,
                PROVIDER_GORILLAPOOL_ARC,
                PROVIDER_TAAL_ARC,
                PROVIDER_BITAILS
            ]
        );
    }

    #[test]
    fn sticky_arcade_itself_goes_first() {
        // Arcade demoted to last by an earlier failure, then it accepted:
        // it may lead again.
        let mut v = list(&[
            PROVIDER_TAAL_ARC,
            PROVIDER_GORILLAPOOL_ARC,
            PROVIDER_ARCADE_V2,
        ]);
        apply_sticky_provider_order(&mut v, PROVIDER_ARCADE_V2, PROVIDER_ARCADE_V2);
        assert_eq!(
            names(&v),
            vec![
                PROVIDER_ARCADE_V2,
                PROVIDER_TAAL_ARC,
                PROVIDER_GORILLAPOOL_ARC
            ]
        );
    }

    #[test]
    fn sticky_unknown_or_already_first_is_a_no_op() {
        let mut v = list(&[PROVIDER_TAAL_ARC, PROVIDER_GORILLAPOOL_ARC]);
        apply_sticky_provider_order(&mut v, "NoSuchProvider", PROVIDER_ARCADE_V2);
        assert_eq!(names(&v), vec![PROVIDER_TAAL_ARC, PROVIDER_GORILLAPOOL_ARC]);
        apply_sticky_provider_order(&mut v, PROVIDER_TAAL_ARC, PROVIDER_ARCADE_V2);
        assert_eq!(names(&v), vec![PROVIDER_TAAL_ARC, PROVIDER_GORILLAPOOL_ARC]);
        let mut empty: Vec<(String, String, ())> = Vec::new();
        apply_sticky_provider_order(&mut empty, PROVIDER_TAAL_ARC, PROVIDER_ARCADE_V2);
        assert!(empty.is_empty());
    }

    // ---- the ladder ---------------------------------------------------------

    #[test]
    fn ladder_positives_are_monotone_and_mined_is_terminal() {
        use BroadcastStatus::*;
        assert_eq!(ladder_step(None, Accepted), LadderStep::Set(Accepted));
        assert_eq!(ladder_step(Some(Accepted), Seen), LadderStep::Set(Seen));
        assert_eq!(ladder_step(Some(Seen), Mined), LadderStep::Set(Mined));
        // Never downgraded.
        assert_eq!(ladder_step(Some(Seen), Accepted), LadderStep::Keep);
        assert_eq!(ladder_step(Some(Mined), Seen), LadderStep::Keep);
        assert_eq!(ladder_step(Some(Mined), Accepted), LadderStep::Keep);
        assert_eq!(ladder_step(Some(Mined), Rejected), LadderStep::Keep);
        assert_eq!(ladder_step(Some(Mined), Unknown), LadderStep::Keep);
        assert_eq!(ladder_step(Some(Mined), Mined), LadderStep::Keep);
        // Same positive again: fresh evidence.
        assert_eq!(ladder_step(Some(Seen), Seen), LadderStep::Refresh);
        assert_eq!(ladder_step(Some(Accepted), Accepted), LadderStep::Refresh);
    }

    #[test]
    fn ladder_negatives_override_positives_below_mined_and_keep_their_clock() {
        use BroadcastStatus::*;
        assert_eq!(ladder_step(Some(Seen), Rejected), LadderStep::Set(Rejected));
        assert_eq!(
            ladder_step(Some(Accepted), Rejected),
            LadderStep::Set(Rejected)
        );
        assert_eq!(ladder_step(Some(Seen), Unknown), LadderStep::Set(Unknown));
        assert_eq!(
            ladder_step(Some(Accepted), Unknown),
            LadderStep::Set(Unknown)
        );
        // A repeated absence keeps the FIRST observed absence.
        assert_eq!(ladder_step(Some(Unknown), Unknown), LadderStep::Keep);
        assert_eq!(ladder_step(Some(Rejected), Rejected), LadderStep::Keep);
        // A rejection is more informative than an absence.
        assert_eq!(ladder_step(Some(Rejected), Unknown), LadderStep::Keep);
        assert_eq!(
            ladder_step(Some(Unknown), Rejected),
            LadderStep::Set(Rejected)
        );
        // Only network evidence supersedes a negative: a provider accepting
        // a re-sent package must not reset the absence clock.
        assert_eq!(ladder_step(Some(Unknown), Accepted), LadderStep::Keep);
        assert_eq!(ladder_step(Some(Rejected), Accepted), LadderStep::Keep);
        assert_eq!(ladder_step(Some(Rejected), Seen), LadderStep::Set(Seen));
        assert_eq!(ladder_step(Some(Unknown), Seen), LadderStep::Set(Seen));
        assert_eq!(ladder_step(Some(Unknown), Mined), LadderStep::Set(Mined));
    }

    #[test]
    fn status_mapping_from_provider_verdicts() {
        use BroadcastStatus::*;
        assert_eq!(BroadcastStatus::from_arcade_status("RECEIVED"), Accepted);
        assert_eq!(
            BroadcastStatus::from_arcade_status("ACCEPTED_BY_NETWORK"),
            Accepted
        );
        assert_eq!(BroadcastStatus::from_arcade_status("SEEN_ON_NETWORK"), Seen);
        assert_eq!(
            BroadcastStatus::from_arcade_status("SEEN_MULTIPLE_NODES"),
            Seen
        );
        assert_eq!(BroadcastStatus::from_arcade_status("MINED"), Mined);
        assert_eq!(BroadcastStatus::from_arcade_status("REJECTED"), Rejected);
        assert_eq!(
            BroadcastStatus::from_arcade_status("DOUBLE_SPEND_ATTEMPTED"),
            Rejected
        );
        assert_eq!(BroadcastStatus::from_arc_status("SEEN_ON_NETWORK"), Seen);
        assert_eq!(BroadcastStatus::from_arc_status("MINED"), Mined);
        assert_eq!(
            BroadcastStatus::from_arc_status("SEEN_IN_ORPHAN_MEMPOOL"),
            Accepted,
            "an orphan is held, not connected"
        );
        assert_eq!(
            BroadcastStatus::from_arc_status("ANNOUNCED_TO_NETWORK"),
            Accepted
        );
        assert_eq!(BroadcastStatus::from_arc_status("REJECTED"), Rejected);
        for s in [Accepted, Seen, Mined, Rejected, Unknown] {
            assert_eq!(BroadcastStatus::parse(s.as_str()), Some(s));
        }
        assert_eq!(BroadcastStatus::parse("bogus"), None);
    }

    // ---- the skip rule --------------------------------------------------------

    #[test]
    fn accepted_alone_never_skips_seen_and_mined_do() {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let c = "cc".repeat(32);
        let d = "dd".repeat(32);
        let txids = vec![a.clone(), b.clone(), c.clone(), d.clone()];
        let records = vec![
            rec(&a, PROVIDER_ARCADE_V2, BROADCAST_STATUS_ACCEPTED, 5),
            rec(&b, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN, 5),
            rec(&c, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_MINED, 5),
        ];
        let seen = seen_set_from_records(PROVIDER_ARCADE_V2, &txids, &records);
        assert!(!seen.contains(&a), "accepted is not network evidence");
        assert!(seen.contains(&b));
        assert!(seen.contains(&c), "the network's rows count for everyone");
        assert!(!seen.contains(&d));
        // Another provider gets only the network row.
        let taal = seen_set_from_records(PROVIDER_TAAL_ARC, &txids, &records);
        assert_eq!(taal, HashSet::from([c.clone()]));
    }

    #[test]
    fn chain_index_rows_count_for_every_provider_like_the_network_rows() {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let txids = vec![a.clone(), b.clone()];
        let records = vec![
            rec(&a, BROADCAST_PROVIDER_CHAIN, BROADCAST_STATUS_SEEN, 5),
            rec(&b, BROADCAST_PROVIDER_CHAIN, BROADCAST_STATUS_MINED, 5),
            rec(&b, PROVIDER_ARCADE_V2, BROADCAST_STATUS_UNKNOWN, 1),
        ];
        assert_eq!(
            seen_set_from_records(PROVIDER_TAAL_ARC, &txids, &records),
            HashSet::from([a.clone(), b.clone()])
        );
        // A provider's negative row still vetoes the chain row for it.
        assert_eq!(
            seen_set_from_records(PROVIDER_ARCADE_V2, &txids, &records),
            HashSet::from([a.clone()])
        );
        assert!(is_global_provider(BROADCAST_PROVIDER_CHAIN));
        assert!(is_global_provider(BROADCAST_PROVIDER_NETWORK));
        assert!(!is_global_provider(PROVIDER_ARCADE_V2));
    }

    #[test]
    fn a_providers_negative_row_vetoes_the_network_row() {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let txids = vec![a.clone(), b.clone()];
        let records = vec![
            rec(&a, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_SEEN, 5),
            rec(&a, PROVIDER_ARCADE_V2, BROADCAST_STATUS_UNKNOWN, 1),
            rec(&b, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_SEEN, 5),
            rec(&b, PROVIDER_ARCADE_V2, BROADCAST_STATUS_REJECTED, 1),
        ];
        assert!(seen_set_from_records(PROVIDER_ARCADE_V2, &txids, &records).is_empty());
        // The veto is provider-specific.
        assert_eq!(
            seen_set_from_records(PROVIDER_TAAL_ARC, &txids, &records),
            HashSet::from([a, b])
        );
    }

    #[test]
    fn stale_seen_names_the_oldest_skipped_ancestor_and_fresh_seen_does_not() {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let c = "cc".repeat(32);
        let skipped = vec![a.clone(), b.clone(), c.clone()];
        let now = Utc::now();
        let fresh = vec![
            rec(&a, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN, 30),
            rec(&b, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN, 60),
            rec(&c, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_MINED, 90),
        ];
        assert_eq!(
            oldest_stale_ancestor(PROVIDER_ARCADE_V2, &skipped, &fresh, now, 600),
            None
        );
        let stale = vec![
            rec(&a, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN, 30),
            rec(&b, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN, 1_800),
            rec(&c, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN, 700),
        ];
        assert_eq!(
            oldest_stale_ancestor(PROVIDER_ARCADE_V2, &skipped, &stale, now, 600),
            Some(b.clone())
        );
        // The freshest qualifying row decides the age: a fresh network row
        // rescues a stale provider row.
        let rescued = vec![
            rec(&b, PROVIDER_ARCADE_V2, BROADCAST_STATUS_SEEN, 1_800),
            rec(&b, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_SEEN, 10),
        ];
        assert_eq!(
            oldest_stale_ancestor(
                PROVIDER_ARCADE_V2,
                std::slice::from_ref(&b),
                &rescued,
                now,
                600
            ),
            None
        );
        // Nothing qualifying: nothing to probe.
        let accepted_only = vec![rec(
            &a,
            PROVIDER_ARCADE_V2,
            BROADCAST_STATUS_ACCEPTED,
            5_000,
        )];
        assert_eq!(
            oldest_stale_ancestor(PROVIDER_ARCADE_V2, &skipped, &accepted_only, now, 600),
            None
        );
    }

    #[tokio::test]
    async fn in_memory_seen_sets_and_network_rows() {
        let m = InMemoryBroadcastMemory::new();
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let c = "cc".repeat(32);
        let d = "dd".repeat(32);
        m.record_broadcast_status(&a, PROVIDER_TAAL_ARC, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        m.record_broadcast_status(&b, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_MINED)
            .await
            .unwrap();
        m.record_broadcast_status(&d, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        let all = vec![a.clone(), b.clone(), c.clone(), d.clone()];

        let taal = m.broadcast_seen_for(PROVIDER_TAAL_ARC, &all).await.unwrap();
        assert!(taal.contains(&a) && taal.contains(&b) && !taal.contains(&c));
        assert!(!taal.contains(&d), "accepted alone never skips");

        // Another provider only gets the network row.
        let gp = m
            .broadcast_seen_for(PROVIDER_GORILLAPOOL_ARC, &all)
            .await
            .unwrap();
        assert!(!gp.contains(&a) && gp.contains(&b));

        let any = m.broadcast_seen_any(&all).await.unwrap();
        assert_eq!(any, HashSet::from([a.clone(), b.clone()]));

        // Idempotent, and mined is never downgraded.
        m.record_broadcast_status(&a, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        m.record_broadcast_status(&b, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        assert_eq!(m.len(), 3);
        assert_eq!(
            m.status_of(&a, PROVIDER_TAAL_ARC).as_deref(),
            Some(BROADCAST_STATUS_SEEN)
        );
        assert_eq!(
            m.status_of(&b, BROADCAST_PROVIDER_NETWORK).as_deref(),
            Some(BROADCAST_STATUS_MINED)
        );
        let record = m
            .broadcast_status_of(&a, PROVIDER_TAAL_ARC)
            .await
            .unwrap()
            .expect("row");
        assert_eq!(record.ladder_status(), Some(BroadcastStatus::Seen));

        // A rejection overrides seen; a fresh seen supersedes it again.
        m.record_broadcast_status(&a, PROVIDER_TAAL_ARC, BROADCAST_STATUS_REJECTED)
            .await
            .unwrap();
        assert!(m
            .broadcast_seen_for(PROVIDER_TAAL_ARC, &all)
            .await
            .unwrap()
            .contains(&a)
            .eq(&false));
        m.record_broadcast_status(&a, PROVIDER_TAAL_ARC, BROADCAST_STATUS_SEEN)
            .await
            .unwrap();
        assert!(m
            .broadcast_seen_for(PROVIDER_TAAL_ARC, &all)
            .await
            .unwrap()
            .contains(&a));

        // The absence clock: a repeated unknown keeps the first seen_at.
        let first = Utc::now() - Duration::minutes(40);
        m.record_broadcast_status_at(
            &c,
            BROADCAST_PROVIDER_NETWORK,
            BROADCAST_STATUS_UNKNOWN,
            first,
        )
        .unwrap();
        m.record_broadcast_status(&c, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_UNKNOWN)
            .await
            .unwrap();
        assert_eq!(m.seen_at_of(&c, BROADCAST_PROVIDER_NETWORK), Some(first));

        assert!(m
            .get_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER)
            .await
            .unwrap()
            .is_none());
        m.set_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER, PROVIDER_TAAL_ARC)
            .await
            .unwrap();
        assert_eq!(
            m.get_broadcast_pref(PREF_LAST_ACCEPTED_PROVIDER)
                .await
                .unwrap()
                .as_deref(),
            Some(PROVIDER_TAAL_ARC)
        );
    }
}
