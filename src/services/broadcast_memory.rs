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

use crate::{Error, Result};

/// A poisoned mutex is reported, never unwrapped.
fn poisoned<T>(_: std::sync::PoisonError<T>) -> Error {
    Error::Internal("broadcast memory mutex poisoned".to_string())
}

/// `broadcast_seen.status`: the provider accepted the txid on a submit
/// (any non-error ARC `txStatus`, an Arcade `202`, an Arcade `/txs`
/// duplicate).
pub const BROADCAST_STATUS_ACCEPTED: &str = "accepted";

/// `broadcast_seen.status`: a push channel (SSE / webhook) reported the txid
/// `SEEN_ON_NETWORK` (or further along).
pub const BROADCAST_STATUS_SEEN: &str = "seen";

/// `broadcast_seen.status`: the txid is proven (mined). Terminal: never
/// downgraded by a later record.
pub const BROADCAST_STATUS_MINED: &str = "mined";

/// The pseudo-provider for facts about the network as a whole (a mined
/// proof, a seen report whose reporting plane is unknown). Rows recorded
/// under it count toward EVERY provider's seen set.
pub const BROADCAST_PROVIDER_NETWORK: &str = "network";

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
/// seen-set query per provider it actually tries (usually one) and one
/// preference read per broadcast.
#[async_trait]
pub trait BroadcastMemory: Send + Sync {
    /// The subset of `txids` that `provider` has accepted or seen: rows
    /// recorded for `provider` itself plus rows recorded for the whole
    /// network ([`BROADCAST_PROVIDER_NETWORK`]).
    async fn broadcast_seen_for(&self, provider: &str, txids: &[String])
        -> Result<HashSet<String>>;

    /// The subset of `txids` seen by ANY provider (or the network).
    async fn broadcast_seen_any(&self, txids: &[String]) -> Result<HashSet<String>>;

    /// Record that `provider` accepted / saw / mined `txid`. Idempotent: a
    /// repeat is an upsert, and [`BROADCAST_STATUS_MINED`] is never
    /// downgraded.
    async fn record_broadcast_seen(&self, txid: &str, provider: &str, status: &str) -> Result<()>;

    /// Record several txids for one provider and status.
    async fn record_broadcast_seen_many(
        &self,
        provider: &str,
        status: &str,
        txids: &[String],
    ) -> Result<()> {
        for txid in txids {
            self.record_broadcast_seen(txid, provider, status).await?;
        }
        Ok(())
    }

    /// Read a broadcast preference (e.g. [`PREF_LAST_ACCEPTED_PROVIDER`]).
    async fn get_broadcast_pref(&self, key: &str) -> Result<Option<String>>;

    /// Write a broadcast preference (upsert).
    async fn set_broadcast_pref(&self, key: &str, value: &str) -> Result<()>;
}

/// Process-local [`BroadcastMemory`]: a `HashMap` behind a mutex. Loses its
/// contents with the process; use the storage-backed implementation for a
/// served wallet.
#[derive(Debug, Default)]
pub struct InMemoryBroadcastMemory {
    seen: Mutex<HashMap<(String, String), String>>,
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
        self.seen
            .lock()
            .ok()
            .and_then(|m| m.get(&(txid.to_string(), provider.to_string())).cloned())
    }
}

#[async_trait]
impl BroadcastMemory for InMemoryBroadcastMemory {
    async fn broadcast_seen_for(
        &self,
        provider: &str,
        txids: &[String],
    ) -> Result<HashSet<String>> {
        let seen = self.seen.lock().map_err(poisoned)?;
        Ok(txids
            .iter()
            .filter(|t| {
                seen.contains_key(&((*t).clone(), provider.to_string()))
                    || seen.contains_key(&((*t).clone(), BROADCAST_PROVIDER_NETWORK.to_string()))
            })
            .cloned()
            .collect())
    }

    async fn broadcast_seen_any(&self, txids: &[String]) -> Result<HashSet<String>> {
        let seen = self.seen.lock().map_err(poisoned)?;
        let wanted: HashSet<&String> = txids.iter().collect();
        Ok(seen
            .keys()
            .filter(|(t, _)| wanted.contains(t))
            .map(|(t, _)| t.clone())
            .collect())
    }

    async fn record_broadcast_seen(&self, txid: &str, provider: &str, status: &str) -> Result<()> {
        let mut seen = self.seen.lock().map_err(poisoned)?;
        let entry = seen
            .entry((txid.to_string(), provider.to_string()))
            .or_insert_with(|| status.to_string());
        if entry != BROADCAST_STATUS_MINED {
            *entry = status.to_string();
        }
        Ok(())
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

    fn names(v: &[(String, String, ())]) -> Vec<&str> {
        v.iter().map(|(_, n, _)| n.as_str()).collect()
    }

    fn list(names: &[&str]) -> Vec<(String, String, ())> {
        names
            .iter()
            .map(|n| ("postBeef".to_string(), n.to_string(), ()))
            .collect()
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

    #[tokio::test]
    async fn in_memory_seen_sets_and_network_rows() {
        let m = InMemoryBroadcastMemory::new();
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let c = "cc".repeat(32);
        m.record_broadcast_seen(&a, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        m.record_broadcast_seen(&b, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_MINED)
            .await
            .unwrap();
        let all = vec![a.clone(), b.clone(), c.clone()];

        let taal = m.broadcast_seen_for(PROVIDER_TAAL_ARC, &all).await.unwrap();
        assert!(taal.contains(&a) && taal.contains(&b) && !taal.contains(&c));

        // Another provider only gets the network row.
        let gp = m
            .broadcast_seen_for(PROVIDER_GORILLAPOOL_ARC, &all)
            .await
            .unwrap();
        assert!(!gp.contains(&a) && gp.contains(&b));

        let any = m.broadcast_seen_any(&all).await.unwrap();
        assert_eq!(any.len(), 2);

        // Idempotent, and mined is never downgraded.
        m.record_broadcast_seen(&a, PROVIDER_TAAL_ARC, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        m.record_broadcast_seen(&b, BROADCAST_PROVIDER_NETWORK, BROADCAST_STATUS_ACCEPTED)
            .await
            .unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(
            m.status_of(&b, BROADCAST_PROVIDER_NETWORK).as_deref(),
            Some(BROADCAST_STATUS_MINED)
        );

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
