//! The toolbox's cadences, named in one place: the stranger's-spend check
//! and the re-ask of a transaction no broadcaster has accepted yet (below,
//! [`send_waiting_reask_minutes`]).
//!
//! Rule 28 (bsv-stack-lean `NORTH-STAR.md`, Rulings, 2026-10-09): a
//! third-party chain explorer is break-glass; the primary truth of the
//! chain is headers plus merkle proofs. Every explorer call is judged by
//! one test, what header, proof or own-index answer we already hold for the
//! question. If none can exist the call stays, named as the irreducible
//! case at the site. And: a routine poll is the smell of a proof or an
//! index we should hold, and the fix is the proof path, never a bigger
//! budget.
//!
//! The owner's ruling on this crate, the same day: a stranger's spend of
//! our output becomes a chain fact only by the spending transaction's
//! merkle proof checked against our headers; the check for a stranger's
//! spend is the irreducible case of Rule 28, run on a cadence the wallet
//! owns and names, until an index of our own watches our outpoints.
//!
//! This module is that name. The question it paces: has someone who is
//! not us spent an output of ours that a failed transaction of ours left
//! locked. Headers and proofs prove inclusion, never that an output is
//! unspent, and our storage knows only our own devices' spends, so the
//! question is asked of explorers ([`WalletServices::is_utxo`]) and a
//! `Spent` answer is taken only with the spender's bytes and its proof.
//!
//! The cadence, whole:
//!
//! - **Which outpoints.** Only the locked inputs of our own failed
//!   transactions (the `locked_input_checks` table). Never the wallet's
//!   whole output set, never a script's history: that would be a scan.
//! - **How often, per outpoint.** [`stranger_spend_recheck_minutes`]: one
//!   minute after the first undecided answer, doubling to
//!   [`STRANGER_SPEND_RECHECK_CAP_MINUTES`], then every cap until it is
//!   decided. A hint (`SpentHint`, `Unknown`) is undecided.
//! - **When it ends.** A proven spend (terminal), an unspent hint (the
//!   input is released), or the source retired as a phantom (dropped).
//! - **How fast, within a pass.** [`STRANGER_SPEND_LOOKUP_PACE`] between
//!   two outpoints, a courtesy to the explorers' public rate.
//! - **Who starts a pass.** The wallet's host, through
//!   `StorageSqlx::recheck_locked_inputs` with its own limit per pass; a
//!   pass asks only about the rows due by the backoff above, so starting
//!   passes more often asks nothing more.
//!
//! These numbers are not a budget to raise. An outpoint that sits at the
//! cap is the smell the rule names: the fix is an index of our own that is
//! pushed the spend of an outpoint we watch, with its proof. When that
//! index exists this module is deleted.
//!
//! [`WalletServices::is_utxo`]: crate::services::WalletServices::is_utxo

use std::time::Duration;

/// Minutes until the first re-check of an outpoint after an undecided
/// answer.
pub const STRANGER_SPEND_RECHECK_FIRST_MINUTES: i64 = 1;

/// The longest pause between two re-checks of one outpoint (minutes).
pub const STRANGER_SPEND_RECHECK_CAP_MINUTES: i64 = 64;

/// Pause between two outpoints of one pass.
pub const STRANGER_SPEND_LOOKUP_PACE: Duration = Duration::from_millis(350);

/// Minutes until the next re-check after `attempts` undecided ones: 1, 2,
/// 4, 8, 16, 32, then [`STRANGER_SPEND_RECHECK_CAP_MINUTES`].
pub fn stranger_spend_recheck_minutes(attempts: u32) -> i64 {
    (STRANGER_SPEND_RECHECK_FIRST_MINUTES << attempts.saturating_sub(1).min(6))
        .min(STRANGER_SPEND_RECHECK_CAP_MINUTES)
}

/// Minutes until the first re-ask of a transaction whose broadcast drew no
/// accepting word (bsv-stack-lean #65).
///
/// The tracker's rule (bsv-stack-lean `docs/charters/tracker.md` section 2):
/// every broadcaster word is a hint that schedules a re-ask and writes no
/// word. A transient word (a 5xx, a 429, a timeout, ARC's 400 for a request
/// it could not read, an orphan-mempool hold) or a transport fault is
/// recorded on the request's history and the transaction is asked about
/// again on this cadence, for as long as it takes. No count of such words
/// retires it: the one retire is the host's explicit act
/// (`StorageSqlx::retire_undeliverable_txid`). A pass of
/// `send_waiting_transactions` posts only the requests due by
/// [`send_waiting_reask_minutes`], so starting passes more often asks
/// nothing more.
pub const SEND_WAITING_REASK_FIRST_MINUTES: i64 = 1;

/// The longest pause between two re-asks of one transaction (minutes).
pub const SEND_WAITING_REASK_CAP_MINUTES: i64 = 64;

/// Minutes from a request's last attempt until its next re-ask, after
/// `attempts` attempts: none before the first, then 1, 2, 4, 8, 16, 32,
/// then [`SEND_WAITING_REASK_CAP_MINUTES`] for every one after.
pub fn send_waiting_reask_minutes(attempts: u32) -> i64 {
    if attempts == 0 {
        return 0;
    }
    (SEND_WAITING_REASK_FIRST_MINUTES << (attempts - 1).min(6)).min(SEND_WAITING_REASK_CAP_MINUTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_send_waiting_reask_doubles_to_its_cap_and_never_ends() {
        let minutes: Vec<i64> = (0..10).map(send_waiting_reask_minutes).collect();
        assert_eq!(minutes, vec![0, 1, 2, 4, 8, 16, 32, 64, 64, 64]);
        assert_eq!(send_waiting_reask_minutes(u32::MAX), 64);
    }
}
