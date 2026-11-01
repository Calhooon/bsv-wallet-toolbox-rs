//! The A8 cycle of the BEEF path's bytes (#556): a BEEF a door judges, read
//! once. bsv-rs's decoder hashes and copies each transaction's body as it
//! reads it (`BeefDecoder::next`, the `Field::Run` arm and `absorb_scratch`
//! of `beef_stream.rs`), so every reading of the same bytes costs one SHA-256
//! of every body and one copy of it. The oracle path reads the internalize
//! door's bytes four times (the subject scan, the structure pass, the word
//! pass's two) and create's judged BEEF four times (the caller's and the
//! assembly's parses, the word pass's two); this path reads each once and
//! hands the elements to every pass by reference.
//!
//! Chosen by [`a8_beef_bytes`]: the `a8-beef-bytes` feature, off by default,
//! so the oracle path is the published behaviour until the owner's word turns
//! it on for a line (A8-6). The verdicts, the texts and the stored rows are
//! the oracle's (the differential target, `tests/a8_beef_bytes.rs` in this
//! module's test file).

use super::{stream_error, verification_result, VerificationHeaders};
use crate::Result;
use bsv_rs::transaction::beef_stream::{BeefIndex, Element, Hash32, TxBody};
use bsv_rs::transaction::BeefStream;
use std::borrow::Cow;

#[cfg(test)]
thread_local! {
    /// The path a test's door calls take on this thread: `Some(true)` the
    /// read-once path, `Some(false)` the oracle, `None` the feature's word.
    pub(crate) static A8_PATH: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// The read-once path is chosen: the `a8-beef-bytes` feature (a test may
/// choose either path on its thread, `A8_PATH`).
pub(crate) fn a8_beef_bytes() -> bool {
    #[cfg(test)]
    if let Some(on) = A8_PATH.with(std::cell::Cell::get) {
        return on;
    }
    cfg!(feature = "a8-beef-bytes")
}

/// Every element of a BEEF and its atomic subject, from one reading of its
/// bytes.
pub(crate) struct ReadOnce {
    pub(crate) elements: Vec<Element>,
    pub(crate) subject: Option<Hash32>,
}

/// Reads `bytes` once through bsv-rs's stream reader. A refusal is the one
/// the oracle's own reading of the same bytes gives, from the same decoder.
pub(crate) fn read_once(bytes: &[u8]) -> Result<ReadOnce> {
    let mut stream = BeefStream::new(bytes);
    let mut elements = Vec::new();
    while let Some(element) = stream.next_element().map_err(stream_error)? {
        elements.push(element);
    }
    Ok(ReadOnce {
        elements,
        subject: stream.subject(),
    })
}

impl ReadOnce {
    /// The elements, borrowed, as the word pass reads a source.
    pub(super) fn borrowed(&self) -> impl Iterator<Item = Result<Cow<'_, Element>>> {
        self.elements
            .iter()
            .map(|element| Ok(Cow::Borrowed(element)))
    }

    /// The transactions read, by txid: each one's last element on the wire
    /// (a later duplicate is the one a parsed `Beef` finds, its index keyed by
    /// the last push), `true` when it is a raw transaction with no BUMP.
    pub(crate) fn raw_unproven(&self) -> std::collections::HashMap<Hash32, bool> {
        let mut seen = std::collections::HashMap::new();
        for element in &self.elements {
            match element {
                Element::Tx {
                    txid, bump_index, ..
                } => seen.insert(*txid, bump_index.is_none()),
                Element::TxidOnly { txid, .. } => seen.insert(*txid, false),
                Element::Bump(_) => None,
            };
        }
        seen
    }

    /// The body of the element at `index`, a transaction, moved out of the
    /// reading with no copy.
    pub(crate) fn into_body(mut self, index: usize) -> Option<TxBody> {
        if index >= self.elements.len() {
            return None;
        }
        match self.elements.swap_remove(index) {
            Element::Tx { body, .. } => Some(body),
            _ => None,
        }
    }
}

/// `verify_stream_structure(bytes, headers, None)`'s answer for an atomic
/// BEEF, from the elements in hand: bsv-rs's `StreamVerifier` folds each
/// element into a structure-only `BeefIndex` held to the prefix's subject as
/// it reads it (`Judge::check_named_subject`), releases the transactions an
/// element frees before the next (`fold` does so in the same order), and at
/// the end judges the atomic rule with the subject named at offset 4
/// (`Judge::conclude`). A decoder refusal cannot arise here: the elements
/// were read whole.
pub(super) fn structure(
    read: &ReadOnce,
    subject: Hash32,
    headers: &VerificationHeaders,
) -> Result<()> {
    let mut index = BeefIndex::structure_only();
    index.hold_to_subject();
    for element in &read.elements {
        if let Err(stop) = index.fold(element, headers) {
            return verification_result(stop.into());
        }
    }
    match index.finish(Some(&subject), 4) {
        Ok(()) => Ok(()),
        Err(refusal) => verification_result(refusal.into()),
    }
}
