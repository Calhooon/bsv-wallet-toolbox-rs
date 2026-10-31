//! The overlay output's chain evidence, verified with the wallet's chain
//! tracker before its identity token is read and again after the
//! certificate's crypto, as the reference's discovery verifies it
//! (`IdentityEvidenceVerifier.parse`, ts-stack@edf6e03
//! `wallet-toolbox/src/utility/identityUtils.ts:600-635`, through the SDK's
//! `TransactionEvidenceCoordinator` under the policy
//! `sdk-spv-overlay-graph-v1`, `sdk/src/transaction/TransactionEvidenceCoordinator.ts`
//! and `TransactionEvidence.ts`). An output whose evidence fails is dropped;
//! an answer past the reference's limits is refused whole, as its
//! `TransactionEvidenceError('limit')` rejects the lookup.

use std::collections::{HashMap, HashSet};

use bsv_rs::block::ChainParams;
use bsv_rs::overlay::OutputListItem;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::beef_stream::{
    display_hex, referenced_outpoints, ChainTip, Element, Hash32, Headers, StreamVerifier, Verdict,
};
use bsv_rs::transaction::{Beef, BeefStream, ChainTracker, MerklePath};

use super::decode::decode_identity_output;
use super::OverlayCertificate;
use crate::Result;

/// `defaultTransactionEvidenceLimits.candidateBytes`
/// (`TransactionEvidence.ts:62`): one output's BEEF.
const CANDIDATE_BYTES: usize = 1024 * 1024;
/// `retainedBytes` (:63): an answer's BEEFs and contexts together
/// (`queryOverlayEvidence`, `identityUtils.ts:421,430-437`).
const RETAINED_BYTES: usize = 16 * 1024 * 1024;
/// `transactions` (:64): the entries of one BEEF.
const TRANSACTIONS: usize = 256;
/// `inputs` (:65): the inputs of the output's unconfirmed graph.
const INPUTS: usize = 4096;
/// `scriptBytes` (:66): the scripts of the output's unconfirmed graph.
const SCRIPT_BYTES: usize = 256 * 1024;
/// `scriptMemoryBytes` (:67): the memory of one input's stacks, the third
/// argument of `tx.verify` (`TransactionEvidenceCoordinator.ts:345-350`).
const SCRIPT_MEMORY_BYTES: usize = 16 * 1024 * 1024;
/// `queryOverlayEvidence`'s `limits.outputs ?? 512` (`identityUtils.ts:423`).
const OUTPUTS: usize = 512;
/// The doors' work budget per input, 2^31 units (the storage's
/// `WORK_PER_INPUT`, the keeper's word of 2026-10-30 on #487); the
/// reference's `verify` bounds no work.
const WORK_PER_INPUT: u64 = 1 << 31;
/// `MerklePath.verify`'s depth for a transaction at offset 0, the block's
/// coinbase (`MerklePath.ts:488-497`).
const COINBASE_DEPTH: u32 = 100;

/// The answer's refusal past a limit (`TransactionEvidenceError('limit')`).
pub(crate) const EVIDENCE_LIMIT: &str = "Overlay evidence: limit";

fn limit() -> crate::Error {
    crate::Error::ServiceError(EVIDENCE_LIMIT.into())
}

/// Why one output's evidence is not taken.
enum Refused {
    /// Past a limit: the whole answer is refused.
    Limit,
    /// The output is dropped, for this reason.
    Dropped(&'static str),
}

use Refused::{Dropped, Limit};

/// The output the evidence verified (`VerifiedTransactionOutput`): its
/// outpoint, its locking script's bytes read from the transaction the BEEF
/// carries, the roots (height, display hex) the tracker held for it and the
/// tracker's height the coinbase rule read.
struct Verified {
    outpoint: String,
    locking_script: Vec<u8>,
    anchors: Vec<(u32, String)>,
    observed_height: Option<u32>,
}

/// What the BEEF's own bytes say, read before the tracker is asked: the
/// output's transaction, its locking script, and the heights of the proven
/// transactions at their block's offset 0.
struct Parsed {
    txid: String,
    locking_script: Vec<u8>,
    coinbases: Vec<u32>,
}

/// The door's overlay certificates from a resolver's outputs, each output's
/// evidence verified with `tracker` under `chain`'s word, then its identity
/// token decoded (`decode_identity_output`) and its certificate verified and
/// decrypted (`verified_and_decrypted`), then its evidence fenced against
/// the tracker again (`identityUtils.ts:611-612`). An output that fails any
/// step is dropped, and a second output at the same outpoint
/// (`parseResults$`, :698-703). An answer past a limit is `EVIDENCE_LIMIT`.
pub(crate) async fn overlay_certificates(
    outputs: Vec<OutputListItem>,
    tracker: &dyn ChainTracker,
    chain: &ChainParams,
) -> Result<Vec<OverlayCertificate>> {
    intake(&outputs)?;
    let mut seen = HashSet::new();
    let mut kept = Vec::new();
    for output in &outputs {
        match certificate(output, tracker, chain).await {
            Ok((outpoint, certificate)) => {
                if seen.insert(outpoint) {
                    kept.push(certificate);
                }
            }
            Err(Limit) => return Err(limit()),
            Err(Dropped(why)) => tracing::debug!("Overlay output dropped: {why}"),
        }
    }
    Ok(kept)
}

/// One output read as `IdentityEvidenceVerifier.parse` reads it
/// (`identityUtils.ts:600-635`): its evidence, its token, its certificate,
/// then the fence. Answers its outpoint and its certificate.
async fn certificate(
    output: &OutputListItem,
    tracker: &dyn ChainTracker,
    chain: &ChainParams,
) -> std::result::Result<(String, OverlayCertificate), Refused> {
    let verified = verify(output, tracker, chain).await?;
    let record = LockingScript::from_binary(&verified.locking_script)
        .map_err(crate::Error::from)
        .and_then(|script| decode_identity_output(&script))
        .map_err(|e| {
            tracing::debug!("Skipping invalid overlay output: {}", e);
            Dropped("its identity token refused")
        })?;
    let certificate = record
        .verified_and_decrypted()
        .await
        .ok_or(Dropped("its certificate refused"))?;
    // "Certificate crypto can await; fence its completion against current
    // chain/session state."
    fence(&verified, tracker).await.map_err(Dropped)?;
    Ok((verified.outpoint, certificate))
}

/// `queryOverlayEvidence`'s intake (`identityUtils.ts:430-437`): at most
/// `OUTPUTS` outputs, each BEEF at most `CANDIDATE_BYTES`, the answer's
/// BEEFs and contexts at most `RETAINED_BYTES`.
fn intake(outputs: &[OutputListItem]) -> Result<()> {
    let mut bytes = 0usize;
    for output in outputs {
        bytes += output.beef.len() + output.context.as_ref().map_or(0, Vec::len);
        if output.beef.len() > CANDIDATE_BYTES {
            return Err(limit());
        }
    }
    if outputs.len() > OUTPUTS || bytes > RETAINED_BYTES {
        return Err(limit());
    }
    Ok(())
}

/// The coordinator's opening pass (`parseEvidence`, `TransactionEvidence.ts:179-201`,
/// then `run`'s `tx.verify` against the tracker, `TransactionEvidenceCoordinator.ts:289-394`):
/// the BEEF read, its transaction the Atomic subject or else its last
/// (`loadEvidenceTransaction`, :110-128), the output index inside it, the
/// unconfirmed graph inside the limits (`assertUnconfirmedGraph`, :155-177),
/// every BUMP root held by the tracker and a transaction at its block's
/// offset 0 `COINBASE_DEPTH` deep (`MerklePath.verify`), then bsv-rs's
/// stream verifier under the network's word: the structure, the proofs
/// against the held roots, and each input of an unproven transaction judged
/// with the work budget, at least one root proven (`anchors.size === 0`,
/// `TransactionEvidenceCoordinator.ts:354`).
async fn verify(
    output: &OutputListItem,
    tracker: &dyn ChainTracker,
    chain: &ChainParams,
) -> std::result::Result<Verified, Refused> {
    let bytes = output.beef.as_slice();
    let parsed = parse(bytes, output.output_index)?;
    let observed_height = coinbase_depth(&parsed.coinbases, tracker).await?;
    let (headers, anchors) = held_roots(bytes, tracker).await?;
    let referenced = referenced_outpoints(bytes).map_err(|_| Dropped("its BEEF refused"))?;
    let mut verifier = StreamVerifier::retaining(bytes, &headers, None, referenced)
        .with_chain_params(chain.clone())
        .with_work_budget(Some(WORK_PER_INPUT));
    verifier.set_memory_limit(Some(SCRIPT_MEMORY_BYTES));
    match verifier.run() {
        Ok(Verdict::Valid { roots, .. }) if !roots.is_empty() => Ok(Verified {
            outpoint: format!("{}.{}", parsed.txid, output.output_index),
            locking_script: parsed.locking_script,
            anchors,
            observed_height,
        }),
        Ok(Verdict::Valid { .. }) => Err(Dropped("no root proven")),
        _ => Err(Dropped("its BEEF refused")),
    }
}

/// `parseEvidence`'s reading (`TransactionEvidence.ts:93-128`): the bytes
/// present and inside `CANDIDATE_BYTES`, the BEEF read with at most
/// `TRANSACTIONS` entries, its transaction the Atomic subject or else its
/// last, the output index inside it, and its unconfirmed graph.
fn parse(bytes: &[u8], output_index: u32) -> std::result::Result<Parsed, Refused> {
    if bytes.is_empty() {
        return Err(Dropped("no chain evidence"));
    }
    if bytes.len() > CANDIDATE_BYTES {
        return Err(Limit);
    }
    let beef = Beef::from_binary(bytes).map_err(|_| Dropped("a BEEF that does not parse"))?;
    if beef.txs.len() > TRANSACTIONS {
        return Err(Limit);
    }
    let txid = beef
        .atomic_txid
        .clone()
        .or_else(|| beef.txs.last().map(|tx| tx.txid()))
        .ok_or(Dropped("its transaction not in its BEEF"))?;
    let txs: HashMap<String, _> = beef
        .txs
        .iter()
        .filter_map(|entry| Some((entry.txid(), entry.tx()?)))
        .collect();
    let tx = txs
        .get(&txid)
        .ok_or(Dropped("its transaction not in its BEEF"))?;
    let locking_script = tx
        .outputs
        .get(output_index as usize)
        .ok_or(Dropped("an output index past its transaction's outputs"))?
        .locking_script
        .to_binary();
    let coinbases = unconfirmed_graph(&beef, &txs, &txid)?;
    Ok(Parsed {
        txid,
        locking_script,
        coinbases,
    })
}

/// `assertUnconfirmedGraph` (`TransactionEvidence.ts:155-177`): from the
/// output's transaction down to its proven ancestors, every parent inside
/// the BEEF, no outpoint spent twice, an unproven transaction with inputs,
/// and the graph's inputs and scripts inside `INPUTS` and `SCRIPT_BYTES`.
/// Answers the heights of the proven transactions reached at their block's
/// offset 0 (`MerklePath.verify`'s `index === 0`, `MerklePath.ts:488`).
fn unconfirmed_graph(
    beef: &Beef,
    txs: &HashMap<String, &bsv_rs::transaction::Transaction>,
    target: &str,
) -> std::result::Result<Vec<u32>, Refused> {
    let mut pending = vec![target.to_string()];
    let mut visited = HashSet::new();
    let mut spent = HashSet::new();
    let mut proven = Vec::new();
    let (mut inputs, mut script_bytes) = (0usize, 0usize);
    while let Some(txid) = pending.pop() {
        if !visited.insert(txid.clone()) {
            continue;
        }
        let tx = txs.get(&txid).ok_or(Dropped("a parent not in its BEEF"))?;
        script_bytes += tx
            .outputs
            .iter()
            .map(|o| o.locking_script.to_binary().len())
            .sum::<usize>();
        script_bytes += tx
            .inputs
            .iter()
            .filter_map(|i| i.unlocking_script.as_ref())
            .map(|s| s.to_binary().len())
            .sum::<usize>();
        inputs += tx.inputs.len();
        if inputs > INPUTS || script_bytes > SCRIPT_BYTES {
            return Err(Limit);
        }
        if let Some(bump) = beef.find_bump(&txid) {
            if at_offset_zero(bump, &txid) {
                proven.push(bump.block_height);
            }
            continue;
        }
        if tx.inputs.is_empty() {
            return Err(Dropped("an unproven transaction with no input"));
        }
        for input in &tx.inputs {
            let source = input
                .get_source_txid()
                .map_err(|_| Dropped("an input with no source"))?;
            if !spent.insert((source.clone(), input.source_output_index)) {
                return Err(Dropped("an outpoint spent twice"));
            }
            pending.push(source);
        }
    }
    Ok(proven)
}

fn at_offset_zero(bump: &MerklePath, txid: &str) -> bool {
    bump.path.first().is_some_and(|level| {
        level
            .iter()
            .any(|leaf| leaf.offset == 0 && leaf.hash.as_deref() == Some(txid))
    })
}

/// `MerklePath.verify`'s coinbase rule (`MerklePath.ts:488-497`): a proven
/// transaction at its block's offset 0 is `COINBASE_DEPTH` blocks deep at
/// the tracker's height. Answers the height read, if one was.
async fn coinbase_depth(
    coinbases: &[u32],
    tracker: &dyn ChainTracker,
) -> std::result::Result<Option<u32>, Refused> {
    let mut observed = None;
    for block_height in coinbases {
        let height = tracker
            .current_height()
            .await
            .map_err(|_| Dropped("no height from the chain tracker"))?;
        observed = Some(observed.map_or(height, |seen: u32| seen.max(height)));
        if block_height.saturating_add(COINBASE_DEPTH) > height {
            return Err(Dropped("a coinbase not 100 blocks deep"));
        }
    }
    Ok(observed)
}

/// The roots the tracker holds, every BUMP's root of the BEEF asked once
/// (the coordinator's `isValidRootForHeight`,
/// `TransactionEvidenceCoordinator.ts:317-327`); a root it refuses,
/// or a tracker fault, drops the output. With the tracker's mining tip.
async fn held_roots(
    bytes: &[u8],
    tracker: &dyn ChainTracker,
) -> std::result::Result<(Roots, Vec<(u32, String)>), Refused> {
    let mut asked = HashSet::new();
    let mut anchors = Vec::new();
    for (block_height, root) in bump_roots(bytes)? {
        if !asked.insert((block_height, root)) {
            continue;
        }
        let height = u32::try_from(block_height).map_err(|_| Dropped("its BEEF refused"))?;
        let root = display_hex(&root);
        match tracker.is_valid_root_for_height(&root, height).await {
            Ok(true) => anchors.push((height, root)),
            _ => return Err(Dropped("a root the chain tracker does not hold")),
        }
    }
    let headers = Roots {
        roots: asked,
        tip: tracker.mining_tip().await.ok().flatten(),
    };
    Ok((headers, anchors))
}

/// Every BUMP's height and root, read by bsv-rs's streaming reader.
fn bump_roots(bytes: &[u8]) -> std::result::Result<Vec<(u64, Hash32)>, Refused> {
    let mut roots = Vec::new();
    for element in BeefStream::new(bytes) {
        if let Element::Bump(bump) = element.map_err(|_| Dropped("its BEEF refused"))? {
            roots.push((bump.block_height, bump.root));
        }
    }
    Ok(roots)
}

/// The coordinator's second pass on the same bytes (`recheck`,
/// `TransactionEvidenceCoordinator.ts:505-533`): the bytes are the door's
/// own and unchanged, so the structure and the scripts stand; each root the
/// earlier pass held is asked again, and the tracker's height is not below the
/// height that pass read.
async fn fence(
    verified: &Verified,
    tracker: &dyn ChainTracker,
) -> std::result::Result<(), &'static str> {
    for (height, root) in &verified.anchors {
        if !matches!(
            tracker.is_valid_root_for_height(root, *height).await,
            Ok(true)
        ) {
            return Err("a root the chain tracker no longer holds");
        }
    }
    if let Some(observed) = verified.observed_height {
        if !matches!(tracker.current_height().await, Ok(height) if height >= observed) {
            return Err("the chain tracker's height fell");
        }
    }
    Ok(())
}

/// The roots the tracker held and its mining tip, as the stream verifier
/// reads headers.
struct Roots {
    roots: HashSet<(u64, Hash32)>,
    tip: Option<ChainTip>,
}

impl Headers for Roots {
    fn mining_tip(&self) -> Option<ChainTip> {
        self.tip
    }

    fn carries(&self, height: u64, root: &Hash32) -> bool {
        self.roots.contains(&(height, *root))
    }
}

#[cfg(test)]
#[path = "lookup_evidence_tests.rs"]
mod tests;
