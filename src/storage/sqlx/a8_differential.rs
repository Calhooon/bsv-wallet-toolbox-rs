//! The A8 cycle's differential (#556, A8-3): the internalize door's reading
//! of a BEEF on the oracle path (the subject scan, then the structure and
//! word passes over the bytes, each its own reading) and on the
//! `a8-beef-bytes` path (one reading for all three), fed the same bytes under
//! four header contexts; any difference in a value, an error or a refusal is
//! the answer. The unit tests (`a8_beef_bytes_tests.rs`) and, with the
//! feature, the fuzz target `a8_beef_bytes` call it.

use super::{read_incoming_once, read_incoming_payment, IncomingPayment};
use crate::storage::sqlx::beef_verification::{
    verify_beef_spends_with_verifier, verify_read_spends,
};
use bsv_rs::transaction::beef_stream::ChainTip;
use bsv_rs::transaction::{ChainTracker, ChainTrackerError};
use std::collections::HashSet;

/// The census scenes' proof height and tip, past mainnet Genesis.
pub(crate) const CENSUS_HEIGHT: u32 = 900_100;

/// A header source with a chosen tip and the roots it carries: every root
/// (`None`) or those named.
pub(crate) struct Headers {
    pub(crate) tip: Option<ChainTip>,
    pub(crate) roots: Option<HashSet<(u32, String)>>,
}

impl Headers {
    pub(crate) fn census(roots: Option<HashSet<(u32, String)>>) -> Self {
        let chain = bsv_rs::block::ChainParams::mainnet();
        let tip = crate::services::chain_tip(&chain, CENSUS_HEIGHT, 1_700_000_000, 419_328, None);
        Self {
            tip: Some(tip),
            roots,
        }
    }
}

#[async_trait::async_trait]
impl ChainTracker for Headers {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        Ok(self
            .roots
            .as_ref()
            .is_none_or(|roots| roots.contains(&(height, root.to_string()))))
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(CENSUS_HEIGHT)
    }

    async fn mining_tip(&self) -> std::result::Result<Option<ChainTip>, ChainTrackerError> {
        Ok(self.tip)
    }
}

/// The header contexts each corpus input is read under: the census tip with
/// every root carried, with none, with no tip, and on testnet's word.
fn contexts() -> Vec<(&'static str, Headers, &'static str)> {
    let none = Some(HashSet::new());
    vec![
        ("tip, every root", Headers::census(None), "main"),
        ("tip, no root", Headers::census(none), "main"),
        (
            "no tip",
            Headers {
                tip: None,
                roots: None,
            },
            "main",
        ),
        ("testnet", Headers::census(None), "test"),
    ]
}

/// One path's answer at the internalize door's read and verification: the
/// subject's txid, roots, body and BUMP claim, or the error's form and text.
type Answer = std::result::Result<(String, String, Option<Vec<u8>>, String), String>;

fn answer(incoming: crate::Result<IncomingPayment>) -> Answer {
    let incoming = incoming.map_err(|e| format!("{e:?} | {e}"))?;
    let claim = incoming.bump_claim.map(|c| {
        let (height, idx, root) = (c.height, c.idx, c.computed_root);
        format!("{height} {idx} {root} {}", hex::encode(c.merkle_path))
    });
    let body = incoming.body.map(|body| {
        let fields = format!("{:?}", (&body.inputs, &body.outputs, body.version));
        [body.raw, fields.into_bytes()].concat()
    });
    Ok((
        incoming.txid,
        format!("{:?}", incoming.roots),
        body,
        format!("{claim:?}"),
    ))
}

/// The oracle: the subject scan, then the door's structure and word passes
/// over the bytes, each its own reading.
async fn oracle(bytes: &[u8], headers: &Headers, network: &str) -> Answer {
    answer(
        async {
            let incoming = read_incoming_payment(bytes)?;
            verify_beef_spends_with_verifier(bytes, &incoming.roots, headers, network, None)
                .await?;
            Ok(incoming)
        }
        .await,
    )
}

/// The read-once path: one reading for the scan and both passes.
async fn read_once_path(bytes: &[u8], headers: &Headers, network: &str) -> Answer {
    answer(
        async {
            let (mut incoming, read, subject_at) = read_incoming_once(bytes)?;
            let roots = &incoming.roots;
            verify_read_spends(bytes, &read, roots, headers, network, None).await?;
            incoming.body = subject_at.and_then(|index| read.into_body(index));
            Ok(incoming)
        }
        .await,
    )
}

/// Both paths agree on `bytes` under every context: how many readings each
/// accepted, or the earliest difference, named.
pub async fn differential(name: &str, bytes: &[u8]) -> Result<usize, String> {
    let mut accepted = 0;
    for (context, headers, network) in contexts() {
        let expected = oracle(bytes, &headers, network).await;
        let actual = read_once_path(bytes, &headers, network).await;
        if expected != actual {
            let short = |a: &Answer| match a {
                Ok((txid, roots, body, claim)) => {
                    let len = body.as_ref().map(Vec::len);
                    format!("Ok({txid}, {roots}, body {len:?}, {claim})")
                }
                Err(e) => format!("Err({e})"),
            };
            return Err(format!(
                "{name} ({context}): the oracle {} and the read-once path {}",
                short(&expected),
                short(&actual)
            ));
        }
        accepted += usize::from(expected.is_ok());
    }
    Ok(accepted)
}
