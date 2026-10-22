//! BananaBlocks service provider: GorillaPool's independent explorer, the
//! third of the services' ladders beside WhatsOnChain and Bitails (the
//! owner's word of 2026-10-22, bsv-wallet-cli#50: each chain scanner has its
//! own rate limits, so when one fails the ladder goes to the next).
//!
//! Provides, live-probed 2026-10-22:
//! - `GET {base}/tx/{txid}/hex`: the raw transaction (text hex); the bytes
//!   must hash to the txid
//! - `GET {base}/txo/{txid}/{vout}`: the output, with its `script_pubkey`
//!   (hex), `spent` and, when spent, `spent_txid` and `spend_scope`; an
//!   outpoint's spend status, bound to the asked script by the script's hash
//! - `GET {base}/tx/{txid}`: `block_hash` / `block_height` once mined
//! - `GET {base}/tx/{txid}/proof`: a TSC proof whose `target` is the block's
//!   merkle root (`"targetType":"merkleRoot"`); `/proof/tsc` is no route
//! - `GET {base}/address/{address}/utxos?limit=1000`: an address's unspent
//!   outputs (`items` of `txid`, `vout`, `value`), LEADS for
//!   `Services::get_utxo_leads`; BananaBlocks lists by address, never by
//!   script hash
//!
//! No key; the host advertises 80 requests a minute (`x-ratelimit-limit:
//! 80`), and this crate paces itself with a token bucket one process shares
//! ([`RateGate::shared`]): a burst of 30 refilled at 0.5/s, so 60 in the
//! first minute and 30 a minute sustained, under BananaBlocks' 80; a 429 is
//! a fault returned at once with its Retry-After, never slept on.
//!
//! BananaBlocks' index misses some mempool spends (2026-10-22: an outpoint
//! WhatsOnChain's mempool shows spent answered `spent: false`, twice), so
//! its `spent: false` is never an answer alone: the utxo ladder takes an
//! unspent only from two explorers. The probe notes are
//! `docs/providers/bananablocks.md`.
//!
//! # API Endpoints
//!
//! - Mainnet: `https://bananablocks.com/api/v1`
//! - Testnet: `https://test.bananablocks.com/api/v1`

use reqwest::{Client, StatusCode};
use std::time::Duration;

use crate::services::providers::pace::RateGate;
use crate::services::traits::{
    convert_script_hash, merkle_path_note, query_value, validate_txid, GetMerklePathResult,
    GetRawTxResult, GetStatusForTxidsResult, GetUtxoStatusOutputFormat, GetUtxoStatusResult,
    TxStatusDetail, UnspentPages, UtxoDetail, LEAD_PAGES_MAX, NOTE_PROOF,
};
use crate::services::Chain;
use crate::{Error, Result};
use bsv_rs::primitives::{sha256, sha256d};
use std::sync::Arc;

/// BananaBlocks mainnet API base.
pub const BANANABLOCKS_MAINNET_URL: &str = "https://bananablocks.com/api/v1";

/// BananaBlocks testnet API base.
pub const BANANABLOCKS_TESTNET_URL: &str = "https://test.bananablocks.com/api/v1";

/// The budget of the bucket this crate paces BananaBlocks with, per
/// process, no key: a burst of 30 refilled at 0.5/s, so 60 in the first
/// minute and 30 a minute sustained, under BananaBlocks' 80
/// (`x-ratelimit-limit: 80`).
pub const BANANABLOCKS_PER_MINUTE: u32 = 60;

/// The name this provider answers under (results, notes, call history).
pub const BANANABLOCKS_NAME: &str = "BananaBlocks";

/// The page an address's unspent list is asked for (`?limit=`): the host
/// serves up to 1000 an answer (probed 2026-10-22; 20 when none is given).
pub const BANANABLOCKS_LEAD_PAGE: usize = 1000;

/// How long a 429 without a Retry-After closes the rung: BananaBlocks'
/// budget is a minute's.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(60);

/// What BananaBlocks says about one outpoint's spend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutpointSpend {
    /// `{"spent": false}`: no spender this index can see.
    Unspent,
    /// `{"spent": true, "spent_txid": ...}`: the named spender (a word; its
    /// bytes bind it).
    SpentBy(String),
}

/// One output as `GET /txo/{txid}/{vout}` gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Txo {
    /// The big-endian sha256 of the output's locking script (the hash the
    /// explorers' script routes carry), from its `script_pubkey`.
    pub script_hash: String,
    /// Its spend.
    pub spend: OutpointSpend,
}

/// BananaBlocks service provider.
pub struct BananaBlocks {
    client: Client,
    base_url: String,
    /// The process's gate for this base URL ([`RateGate::shared`]).
    gate: Arc<RateGate>,
}

impl BananaBlocks {
    /// The provider for a chain at its default API base.
    pub fn new(chain: Chain) -> Result<Self> {
        let base = match chain {
            Chain::Main => BANANABLOCKS_MAINNET_URL,
            Chain::Test => BANANABLOCKS_TESTNET_URL,
        };
        Self::with_base(base, None)
    }

    /// The provider at another API base (a mirror). `timeout_secs` defaults
    /// to 30, as every explorer's. Every provider this process builds for
    /// one base shares one gate: one budget, one Retry-After window.
    pub fn with_base(base_url: &str, timeout_secs: Option<u64>) -> Result<Self> {
        let base_url = base_url.trim_end_matches('/');
        let gate = RateGate::shared(&format!("{BANANABLOCKS_NAME} {base_url}"), Self::gate);
        Self::with_gate(base_url, timeout_secs, gate)
    }

    /// A provider with a gate of its own, not the process's: a local
    /// fixture in a test (fixture servers are recycled, so a gate keyed by
    /// their URL would carry one test's 429 into the next).
    #[cfg(test)]
    pub(crate) fn with_base_unshared(base_url: &str) -> Self {
        Self::with_gate(base_url, None, Arc::new(Self::gate())).expect("client")
    }

    fn gate() -> RateGate {
        RateGate::new(BANANABLOCKS_NAME, DEFAULT_RETRY_AFTER)
            .with_per_minute(BANANABLOCKS_PER_MINUTE)
    }

    fn with_gate(base_url: &str, timeout_secs: Option<u64>, gate: Arc<RateGate>) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs.unwrap_or(30)))
            .build()
            .map_err(|e| Error::NetworkError(format!("Failed to create HTTP client: {}", e)))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            gate,
        })
    }

    /// The gate this provider's requests pass (the process's for its base).
    pub fn rate_gate(&self) -> &Arc<RateGate> {
        &self.gate
    }

    /// The API base (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// One GET through the gate: no request inside a Retry-After window or
    /// with no token left; a 429 closes the rung and is the fault returned.
    async fn get(&self, url: &str) -> Result<(StatusCode, String)> {
        self.gate.admit()?;
        let response = self
            .client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| Error::NetworkError(format!("BananaBlocks request failed: {}", e)))?;
        let status = response.status();
        self.gate.check(status, response.headers())?;
        let body = response
            .text()
            .await
            .map_err(|e| Error::NetworkError(format!("BananaBlocks read failed: {}", e)))?;
        Ok((status, body))
    }

    // =========================================================================
    // Raw Transaction
    // =========================================================================

    /// A transaction's bytes by txid (Rule 28, T8 and T9: a courier of a
    /// foreign ancestor's bytes). The answer is self-verifying: bytes that
    /// do not hash to the txid are a fault. A 404 is "no such transaction";
    /// every other failure is an error ("could not look").
    pub async fn get_raw_tx(&self, txid: &str) -> Result<GetRawTxResult> {
        let (status, body) = self
            .get(&format!("{}/tx/{}/hex", self.base_url, txid))
            .await?;
        match status {
            StatusCode::OK => {
                let raw_tx = hex::decode(body.trim()).map_err(|e| {
                    Error::ValidationError(format!("BananaBlocks raw tx hex: {}", e))
                })?;
                validate_txid(&raw_tx, txid)?;
                Ok(GetRawTxResult {
                    name: BANANABLOCKS_NAME.to_string(),
                    txid: txid.to_string(),
                    raw_tx: Some(raw_tx),
                    error: None,
                    could_not_look: false,
                })
            }
            StatusCode::NOT_FOUND => Ok(GetRawTxResult {
                name: BANANABLOCKS_NAME.to_string(),
                txid: txid.to_string(),
                raw_tx: None,
                error: None,
                could_not_look: false,
            }),
            status => Err(Error::ServiceError(format!(
                "BananaBlocks getRawTx failed with status {}",
                status
            ))),
        }
    }

    // =========================================================================
    // Outpoint spend
    // =========================================================================

    /// One output from `GET /txo/{txid}/{vout}`. Only a 200 with the
    /// documented shape answers: a `script_pubkey` (hex; with a
    /// `script_hash` beside it the two must agree), a boolean `spent`, and
    /// for `spent: true` a 64-hex `spent_txid` that is not the outpoint's
    /// own transaction. A 404 (the index does not know the output), a 429,
    /// a 5xx, a malformed body are faults, never "unspent".
    pub async fn get_txo(&self, txid: &str, vout: u32) -> Result<Txo> {
        let (status, body) = self
            .get(&format!("{}/txo/{}/{}", self.base_url, txid, vout))
            .await?;
        parse_txo_body(status, &body, txid, vout)
    }

    /// An outpoint's spend status ([`Self::get_txo`]'s `spend`): the name
    /// `spend_evidence` binds by the spender's bytes.
    pub async fn get_spend(&self, txid: &str, vout: u32) -> Result<OutpointSpend> {
        Ok(self.get_txo(txid, vout).await?.spend)
    }

    /// The outpoint question of `getUtxoStatus` (Rule 28, T10). BananaBlocks
    /// answers an outpoint directly; it has no script's unspent set, so a
    /// question without an outpoint is not one it answers (the ladder does
    /// not ask it; asked directly, the answer is `status: "error"`, never
    /// a negative).
    ///
    /// The answer is about the asked output only when the output's script
    /// hashes to the asked hash (`output` in `output_format`, as every
    /// explorer reads it): another script, the empty script included, is a
    /// fault, so an outpoint whose script is not held never comes back
    /// unspent from here. `spent: false` is a positive, a named spender a
    /// negative; the ladder takes either only with a second explorer's.
    pub async fn get_utxo_status(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> Result<GetUtxoStatusResult> {
        self.get_utxo_status_with_txo(output, output_format, outpoint)
            .await
            .0
    }

    /// [`Self::get_utxo_status`], with the `/txo` answer it read (`None`
    /// when no output was read: no outpoint, or a fault before a whole
    /// body). The answer's `spend` is the outpoint's whatever script was
    /// asked, so `is_utxo`'s `spend_evidence` takes the spender from here
    /// instead of asking `/txo` a second time.
    pub(crate) async fn get_utxo_status_with_txo(
        &self,
        output: &str,
        output_format: Option<GetUtxoStatusOutputFormat>,
        outpoint: Option<&str>,
    ) -> (Result<GetUtxoStatusResult>, Option<Txo>) {
        let Some(outpoint) = outpoint else {
            let result = Ok(GetUtxoStatusResult {
                name: BANANABLOCKS_NAME.to_string(),
                status: "error".to_string(),
                is_utxo: None,
                details: Vec::new(),
                error: Some(
                    "BananaBlocks answers an outpoint, not a script's unspent set".to_string(),
                ),
            });
            return (result, None);
        };
        let asked = match convert_script_hash(output, output_format) {
            Ok(asked) => asked.to_ascii_lowercase(),
            Err(e) => return (Err(e), None),
        };
        let (txid, vout) = match parse_outpoint(outpoint) {
            Ok(parsed) => parsed,
            Err(e) => return (Err(e), None),
        };
        let txo = match self.get_txo(txid, vout).await {
            Ok(txo) => txo,
            Err(e) => return (Err(e), None),
        };
        let result = if txo.script_hash != asked {
            Err(Error::ServiceError(format!(
                "BananaBlocks txo {outpoint}: its script hashes to {}, not the asked {asked}",
                txo.script_hash
            )))
        } else {
            Ok(GetUtxoStatusResult {
                name: BANANABLOCKS_NAME.to_string(),
                status: "success".to_string(),
                is_utxo: Some(txo.spend == OutpointSpend::Unspent),
                details: Vec::new(),
                error: None,
            })
        };
        (result, Some(txo))
    }

    // =========================================================================
    // Address unspent list (leads)
    // =========================================================================

    /// The unspent outputs BananaBlocks lists for a P2PKH address:
    /// `GET /address/{address}/utxos?limit=1000`, then `&cursor=` while the
    /// answer says `hasMore`, each page through the gate, at most
    /// [`LEAD_PAGES_MAX`] pages. LEADS for `Services::get_utxo_leads`,
    /// never a verdict. BananaBlocks lists by address only: no route takes
    /// a script hash (probed 2026-10-22: `/script/…`, `/scripthash/…`,
    /// `/script_hash/…` are 404), so only a P2PKH script handed as itself
    /// can be asked here.
    ///
    /// Only a 200 with an `items` list of `{txid, vout, value}` answers (an
    /// address never seen answers `{"items":[],"hasMore":false}`); on the
    /// first page a 404, a 429, a 5xx, a malformed body are faults, never
    /// an empty list. A later page that could not be read (a transport
    /// fault, a 429, another status) cuts the list short; a later page that
    /// was read and is malformed is the explorer's malformed answer. The
    /// page cap reached with `hasMore` still set cuts it short too.
    pub async fn list_address_unspent(&self, address: &str) -> Result<UnspentPages> {
        let mut list = UnspentPages::default();
        let mut cursor: Option<String> = None;
        for page in 0..LEAD_PAGES_MAX {
            let mut url = format!(
                "{}/address/{}/utxos?limit={}",
                self.base_url, address, BANANABLOCKS_LEAD_PAGE
            );
            if let Some(cursor) = &cursor {
                url.push_str("&cursor=");
                url.push_str(&query_value(cursor));
            }
            let (status, body) = match self.get(&url).await {
                Ok(answer) => answer,
                Err(e) => return list.unread(page, e),
            };
            if status != StatusCode::OK {
                return list.unread(
                    page,
                    Error::ServiceError(format!(
                        "BananaBlocks address {address} utxos: HTTP {status}"
                    )),
                );
            }
            let (details, next) = parse_address_utxos(status, &body, address)?;
            list.details.extend(details);
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok(list),
            }
        }
        tracing::info!(
            address = %address,
            listed = list.details.len(),
            "BananaBlocks lists more unspent outputs than the page cap; the pages' leads are taken"
        );
        Ok(list.capped())
    }

    // =========================================================================
    // Merkle Path
    // =========================================================================

    /// A transaction's inclusion proof (Rule 28, T6 and T7: a courier of a
    /// proof nobody pushed to us; `Services::get_merkle_path` checks its
    /// root against the header service before it is returned).
    ///
    /// `GET /tx/{txid}` first: a 404 means this index does not know the
    /// transaction, which says nothing about the chain (inconclusive, never
    /// "not mined"); no block is a clean "not mined". For a mined
    /// transaction `GET /tx/{txid}/proof` serves a TSC proof whose target is
    /// the merkle root; its nodes must fold to that root, and it is handed
    /// on with the block's hash as its target, so the ladder resolves the
    /// header from the header service as for every TSC courier.
    pub async fn get_merkle_path(&self, txid: &str) -> Result<GetMerklePathResult> {
        let (status, body) = self.get(&format!("{}/tx/{}", self.base_url, txid)).await?;
        let block = match status {
            StatusCode::OK => parse_tx_block(&body, txid)?,
            StatusCode::NOT_FOUND => {
                return Ok(no_path(None, "getMerklePathNotIndexed"));
            }
            status => {
                return Err(Error::ServiceError(format!(
                    "BananaBlocks tx failed with status {}",
                    status
                )))
            }
        };
        let Some((block_hash, _height)) = block else {
            return Ok(no_path(None, "getMerklePathNotMined"));
        };

        let (status, body) = self
            .get(&format!("{}/tx/{}/proof", self.base_url, txid))
            .await?;
        if status != StatusCode::OK {
            return Err(Error::ServiceError(format!(
                "BananaBlocks proof failed with status {} for a transaction it places in block {}",
                status, block_hash
            )));
        }
        let proof = parse_tsc_proof(&body, txid, &block_hash)?;
        Ok(GetMerklePathResult {
            name: Some(BANANABLOCKS_NAME.to_string()),
            merkle_path: Some(proof),
            header: None,
            error: None,
            notes: vec![merkle_path_note(BANANABLOCKS_NAME, NOTE_PROOF, None)],
        })
    }

    // =========================================================================
    // Transaction Status
    // =========================================================================

    /// Status for txids (Rule 28, T11 and T12), one `GET /tx/{txid}` each:
    /// in a block is `mined` at depth 1 (the tip is the header service's,
    /// T15), no block is `known`, a 404 is `unknown`. The index lags the
    /// mempool, so the ladder treats this provider's `unknown` as a gap and
    /// its `known` as a word (the ladder's `is_chain_index` is
    /// false). A fault stops the batch: the txids not yet looked at are left
    /// out (no word), and a batch where nothing was looked at is the fault.
    pub async fn get_status_for_txids(&self, txids: &[String]) -> Result<GetStatusForTxidsResult> {
        let mut results = Vec::new();
        let mut fault: Option<Error> = None;
        for txid in txids {
            let answer = match self.get(&format!("{}/tx/{}", self.base_url, txid)).await {
                Ok((StatusCode::OK, body)) => parse_tx_block(&body, txid).map(|b| match b {
                    Some(_) => ("mined", Some(1)),
                    None => ("known", Some(0)),
                }),
                Ok((StatusCode::NOT_FOUND, _)) => Ok(("unknown", None)),
                Ok((status, _)) => Err(Error::ServiceError(format!(
                    "BananaBlocks tx failed with status {}",
                    status
                ))),
                Err(e) => Err(e),
            };
            match answer {
                Ok((status, depth)) => results.push(TxStatusDetail {
                    txid: txid.clone(),
                    status: status.to_string(),
                    depth,
                    ..Default::default()
                }),
                Err(e) => {
                    fault = Some(e);
                    break;
                }
            }
        }
        if results.is_empty() {
            if let Some(e) = fault {
                return Err(e);
            }
        }
        Ok(GetStatusForTxidsResult {
            name: BANANABLOCKS_NAME.to_string(),
            status: "success".to_string(),
            error: fault.map(|e| format!("stopped early: {}", e)),
            results,
        })
    }
}

/// `txid.vout` as the services' outpoints are written.
fn parse_outpoint(outpoint: &str) -> Result<(&str, u32)> {
    let (txid, vout) = outpoint
        .split_once('.')
        .ok_or_else(|| Error::InvalidArgument(format!("outpoint {outpoint}: not txid.vout")))?;
    if !is_hex64(txid) {
        return Err(Error::InvalidArgument(format!(
            "outpoint {outpoint}: not a txid"
        )));
    }
    let vout = vout
        .parse::<u32>()
        .map_err(|_| Error::InvalidArgument(format!("outpoint {outpoint}: not a vout")))?;
    Ok((txid, vout))
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// A char-boundary-safe excerpt of a body for a fault message.
fn excerpt(body: &str) -> &str {
    let mut cut = body.len().min(120);
    while !body.is_char_boundary(cut) {
        cut -= 1;
    }
    &body[..cut]
}

/// The `/txo/{txid}/{vout}` rule: only a 200 with the documented shape
/// answers (the overlay's `parse_bananablocks_spent` rule for `spent`, and
/// the output's script hash for the binding to the asked script).
pub(crate) fn parse_txo_body(
    status: StatusCode,
    body: &str,
    outpoint_txid: &str,
    vout: u32,
) -> Result<Txo> {
    let bad = |why: &str| {
        Error::ServiceError(format!(
            "BananaBlocks txo {outpoint_txid}.{vout}: {why}: {}",
            excerpt(body)
        ))
    };
    if status != StatusCode::OK {
        return Err(bad(&format!("HTTP {status}")));
    }
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| bad(&format!("bad json: {e}")))?;
    if let Some(index) = v.get("index") {
        if index.as_u64() != Some(u64::from(vout)) {
            return Err(bad("the body is about another output"));
        }
    }
    let from_script = match v.get("script_pubkey") {
        None => None,
        Some(script) => {
            let bytes = script
                .as_str()
                .and_then(|h| hex::decode(h).ok())
                .ok_or_else(|| bad("script_pubkey is not hex"))?;
            let mut hash = sha256(&bytes).to_vec();
            hash.reverse();
            Some(hex::encode(hash))
        }
    };
    let given = match v.get("script_hash") {
        None => None,
        Some(h) => Some(
            h.as_str()
                .filter(|h| is_hex64(h))
                .map(str::to_ascii_lowercase)
                .ok_or_else(|| bad("script_hash is not 64 hex"))?,
        ),
    };
    let script_hash = match (from_script, given) {
        (Some(a), Some(b)) if a != b => {
            return Err(bad("script_hash is not the hash of script_pubkey"))
        }
        (Some(h), _) | (None, Some(h)) => h,
        (None, None) => return Err(bad("no script (schema drift?)")),
    };
    let spend = match v.get("spent").and_then(serde_json::Value::as_bool) {
        Some(false) => OutpointSpend::Unspent,
        Some(true) => match v.get("spent_txid").and_then(|t| t.as_str()) {
            Some(t) if is_hex64(t) && !t.eq_ignore_ascii_case(outpoint_txid) => {
                OutpointSpend::SpentBy(t.to_ascii_lowercase())
            }
            _ => return Err(bad("spent:true without a well-formed spent_txid")),
        },
        None => return Err(bad("no boolean `spent` (schema drift?)")),
    };
    Ok(Txo { script_hash, spend })
}

/// The `/address/{address}/utxos` rule: only a 200 whose `items` are each a
/// 64-hex `txid`, a `vout` and a `value` answers; the next page's cursor
/// when `hasMore` (a `hasMore` without a cursor is malformed). An item
/// whose `script_type` is given and is not `p2pkh` is another script's
/// output, not this address's P2PKH one: it is left out.
pub(crate) fn parse_address_utxos(
    status: StatusCode,
    body: &str,
    address: &str,
) -> Result<(Vec<UtxoDetail>, Option<String>)> {
    let bad = |why: &str| {
        Error::ServiceError(format!(
            "BananaBlocks address {address} utxos: {why}: {}",
            excerpt(body)
        ))
    };
    if status != StatusCode::OK {
        return Err(bad(&format!("HTTP {status}")));
    }
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| bad(&format!("bad json: {e}")))?;
    let items = v
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| bad("no `items` list (schema drift?)"))?;
    let next = if v.get("hasMore").and_then(serde_json::Value::as_bool) == Some(true) {
        Some(
            v.get("cursor")
                .and_then(serde_json::Value::as_str)
                .filter(|c| !c.is_empty())
                .ok_or_else(|| bad("`hasMore` without a cursor"))?
                .to_string(),
        )
    } else {
        None
    };
    let details = items
        .iter()
        .filter(|item| {
            item.get("script_type")
                .and_then(serde_json::Value::as_str)
                .is_none_or(|t| t.eq_ignore_ascii_case("p2pkh"))
        })
        .map(|item| {
            let txid = item
                .get("txid")
                .and_then(|t| t.as_str())
                .filter(|t| is_hex64(t))
                .ok_or_else(|| bad("an item without a 64-hex txid"))?;
            let vout = item
                .get("vout")
                .and_then(serde_json::Value::as_u64)
                .and_then(|v| u32::try_from(v).ok())
                .ok_or_else(|| bad("an item without a vout"))?;
            let satoshis = item
                .get("value")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| bad("an item without a value"))?;
            Ok(UtxoDetail {
                txid: txid.to_ascii_lowercase(),
                index: vout,
                satoshis,
                height: item
                    .get("block_height")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|h| u32::try_from(h).ok()),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((details, next))
}

/// `GET /tx/{txid}` → the block `(hash, height)` it is in, `None` when it is
/// in none. A body for another txid, or a half-given block, is a fault.
pub(crate) fn parse_tx_block(body: &str, txid: &str) -> Result<Option<(String, u32)>> {
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| {
        Error::ServiceError(format!(
            "BananaBlocks tx: bad json: {}: {}",
            e,
            excerpt(body)
        ))
    })?;
    match v.get("txid").and_then(|t| t.as_str()) {
        Some(t) if t.eq_ignore_ascii_case(txid) => {}
        _ => {
            return Err(Error::ServiceError(format!(
                "BananaBlocks tx: the body is not about {}: {}",
                txid,
                excerpt(body)
            )))
        }
    }
    let hash = v
        .get("block_hash")
        .and_then(|h| h.as_str())
        .filter(|h| !h.is_empty());
    let height = v
        .get("block_height")
        .and_then(|h| h.as_u64())
        .filter(|h| *h > 0);
    match (hash, height) {
        (None, None) => Ok(None),
        (Some(hash), Some(height)) if is_hex64(hash) && height <= u64::from(u32::MAX) => {
            Ok(Some((hash.to_ascii_lowercase(), height as u32)))
        }
        _ => Err(Error::ServiceError(format!(
            "BananaBlocks tx: a malformed block: {}",
            excerpt(body)
        ))),
    }
}

/// The `/tx/{txid}/proof` TSC body, checked and handed on with the block's
/// hash as its target. Its `txOrId` must be the txid, its nodes 64 hex (or
/// `*`), and they must fold to its target merkle root.
pub(crate) fn parse_tsc_proof(body: &str, txid: &str, block_hash: &str) -> Result<String> {
    let bad =
        |why: &str| Error::ServiceError(format!("BananaBlocks proof: {}: {}", why, excerpt(body)));
    let v: serde_json::Value = serde_json::from_str(body).map_err(|_| bad("bad json"))?;
    let index = v
        .get("index")
        .and_then(|i| i.as_u64())
        .ok_or_else(|| bad("no index"))?;
    match v.get("txOrId").and_then(|t| t.as_str()) {
        Some(t) if t.eq_ignore_ascii_case(txid) => {}
        _ => return Err(bad("txOrId is not the txid")),
    }
    if v.get("targetType").and_then(|t| t.as_str()) != Some("merkleRoot") {
        return Err(bad("target is not a merkle root"));
    }
    let root = v
        .get("target")
        .and_then(|t| t.as_str())
        .filter(|t| is_hex64(t))
        .ok_or_else(|| bad("no merkle root"))?;
    let nodes: Vec<String> = v
        .get("nodes")
        .and_then(|n| n.as_array())
        .ok_or_else(|| bad("no nodes"))?
        .iter()
        .map(|n| match n.as_str() {
            Some(s) if s == "*" || is_hex64(s) => Ok(s.to_ascii_lowercase()),
            _ => Err(bad("a node is not a hash")),
        })
        .collect::<Result<_>>()?;
    if fold_tsc(txid, index, &nodes)? != root.to_ascii_lowercase() {
        return Err(bad("the nodes do not fold to the merkle root"));
    }
    Ok(serde_json::json!({
        "index": index,
        "txOrId": txid.to_ascii_lowercase(),
        "target": block_hash,
        "nodes": nodes,
    })
    .to_string())
}

/// Fold a TSC path to its root (display-order hex).
fn fold_tsc(txid: &str, mut index: u64, nodes: &[String]) -> Result<String> {
    let to_internal = |h: &str| -> Result<Vec<u8>> {
        let mut b = hex::decode(h).map_err(|e| Error::ServiceError(e.to_string()))?;
        b.reverse();
        Ok(b)
    };
    let mut current = to_internal(txid)?;
    for node in nodes {
        let sibling = if node == "*" {
            current.clone()
        } else {
            to_internal(node)?
        };
        let mut joined = Vec::with_capacity(64);
        if index & 1 == 1 {
            joined.extend_from_slice(&sibling);
            joined.extend_from_slice(&current);
        } else {
            joined.extend_from_slice(&current);
            joined.extend_from_slice(&sibling);
        }
        current = sha256d(&joined).to_vec();
        index >>= 1;
    }
    current.reverse();
    Ok(hex::encode(current))
}

fn no_path(error: Option<String>, what: &str) -> GetMerklePathResult {
    GetMerklePathResult {
        name: Some(BANANABLOCKS_NAME.to_string()),
        merkle_path: None,
        header: None,
        error,
        notes: vec![merkle_path_note(BANANABLOCKS_NAME, what, None)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::traits::ProviderVerdict;
    use std::time::Instant;

    const OUT: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const SPENDER: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    /// The output's locking script, as `/txo` carries it (`script_pubkey`).
    const SCRIPT: &str = "76a914e0de61b233403b703ef20fb6b0f5dc0389b5c48a88ac";

    /// The script's hash in the three formats a `get_utxo_status` asks in:
    /// little-endian (sha256 as computed), big-endian (reversed), the script.
    fn asked(format: GetUtxoStatusOutputFormat) -> String {
        let le = sha256(&hex::decode(SCRIPT).unwrap()).to_vec();
        match format {
            GetUtxoStatusOutputFormat::HashLE => hex::encode(&le),
            GetUtxoStatusOutputFormat::HashBE => {
                let mut be = le;
                be.reverse();
                hex::encode(be)
            }
            GetUtxoStatusOutputFormat::Script => SCRIPT.to_string(),
        }
    }

    fn unspent_body() -> String {
        format!(
            r#"{{"value":1945,"index":0,"script_pubkey":"{SCRIPT}","script_type":"p2pkh","spent":false}}"#
        )
    }

    fn spent_body(spender: &str) -> String {
        format!(
            r#"{{"value":1945,"index":0,"script_pubkey":"{SCRIPT}","spent_txid":"{spender}","spent":true,"spend_scope":"confirmed"}}"#
        )
    }

    async fn fixture() -> (mockito::ServerGuard, BananaBlocks) {
        let server = mockito::Server::new_async().await;
        let bb = BananaBlocks::with_base_unshared(&server.url());
        (server, bb)
    }

    async fn txo_route(
        server: &mut mockito::ServerGuard,
        status: usize,
        body: &str,
    ) -> mockito::Mock {
        server
            .mock("GET", format!("/txo/{OUT}/0").as_str())
            .with_status(status)
            .with_body(body)
            .create_async()
            .await
    }

    /// The live shapes (2026-10-22): `spent:false` is a clean unspent, and
    /// `spent:true` with a 64-hex `spent_txid` names the spender (lowered);
    /// the script hash is the big-endian sha256 of `script_pubkey`.
    #[tokio::test]
    async fn the_txo_answers_unspent_and_names_a_spender() {
        let be = asked(GetUtxoStatusOutputFormat::HashBE);
        let (mut server, bb) = fixture().await;
        let _m = txo_route(&mut server, 200, &unspent_body()).await;
        assert_eq!(
            bb.get_txo(OUT, 0).await.unwrap(),
            Txo {
                script_hash: be.clone(),
                spend: OutpointSpend::Unspent
            }
        );

        let (mut server, bb) = fixture().await;
        let _m = txo_route(&mut server, 200, &spent_body(&SPENDER.to_ascii_uppercase())).await;
        assert_eq!(
            bb.get_spend(OUT, 0).await.unwrap(),
            OutpointSpend::SpentBy(SPENDER.to_string())
        );

        // A `script_hash` beside the script must be its hash.
        let (mut server, bb) = fixture().await;
        let body = format!(r#"{{"script_hash":"{be}","script_pubkey":"{SCRIPT}","spent":false}}"#);
        let _m = txo_route(&mut server, 200, &body).await;
        assert_eq!(bb.get_txo(OUT, 0).await.unwrap().script_hash, be);
    }

    /// Every other txo answer is a fault ("could not look"), never
    /// "unspent": a 404 (the index does not know the output), a 5xx, a
    /// malformed body, no script, a `script_hash` that is not the script's,
    /// another output's body, no boolean `spent`, `spent:true` without a
    /// spender, a spender that is not 64 hex, one naming the outpoint's own
    /// transaction.
    #[tokio::test]
    async fn every_other_txo_answer_is_a_fault() {
        let with = |rest: &str| format!(r#"{{"script_pubkey":"{SCRIPT}",{rest}}}"#);
        for (status, body) in [
            (404, r#"{"error":"TXO not found"}"#.to_string()),
            (500, "boom".to_string()),
            (502, String::new()),
            (200, "<html>".to_string()),
            (200, "{}".to_string()),
            (200, r#"{"spent":false}"#.to_string()),
            (200, r#"{"script_pubkey":"zz","spent":false}"#.to_string()),
            (
                200,
                with(&format!(
                    r#""script_hash":"{}","spent":false"#,
                    "ab".repeat(32)
                )),
            ),
            (200, with(r#""index":1,"spent":false"#)),
            (200, with(r#""spent":"false""#)),
            (200, with(r#""spent":true"#)),
            (
                200,
                with(&format!(
                    r#""spent":true,"spent_txid":"{}""#,
                    "zz".repeat(32)
                )),
            ),
            (200, with(r#""spent":true,"spent_txid":"abcd""#)),
            (200, with(&format!(r#""spent":true,"spent_txid":"{OUT}""#))),
        ] {
            let (mut server, bb) = fixture().await;
            let _m = txo_route(&mut server, status, &body).await;
            let answer = bb.get_spend(OUT, 0).await;
            assert!(answer.is_err(), "{status} {body}: {answer:?}");
            let utxo = bb
                .get_utxo_status(
                    &asked(GetUtxoStatusOutputFormat::HashLE),
                    None,
                    Some(&format!("{OUT}.0")),
                )
                .await;
            assert!(utxo.is_err(), "{status} {body}: {utxo:?}");
        }
    }

    /// A 429 is a fault returned at once with its Retry-After, never slept
    /// on; the next call inside the window makes no request.
    #[tokio::test]
    async fn a_429_is_a_fault_at_once_and_the_rung_is_skipped_until_retry_after() {
        let (mut server, bb) = fixture().await;
        let route = server
            .mock("GET", format!("/txo/{OUT}/0").as_str())
            .with_status(429)
            .with_header("retry-after", "17")
            .expect(1)
            .create_async()
            .await;
        let started = Instant::now();
        match bb.get_spend(OUT, 0).await {
            Err(Error::RateLimited {
                provider,
                retry_after_secs,
            }) => {
                assert_eq!(provider, BANANABLOCKS_NAME);
                assert_eq!(retry_after_secs, 17);
            }
            other => panic!("a 429 is a rate-limit fault: {other:?}"),
        }
        match bb.get_raw_tx(OUT).await {
            Err(Error::RateLimited {
                retry_after_secs, ..
            }) => assert!((16..=17).contains(&retry_after_secs)),
            other => panic!("inside Retry-After the rung is skipped: {other:?}"),
        }
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "no sleep: {:?}",
            started.elapsed()
        );
        route.assert_async().await;
    }

    /// The bucket: a burst of 30 refilled at 0.5/s (60 in the first
    /// minute, 30 a minute sustained), and the 31st call at once is a
    /// rate-limit fault without a request.
    #[tokio::test]
    async fn the_provider_paces_itself_at_sixty_a_minute() {
        let (mut server, bb) = fixture().await;
        let route = server
            .mock("GET", format!("/txo/{OUT}/0").as_str())
            .with_status(200)
            .with_body(unspent_body())
            .expect(30)
            .create_async()
            .await;
        for _ in 0..30 {
            assert_eq!(bb.get_spend(OUT, 0).await.unwrap(), OutpointSpend::Unspent);
        }
        assert!(matches!(
            bb.get_spend(OUT, 0).await,
            Err(Error::RateLimited { .. })
        ));
        route.assert_async().await;
    }

    /// The outpoint question, in each format the asked output comes in:
    /// `spent:false` is a positive and a spender a negative, both only for
    /// the asked script; a script's question (no outpoint) is not one it
    /// answers, and asks nothing.
    #[tokio::test]
    async fn the_utxo_answer_is_by_outpoint_and_for_the_asked_script_only() {
        let outpoint = format!("{OUT}.0");
        for format in [
            GetUtxoStatusOutputFormat::HashLE,
            GetUtxoStatusOutputFormat::HashBE,
            GetUtxoStatusOutputFormat::Script,
        ] {
            let (mut server, bb) = fixture().await;
            let _m = txo_route(&mut server, 200, &unspent_body()).await;
            let r = bb
                .get_utxo_status(&asked(format), Some(format), Some(&outpoint))
                .await
                .unwrap();
            assert_eq!(
                (r.status.as_str(), r.is_utxo),
                ("success", Some(true)),
                "{format:?}"
            );
        }
        // No format is little-endian.
        let (mut server, bb) = fixture().await;
        let _m = txo_route(&mut server, 200, &spent_body(SPENDER)).await;
        let r = bb
            .get_utxo_status(
                &asked(GetUtxoStatusOutputFormat::HashLE),
                None,
                Some(&outpoint),
            )
            .await
            .unwrap();
        assert_eq!((r.status.as_str(), r.is_utxo), ("success", Some(false)));

        let (mut server, bb) = fixture().await;
        let route = server
            .mock("GET", mockito::Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let r = bb
            .get_utxo_status(&asked(GetUtxoStatusOutputFormat::HashLE), None, None)
            .await
            .unwrap();
        assert_eq!((r.status.as_str(), r.is_utxo), ("error", None));
        assert!(bb
            .get_utxo_status(
                &asked(GetUtxoStatusOutputFormat::HashLE),
                None,
                Some("nonsense")
            )
            .await
            .is_err());
        route.assert_async().await;
    }

    /// Another script's hash (a big-endian hash asked as little-endian, the
    /// empty script `is_utxo` asks with when none is stored) is a fault,
    /// never a positive, though the index says `spent: false`.
    #[tokio::test]
    async fn a_script_hash_mismatch_is_a_fault_never_unspent() {
        let outpoint = format!("{OUT}.0");
        let empty_script_le = hex::encode(sha256(&[]));
        for (output, format) in [
            (asked(GetUtxoStatusOutputFormat::HashBE), None),
            (
                asked(GetUtxoStatusOutputFormat::HashLE),
                Some(GetUtxoStatusOutputFormat::HashBE),
            ),
            (empty_script_le, None),
            (String::new(), Some(GetUtxoStatusOutputFormat::Script)),
        ] {
            let (mut server, bb) = fixture().await;
            let _m = txo_route(&mut server, 200, &unspent_body()).await;
            let r = bb.get_utxo_status(&output, format, Some(&outpoint)).await;
            match r {
                Err(Error::ServiceError(e)) => assert!(e.contains("not the asked"), "{e}"),
                other => panic!("{output} {format:?}: a mismatch is a fault: {other:?}"),
            }
        }
    }

    /// Two providers built for one base share the process's gate: a 429
    /// one met closes the rung for the other, with no request (the daemon
    /// builds two `Services` per tenant).
    #[tokio::test]
    async fn providers_for_one_base_share_one_gate() {
        // Never reached: the gate stops the call before a request.
        let base = "http://127.0.0.1:9/bananablocks-shared-gate-test";
        let a = BananaBlocks::with_base(base, None).unwrap();
        let b = BananaBlocks::with_base(&format!("{base}/"), None).unwrap();
        let other = BananaBlocks::with_base("http://127.0.0.1:9/another-base", None).unwrap();
        assert!(Arc::ptr_eq(a.rate_gate(), b.rate_gate()));
        assert!(!Arc::ptr_eq(a.rate_gate(), other.rate_gate()));
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "120".parse().unwrap());
        let _ = a.rate_gate().close(&headers);
        let started = Instant::now();
        match b.get_spend(OUT, 0).await {
            Err(Error::RateLimited {
                retry_after_secs, ..
            }) => assert!((119..=120).contains(&retry_after_secs)),
            other => panic!("the shared rung is closed: {other:?}"),
        }
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    /// A one-input transaction and its txid.
    fn a_tx() -> (String, String) {
        use bsv_rs::script::{LockingScript, UnlockingScript};
        use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
        let mut tx = Transaction::new();
        let mut input = TransactionInput::new(OUT.to_string(), 0);
        input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
        tx.inputs.push(input);
        tx.outputs.push(TransactionOutput {
            satoshis: Some(1),
            locking_script: LockingScript::from_hex("51").unwrap(),
            change: false,
        });
        (tx.id(), tx.to_hex())
    }

    /// Bytes that hash to the txid are served; bytes that do not are a
    /// fault, never served; a 404 is "no such transaction"; a 5xx a fault.
    #[tokio::test]
    async fn raw_tx_bytes_must_hash_to_the_txid() {
        let (txid, hex) = a_tx();
        let (mut server, bb) = fixture().await;
        let _m = server
            .mock("GET", format!("/tx/{txid}/hex").as_str())
            .with_status(200)
            .with_body(format!("{hex}\n"))
            .create_async()
            .await;
        let r = bb.get_raw_tx(&txid).await.unwrap();
        assert_eq!(r.raw_tx, Some(hex::decode(&hex).unwrap()));
        assert_eq!(r.name, BANANABLOCKS_NAME);

        let other = "33".repeat(32);
        let (mut server, bb) = fixture().await;
        let _m = server
            .mock("GET", format!("/tx/{other}/hex").as_str())
            .with_status(200)
            .with_body(&hex)
            .create_async()
            .await;
        assert!(bb.get_raw_tx(&other).await.is_err(), "bytes of another tx");

        let (mut server, bb) = fixture().await;
        let _m = server
            .mock("GET", format!("/tx/{other}/hex").as_str())
            .with_status(404)
            .with_body(r#"{"error":"Transaction not found"}"#)
            .create_async()
            .await;
        let r = bb.get_raw_tx(&other).await.unwrap();
        assert!(r.raw_tx.is_none() && !r.could_not_look && r.error.is_none());

        let (mut server, bb) = fixture().await;
        let _m = server
            .mock("GET", format!("/tx/{other}/hex").as_str())
            .with_status(503)
            .create_async()
            .await;
        assert!(bb.get_raw_tx(&other).await.is_err());
    }

    /// A two-transaction block: txid `a` at index 0, `b` its sibling.
    fn two_tx_block() -> (String, String, String) {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let root = fold_tsc(&a, 0, std::slice::from_ref(&b)).unwrap();
        (a, b, root)
    }

    fn tx_body(txid: &str, block: Option<(&str, u32)>) -> String {
        match block {
            Some((hash, height)) => format!(
                r#"{{"txid":"{txid}","block_hash":"{hash}","block_height":{height},"confirmations":3}}"#
            ),
            None => format!(r#"{{"txid":"{txid}","confirmations":0}}"#),
        }
    }

    fn verdicts(r: &GetMerklePathResult) -> Vec<ProviderVerdict> {
        r.provider_verdicts()
    }

    /// The live proof shape (`targetType: merkleRoot`) is checked (its
    /// nodes fold to its root) and handed on with the block's hash as its
    /// target, for the ladder to resolve against the header service.
    #[tokio::test]
    async fn a_mined_transactions_proof_is_handed_on_with_the_block_hash() {
        let (a, b, root) = two_tx_block();
        let block_hash = "cc".repeat(32);
        let (mut server, bb) = fixture().await;
        let _tx = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(200)
            .with_body(tx_body(&a, Some((&block_hash, 900_000))))
            .create_async()
            .await;
        let _proof = server
            .mock("GET", format!("/tx/{a}/proof").as_str())
            .with_status(200)
            .with_body(format!(
                r#"{{"index":0,"txOrId":"{a}","targetType":"merkleRoot","target":"{root}","nodes":["{b}"]}}"#
            ))
            .create_async()
            .await;
        let r = bb.get_merkle_path(&a).await.unwrap();
        let mp: serde_json::Value =
            serde_json::from_str(r.merkle_path.as_deref().unwrap()).unwrap();
        assert_eq!(mp["target"], block_hash);
        assert_eq!(mp["txOrId"], a);
        assert_eq!(mp["index"], 0);
        assert_eq!(mp["nodes"], serde_json::json!([b]));
        assert_eq!(
            verdicts(&r),
            vec![ProviderVerdict::Proof {
                provider: BANANABLOCKS_NAME.to_string()
            }]
        );
    }

    /// A proof whose nodes do not fold to its root, or for another txid,
    /// or of another target type, is a fault; so is a 404 at `/proof` for
    /// a transaction the index placed in a block.
    #[tokio::test]
    async fn a_proof_that_does_not_hold_is_a_fault() {
        let (a, b, root) = two_tx_block();
        let block_hash = "cc".repeat(32);
        let wrong = "dd".repeat(32);
        for (status, body) in [
            (
                200,
                format!(
                    r#"{{"index":0,"txOrId":"{a}","targetType":"merkleRoot","target":"{wrong}","nodes":["{b}"]}}"#
                ),
            ),
            (
                200,
                format!(
                    r#"{{"index":1,"txOrId":"{a}","targetType":"merkleRoot","target":"{root}","nodes":["{b}"]}}"#
                ),
            ),
            (
                200,
                format!(
                    r#"{{"index":0,"txOrId":"{b}","targetType":"merkleRoot","target":"{root}","nodes":["{a}"]}}"#
                ),
            ),
            (
                200,
                format!(
                    r#"{{"index":0,"txOrId":"{a}","targetType":"header","target":"{root}","nodes":["{b}"]}}"#
                ),
            ),
            (
                200,
                format!(
                    r#"{{"index":0,"txOrId":"{a}","targetType":"merkleRoot","target":"{root}","nodes":["xyz"]}}"#
                ),
            ),
            (404, r#"{"error":"Transaction not confirmed"}"#.to_string()),
        ] {
            let (mut server, bb) = fixture().await;
            let _tx = server
                .mock("GET", format!("/tx/{a}").as_str())
                .with_status(200)
                .with_body(tx_body(&a, Some((&block_hash, 900_000))))
                .create_async()
                .await;
            let _proof = server
                .mock("GET", format!("/tx/{a}/proof").as_str())
                .with_status(status)
                .with_body(&body)
                .create_async()
                .await;
            let r = bb.get_merkle_path(&a).await;
            assert!(r.is_err(), "{status} {body}: {r:?}");
        }
    }

    /// No block is a clean "not mined"; a 404 at `/tx` says only that this
    /// index does not know the transaction: inconclusive, never "not
    /// mined" (a reorg re-prove never demotes on it). No proof is asked
    /// for either.
    #[tokio::test]
    async fn not_mined_and_not_indexed_are_told_apart() {
        let (a, _, _) = two_tx_block();
        let (mut server, bb) = fixture().await;
        let _tx = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(200)
            .with_body(tx_body(&a, None))
            .create_async()
            .await;
        let proof = server
            .mock("GET", format!("/tx/{a}/proof").as_str())
            .expect(0)
            .create_async()
            .await;
        let r = bb.get_merkle_path(&a).await.unwrap();
        assert!(r.merkle_path.is_none());
        assert_eq!(
            verdicts(&r),
            vec![ProviderVerdict::NotMined {
                provider: BANANABLOCKS_NAME.to_string()
            }]
        );
        proof.assert_async().await;

        let (mut server, bb) = fixture().await;
        let _tx = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(404)
            .with_body(r#"{"error":"Transaction not found"}"#)
            .create_async()
            .await;
        let r = bb.get_merkle_path(&a).await.unwrap();
        assert!(r.merkle_path.is_none());
        assert_eq!(
            verdicts(&r),
            vec![ProviderVerdict::Inconclusive {
                provider: BANANABLOCKS_NAME.to_string()
            }]
        );
    }

    /// `/tx/{txid}` per txid: in a block is mined at depth 1 (no tip read),
    /// no block is known, a 404 unknown. A fault stops the batch and the
    /// txids not looked at are left out; a fault on the first is the error.
    #[tokio::test]
    async fn the_status_answers_and_a_fault_leaves_the_rest_out() {
        let (a, b, _) = two_tx_block();
        let c = "ee".repeat(32);
        let d = "ff".repeat(32);
        let (mut server, bb) = fixture().await;
        let _a = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(200)
            .with_body(tx_body(&a, Some((&"cc".repeat(32), 900_000))))
            .create_async()
            .await;
        let _b = server
            .mock("GET", format!("/tx/{b}").as_str())
            .with_status(200)
            .with_body(tx_body(&b, None))
            .create_async()
            .await;
        let _c = server
            .mock("GET", format!("/tx/{c}").as_str())
            .with_status(404)
            .create_async()
            .await;
        let _d = server
            .mock("GET", format!("/tx/{d}").as_str())
            .with_status(429)
            .with_header("retry-after", "5")
            .create_async()
            .await;
        let txids = vec![a.clone(), b.clone(), c.clone(), d.clone(), a.clone()];
        let r = bb.get_status_for_txids(&txids).await.unwrap();
        let got: Vec<(&str, &str, Option<u32>)> = r
            .results
            .iter()
            .map(|d| (d.txid.as_str(), d.status.as_str(), d.depth))
            .collect();
        assert_eq!(
            got,
            vec![
                (a.as_str(), "mined", Some(1)),
                (b.as_str(), "known", Some(0)),
                (c.as_str(), "unknown", None),
            ]
        );
        assert!(r
            .error
            .as_deref()
            .is_some_and(|e| e.contains("Rate limited")));

        let (mut server, bb) = fixture().await;
        let _a = server
            .mock("GET", format!("/tx/{a}").as_str())
            .with_status(500)
            .create_async()
            .await;
        assert!(bb
            .get_status_for_txids(std::slice::from_ref(&a))
            .await
            .is_err());
    }

    #[test]
    fn the_tsc_fold_matches_the_live_block() {
        // Block 972169, txid c375f88e… at index 1, as the 2026-10-22 probe
        // served it: the path folds to the header service's merkle root.
        let nodes: Vec<String> = [
            "bc0016e76dda341b06e625c41a1d3a313fdf1eedc58f1254b8ba2564422c1763",
            "0bdf27ab5f900d8a3f2342c12ad7bb752b57dd803cbddf1b137eb80589f23604",
            "84699274058213409e91dddfd5a1707388ce5c4415c110e789c9a46ddc6438ba",
            "30b54bed547e704b448cabf8d4a767449cbef235524240cd31a603563d8f1141",
            "fce35d818426c5fca55c0e37910df80f51f99c1890cda9644efc3dcf67757c36",
            "e4c8d64c64a2bb59fdafc3a453bfabec04bc1827e15316d2051b52e646b720ea",
            "5d77c1a44f469d03aae0a677c77e5015be3b643363a09626d82645653b81778e",
            "70806389e388214fc269dbd2c4a9e58011ada124c8501c26e4a69c5490075cf1",
            "0e996b88155cd8e10e694374eaecec0e8310359d3d7b70a0477e864d6e16c517",
            "fa0a715902e46ba19935b54daa163d041c071719f95770021c84876ab6cc5cf9",
            "4a29b4ff46d188e901b7203e1d74bbc9efc1f89eb2ab1d7d655dd31ec491a15f",
            "ef4eba8c0fab7e7179e57fbcb8ed480250aee9c35c3b72e37b4e4f7981fdf0c0",
            "d82f7a0829d70b35a192426de0d41e6ed34062cdc356fffa66c59f9765371314",
            "56c96ac4d7041fa2b3c622d3709791f11881054bc14d496e3cc99aca45aa891d",
            "222c3c0f643aca0b2ab6a1e0ffaa3593562a47a177cf4413a770d4414faf1b2f",
            "b5b99ec3ba26d0c561994a652710b415d69ea6da6f736468975273fbce12ec66",
            "3af74674dc467f3ccd5f982d30e66bf561bc78b7484dccad47ac74c900d57645",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            fold_tsc(
                "c375f88e925f504dbe440102b368087009db1fc70e55382fc9cdbc15b1c82076",
                1,
                &nodes
            )
            .unwrap(),
            "79f8aaee78f918ac6588ec3f308732549e61caacf9fb9489d46938f834b2a557"
        );
    }

    /// The address list's live shapes (2026-10-22): a page with `hasMore`
    /// is an answer (its leads), an address never seen answers an empty
    /// list; anything else is a fault, never an empty list.
    #[test]
    fn the_address_list_answers_only_in_its_documented_shape() {
        let address = "1MVzmKLnHNJ9Dkv7xb8iQUzy2mKrJNzTcH";
        let page = r#"{"items":[{"txid":"b4ebcb258d4ac9b7811ae032ec66a68437a6f370b14bccd8b29b3f4665ab0600","vout":1,"value":1945,"script_type":"p2pkh","block_height":972200,"block_time":1792697684}],"cursor":"OTcyMjAwOjE6MDA=","hasMore":true}"#;
        let (listed, next) = parse_address_utxos(StatusCode::OK, page, address).unwrap();
        assert_eq!(next.as_deref(), Some("OTcyMjAwOjE6MDA="));
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].txid,
            "b4ebcb258d4ac9b7811ae032ec66a68437a6f370b14bccd8b29b3f4665ab0600"
        );
        assert_eq!(
            (listed[0].index, listed[0].satoshis, listed[0].height),
            (1, 1945, Some(972200))
        );
        let never_seen = r#"{"items":[],"hasMore":false}"#;
        let (listed, next) = parse_address_utxos(StatusCode::OK, never_seen, address).unwrap();
        assert!(listed.is_empty() && next.is_none());
        // Another script type under the address is not its P2PKH output.
        let mixed = r#"{"items":[{"txid":"b4ebcb258d4ac9b7811ae032ec66a68437a6f370b14bccd8b29b3f4665ab0600","vout":1,"value":1945,"script_type":"p2pk"},{"txid":"b4ebcb258d4ac9b7811ae032ec66a68437a6f370b14bccd8b29b3f4665ab0600","vout":2,"value":5}],"hasMore":false}"#;
        let (listed, _) = parse_address_utxos(StatusCode::OK, mixed, address).unwrap();
        assert_eq!(
            listed.iter().map(|d| d.index).collect::<Vec<_>>(),
            vec![2],
            "a p2pk item is left out, an item with no script_type kept"
        );

        for (status, body) in [
            (StatusCode::NOT_FOUND, "404 page not found"),
            (StatusCode::INTERNAL_SERVER_ERROR, ""),
            (StatusCode::OK, "<html></html>"),
            (StatusCode::OK, r#"{"utxos":[]}"#),
            (
                StatusCode::OK,
                r#"{"items":[{"txid":"zz","vout":0,"value":1}]}"#,
            ),
            (StatusCode::OK, r#"{"items":[],"hasMore":true}"#),
            (
                StatusCode::OK,
                r#"{"items":[{"txid":"b4ebcb258d4ac9b7811ae032ec66a68437a6f370b14bccd8b29b3f4665ab0600","value":1}]}"#,
            ),
        ] {
            assert!(
                parse_address_utxos(status, body, address).is_err(),
                "{status} {body}"
            );
        }
    }
}
