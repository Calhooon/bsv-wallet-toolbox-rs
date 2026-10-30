use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bsv_rs::primitives::{hash160, PrivateKey};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    Beef, ChainTracker, ChainTrackerError, MerklePath, Transaction, TransactionInput,
    TransactionOutput,
};
use bsv_rs::wallet::{Counterparty, KeyDeriverApi, ProtoWallet, Protocol, SecurityLevel};
use bsv_wallet_toolbox_rs::services::mock::{MockErrorKind, MockResponse, MockWalletServices};
use bsv_wallet_toolbox_rs::services::Chain;
use bsv_wallet_toolbox_rs::storage::sqlx::StorageSqlxOptions;
use bsv_wallet_toolbox_rs::storage::{AuthId, MonitorStorage, WalletStorageWriter};
use bsv_wallet_toolbox_rs::StorageSqlx;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;

use super::{
    model::{Inputs, SeedOutput, SeedTransaction},
    Replay,
};

pub(super) const PREFIX: &str = "Pg==";
pub(super) const SUFFIX: &str = "Sg==";

pub(super) struct Seeded {
    pub tx: Transaction,
    pub parent: Option<String>,
    pub proof: Option<MerklePath>,
}

// Synthetic roots are explicit fixture inputs, never chain evidence.
struct FixtureTracker(Vec<(u32, String)>);

#[async_trait]
impl ChainTracker for FixtureTracker {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> std::result::Result<bool, ChainTrackerError> {
        Ok(self.0.iter().any(|(h, r)| *h == height && r == root))
    }

    async fn current_height(&self) -> std::result::Result<u32, ChainTrackerError> {
        Ok(880_000)
    }

    async fn mining_tip(
        &self,
    ) -> std::result::Result<Option<bsv_rs::transaction::beef_stream::ChainTip>, ChainTrackerError>
    {
        #[allow(deprecated)]
        let tip = bsv_rs::transaction::beef_stream::ChainTip {
            height: 880_000,
            median_time_past: 1_700_000_000,
            // The deprecated copy (bsv-rs 0.9.0-beta.4) from ChainParams; the
            // verifiers read ChainParams alone (#451).
            genesis_height: bsv_rs::block::ChainParams::mainnet().genesis_height,
            csv_height: 419_328,
            next_block_time: None,
        };
        Ok(Some(tip))
    }
}

pub(super) fn derived_lock(
    root: &PrivateKey,
    prefix: &str,
    suffix: &str,
    sender: Option<&str>,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let proto = ProtoWallet::new(Some(root.clone()));
    let counterparty = match sender {
        Some(key) => Counterparty::Other(bsv_rs::primitives::PublicKey::from_hex(key)?),
        None => Counterparty::Self_,
    };
    let key = proto.key_deriver().derive_private_key(
        &Protocol::new(SecurityLevel::Counterparty, "3241645161d8"),
        &format!("{prefix} {suffix}"),
        &counterparty,
    )?;
    let public = key.public_key().to_compressed().to_vec();
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend(hash160(&public));
    script.extend([0x88, 0xac]);
    Ok((script, public))
}

/// Regtest replays model the post-Genesis era at tip 880000 (#422).
pub(super) async fn setup(inputs: &Inputs) -> Result<Replay> {
    let keys: Value =
        serde_json::from_str(include_str!("../../../test_vectors/keys/test_users.json"))?;
    let user = keys["test_users"]
        .as_array()
        .context("test users")?
        .iter()
        .find(|u| u["name"] == inputs.user)
        .context("test user name")?;
    let root = PrivateKey::from_hex(user["privateKeyHex"].as_str().context("test private key")?)?;
    let identity = root.public_key().to_hex();
    let storage = StorageSqlx::new_with_options(
        "sqlite::memory:",
        StorageSqlxOptions {
            chain: Chain::Regtest,
            ..Default::default()
        },
    )
    .await?;
    storage
        .migrate(
            "storage-fixtures",
            keys["storage_configuration"]["storageIdentityKey"]
                .as_str()
                .unwrap(),
        )
        .await?;
    storage.make_available().await?;
    let (user, _) = storage.find_or_insert_user(&identity).await?;
    let mut replay = Replay {
        storage,
        auth: AuthId::with_user_id(identity, user.user_id),
        root,
        seeds: BTreeMap::new(),
        drafts: BTreeMap::new(),
        wallet: None,
        wallet_handles: BTreeMap::new(),
    };
    for basket in &inputs.baskets {
        basket_id(&replay, &basket.name).await?;
        sqlx::query("UPDATE output_baskets SET number_of_desired_utxos = ?, minimum_desired_utxo_value = ? WHERE name = ?")
            .bind(i64::from(basket.desired)).bind(basket.minimum as i64).bind(&basket.name)
            .execute(replay.storage.writer()).await?;
    }
    for (index, transaction) in inputs.transactions.iter().enumerate() {
        seed_transaction(&mut replay, transaction, index).await?;
    }
    let roots = replay
        .seeds
        .values()
        .filter_map(|seed| seed.proof.as_ref())
        .map(|p| Ok((p.block_height, p.compute_root(None)?)))
        .collect::<Result<Vec<_>>>()?;
    replay
        .storage
        .set_chain_tracker(Arc::new(FixtureTracker(roots)))
        .await;
    replay
        .storage
        .set_max_acceptable_proof_height(880_000)
        .await?;
    let wallet = bsv_wallet_toolbox_rs::Wallet::new(
        Some(replay.root.clone()),
        replay.storage.clone(),
        services(replay.resolve(inputs.services.clone())?)?,
    )
    .await?;
    replay.wallet = Some(wallet);
    Ok(replay)
}

async fn basket_id(replay: &Replay, name: &str) -> Result<i64> {
    let id = sqlx::query_scalar("INSERT INTO output_baskets (user_id, name) VALUES (?, ?) ON CONFLICT(name, user_id) DO UPDATE SET name = excluded.name RETURNING basket_id")
        .bind(replay.auth.user_id).bind(name).fetch_one(replay.storage.writer()).await?;
    Ok(id)
}

fn script(replay: &Replay, output: &SeedOutput) -> Result<Vec<u8>> {
    match &output.script {
        Some(script) => Ok(hex::decode(script)?),
        None if output.change => Ok(derived_lock(&replay.root, PREFIX, SUFFIX, None)?.0),
        None => Ok(vec![0x75, 0x51]),
    }
}

async fn seed_transaction(replay: &mut Replay, seed: &SeedTransaction, index: usize) -> Result<()> {
    anyhow::ensure!(
        !replay.seeds.contains_key(&seed.name),
        "duplicate seed name"
    );
    let (parent, source_txid, vout) = if let Some(outpoint) = &seed.spends {
        let (name, vout) = outpoint.rsplit_once('.').context("seed spends name.vout")?;
        (
            Some(name.to_string()),
            replay.seeds.get(name).context("seed parent")?.tx.id(),
            vout.parse()?,
        )
    } else {
        (None, "11".repeat(32), index as u32)
    };
    let expanded: Vec<_> = seed
        .outputs
        .iter()
        .flat_map(|o| std::iter::repeat_n(o, o.repeat))
        .collect();
    anyhow::ensure!(!expanded.is_empty(), "seed has no outputs");
    let outputs = expanded
        .iter()
        .map(|o| {
            Ok(TransactionOutput {
                satoshis: Some(o.satoshis),
                locking_script: LockingScript::from_binary(&script(replay, o)?)?,
                change: false,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let tx = Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some(source_txid),
            source_output_index: vout,
            unlocking_script: Some(match &seed.unlocking {
                Some(script) => UnlockingScript::from_binary(&hex::decode(script)?)?,
                None => UnlockingScript::from_binary(&[0x00])?,
            }),
            unlocking_script_template: None,
            sequence: u32::MAX,
        }],
        outputs,
        index as u32,
    );
    let txid = tx.id();
    let proof = seed
        .proven
        .then(|| MerklePath::from_coinbase_txid(&txid, 100_000 + index as u32));
    if seed.store {
        let proven_id = if let Some(proof) = &proof {
            Some(sqlx::query("INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) VALUES (?, ?, 0, ?, ?, ?, ?)")
                .bind(&txid).bind(i64::from(proof.block_height)).bind("aa".repeat(32)).bind(&txid)
                .bind(proof.to_binary()).bind(tx.to_binary()).execute(replay.storage.writer()).await?.last_insert_rowid())
        } else {
            None
        };
        let tx_row = sqlx::query("INSERT INTO transactions (user_id, proven_tx_id, status, reference, is_outgoing, satoshis, version, lock_time, description, txid, raw_tx) VALUES (?, ?, ?, ?, 0, ?, 1, ?, ?, ?, ?)")
            .bind(replay.auth.user_id).bind(proven_id).bind(&seed.status).bind(format!("seed-{}", seed.name))
            .bind(expanded.iter().map(|o| o.satoshis as i64).sum::<i64>()).bind(index as i64)
            .bind(format!("seed {}", seed.name)).bind(&txid).bind(tx.to_binary())
            .execute(replay.storage.writer()).await?.last_insert_rowid();
        for (vout, output) in expanded.iter().enumerate() {
            let basket = match &output.basket {
                Some(name) => Some(basket_id(replay, name).await?),
                None => None,
            };
            let locking = script(replay, output)?;
            let output_id = sqlx::query("INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, vout, satoshis, provided_by, purpose, type, output_description, txid, derivation_prefix, derivation_suffix, custom_instructions, locking_script) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(replay.auth.user_id).bind(tx_row).bind(basket).bind(output.change).bind(vout as i64)
                .bind(output.satoshis as i64).bind(if output.change { "storage" } else { "you" })
                .bind(if output.change { "change" } else { "" }).bind(if locking.len() == 25 { "P2PKH" } else { "custom" })
                .bind(format!("{} output {vout}", seed.name)).bind(&txid)
                .bind(output.change.then_some(PREFIX)).bind(output.change.then_some(SUFFIX))
                .bind(&output.custom_instructions).bind(locking).execute(replay.storage.writer()).await?.last_insert_rowid();
            for tag in &output.tags {
                let tag_id: i64 = sqlx::query_scalar("INSERT INTO output_tags (user_id, tag) VALUES (?, ?) ON CONFLICT(tag, user_id) DO UPDATE SET tag = excluded.tag RETURNING output_tag_id")
                    .bind(replay.auth.user_id).bind(tag).fetch_one(replay.storage.writer()).await?;
                sqlx::query("INSERT INTO output_tags_map (output_tag_id, output_id) VALUES (?, ?)")
                    .bind(tag_id)
                    .bind(output_id)
                    .execute(replay.storage.writer())
                    .await?;
            }
        }
        for label in &seed.labels {
            let label_id: i64 = sqlx::query_scalar("INSERT INTO tx_labels (user_id, label) VALUES (?, ?) ON CONFLICT(label, user_id) DO UPDATE SET label = excluded.label RETURNING tx_label_id")
                .bind(replay.auth.user_id).bind(label).fetch_one(replay.storage.writer()).await?;
            sqlx::query("INSERT INTO tx_labels_map (tx_label_id, transaction_id) VALUES (?, ?)")
                .bind(label_id)
                .bind(tx_row)
                .execute(replay.storage.writer())
                .await?;
        }
        if let Some(satoshis) = seed.commission_satoshis {
            replay
                .storage
                .insert_commission(
                    replay.auth.user_id.unwrap(),
                    tx_row,
                    satoshis as i64,
                    &[0x51],
                    "fixture-commission",
                )
                .await?;
        }
    }
    replay
        .seeds
        .insert(seed.name.clone(), Seeded { tx, parent, proof });
    Ok(())
}

impl Replay {
    pub(super) fn beef(&self, name: &str) -> Result<Vec<u8>> {
        fn merge(replay: &Replay, name: &str, beef: &mut Beef) -> Result<()> {
            let seed = replay.seeds.get(name).context("unknown BEEF seed")?;
            if seed.proof.is_none() {
                if let Some(parent) = &seed.parent {
                    merge(replay, parent, beef)?;
                }
            }
            let bump = seed.proof.as_ref().map(|p| beef.merge_bump(p.clone()));
            beef.merge_raw_tx(seed.tx.to_binary(), bump);
            Ok(())
        }
        let mut beef = Beef::new();
        merge(self, name, &mut beef)?;
        Ok(beef.to_binary_atomic(&self.seeds[name].tx.id())?)
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Answer<T> {
    Success { value: T },
    Error { message: String },
    Sequence { values: Vec<Answer<T>> },
}

impl<T: Clone> Answer<T> {
    fn mock(self) -> MockResponse<T> {
        match self {
            Self::Success { value } => MockResponse::Success(value),
            Self::Error { message } => MockResponse::Error(MockErrorKind::ServiceError, message),
            Self::Sequence { values } => {
                MockResponse::Sequence(values.into_iter().map(Self::mock).collect())
            }
        }
    }
}

fn answer<T: Clone + DeserializeOwned>(value: Value) -> Result<MockResponse<T>> {
    Ok(serde_json::from_value::<Answer<T>>(value)?.mock())
}

fn services(config: Value) -> Result<MockWalletServices> {
    let mut builder = MockWalletServices::builder();
    if !config.is_null() {
        for (method, response) in config.as_object().context("services object")? {
            builder = match method.as_str() {
                "get_utxo_leads" => builder.get_utxo_leads_response(answer(response.clone())?),
                "get_utxo_status" => builder.get_utxo_status_response(answer(response.clone())?),
                "get_raw_tx" => builder.get_raw_tx_response(answer(response.clone())?),
                "get_merkle_path" => builder.get_merkle_path_response(answer(response.clone())?),
                "post_beef" => builder.post_beef_response(answer(response.clone())?),
                "is_utxo" => builder.is_utxo_response(answer(response.clone())?),
                _ => anyhow::bail!("unknown mock method {method}"),
            };
        }
    }
    Ok(builder.build())
}
