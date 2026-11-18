//! Shared fixtures of the sync row tests: stores, chunk and row builders.
#![allow(dead_code)]

pub mod cells;
pub mod double;

use std::sync::Arc;

use bsv_wallet_toolbox_rs::storage::entities::*;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;
use chrono::{DateTime, Duration, Utc};

pub const ALICE: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const BOB: &str = "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub const SRC: &str = "1111111111111111111111111111111111111111111111111111111111111111";
pub const DST: &str = "2222222222222222222222222222222222222222222222222222222222222222";

pub const ENTITIES: [&str; 12] = [
    "outputBasket",
    "provenTx",
    "provenTxReq",
    "txLabel",
    "outputTag",
    "transaction",
    "output",
    "txLabelMap",
    "outputTagMap",
    "certificate",
    "certificateField",
    "commission",
];

pub async fn store(sik: &str) -> Arc<StorageSqlx> {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate("rows", sik).await.unwrap();
    s.make_available().await.unwrap();
    s
}

pub fn args_for(identity: &str) -> RequestSyncChunkArgs {
    RequestSyncChunkArgs {
        from_storage_identity_key: SRC.to_string(),
        to_storage_identity_key: DST.to_string(),
        identity_key: identity.to_string(),
        since: None,
        max_rough_size: 10_000_000,
        max_items: 1000,
        offsets: ENTITIES
            .iter()
            .map(|n| SyncOffset {
                name: n.to_string(),
                offset: 0,
            })
            .collect(),
    }
}

pub fn chunk_for(identity: &str) -> SyncChunk {
    SyncChunk {
        from_storage_identity_key: SRC.to_string(),
        to_storage_identity_key: DST.to_string(),
        user_identity_key: identity.to_string(),
        ..Default::default()
    }
}

pub async fn exec(s: &StorageSqlx, sql: &str) -> i64 {
    ::sqlx::query(sql)
        .execute(s.writer())
        .await
        .unwrap()
        .last_insert_rowid()
}

pub async fn count(s: &StorageSqlx, sql: &str) -> i64 {
    ::sqlx::query_scalar(sql).fetch_one(s.pool()).await.unwrap()
}

pub fn at(days: i64) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
        + Duration::days(days)
}

pub fn basket(id: i64, name: &str) -> TableOutputBasket {
    TableOutputBasket {
        basket_id: id,
        user_id: 1,
        name: name.to_string(),
        number_of_desired_utxos: 6,
        minimum_desired_utxo_value: 1000,
        created_at: at(0),
        updated_at: at(0),
    }
}

pub fn transaction(id: i64, reference: &str, txid: Option<&str>) -> TableTransaction {
    TableTransaction {
        transaction_id: id,
        user_id: 1,
        txid: txid.map(str::to_string),
        status: TransactionStatus::Unproven,
        reference: reference.to_string(),
        description: "synced".to_string(),
        satoshis: 0,
        version: 1,
        lock_time: 0,
        raw_tx: None,
        input_beef: None,
        is_outgoing: false,
        proof_txid: None,
        created_at: at(0),
        updated_at: at(0),
    }
}

/// One row of every entity for `uid`, every nullable column set (`set`) or
/// unset, the transaction linked to its proof when `link`. `tag` is one hex
/// digit: the user's txid is that digit 64 times.
pub async fn seed_all(s: &StorageSqlx, uid: i64, tag: char, set: bool, link: bool) -> String {
    let now = "'2026-01-01 00:00:00'";
    let n = |v: &str| {
        if set {
            v.to_string()
        } else {
            "NULL".to_string()
        }
    };
    let txid = tag.to_string().repeat(64);
    let ptx = exec(s, &format!("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES ('{txid}',1,0,'{h}','{h}',X'00',X'00',{now},{now})", h = "e".repeat(64))).await;
    exec(s, &format!("INSERT INTO proven_tx_reqs (proven_tx_id,txid,status,attempts,history,notified,notify,raw_tx,input_beef,batch,created_at,updated_at) VALUES ({},'{txid}','completed',0,'{{}}',0,'{{}}',X'00',{},{},{now},{now})", n(&ptx.to_string()), n("X'01'"), n("'b'"))).await;
    let basket = exec(s, &format!("INSERT INTO output_baskets (user_id,name,number_of_desired_utxos,minimum_desired_utxo_value,created_at,updated_at) VALUES ({uid},'{tag}bk',6,1000,{now},{now})")).await;
    let label = exec(s, &format!("INSERT INTO tx_labels (user_id,label,created_at,updated_at) VALUES ({uid},'{tag}lb',{now},{now})")).await;
    let otag = exec(s, &format!("INSERT INTO output_tags (user_id,tag,created_at,updated_at) VALUES ({uid},'{tag}tg',{now},{now})")).await;
    let link_col = if link {
        ptx.to_string()
    } else {
        "NULL".to_string()
    };
    let tx = exec(s, &format!("INSERT INTO transactions (user_id,proven_tx_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,input_beef,raw_tx,created_at,updated_at) VALUES ({uid},{link_col},'completed','{tag}ref',0,5,1,0,'desc','{txid}',{},{},{now},{now})", n("X'02'"), n("X'03'"))).await;
    let out = exec(s, &format!("INSERT INTO outputs (user_id,transaction_id,basket_id,spendable,change,vout,satoshis,provided_by,purpose,type,output_description,txid,sender_identity_key,derivation_prefix,derivation_suffix,custom_instructions,spent_by,sequence_number,spending_description,script_length,script_offset,locking_script,created_at,updated_at) VALUES ({uid},{tx},{},1,1,0,1000,'you','change','P2PKH',{},'{txid}',{},{},{},{},{},{},{},{},{},{},{now},{now})",
        n(&basket.to_string()), n("'od'"), n("'02ab'"), n("'pre'"), n("'suf'"), n("'ci'"), n(&tx.to_string()), n("7"), n("'sd'"), n("25"), n("0"), n("X'76a914000000000000000000000000000000000000000088ac'"))).await;
    exec(s, &format!("INSERT INTO tx_labels_map (tx_label_id,transaction_id,created_at,updated_at) VALUES ({label},{tx},{now},{now})")).await;
    exec(s, &format!("INSERT INTO output_tags_map (output_tag_id,output_id,created_at,updated_at) VALUES ({otag},{out},{now},{now})")).await;
    let cert = exec(s, &format!("INSERT INTO certificates (user_id,type,serial_number,certifier,subject,verifier,revocation_outpoint,signature,created_at,updated_at) VALUES ({uid},'{tag}ty','sn','ce','su',{},'op.0','sig',{now},{now})", n("'ve'"))).await;
    exec(s, &format!("INSERT INTO certificate_fields (certificate_id,user_id,field_name,field_value,master_key,created_at,updated_at) VALUES ({cert},{uid},'f','v','mk',{now},{now})")).await;
    exec(s, &format!("INSERT INTO commissions (user_id,transaction_id,satoshis,key_offset,is_redeemed,locking_script,created_at,updated_at) VALUES ({uid},{tx},10,'ko',0,X'51',{now},{now})")).await;
    txid
}

pub fn proof(id: i64, txid: &str) -> TableProvenTx {
    TableProvenTx {
        proven_tx_id: id,
        txid: txid.to_string(),
        height: 1,
        index: 0,
        block_hash: "e".repeat(64),
        merkle_root: "e".repeat(64),
        merkle_path: vec![0],
        raw_tx: vec![0],
        created_at: at(0),
        updated_at: at(0),
    }
}

/// The store holds `p` as its own proof row (written by SQL, not through
/// the sync door, which validates a proof row it does not hold).
pub async fn hold(s: &StorageSqlx, p: &TableProvenTx) {
    ::sqlx::query("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)")
        .bind(&p.txid)
        .bind(p.height)
        .bind(p.index)
        .bind(&p.block_hash)
        .bind(&p.merkle_root)
        .bind(&p.merkle_path)
        .bind(&p.raw_tx)
        .bind(p.created_at)
        .bind(p.updated_at)
        .execute(s.writer())
        .await
        .unwrap();
}

pub fn request(id: i64, txid: &str, proof_id: Option<i64>) -> TableProvenTxReq {
    TableProvenTxReq {
        proven_tx_req_id: id,
        txid: txid.to_string(),
        status: ProvenTxReqStatus::Unmined,
        attempts: 0,
        history: "{}".to_string(),
        notified: false,
        notify: "{}".to_string(),
        raw_tx: Some(vec![1]),
        input_beef: None,
        proven_tx_id: proof_id,
        batch: None,
        created_at: at(0),
        updated_at: at(0),
    }
}

pub async fn stored_link(s: &StorageSqlx, table: &str, txid: &str) -> Option<String> {
    ::sqlx::query_scalar::<_, Option<String>>(&format!(
        "SELECT p.txid FROM {table} r LEFT JOIN proven_txs p ON p.proven_tx_id = r.proven_tx_id WHERE r.txid = ?"
    ))
    .bind(txid)
    .fetch_one(s.pool())
    .await
    .unwrap()
}

pub fn output_row(id: i64, transaction_id: i64, txid: &str) -> TableOutput {
    TableOutput {
        output_id: id,
        user_id: 1,
        transaction_id,
        basket_id: None,
        spendable: true,
        change: true,
        output_description: None,
        vout: 0,
        satoshis: 1000,
        provided_by: "you".to_string(),
        purpose: Some("change".to_string()),
        output_type: "P2PKH".to_string(),
        txid: txid.to_string(),
        sender_identity_key: None,
        derivation_prefix: Some("pre".to_string()),
        derivation_suffix: Some("suf".to_string()),
        custom_instructions: None,
        spent_by: None,
        sequence_number: None,
        spending_description: None,
        script_length: 1,
        script_offset: 0,
        locking_script: Some(vec![0x51]),
        created_at: at(0),
        updated_at: at(0),
    }
}

pub async fn active_storage_of(s: &StorageSqlx, identity: &str) -> Option<String> {
    s.find_user(identity).await.unwrap().unwrap().active_storage
}

pub async fn req_row(
    s: &StorageSqlx,
    txid: &str,
) -> (
    String,
    i64,
    Vec<u8>,
    Option<Vec<u8>>,
    Option<i64>,
    String,
    String,
    Option<String>,
) {
    ::sqlx::query_as(
        "SELECT status, attempts, raw_tx, input_beef, proven_tx_id, history, notify, batch FROM proven_tx_reqs WHERE txid = ?",
    )
    .bind(txid)
    .fetch_one(s.pool())
    .await
    .unwrap()
}

/// A call's refusal text, or `ok`.
pub fn refusal<T>(r: &bsv_wallet_toolbox_rs::Result<T>) -> String {
    match r {
        Ok(_) => "ok".to_string(),
        Err(e) => e.to_string(),
    }
}

/// Awaits `f` for at most ten seconds; past that the test fails naming
/// `what`, where it would otherwise wait without end.
pub async fn bounded<F: std::future::Future>(what: &str, f: F) -> F::Output {
    tokio::time::timeout(std::time::Duration::from_secs(10), f)
        .await
        .unwrap_or_else(|_| panic!("{what}: no answer within 10 s"))
}
