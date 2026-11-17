//! Sync rows: what a store's sync reader sends and what its sync writer takes
//! from a chunk. Each test is one rule; the rule is named in its doc line.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_wallet_toolbox_rs::storage::entities::*;
use bsv_wallet_toolbox_rs::storage::*;
use bsv_wallet_toolbox_rs::StorageSqlx;
use chrono::{DateTime, Duration, Utc};

const ALICE: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "02bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SRC: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const DST: &str = "2222222222222222222222222222222222222222222222222222222222222222";

const ENTITIES: [&str; 12] = [
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

async fn store(sik: &str) -> Arc<StorageSqlx> {
    let s = Arc::new(StorageSqlx::in_memory().await.unwrap());
    s.migrate("rows", sik).await.unwrap();
    s.make_available().await.unwrap();
    s
}

fn args_for(identity: &str) -> RequestSyncChunkArgs {
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

fn chunk_for(identity: &str) -> SyncChunk {
    SyncChunk {
        from_storage_identity_key: SRC.to_string(),
        to_storage_identity_key: DST.to_string(),
        user_identity_key: identity.to_string(),
        ..Default::default()
    }
}

async fn exec(s: &StorageSqlx, sql: &str) -> i64 {
    ::sqlx::query(sql)
        .execute(s.writer())
        .await
        .unwrap()
        .last_insert_rowid()
}

async fn count(s: &StorageSqlx, sql: &str) -> i64 {
    ::sqlx::query_scalar(sql).fetch_one(s.pool()).await.unwrap()
}

fn at(days: i64) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
        + Duration::days(days)
}

fn basket(id: i64, name: &str) -> TableOutputBasket {
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

fn transaction(id: i64, reference: &str, txid: Option<&str>) -> TableTransaction {
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

/// One chunk, one transaction: a chunk whose transaction row fails leaves none
/// of the rows it wrote before it.
#[tokio::test]
async fn a_failing_chunk_leaves_none_of_its_rows() {
    let s = store(DST).await;
    let (bob, _) = s.find_or_insert_user(BOB).await.unwrap();
    s.find_or_insert_user(ALICE).await.unwrap();
    exec(
        &s,
        &format!(
            "INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,description,created_at,updated_at) \
             VALUES ({},'unproven','clash',0,0,'bob''s',datetime('now'),datetime('now'))",
            bob.user_id
        ),
    )
    .await;
    let mut chunk = chunk_for(ALICE);
    chunk.output_baskets = Some(vec![basket(7, "left-behind")]);
    chunk.transactions = Some(vec![transaction(9, "clash", None)]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    assert!(r.is_err(), "the chunk fails on its transaction row: {r:?}");
    let left = count(
        &s,
        "SELECT COUNT(*) FROM output_baskets WHERE name = 'left-behind'",
    )
    .await;
    let states = count(&s, "SELECT COUNT(*) FROM sync_states").await;
    println!("WITNESS B20 rows left by the failed chunk: baskets={left} sync_states={states}");
    assert_eq!((left, states), (0, 0));
}

/// One row of every entity for `uid`, every nullable column set (`set`) or
/// unset, the transaction linked to its proof when `link`. `tag` is one hex
/// digit: the user's txid is that digit 64 times.
async fn seed_all(s: &StorageSqlx, uid: i64, tag: char, set: bool, link: bool) -> String {
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

/// The reader answers every fetch for a store holding every entity, nullable
/// columns set and unset, and a completed transaction linked to its proof;
/// a backup of that store through the manager completes, twice, and the
/// backup's transaction is linked to the backup's own row of that proof.
#[tokio::test]
async fn the_reader_answers_for_a_transaction_linked_to_its_proof() {
    for (set, link) in [(true, false), (false, false), (true, true), (false, true)] {
        let s = store(SRC).await;
        let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
        seed_all(&s, u.user_id, 'a', set, link).await;
        for e in ENTITIES {
            let mut a = args_for(ALICE);
            a.offsets.retain(|o| o.name == e);
            let s2 = s.clone();
            let r = tokio::spawn(async move { s2.get_sync_chunk(a).await }).await;
            let verdict = match r {
                Ok(Ok(_)) => "ok".to_string(),
                Ok(Err(e)) => format!("ERR {e}"),
                Err(_) => "PANIC".to_string(),
            };
            if verdict != "ok" {
                println!("WITNESS A1 set={set} link={link} {e}: {verdict}");
            }
            assert_eq!(verdict, "ok", "{e} set={set} link={link}");
        }
    }

    let act = store(SRC).await;
    let bak = store(DST).await;
    let (u, _) = act.find_or_insert_user(ALICE).await.unwrap();
    let txid = seed_all(&act, u.user_id, 'a', true, true).await;
    let (bu, _) = bak.find_or_insert_user(ALICE).await.unwrap();
    bak.update_user_active_storage(bu.user_id, SRC)
        .await
        .unwrap();
    let mgr = Arc::new(bsv_wallet_toolbox_rs::managers::WalletStorageManager::new(
        ALICE.to_string(),
        Some(act.clone() as Arc<dyn MonitorStorage>),
        Some(vec![bak.clone() as Arc<dyn MonitorStorage>]),
    ));
    mgr.make_available().await.unwrap();
    for round in 1..=2 {
        let m = mgr.clone();
        let r = tokio::spawn(async move { m.update_backups().await }).await;
        let verdict = match &r {
            Ok(Ok(_)) => "ok".to_string(),
            Ok(Err(e)) => format!("ERR {e}"),
            Err(_) => "PANIC".to_string(),
        };
        println!("WITNESS A1 update_backups round {round}: {verdict}");
        assert_eq!(verdict, "ok");
    }
    let linked = count(
        &bak,
        &format!(
            "SELECT COUNT(*) FROM transactions t JOIN proven_txs p ON p.proven_tx_id = t.proven_tx_id \
             WHERE t.txid = '{txid}' AND p.txid = '{txid}'"
        ),
    )
    .await;
    println!("WITNESS A1 backup transaction linked to its own proof row: {linked}");
    assert_eq!(linked, 1);
}

fn proof(id: i64, txid: &str) -> TableProvenTx {
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

fn request(id: i64, txid: &str, proof_id: Option<i64>) -> TableProvenTxReq {
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

async fn stored_link(s: &StorageSqlx, table: &str, txid: &str) -> Option<String> {
    ::sqlx::query_scalar::<_, Option<String>>(&format!(
        "SELECT p.txid FROM {table} r LEFT JOIN proven_txs p ON p.proven_tx_id = r.proven_tx_id WHERE r.txid = ?"
    ))
    .bind(txid)
    .fetch_one(s.pool())
    .await
    .unwrap()
}

/// A transaction's and a request's proof link travel through the chunk's
/// proof rows: a request names its proof by the source's id, a transaction by
/// the proof's txid; a link that maps to no proof row is not stored.
#[tokio::test]
async fn a_proof_link_is_mapped_through_the_chunks_proof_rows() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    // A proof row of another transaction this store already holds (local id 1).
    let other = "c".repeat(64);
    exec(&s, &format!("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES ('{other}',1,0,'{h}','{h}',X'00',X'00',datetime('now'),datetime('now'))", h = "e".repeat(64))).await;
    let mined = "d".repeat(64);
    let pending = "f".repeat(64);
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![proof(7, &mined)]);
    chunk.proven_tx_reqs = Some(vec![
        request(3, &mined, Some(7)),
        request(4, &pending, Some(99)),
    ]);
    let mut t_mined = transaction(5, "r-mined", Some(&mined));
    t_mined.proof_txid = Some(mined.clone());
    let mut t_numbered = transaction(6, "r-numbered", Some(&pending));
    t_numbered.proof_txid = Some("1".to_string());
    chunk.transactions = Some(vec![t_mined, t_numbered]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    println!(
        "WITNESS B7/B10 chunk: {:?}",
        r.as_ref()
            .map(|r| (r.inserts, r.updates))
            .map_err(|e| e.to_string())
    );
    assert!(r.is_ok());
    let links = (
        stored_link(&s, "proven_tx_reqs", &mined).await,
        stored_link(&s, "proven_tx_reqs", &pending).await,
        stored_link(&s, "transactions", &mined).await,
        stored_link(&s, "transactions", &pending).await,
    );
    println!("WITNESS B7/B10 links (req mined, req unmapped, tx mined, tx numbered): {links:?}");
    assert_eq!(
        links,
        (Some(mined.clone()), None, Some(mined.clone()), None)
    );
}

/// Two rows of one chunk that claim one source id are refused, and nothing
/// of the chunk is written.
#[tokio::test]
async fn a_chunk_mapping_one_source_id_twice_is_refused() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    let mut chunk = chunk_for(ALICE);
    chunk.output_baskets = Some(vec![basket(5, "one"), basket(5, "two")]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let rows = count(
        &s,
        "SELECT COUNT(*) FROM output_baskets WHERE name IN ('one','two')",
    )
    .await;
    println!(
        "WITNESS B22 chunk: {:?} rows={rows}",
        r.as_ref().map(|r| r.inserts).map_err(|e| e.to_string())
    );
    assert!(r.is_err());
    assert_eq!(rows, 0);
}

/// A transaction whose proof link is a number is not linked to the local
/// proof row that number names.
#[tokio::test]
async fn a_numbered_proof_link_names_no_local_row() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    let other = "c".repeat(64);
    exec(&s, &format!("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES ('{other}',1,0,'{h}','{h}',X'00',X'00',datetime('now'),datetime('now'))", h = "e".repeat(64))).await;
    let pending = "f".repeat(64);
    let mut t = transaction(6, "r-numbered", Some(&pending));
    t.proof_txid = Some("1".to_string());
    let mut chunk = chunk_for(ALICE);
    chunk.transactions = Some(vec![t]);
    s.process_sync_chunk(args_for(ALICE), chunk).await.unwrap();
    let link = stored_link(&s, "transactions", &pending).await;
    println!("WITNESS B10 numbered link stored as: {link:?}");
    assert_eq!(link, None);
}

/// A user's chunk carries the proofs and the requests of that user's
/// transactions alone (the reference reads them through the user's
/// transactions).
#[tokio::test]
async fn a_chunk_carries_only_its_users_proofs_and_requests() {
    let s = store(SRC).await;
    let (a, _) = s.find_or_insert_user(ALICE).await.unwrap();
    let (b, _) = s.find_or_insert_user(BOB).await.unwrap();
    let alice_txid = seed_all(&s, a.user_id, 'a', true, true).await;
    seed_all(&s, b.user_id, 'b', true, true).await;
    let mut args = args_for(ALICE);
    args.offsets
        .retain(|o| o.name == "provenTx" || o.name == "provenTxReq");
    let chunk = s.get_sync_chunk(args).await.unwrap();
    let proofs: Vec<String> = chunk
        .proven_txs
        .unwrap_or_default()
        .into_iter()
        .map(|p| p.txid)
        .collect();
    let reqs: Vec<String> = chunk
        .proven_tx_reqs
        .unwrap_or_default()
        .into_iter()
        .map(|r| r.txid)
        .collect();
    println!("WITNESS A2/A3 alice's chunk: proofs={proofs:?} requests={reqs:?}");
    assert_eq!((proofs, reqs), (vec![alice_txid.clone()], vec![alice_txid]));
}

async fn active_storage_of(s: &StorageSqlx, identity: &str) -> Option<String> {
    s.find_user(identity).await.unwrap().unwrap().active_storage
}

/// A chunk's user row is merged only for the user the chunk is for: a row
/// naming another user of the store is refused, and that user's row and the
/// chunk's user's row are unchanged.
#[tokio::test]
async fn a_chunk_user_row_naming_another_user_is_refused() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    s.find_or_insert_user(BOB).await.unwrap();
    let mut alice_row = s.find_user(ALICE).await.unwrap().unwrap();
    alice_row.active_storage = Some("f".repeat(64));
    alice_row.updated_at = Utc::now() + Duration::days(1);
    let mut chunk = chunk_for(BOB);
    chunk.user = Some(alice_row);
    let r = s.process_sync_chunk(args_for(BOB), chunk).await;
    let actives = (
        active_storage_of(&s, ALICE).await,
        active_storage_of(&s, BOB).await,
    );
    println!(
        "WITNESS B1 chunk for bob naming alice: {:?}; active storage (alice, bob) = {actives:?}",
        r.as_ref()
            .map(|r| (r.inserts, r.updates))
            .map_err(|e| e.to_string())
    );
    assert!(r.is_err());
    assert_eq!(actives, (Some(DST.to_string()), Some(DST.to_string())));
}

/// Syncing from a reader keeps the wallet's active storage as the wallet
/// holds it: the reader's user row does not choose it.
#[tokio::test]
async fn sync_from_a_reader_keeps_the_active_storage() {
    let act = store(SRC).await;
    act.find_or_insert_user(ALICE).await.unwrap();
    let mgr = bsv_wallet_toolbox_rs::managers::WalletStorageManager::new(
        ALICE.to_string(),
        Some(act.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    mgr.make_available().await.unwrap();
    let reader_key = "3".repeat(64);
    let reader = store(&reader_key).await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    reader.find_or_insert_user(ALICE).await.unwrap();
    let r = mgr.sync_from_reader(ALICE, reader.clone()).await;
    let active = active_storage_of(&act, ALICE).await;
    println!(
        "WITNESS C2 sync_from_reader: {:?}; active storage now {active:?}",
        r.as_ref()
            .map(|r| (r.inserts, r.updates))
            .map_err(|e| e.to_string())
    );
    assert!(r.is_ok());
    assert_eq!(active, Some(SRC.to_string()));
}

/// A chunk never creates its user: a chunk for a user the store does not
/// hold is refused and the store holds no user after it.
#[tokio::test]
async fn a_chunk_for_an_unknown_user_is_refused() {
    let s = store(DST).await;
    let r = s
        .process_sync_chunk(args_for(ALICE), chunk_for(ALICE))
        .await;
    let users = count(&s, "SELECT COUNT(*) FROM users").await;
    println!(
        "WITNESS B2 empty chunk for an unknown user: {:?}; users rows {users}",
        r.as_ref().map(|r| r.done).map_err(|e| e.to_string())
    );
    assert!(r.is_err());
    assert_eq!(users, 0);
}

/// A backup of a wallet to a fresh store completes: the manager makes the
/// store's user before any chunk.
#[tokio::test]
async fn a_backup_to_a_fresh_store_completes() {
    let act = store(SRC).await;
    let (u, _) = act.find_or_insert_user(ALICE).await.unwrap();
    seed_all(&act, u.user_id, 'a', true, true).await;
    let mgr = bsv_wallet_toolbox_rs::managers::WalletStorageManager::new(
        ALICE.to_string(),
        Some(act.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    mgr.make_available().await.unwrap();
    let fresh = store(DST).await;
    let r = mgr.sync_to_writer(ALICE, fresh.clone()).await;
    let rows = count(&fresh, "SELECT COUNT(*) FROM transactions").await;
    println!(
        "WITNESS B2 sync_to_writer to a fresh store: {:?}; transactions rows {rows}",
        r.as_ref()
            .map(|r| (r.inserts, r.updates))
            .map_err(|e| e.to_string())
    );
    assert!(r.is_ok());
    assert_eq!(rows, 1);
}

/// The manager's sync surface serves the manager's identity alone: a chunk
/// request or a chunk for another identity is refused.
#[tokio::test]
async fn the_manager_sync_surface_refuses_another_identity() {
    let act = store(SRC).await;
    act.find_or_insert_user(ALICE).await.unwrap();
    act.find_or_insert_user(BOB).await.unwrap();
    let mgr = bsv_wallet_toolbox_rs::managers::WalletStorageManager::new(
        ALICE.to_string(),
        Some(act.clone() as Arc<dyn MonitorStorage>),
        None,
    );
    mgr.make_available().await.unwrap();
    let read = mgr.get_sync_chunk(args_for(BOB)).await;
    let mut chunk = chunk_for(BOB);
    chunk.output_baskets = Some(vec![basket(1, "planted")]);
    let write = mgr.process_sync_chunk(args_for(BOB), chunk).await;
    let planted = count(
        &act,
        "SELECT COUNT(*) FROM output_baskets WHERE name = 'planted'",
    )
    .await;
    println!(
        "WITNESS C1 manager for alice, bob's chunk: read {:?} write {:?} rows {planted}",
        read.as_ref().map(|_| "ok").map_err(|e| e.to_string()),
        write.as_ref().map(|_| "ok").map_err(|e| e.to_string())
    );
    assert!(read.is_err() && write.is_err());
    assert_eq!(planted, 0);
}

/// `set_active` writes each store's row of the wallet's user by that store's
/// own user id: a store that numbers its users differently switches the
/// wallet's user and leaves its other users' rows as they were.
#[tokio::test]
async fn set_active_writes_each_stores_own_user_row() {
    let a = store(SRC).await;
    let b = store(DST).await;
    a.find_or_insert_user(ALICE).await.unwrap();
    let (filler, _) = b.find_or_insert_user(BOB).await.unwrap();
    let nine = "9".repeat(64);
    b.update_user_active_storage(filler.user_id, &nine)
        .await
        .unwrap();
    let (bu, _) = b.find_or_insert_user(ALICE).await.unwrap();
    b.update_user_active_storage(bu.user_id, SRC).await.unwrap();
    let mgr = bsv_wallet_toolbox_rs::managers::WalletStorageManager::new(
        ALICE.to_string(),
        Some(a.clone() as Arc<dyn MonitorStorage>),
        Some(vec![b.clone() as Arc<dyn MonitorStorage>]),
    );
    mgr.make_available().await.unwrap();
    let r = mgr.set_active(DST).await;
    let rows = (
        active_storage_of(&b, ALICE).await,
        active_storage_of(&b, BOB).await,
        mgr.get_active_store().await.ok(),
    );
    println!(
        "WITNESS C3 set_active(B) with B's ids (bob {}, alice {}): {:?}; B rows (alice, bob), active = {rows:?}",
        filler.user_id,
        bu.user_id,
        r.as_ref().map(|_| "ok").map_err(|e| e.to_string())
    );
    assert!(r.is_ok());
    assert_eq!(
        rows,
        (Some(DST.to_string()), Some(nine), Some(DST.to_string()))
    );
}

async fn req_row(
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

/// An existing request is merged as the reference merges one: its status,
/// attempts, proof link and bytes stay as held; the history and the notify
/// ids are unioned; a batch is taken where none is held.
#[tokio::test]
async fn an_existing_request_is_merged_not_replaced() {
    let s = store(DST).await;
    let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
    let t = "a".repeat(64);
    let held_tx = exec(&s, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,description,txid,created_at,updated_at) VALUES ({},'unproven','held',0,0,'held','{t}','2026-01-01 00:00:00','2026-01-01 00:00:00')", u.user_id)).await;
    exec(&s, &format!("INSERT INTO proven_tx_reqs (txid,status,attempts,history,notified,notify,raw_tx,input_beef,created_at,updated_at) VALUES ('{t}','unmined',2,'{{\"notes\":[{{\"what\":\"a\",\"when\":\"2026-01-01T00:00:00Z\"}}]}}',1,'{{\"transactionIds\":[{held_tx}]}}',X'aa',X'bb','2026-01-01 00:00:00','2026-01-01 00:00:00')")).await;
    let other = "b".repeat(64);
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![proof(7, &t)]);
    chunk.transactions = Some(vec![transaction(50, "theirs", Some(&other))]);
    let mut theirs = request(3, &t, Some(7));
    theirs.status = ProvenTxReqStatus::Invalid;
    theirs.attempts = 99;
    theirs.raw_tx = Some(vec![]);
    theirs.input_beef = None;
    theirs.history = r#"{"notes":[{"what":"b","when":"2026-01-02T00:00:00Z"}]}"#.to_string();
    theirs.notify = r#"{"transactionIds":[50]}"#.to_string();
    theirs.batch = Some("B1".to_string());
    theirs.updated_at = at(30);
    chunk.proven_tx_reqs = Some(vec![theirs]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    assert!(r.is_ok(), "{r:?}");
    let row = req_row(&s, &t).await;
    println!("WITNESS B5 merged request (status, attempts, raw, beef, link, history, notify, batch): {row:?}");
    let theirs_local = held_tx + 1;
    assert_eq!(
        row,
        (
            "unmined".to_string(),
            2,
            vec![0xaa],
            Some(vec![0xbb]),
            None,
            r#"{"notes":[{"what":"a","when":"2026-01-01T00:00:00Z"},{"what":"b","when":"2026-01-02T00:00:00Z"}]}"#.to_string(),
            format!(r#"{{"transactionIds":[{held_tx},{theirs_local}]}}"#),
            Some("B1".to_string()),
        )
    );
}

/// A request whose batch differs from the held one refuses the chunk.
#[tokio::test]
async fn a_request_with_another_batch_is_refused() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    let t = "a".repeat(64);
    exec(&s, &format!("INSERT INTO proven_tx_reqs (txid,status,attempts,history,notified,notify,raw_tx,batch,created_at,updated_at) VALUES ('{t}','unsent',0,'{{}}',0,'{{}}',X'aa','B0','2026-01-01 00:00:00','2026-01-01 00:00:00')")).await;
    let mut theirs = request(3, &t, None);
    theirs.batch = Some("B1".to_string());
    let mut chunk = chunk_for(ALICE);
    chunk.proven_tx_reqs = Some(vec![theirs]);
    let r = s.process_sync_chunk(args_for(ALICE), chunk).await;
    let batch = req_row(&s, &t).await.7;
    println!(
        "WITNESS B5 batch mismatch: {:?}; held batch {batch:?}",
        r.as_ref().map(|_| "ok").map_err(|e| e.to_string())
    );
    assert!(r.is_err());
    assert_eq!(batch.as_deref(), Some("B0"));
}

/// A new request's status is stored in the reference's own words.
#[tokio::test]
async fn a_new_requests_status_is_stored_in_the_references_words() {
    let s = store(DST).await;
    s.find_or_insert_user(ALICE).await.unwrap();
    let mut reqs = vec![];
    for (i, st) in [
        ProvenTxReqStatus::DoubleSpend,
        ProvenTxReqStatus::InProgress,
        ProvenTxReqStatus::NotFound,
        ProvenTxReqStatus::NoSend,
        ProvenTxReqStatus::NonFinal,
    ]
    .into_iter()
    .enumerate()
    {
        let mut r = request(i as i64 + 1, &format!("{i}").repeat(64), None);
        r.status = st;
        reqs.push(r);
    }
    let mut chunk = chunk_for(ALICE);
    chunk.proven_tx_reqs = Some(reqs);
    s.process_sync_chunk(args_for(ALICE), chunk).await.unwrap();
    let words: Vec<String> =
        ::sqlx::query_scalar("SELECT status FROM proven_tx_reqs ORDER BY txid")
            .fetch_all(s.pool())
            .await
            .unwrap();
    println!("WITNESS B6 stored statuses: {words:?}");
    assert_eq!(
        words,
        [
            "doubleSpend",
            "inProgress",
            "notFound",
            "nosend",
            "nonfinal"
        ]
    );
}

/// An older transaction row that carries a proof link where the store holds
/// none merges its proof evidence alone (status and link; the held bytes
/// stay); an older row without one changes nothing.
#[tokio::test]
async fn an_older_transaction_row_merges_only_its_proof_evidence() {
    let s = store(DST).await;
    let (u, _) = s.find_or_insert_user(ALICE).await.unwrap();
    let t = "a".repeat(64);
    let quiet = "b".repeat(64);
    for (r, txid) in [("r-proof", &t), ("r-quiet", &quiet)] {
        exec(&s, &format!("INSERT INTO transactions (user_id,status,reference,is_outgoing,satoshis,description,txid,raw_tx,created_at,updated_at) VALUES ({},'unproven','{r}',0,0,'held','{txid}',X'aa','2026-01-11 00:00:00','2026-01-11 00:00:00')", u.user_id)).await;
    }
    let mut proven = transaction(5, "r-proof", Some(&t));
    proven.status = TransactionStatus::Completed;
    proven.proof_txid = Some(t.clone());
    proven.raw_tx = Some(vec![0xbb]);
    proven.description = "theirs".to_string();
    proven.updated_at = at(5);
    let mut quiet_row = transaction(6, "r-quiet", Some(&quiet));
    quiet_row.status = TransactionStatus::Failed;
    quiet_row.updated_at = at(5);
    let mut chunk = chunk_for(ALICE);
    chunk.proven_txs = Some(vec![proof(7, &t)]);
    chunk.transactions = Some(vec![proven, quiet_row]);
    s.process_sync_chunk(args_for(ALICE), chunk).await.unwrap();
    let rows: Vec<(String, String, Option<String>, Vec<u8>)> = ::sqlx::query_as(
        "SELECT t.status, t.description, p.txid, t.raw_tx FROM transactions t LEFT JOIN proven_txs p ON p.proven_tx_id = t.proven_tx_id ORDER BY t.reference",
    )
    .fetch_all(s.pool())
    .await
    .unwrap();
    println!("WITNESS B11 (status, description, proof, raw) r-proof, r-quiet: {rows:?}");
    assert_eq!(
        rows,
        vec![
            (
                "completed".to_string(),
                "held".to_string(),
                Some(t.clone()),
                vec![0xaa]
            ),
            ("unproven".to_string(), "held".to_string(), None, vec![0xaa]),
        ]
    );
}
