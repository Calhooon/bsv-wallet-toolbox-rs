//! The status review pass's linked-proof kind takes a row by what its
//! write reads: whether the proof row is checked is decided by the root
//! its stored path computes, never by its `merkle_root` column. A proof
//! row whose column names a root this store recorded as checked, while its
//! path computes another, is a candidate: the tracker is asked for the
//! computed root and, confirming, the row completes.
#![cfg(feature = "sqlite")]

use std::sync::Arc;

use bsv_wallet_toolbox_rs::storage::*;

mod proof_fixture;
mod sync_support;
use proof_fixture::*;
use sync_support::exec;

#[tokio::test]
async fn a_column_root_with_a_record_does_not_hide_a_path_that_computes_another() {
    let n = 91;
    let headers = Arc::new(Headers::default());
    headers.carry(n);
    let w = w_store(Tracker::Confirms, &headers).await;
    let t = held_at(&w, n, "unproven", "wy", NOW).await;
    let p = proof_row(&w, n, OLD).await;
    let (txid, _, _, _) = mined(n);
    let other = "cd".repeat(32);
    exec(
        &w,
        &format!("UPDATE proven_txs SET merkle_root = '{other}' WHERE proven_tx_id = {p}"),
    )
    .await;
    exec(&w, &format!("INSERT INTO proof_root_checks (txid,height,merkle_root,checked_at) VALUES ('{txid}',{HEIGHT},'{other}',{NOW})")).await;
    exec(
        &w,
        &format!("UPDATE transactions SET proven_tx_id = {p} WHERE transaction_id = {t}"),
    )
    .await;
    MonitorStorage::review_status(&*w).await.unwrap();
    let got = row(&w, "wy").await;
    println!(
        "WITNESS column root recorded, path computes another: {got} ({} reads) | {}",
        headers.reads(),
        proof_state(&w, n).await
    );
    assert_eq!(got, "completed:linked");
    assert_eq!(headers.reads(), 1, "the computed root is asked");
    assert_eq!(proof_state(&w, n).await, "proof rows 1 record checked");
}

/// The same reader, on a row that is not `completed` and links a proof row
/// this store checked (no `failed` row of the txid): the pass completes it
/// with no header read.
#[tokio::test]
async fn a_row_linking_a_checked_proof_completes_with_no_header_read() {
    let n = 92;
    let headers = Arc::new(Headers::default());
    let w = w_store(Tracker::Refutes, &headers).await;
    let t = held_at(&w, n, "unproven", "wy", NOW).await;
    let p = checked_proof_row(&w, n).await;
    exec(
        &w,
        &format!("UPDATE transactions SET proven_tx_id = {p} WHERE transaction_id = {t}"),
    )
    .await;
    MonitorStorage::review_status(&*w).await.unwrap();
    let got = row(&w, "wy").await;
    println!(
        "WITNESS linked to a checked proof: {got} ({} reads)",
        headers.reads()
    );
    assert_eq!(got, "completed:linked");
    assert_eq!(headers.reads(), 0);
}

/// Seed proof row `k` of an arbitrary txid, linked by a row reading
/// `status` under reference `k<k>`, beside a request reading `word`. With
/// `checked` the proof carries this store's record; with `no_root` its
/// stored path is another txid's and computes no root for it.
async fn seed(
    w: &bsv_wallet_toolbox_rs::StorageSqlx,
    k: u32,
    status: &str,
    word: &str,
    checked: bool,
    no_root: bool,
) {
    use bsv_rs::transaction::{MerklePath, MerklePathLeaf};
    let txid = format!("{:064x}", 0xabc0_0000u64 + u64::from(k));
    let path_txid = if no_root {
        "ee".repeat(32)
    } else {
        txid.clone()
    };
    let path = MerklePath::new(
        HEIGHT,
        vec![vec![MerklePathLeaf::new_txid(0, path_txid.clone())]],
    )
    .unwrap();
    let root = path.compute_root(Some(&path_txid)).unwrap();
    let p = exec(w, &format!("INSERT INTO proven_txs (txid,height,idx,block_hash,merkle_root,merkle_path,raw_tx,created_at,updated_at) VALUES ('{txid}',{HEIGHT},0,'{h}','{root}',X'{p}',X'00',{OLD},{OLD})", h = "e".repeat(64), p = hex::encode(path.to_binary()))).await;
    if checked {
        exec(w, &format!("INSERT INTO proof_root_checks (txid,height,merkle_root,checked_at) VALUES ('{txid}',{HEIGHT},'{root}',{NOW})")).await;
    }
    exec(w, &format!("INSERT INTO transactions (user_id,proven_tx_id,status,reference,is_outgoing,satoshis,version,lock_time,description,txid,created_at,updated_at) VALUES (1,{p},'{status}','k{k}',1,-1000,1,0,'pay','{txid}',{NOW},{NOW})")).await;
    exec(w, &format!("INSERT INTO proven_tx_reqs (txid,status,attempts,history,notified,notify,raw_tx,created_at,updated_at) VALUES ('{txid}','{word}',0,'{{}}',0,'{{}}',X'00',{NOW},{NOW})")).await;
}

/// Rows that never leave the candidate set take none of the pass's 100
/// places: `failed` rows each linking a proof this store holds checked,
/// and rows linking a proof row whose path computes no root. A candidate
/// behind 150 and behind 350 of either completes at pass one, and the
/// rows ahead of it cost no header read.
#[tokio::test]
async fn a_candidate_behind_rows_that_never_leave_completes_at_pass_one() {
    for no_root in [false, true] {
        for ahead in [150u32, 350] {
            let headers = Arc::new(Headers::default());
            let w = w_store(Tracker::Confirms, &headers).await;
            for k in 0..ahead {
                if no_root {
                    seed(&w, k, "nosend", "nosend", false, true).await;
                } else {
                    seed(&w, k, "failed", "invalid", true, false).await;
                }
            }
            seed(&w, ahead, "nosend", "nosend", false, false).await;
            let txid = format!("{:064x}", 0xabc0_0000u64 + u64::from(ahead));
            headers.roots.lock().unwrap().insert((HEIGHT, txid));
            MonitorStorage::review_status(&*w).await.unwrap();
            let got = row(&w, &format!("k{ahead}")).await;
            println!(
                "WITNESS {ahead} rows ahead ({}): the candidate reads {got} after pass one ({} reads)",
                if no_root { "no root" } else { "checked beside failed" },
                headers.reads()
            );
            assert_eq!(got, "completed:linked");
            assert_eq!(headers.reads(), 1, "the rows ahead cost no header read");
            // A second pass reads no header and moves none of them.
            MonitorStorage::review_status(&*w).await.unwrap();
            assert_eq!(headers.reads(), 1);
            let ahead_status = if no_root {
                "nosend:linked"
            } else {
                "failed:linked"
            };
            assert_eq!(row(&w, "k0").await, ahead_status);
        }
    }
}
