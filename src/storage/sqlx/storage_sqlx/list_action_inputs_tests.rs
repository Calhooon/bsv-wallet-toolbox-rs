//! `list_actions` with `includeInputs` serves each input's `sequenceNumber`
//! and `unlockingScript` from the action's parsed raw transaction, matched by
//! source txid and vout, with the reference's fallback of 0 (#307;
//! listActionsKnex.ts:73-94 at ts-stack edf6e03), and serves the inputs in
//! the finder's order (the unpaged `findOutputs` at :73 adds no ORDER BY, so
//! the rows come by `outputId`).

use crate::storage::{AuthId, StorageSqlx, WalletStorageReader, WalletStorageWriter};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{ListActionsArgs, WalletActionInput};

/// One output the action spends: its outpoint and its `sequence_number` column.
struct Spent {
    txid: String,
    vout: u32,
    column: Option<i64>,
}

fn spent(fill: char, vout: u32, column: Option<i64>) -> Spent {
    Spent {
        txid: fill.to_string().repeat(64),
        vout,
        column,
    }
}

/// A raw transaction spending `(txid, vout, sequence, unlocking script)`.
fn raw_tx(inputs: &[(&Spent, u32, &str)]) -> Vec<u8> {
    let mut tx = Transaction::new();
    for (source, sequence, unlocking) in inputs {
        let mut input = TransactionInput::new(source.txid.clone(), source.vout);
        input.sequence = *sequence;
        input.unlocking_script = Some(UnlockingScript::from_hex(unlocking).unwrap());
        tx.add_input(input).unwrap();
    }
    tx.add_output(TransactionOutput::new(
        1,
        LockingScript::from_hex("51").unwrap(),
    ))
    .unwrap();
    tx.to_binary()
}

/// A store with one spending action holding `raw` and the `spent` rows
/// inserted in order (so `output_id` follows the slice), each spent by it
/// and created by a funding action of its txid.
async fn store(raw: Option<Vec<u8>>, spent: &[Spent]) -> (StorageSqlx, AuthId) {
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("inputs", "inputs").await.unwrap();
    storage.make_available().await.unwrap();
    let identity = "c".repeat(66);
    let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
    let spender = sqlx::query(
        "INSERT INTO transactions (user_id, status, reference, is_outgoing, description, txid,
                                   raw_tx)
         VALUES (?, 'completed', 'spender', 1, 'spending action', ?, ?)",
    )
    .bind(user.user_id)
    .bind("f".repeat(64))
    .bind(raw)
    .execute(storage.writer())
    .await
    .unwrap()
    .last_insert_rowid();
    let mut funding = std::collections::HashMap::new();
    for s in spent {
        if !funding.contains_key(&s.txid) {
            let id = sqlx::query(
                "INSERT INTO transactions (user_id, status, reference, is_outgoing, description,
                                           txid)
                 VALUES (?, 'completed', ?, 0, 'funding action', ?)",
            )
            .bind(user.user_id)
            .bind(format!("funding {}", s.txid))
            .bind(&s.txid)
            .execute(storage.writer())
            .await
            .unwrap()
            .last_insert_rowid();
            funding.insert(s.txid.clone(), id);
        }
        sqlx::query(
            "INSERT INTO outputs (user_id, transaction_id, vout, satoshis, provided_by, purpose,
                                  type, spendable, txid, script_length, script_offset,
                                  locking_script, spent_by, sequence_number)
             VALUES (?, ?, ?, 42, 'storage', 'change', 'P2PKH', 0, ?, 1, 0, X'51', ?, ?)",
        )
        .bind(user.user_id)
        .bind(funding[&s.txid])
        .bind(s.vout as i64)
        .bind(&s.txid)
        .bind(spender)
        .bind(s.column)
        .execute(storage.writer())
        .await
        .unwrap();
    }
    (storage, AuthId::with_user_id(identity, user.user_id))
}

/// The spending action's inputs, with unlocking scripts.
async fn inputs(storage: &StorageSqlx, auth: &AuthId) -> Vec<WalletActionInput> {
    let result = storage
        .list_actions(
            auth,
            ListActionsArgs {
                labels: vec![],
                label_query_mode: None,
                include_labels: None,
                include_inputs: Some(true),
                include_input_source_locking_scripts: None,
                include_input_unlocking_scripts: Some(true),
                include_outputs: None,
                include_output_locking_scripts: None,
                limit: Some(10),
                offset: Some(0),
                seek_permission: None,
            },
        )
        .await
        .unwrap();
    result
        .actions
        .into_iter()
        .flat_map(|action| action.inputs.unwrap_or_default())
        .collect()
}

/// (a) The raw transaction's nSequence wins over a column that disagrees.
#[tokio::test]
async fn a_column_that_disagrees_is_served_the_raw_sequence() {
    let a = spent('1', 0, Some(7));
    let raw = raw_tx(&[(&a, 5, "52")]);
    let (storage, auth) = store(Some(raw), &[a]).await;
    let served = inputs(&storage, &auth).await;
    assert_eq!(served.len(), 1);
    assert_eq!(served[0].sequence_number, 5);
}

/// (b) A NULL column with the raw transaction present is served the raw
/// nSequence, not 0xFFFFFFFF.
#[tokio::test]
async fn a_null_column_is_served_the_raw_sequence() {
    let a = spent('1', 0, None);
    let raw = raw_tx(&[(&a, 5, "52")]);
    let (storage, auth) = store(Some(raw), &[a]).await;
    let served = inputs(&storage, &auth).await;
    assert_eq!(served.len(), 1);
    assert_eq!(served[0].sequence_number, 5);
}

/// (c) No raw transaction serves 0, whatever the column holds
/// (`input?.sequence || 0`, listActionsKnex.ts:89).
#[tokio::test]
async fn no_raw_transaction_serves_zero() {
    for column in [Some(7), None] {
        let (storage, auth) = store(None, &[spent('1', 0, column)]).await;
        let served = inputs(&storage, &auth).await;
        assert_eq!(served.len(), 1);
        assert_eq!(served[0].sequence_number, 0, "column {column:?}");
        assert_eq!(served[0].unlocking_script, None, "column {column:?}");
    }
}

/// (c) A raw transaction that lacks the input serves 0 and no unlocking
/// script: the reference's `find` at :84 yields undefined.
#[tokio::test]
async fn a_raw_transaction_without_the_input_serves_zero() {
    let other = spent('2', 3, None);
    let raw = raw_tx(&[(&other, 5, "52")]);
    let (storage, auth) = store(Some(raw), &[spent('1', 0, Some(7))]).await;
    let served = inputs(&storage, &auth).await;
    assert_eq!(served.len(), 1);
    assert_eq!(served[0].sequence_number, 0);
    assert_eq!(served[0].unlocking_script, None);
}

/// (d) The inputs come in the finder's order (`outputId`), not by the
/// column; each is matched to its own raw input by txid and vout, and the
/// unlocking script comes from the same input.
#[tokio::test]
async fn inputs_come_by_output_id_each_matched_by_outpoint() {
    let first = spent('1', 1, Some(9));
    let second = spent('1', 0, Some(3));
    let third = spent('3', 1, Some(6));
    // The raw order differs from both the column order and the row order.
    let raw = raw_tx(&[(&third, 30, "53"), (&second, 20, "52"), (&first, 10, "51")]);
    let (storage, auth) = store(Some(raw), &[first, second, third]).await;
    let served = inputs(&storage, &auth).await;
    let got: Vec<(u32, u32, Vec<u8>)> = served
        .iter()
        .map(|input| {
            (
                input.source_outpoint.vout,
                input.sequence_number,
                input.unlocking_script.clone().unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (1, 10, vec![0x51]),
            (0, 20, vec![0x52]),
            (1, 30, vec![0x53])
        ]
    );
}
