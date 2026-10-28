//! The internalize door bounds `insertionRemittance.customInstructions` at
//! 1,000 UTF-8 bytes (#138; BRC-100 wallet/0100.md:867, ruled conform on
//! 2026-10-24). The keeper's scenario of 2026-10-24
//! (`wallet-internalize-custom-instructions-bound`), steps 1 to 4: S1001 is
//! refused with nothing stored; S1000 is stored whole and read back whole.
//!
//! Real SQLite storage, mock headers, no network.

#![cfg(feature = "sqlite")]

use bsv_rs::primitives::hash::sha256;
use bsv_rs::script::{LockingScript, ProtocolEra, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{BasketInsertion, InternalizeActionArgs, InternalizeOutput, ListOutputsArgs};
use bsv_wallet_toolbox_rs::{AuthId, Error, StorageSqlx, WalletStorageReader, WalletStorageWriter};

/// The scenario's SHA-256 of S1000's UTF-8 bytes.
const S1000_SHA256: &str = "483def2f91d8aa6b973d939b16c92797a25ca9a95b335a414db845dc5ec6d0ef";

/// 100 `a`, 150 `é`, 100 `€`, 75 `𝄞`: 1,000 bytes, 425 code points.
fn s1000() -> String {
    [
        "a".repeat(100),
        "é".repeat(150),
        "€".repeat(100),
        "𝄞".repeat(75),
    ]
    .concat()
}

fn op_true_output(satoshis: u64) -> TransactionOutput {
    TransactionOutput {
        satoshis: Some(satoshis),
        locking_script: LockingScript::from_hex("7551").unwrap(),
        change: false,
    }
}

/// A0 mined after Genesis with two outputs; the AtomicBEEF of an unmined
/// child spending A0's output `vout`.
fn child_beef(vout: u32) -> Vec<u8> {
    let mut a0 = Transaction::new();
    let mut input = TransactionInput::new("11".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    a0.inputs.push(input);
    a0.outputs
        .extend([op_true_output(20_000), op_true_output(20_000)]);
    let a0_txid = a0.id();
    let mut child = Transaction::new();
    let mut input = TransactionInput::new(a0_txid.clone(), vout);
    input.unlocking_script = Some(UnlockingScript::from_hex("00").unwrap());
    child.inputs.push(input);
    child.outputs.push(op_true_output(9_999));
    let txid = child.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(
        &a0_txid,
        ProtocolEra::MAINNET_GENESIS_HEIGHT + 100,
    ));
    beef.merge_raw_tx(a0.to_binary(), Some(bump));
    beef.merge_transaction(child);
    beef.to_binary_atomic(&txid).unwrap()
}

fn insertion(tx: Vec<u8>, custom_instructions: String) -> InternalizeActionArgs {
    InternalizeActionArgs {
        tx,
        outputs: vec![InternalizeOutput {
            output_index: 0,
            protocol: "basket insertion".to_string(),
            payment_remittance: None,
            insertion_remittance: Some(BasketInsertion {
                basket: "bound".to_string(),
                custom_instructions: Some(custom_instructions),
                tags: None,
            }),
        }],
        description: "custom instructions bound".to_string(),
        labels: None,
        seek_permission: None,
    }
}

async fn count(storage: &StorageSqlx, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(storage.pool())
        .await
        .unwrap()
}

async fn bound_outputs(storage: &StorageSqlx, auth: &AuthId) -> Vec<Option<String>> {
    let args = ListOutputsArgs {
        basket: "bound".to_string(),
        tags: None,
        tag_query_mode: None,
        include: None,
        include_custom_instructions: Some(true),
        include_tags: None,
        include_labels: None,
        limit: Some(10),
        offset: None,
        seek_permission: None,
    };
    let listed = storage.list_outputs(auth, args).await.unwrap();
    listed
        .outputs
        .into_iter()
        .map(|o| o.custom_instructions)
        .collect()
}

#[tokio::test]
async fn custom_instructions_over_1000_bytes_is_refused_and_1000_is_stored_whole() {
    let s1000 = s1000();
    assert_eq!((s1000.len(), s1000.chars().count()), (1000, 425));
    assert_eq!(hex::encode(sha256(s1000.as_bytes())), S1000_SHA256);
    let s1001 = format!("{s1000}a");

    let storage = StorageSqlx::in_memory().await.unwrap();
    let identity = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
    storage.migrate("bound-tests", identity).await.unwrap();
    storage.make_available().await.unwrap();
    bsv_wallet_toolbox_rs::WalletStorageProvider::set_services(
        &storage,
        std::sync::Arc::new(bsv_wallet_toolbox_rs::services::mock::MockWalletServices::new()),
    );
    let (user, _) = storage.find_or_insert_user(identity).await.unwrap();
    let auth = AuthId {
        identity_key: identity.to_string(),
        user_id: Some(user.user_id),
        is_active: None,
    };

    // Steps 1 and 2: S1001 is refused at the door, nothing stored.
    let refused = storage
        .internalize_action(&auth, insertion(child_beef(0), s1001))
        .await;
    match refused {
        Err(Error::ValidationError(message)) => {
            assert!(message.contains("customInstructions"), "{message}");
            assert!(message.contains("1001"), "{message}");
        }
        other => panic!("S1001 must be refused, got {other:?}"),
    }
    for table in ["outputs", "transactions", "proven_tx_reqs"] {
        assert_eq!(count(&storage, table).await, 0, "{table} gained a row");
    }
    assert!(bound_outputs(&storage, &auth).await.is_empty());

    // Steps 3 and 4: S1000 is stored whole and read back whole.
    let accepted = storage
        .internalize_action(&auth, insertion(child_beef(1), s1000.clone()))
        .await
        .unwrap();
    assert!(accepted.base.accepted);
    let listed = bound_outputs(&storage, &auth).await;
    assert_eq!(listed, vec![Some(s1000.clone())]);
    let stored = listed[0].as_deref().unwrap();
    assert_eq!(hex::encode(sha256(stored.as_bytes())), S1000_SHA256);
}
