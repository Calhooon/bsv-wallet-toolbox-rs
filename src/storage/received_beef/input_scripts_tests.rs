use super::*;

fn tip(height: u32) -> ChainTip {
    ChainTip {
        height,
        median_time_past: 0,
        genesis_height: 0,
        csv_height: 0,
        next_block_time: None,
    }
}

/// bsv-rs's own vectors for `word_for_input(Word::Standard, ..)` on the
/// 0.14 line (`src/script/input_flags.rs`,
/// `transaction_era_and_both_utxo_markers_follow_their_own_heights`), on
/// regtest: the spend at the block after the tip, the coin at its proof.
#[test]
fn the_word_matches_the_reference_vectors_on_regtest() {
    for (spend, source, standard) in [
        (10_000u32, Some(10_000u64), 0x0d47ffu32),
        (14_999, None, 0x0d47ff),
        (15_000, Some(14_999), 0x1d47ff),
        (15_000, Some(15_000), 0x3d47ff),
        (15_000, None, 0x3d47ff),
    ] {
        let word = input_word("regtest", Some(tip(spend - 1)), source).unwrap();
        assert_eq!(word.bits(), standard, "{spend} {source:?}");
    }
}

/// With no tip the spend takes the latest activation the network knows; a
/// mined coin its proof's era, an unproven coin the spend's.
#[test]
fn no_tip_gives_the_spend_the_latest_activation() {
    let g = ProtocolEra::MAINNET_GENESIS_HEIGHT;
    let c = ProtocolEra::MAINNET_CHRONICLE_HEIGHT;
    let chronicle = input_word("main", None, None).unwrap();
    assert_eq!(
        chronicle,
        input_word("main", None, Some(u64::from(c))).unwrap()
    );
    let genesis_coin = input_word("main", None, Some(u64::from(g))).unwrap();
    assert!(chronicle.contains(ScriptFlags::UTXO_AFTER_CHRONICLE));
    assert!(!genesis_coin.contains(ScriptFlags::UTXO_AFTER_CHRONICLE));
    assert!(genesis_coin.contains(ScriptFlags::UTXO_AFTER_GENESIS));
    assert_eq!(chronicle, input_word("mainnet", None, None).unwrap());
}

/// A coin proven below Genesis, or a spend below it, is not judged.
#[test]
fn a_pre_genesis_coin_or_spend_has_no_word() {
    let g = ProtocolEra::MAINNET_GENESIS_HEIGHT;
    assert_eq!(
        input_word("main", None, Some(600_000)),
        Err(NoWord::PreGenesisCoin {
            height: 600_000,
            genesis_height: g
        })
    );
    assert_eq!(
        input_word("main", Some(tip(g - 2)), None),
        Err(NoWord::PreGenesisSpend {
            height: g - 1,
            genesis_height: g
        })
    );
    assert_eq!(
        NoWord::PreGenesisCoin {
            height: 600_000,
            genesis_height: g
        }
        .why(0),
        "input 0 not judged: coin height 600000 is before Genesis height 620538."
    );
    for network in ["test", "testnet"] {
        assert_eq!(
            input_word(network, None, Some(1_344_301)),
            Err(NoWord::PreGenesisCoin {
                height: 1_344_301,
                genesis_height: 1_344_302
            })
        );
    }
}

#[test]
fn an_unknown_network_has_no_word() {
    assert_eq!(input_word("other", None, None), Err(NoWord::Network));
    assert_eq!(NoWord::Network.why(3), NO_NETWORK_POLICY);
}

/// The 0.14 line's figures (`DOOR_WORK_BUDGET`, `DOOR_STACK_MEMORY_BYTES`).
#[test]
fn the_bounds_are_the_reference_figures() {
    assert_eq!(INPUT_WORK_LIMIT, 2_147_483_648);
    assert_eq!(TRANSACTION_WORK_LIMIT, 2_147_483_648);
    assert_eq!(STACK_MEMORY_BYTES, 100_000_000);
}

mod runs {
    use super::*;
    use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};

    /// The 0.14 line's fixture lock, which the input `00` spends.
    fn anyone_lock() -> LockingScript {
        LockingScript::from_hex(&format!("14{}757551", "00".repeat(20))).unwrap()
    }

    fn input(txid: &str, vout: u32, unlock: &str) -> TransactionInput {
        let mut input = TransactionInput::new(txid.to_string(), vout);
        input.unlocking_script = Some(UnlockingScript::from_hex(unlock).unwrap());
        input
    }

    /// A parent of `outputs` outputs locked by `anyone_lock` proven after
    /// Genesis, and a child spending every one with `unlock`; `proven`
    /// gives the child a BUMP of its own. The AtomicBEEF and the txid.
    fn sweep(outputs: u32, unlock: &str, proven: bool) -> (Vec<u8>, String) {
        let mut parent = Transaction::new();
        parent.inputs.push(input(&"11".repeat(32), 0, "00"));
        for _ in 0..outputs {
            parent.outputs.push(TransactionOutput {
                satoshis: Some(1_000),
                locking_script: anyone_lock(),
                change: false,
            });
        }
        let parent_txid = parent.id();
        let mut child = Transaction::new();
        child.version = 1;
        for vout in 0..outputs {
            child.inputs.push(input(&parent_txid, vout, unlock));
        }
        child.outputs.push(TransactionOutput {
            satoshis: Some(u64::from(outputs) * 1_000 - 500),
            locking_script: anyone_lock(),
            change: false,
        });
        let txid = child.id();
        let height = ProtocolEra::MAINNET_GENESIS_HEIGHT + 101;
        let mut beef = Beef::new();
        let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, height));
        beef.merge_raw_tx(parent.to_binary(), Some(bump));
        if proven {
            let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&txid, height + 1));
            beef.merge_raw_tx(child.to_binary(), Some(bump));
        } else {
            beef.merge_raw_tx(child.to_binary(), None);
        }
        (beef.to_binary_atomic(&txid).unwrap(), txid)
    }

    /// The sweep of `tests/common` (800 inputs) passes, and the time of the
    /// check is printed (`--nocapture`).
    #[test]
    fn a_sweep_of_800_inputs_passes() {
        let (bytes, txid) = sweep(800, "00", false);
        let start = std::time::Instant::now();
        let answer = verify_subject_inputs(&bytes, &txid, "main", None);
        println!(
            "the check of an 800-input sweep ({} bytes): {:?} in {:?}",
            bytes.len(),
            answer,
            start.elapsed()
        );
        assert!(answer.is_ok());
    }

    /// A subject the BEEF proves is not run, whatever its scripts.
    #[test]
    fn a_proven_subject_is_not_run() {
        let (bytes, txid) = sweep(2, "", true);
        assert!(verify_subject_inputs(&bytes, &txid, "main", None).is_ok());
        let (bytes, txid) = sweep(2, "", false);
        assert!(verify_subject_inputs(&bytes, &txid, "main", None).is_err());
    }

    fn with_work<T>(limits: (u64, u64), run: impl FnOnce() -> T) -> T {
        WORK_OVERRIDE.with(|w| w.set(Some(limits)));
        let answer = run();
        WORK_OVERRIDE.with(|w| w.set(None));
        answer
    }

    /// One input past its work bound is not judged, and refused.
    #[test]
    fn an_input_past_its_work_bound_is_not_judged() {
        let (bytes, txid) = sweep(1, "00", false);
        let answer = with_work((2, u64::MAX), || {
            verify_subject_inputs(&bytes, &txid, "main", None)
        });
        let text = answer.unwrap_err().to_string();
        assert!(
            text.contains(&format!(
                "input 0 of {txid}: input 0 not judged: exceeded work budget 2 units after "
            )),
            "{text}"
        );
    }

    /// The inputs together past the transaction's bound are not judged, at
    /// the input that crosses it.
    #[test]
    fn inputs_past_the_transaction_work_bound_are_not_judged() {
        let (bytes, txid) = sweep(3, "00", false);
        let one = with_work((u64::MAX, u64::MAX), || {
            verify_subject_inputs(&bytes, &txid, "main", None)
        });
        assert!(one.is_ok());
        let answer = with_work((u64::MAX, 5), || {
            verify_subject_inputs(&bytes, &txid, "main", None)
        });
        let text = answer.unwrap_err().to_string();
        assert!(
            text.contains("not judged: exceeded transaction work budget 5 units after "),
            "{text}"
        );
    }

    /// An honest sweep: 800 P2PKH coins of one key, each input signed
    /// (SIGHASH_ALL | FORKID); it passes, and its time is printed.
    #[test]
    fn a_signed_sweep_of_800_p2pkh_inputs_passes() {
        use bsv_rs::primitives::PrivateKey;
        use bsv_rs::script::templates::P2PKH;
        use bsv_rs::script::{SignOutputs, SigningContext};
        let key = PrivateKey::random();
        let mut lock = vec![0x76, 0xa9, 0x14];
        lock.extend(bsv_rs::primitives::hash160(
            &key.public_key().to_compressed(),
        ));
        lock.extend([0x88, 0xac]);
        let lock = LockingScript::from_binary(&lock).unwrap();
        let mut parent = Transaction::new();
        parent.inputs.push(input(&"11".repeat(32), 0, "00"));
        for _ in 0..800 {
            parent.outputs.push(TransactionOutput {
                satoshis: Some(1_000),
                locking_script: lock.clone(),
                change: false,
            });
        }
        let parent_txid = parent.id();
        let mut child = Transaction::new();
        child.version = 1;
        for vout in 0..800 {
            child.inputs.push(input(&parent_txid, vout, ""));
        }
        child.outputs.push(TransactionOutput {
            satoshis: Some(799_500),
            locking_script: anyone_lock(),
            change: false,
        });
        let unsigned = child.to_binary();
        let unlock = P2PKH::unlock(&key, SignOutputs::All, false);
        // A fresh transaction carries the scripts (the unsigned one keeps
        // the bytes it serialized).
        let mut signed = Transaction::new();
        signed.version = 1;
        signed.outputs = child.outputs.clone();
        for vin in 0..800 {
            let context = SigningContext::new(&unsigned, vin, 1_000, lock.as_script());
            let mut spend = TransactionInput::new(parent_txid.clone(), vin as u32);
            spend.unlocking_script = Some(unlock.sign(&context).unwrap());
            signed.inputs.push(spend);
        }
        let child = signed;
        let txid = child.id();
        let height = ProtocolEra::MAINNET_GENESIS_HEIGHT + 101;
        let mut beef = Beef::new();
        let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, height));
        beef.merge_raw_tx(parent.to_binary(), Some(bump));
        beef.merge_raw_tx(child.to_binary(), None);
        let bytes = beef.to_binary_atomic(&txid).unwrap();
        let start = std::time::Instant::now();
        let answer = verify_subject_inputs(&bytes, &txid, "main", None);
        println!(
            "the check of a signed 800-input P2PKH sweep ({} bytes): {:?} in {:?}",
            bytes.len(),
            answer,
            start.elapsed()
        );
        assert!(answer.is_ok());
        // One signature broken: refused at that input.
        let mut broken = Transaction::new();
        broken.version = 1;
        broken.outputs = child.outputs.clone();
        broken.inputs = child.inputs.clone();
        broken.inputs[799].unlocking_script = child.inputs[0].unlocking_script.clone();
        let txid = broken.id();
        let mut beef = Beef::new();
        let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&parent_txid, height));
        beef.merge_raw_tx(parent.to_binary(), Some(bump));
        beef.merge_raw_tx(broken.to_binary(), None);
        let bytes = beef.to_binary_atomic(&txid).unwrap();
        let text = verify_subject_inputs(&bytes, &txid, "main", None)
            .unwrap_err()
            .to_string();
        assert!(text.contains(&format!("input 799 of {txid}: ")), "{text}");
    }
}
