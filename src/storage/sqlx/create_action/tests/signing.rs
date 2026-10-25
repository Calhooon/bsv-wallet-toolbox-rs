use super::super::*;
use super::*;

#[test]
fn test_der_signature_sighash_reads_the_trailing_flag_byte() {
    assert_eq!(der_signature_sighash(&fake_der_signature(0x41)), Some(0x41));
    assert_eq!(der_signature_sighash(&fake_der_signature(0xc3)), Some(0xc3));
    // Minimal DER (1-byte r and s) + flags = 9 bytes.
    assert_eq!(
        der_signature_sighash(&[0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01, 0x43]),
        Some(0x43)
    );
    // Not a signature: a pubkey, a hash, empty, wrong tags, bad base type.
    let mut pubkey = vec![0x02u8];
    pubkey.extend(std::iter::repeat_n(0xabu8, 32));
    assert_eq!(der_signature_sighash(&pubkey), None);
    assert_eq!(der_signature_sighash(&[0xaa; 32]), None);
    assert_eq!(der_signature_sighash(&[]), None);
    let mut wrong_len = fake_der_signature(0x41);
    wrong_len[1] = 0x40;
    assert_eq!(der_signature_sighash(&wrong_len), None);
    assert_eq!(der_signature_sighash(&fake_der_signature(0x40)), None);
    assert_eq!(der_signature_sighash(&fake_der_signature(0x44)), None);
}

#[test]
fn test_script_pushes_handles_every_push_encoding_and_stops_on_truncation() {
    // direct push, OP_PUSHDATA1, OP_PUSHDATA2, an opcode, OP_0
    let mut script = vec![0x02, 0xaa, 0xbb];
    script.extend([0x4c, 0x01, 0xcc]);
    script.extend([0x4d, 0x02, 0x00, 0xdd, 0xee]);
    script.push(0xac); // OP_CHECKSIG
    script.push(0x00); // OP_0
    let pushes = script_pushes(&script);
    assert_eq!(
        pushes,
        vec![&[0xaa, 0xbb][..], &[0xcc][..], &[0xdd, 0xee][..]]
    );

    // A push that claims more bytes than remain ends the walk.
    let truncated = vec![0x01, 0xaa, 0x05, 0xbb];
    assert_eq!(script_pushes(&truncated), vec![&[0xaa][..]]);
    assert!(script_pushes(&[]).is_empty());
}

#[test]
fn test_input_commitment_by_sighash() {
    // SIGHASH_ALL|FORKID and ALL|FORKID|... commit to the whole tx.
    assert_eq!(
        input_commitment(&p2pkh_unlock(0x41)),
        InputCommitment::WholeTx
    );
    assert_eq!(
        input_commitment(&p2pkh_unlock(0x01)),
        InputCommitment::WholeTx
    );
    // ALL|ANYONECANPAY seals the output set: still not fundable.
    assert_eq!(
        input_commitment(&p2pkh_unlock(0xc1)),
        InputCommitment::WholeTx
    );
    // SINGLE|ANYONECANPAY|FORKID (the Zanaadu covenant) and NONE|ANYONECANPAY
    // commit to their own input (+ own output) only.
    assert_eq!(
        input_commitment(&p2pkh_unlock(0xc3)),
        InputCommitment::OwnOnly
    );
    assert_eq!(
        input_commitment(&p2pkh_unlock(0xc2)),
        InputCommitment::OwnOnly
    );
    assert_eq!(
        input_commitment(&p2pkh_unlock(0x83)),
        InputCommitment::OwnOnly
    );
    // A covenant unlock: preimage-like data pushes around the signature —
    // position-independent detection.
    let mut covenant = vec![0x4c, 0x60];
    covenant.extend(std::iter::repeat_n(0x01u8, 0x60));
    let sig = fake_der_signature(0xc3);
    covenant.push(sig.len() as u8);
    covenant.extend(sig);
    covenant.push(0x51); // OP_1
    assert_eq!(input_commitment(&covenant), InputCommitment::OwnOnly);
    // Two signatures, one of them SIGHASH_ALL: the whole tx is committed.
    let mut multisig = vec![0x00];
    for flags in [0xc3u8, 0x41] {
        let sig = fake_der_signature(flags);
        multisig.push(sig.len() as u8);
        multisig.extend(sig);
    }
    assert_eq!(input_commitment(&multisig), InputCommitment::WholeTx);
    // No signature at all: nothing constrains the shape.
    assert_eq!(input_commitment(&[0x51]), InputCommitment::Unconstrained);
    assert_eq!(
        input_commitment(&[0x04, 0xde, 0xad, 0xbe, 0xef]),
        InputCommitment::Unconstrained
    );
    assert_eq!(input_commitment(&[]), InputCommitment::Unconstrained);
}
