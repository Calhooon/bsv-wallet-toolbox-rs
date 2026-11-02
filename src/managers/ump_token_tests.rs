//! The UMP token format tests (#560).

use super::*;

fn factors(password: &str, salt: &[u8], kdf: Option<&PasswordKdf>) -> Factors {
    Factors {
        password_key: derive_password_key(salt, kdf, password).unwrap(),
        presentation_key: Zeroizing::new(vec![0x11; 32]),
        recovery_key: Zeroizing::new(vec![0x22; 32]),
        root_primary: Zeroizing::new(vec![0x33; 32]),
        root_privileged: Zeroizing::new(vec![0x44; 32]),
    }
}

fn profile() -> RefProfile {
    RefProfile {
        name: "Work".into(),
        id: vec![7; 16],
        primary_pad: vec![8; 32],
        privileged_pad: vec![9; 32],
        created_at: 1_730_000_000,
    }
}

fn token(kdf: PasswordKdf) -> UmpToken {
    let salt = vec![0x55; 32];
    build_token(
        salt.clone(),
        kdf.clone(),
        &factors("pw", &salt, Some(&kdf)),
        &[profile()],
    )
    .unwrap()
}

#[test]
fn a_v3_token_round_trips_in_the_reference_layout() {
    let mut t = token(PasswordKdf::pbkdf2(7777));
    let bytes = t.to_bytes();
    assert_eq!(bytes[0], 3);
    assert_eq!(&bytes[1..34], &[&[32u8][..], &[0x55; 32][..]].concat()[..]);
    assert_eq!(UmpToken::from_bytes(&bytes).unwrap(), t);
    assert!(
        bytes.ends_with(&[0]),
        "an unpublished token writes an empty outpoint"
    );
    t.current_outpoint = Some(format!("{}.0", "ab".repeat(32)));
    assert_eq!(UmpToken::from_bytes(&t.to_bytes()).unwrap(), t);
    let params = br#"{"iterations":7777}"#;
    assert!(bytes.windows(params.len()).any(|w| w == params));
}

#[test]
fn a_token_opens_to_its_root_keys_profiles_and_wrapped_factors() {
    let t = token(PasswordKdf::pbkdf2(7777));
    let opened = open_token(&t, &[0x11; 32], "pw").unwrap();
    assert_eq!(*opened.root_primary, vec![0x33; 32]);
    assert_eq!(opened.profiles[0].primary_pad, vec![8; 32]);
    let privileged = open_root_privileged(&t, &opened).unwrap();
    assert_eq!(*privileged, vec![0x44; 32]);
    let recovery = unwrap_factor(&privileged, &t.recovery_key_encrypted).unwrap();
    assert_eq!(*recovery, vec![0x22; 32]);
    let presentation = unwrap_factor(&privileged, &t.presentation_key_encrypted).unwrap();
    assert_eq!(*presentation, vec![0x11; 32]);
    assert!(matches!(
        open_token(&t, &[0x11; 32], "other"),
        Err(Error::AccessDenied(_))
    ));
    assert_eq!(t.presentation_hash, sha256(&[0x11; 32]).to_vec());
}

#[test]
fn a_legacy_token_has_no_kdf_metadata_and_derives_at_7777() {
    let salt = vec![0x66; 32];
    let mut t = build_token(
        salt.clone(),
        PasswordKdf::pbkdf2(7777),
        &factors("pw", &salt, None),
        &[],
    )
    .unwrap();
    (t.ump_version, t.password_kdf) = (None, None);
    let bytes = t.to_bytes();
    assert_eq!(bytes[0], 2);
    let back = UmpToken::from_bytes(&bytes).unwrap();
    assert_eq!(back.password_kdf, None);
    assert_eq!(
        *open_token(&back, &[0x11; 32], "pw").unwrap().root_primary,
        vec![0x33; 32]
    );
    let at_7777 = derive_password_key(&salt, None, "pw").unwrap();
    assert_eq!(at_7777, pbkdf2_sha512("pw", &salt, 7777).unwrap());
    assert_ne!(at_7777, pbkdf2_sha512("pw", &salt, 7776).unwrap());
}

#[test]
fn the_reference_bounds_refuse_what_its_reader_refuses() {
    let good = token(PasswordKdf::pbkdf2(7777)).to_bytes();
    let refuse = |bytes: &[u8]| UmpToken::from_bytes(bytes).is_err();
    assert!(refuse(&[4]), "version 4");
    let mut empty_field = good.clone();
    empty_field[1] = 0;
    assert!(refuse(&empty_field), "a zero-length field");
    assert!(refuse(&[3, 0xfd, 0x20, 0x00]), "a non-minimal varint");
    for bad in ["abc", "a b.0", ".0", "ab.", "ab.x"] {
        let mut t = token(PasswordKdf::pbkdf2(7777));
        t.current_outpoint = Some(bad.into());
        assert!(refuse(&t.to_bytes()), "outpoint {bad:?}");
    }
    for iterations in [0, 10_000_001] {
        let mut t = token(PasswordKdf::pbkdf2(7777));
        t.password_kdf = Some(PasswordKdf::pbkdf2(iterations));
        assert!(refuse(&t.to_bytes()), "iterations {iterations}");
    }
    assert!(!refuse(&good));
}

#[test]
fn argon2id_parses_and_is_refused_at_derivation_until_stage_two() {
    let argon = PasswordKdf {
        algorithm: KdfAlgorithm::Argon2id,
        iterations: 7,
        memory_kib: Some(131_072),
        parallelism: Some(1),
        hash_length: Some(32),
    };
    assert!(argon.is_valid());
    let mut t = token(PasswordKdf::pbkdf2(7777));
    t.password_kdf = Some(argon.clone());
    assert_eq!(
        UmpToken::from_bytes(&t.to_bytes()).unwrap().password_kdf,
        Some(argon)
    );
    assert!(matches!(
        open_token(&t, &[0x11; 32], "pw"),
        Err(Error::InvalidOperation(_))
    ));
}

#[test]
fn the_snapshot_reads_and_writes_the_reference_layout() {
    let s = WalletSnapshot::new(vec![0xAA; 32], vec![0x01; 16], vec![0xBB; 64]);
    let bytes = s.to_bytes();
    assert_eq!(bytes.len(), 1 + 32 + 16 + 64);
    assert_eq!(WalletSnapshot::from_bytes(&bytes).unwrap(), s);
    let v1 = WalletSnapshot::from_bytes(&[&[1u8][..], &[0xAA; 32], &[0xBB; 8]].concat()).unwrap();
    assert_eq!(v1.active_profile_id, vec![0; 16]);
    assert!(WalletSnapshot::from_bytes(&[3; 40]).is_err());
}
