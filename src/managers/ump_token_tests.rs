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
    let unpublished = [&[66u8][..], UNPUBLISHED_OUTPOINT.as_bytes()].concat();
    assert!(
        bytes.ends_with(&unpublished),
        "a token not on chain writes the placeholder the reference reads"
    );
    t.current_outpoint = Some(format!("{}.0", "ab".repeat(32)));
    assert_eq!(UmpToken::from_bytes(&t.to_bytes()).unwrap(), t);
    let params = br#"{"iterations":7777,"memoryKiB":131072,"parallelism":1,"hashLength":32}"#;
    assert!(bytes.windows(params.len()).any(|w| w == params));
}

#[test]
fn one_changed_byte_of_a_v3_salt_refuses_the_password() {
    let mut t = token(PasswordKdf::pbkdf2(7777));
    assert!(open_token(&t, &[0x11; 32], "pw").is_ok());
    t.password_salt[0] ^= 1;
    assert!(matches!(
        open_token(&t, &[0x11; 32], "pw"),
        Err(Error::AccessDenied(_))
    ));
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
    for bad in ["", "abc", "a b.0", ".0", "ab.", "ab.x"] {
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

fn vector(name: &str) -> serde_json::Value {
    let text = match name {
        "reference" => include_str!("../../test_vectors/keys/ump-token-reference.json"),
        _ => include_str!("../../test_vectors/keys/ump-token-rust.json"),
    };
    serde_json::from_str(text).unwrap()
}

fn hex_at(v: &serde_json::Value, path: &[&str]) -> Vec<u8> {
    let field = path.iter().fold(v, |v, k| &v[*k]);
    hex::decode(field.as_str().unwrap()).unwrap()
}

/// Opens a token with the vector's inputs and checks every recorded key.
fn opens_to_the_recorded_keys(v: &serde_json::Value, token: &UmpToken, recovery: &[u8]) {
    let password = v["inputs"]["password"].as_str().unwrap();
    let opened = open_token(
        token,
        &hex_at(v, &["inputs", "presentationKeyHex"]),
        password,
    )
    .unwrap();
    assert_eq!(
        *opened.root_primary,
        hex_at(v, &["expected", "rootPrimaryKeyHex"])
    );
    let privileged = open_root_privileged(token, &opened).unwrap();
    assert_eq!(
        *privileged,
        hex_at(v, &["expected", "rootPrivilegedKeyHex"])
    );
    assert_eq!(
        *unwrap_factor(&privileged, &token.recovery_key_encrypted).unwrap(),
        recovery
    );
    let profile = &opened.profiles[0];
    assert_eq!(profile.id, hex_at(v, &["expected", "profileIdHex"]));
    assert_eq!(
        *xor(&opened.root_primary, &profile.primary_pad),
        hex_at(v, &["expected", "profilePrimaryKeyHex"])
    );
}

#[test]
fn the_reference_written_v3_token_opens_to_its_recorded_root_keys() {
    let v = vector("reference");
    let bytes = hex_at(&v, &["tokenV3Hex"]);
    let token = UmpToken::from_bytes(&bytes).unwrap();
    assert_eq!(token.to_bytes(), bytes, "re-serialized byte for byte");
    assert_eq!(
        token.password_salt,
        hex_at(&v, &["expected", "passwordSaltHex"])
    );
    let kdf = token.password_kdf.clone().unwrap();
    assert_eq!(
        kdf,
        PasswordKdf::pbkdf2(7777),
        "the reference's full params"
    );
    assert_eq!(kdf.params_json(), PasswordKdf::pbkdf2(7777).params_json());
    assert_eq!(
        token.current_outpoint.as_deref(),
        v["expected"]["currentOutpoint"].as_str()
    );
    opens_to_the_recorded_keys(
        &v,
        &token,
        &hex_at(&v, &["drawnByReference", "recoveryKeyHex"]),
    );
}

#[test]
fn the_reference_legacy_token_opens_at_7777_rounds() {
    let v = vector("reference");
    let bytes = hex_at(&v, &["tokenLegacyHex"]);
    let mut token = UmpToken::from_bytes(&bytes).unwrap();
    assert_eq!(
        (bytes[0], token.ump_version, &token.password_kdf),
        (2, None, &None)
    );
    assert_eq!(token.to_bytes(), bytes);
    opens_to_the_recorded_keys(
        &v,
        &token,
        &hex_at(&v, &["drawnByReference", "recoveryKeyHex"]),
    );
    token.password_kdf = Some(PasswordKdf::pbkdf2(7776));
    token.ump_version = Some(UMP_VERSION);
    let password = v["inputs"]["password"].as_str().unwrap();
    let presentation = hex_at(&v, &["inputs", "presentationKeyHex"]);
    assert!(
        open_token(&token, &presentation, password).is_err(),
        "another count does not open it"
    );
}

#[test]
fn the_rust_written_vector_still_opens_in_rust() {
    let v = vector("rust");
    let token = UmpToken::from_bytes(&hex_at(&v, &["tokenHex"])).unwrap();
    assert_eq!(token.ump_version, Some(UMP_VERSION));
    opens_to_the_recorded_keys(&v, &token, &hex_at(&v, &["expected", "recoveryKeyHex"]));
}

/// Writes `test_vectors/keys/ump-token-rust.json`'s body to stdout: a token
/// the Rust code builds from drawn factors, with no outpoint (written as
/// `UNPUBLISHED_OUTPOINT`). Run with `--ignored --nocapture`.
#[test]
#[ignore = "records the Rust-written vector"]
fn record_a_rust_written_token_for_the_reference() {
    let draw = || {
        Zeroizing::new(crate::managers::cwi_style_wallet_manager::draw_for_vector(
            32,
        ))
    };
    let password = "rust vector password #560";
    let salt = draw().to_vec();
    let kdf = PasswordKdf::pbkdf2(PBKDF2_NUM_ROUNDS);
    let mut k = factors(password, &salt, Some(&kdf));
    (k.presentation_key, k.recovery_key, k.root_primary) = (draw(), draw(), draw());
    k.root_privileged = crate::managers::cwi_style_wallet_manager::draw_scalar_for_vector();
    let p = RefProfile {
        name: "Rust".into(),
        id: draw()[..16].to_vec(),
        primary_pad: draw().to_vec(),
        privileged_pad: draw().to_vec(),
        created_at: 1_762_000_000,
    };
    let token = build_token(salt, kdf, &k, std::slice::from_ref(&p)).unwrap();
    assert_eq!(token.current_outpoint, None);
    let out = serde_json::json!({
        "description": "A UMP token written by bsv-wallet-toolbox-rs (#560, src/managers/ump_token.rs build_token and to_bytes) from OS-drawn factors, one profile, with no outpoint: to_bytes writes UNPUBLISHED_OUTPOINT, the null txid at index 0. ump-token-reference.mjs verify opens it on the reference's code (its deserializeUMPToken, no locator set) and writes ump-token-rust.result.json.",
        "inputs": { "password": password, "presentationKeyHex": hex::encode(&*k.presentation_key) },
        "expected": {
            "rootPrimaryKeyHex": hex::encode(&*k.root_primary),
            "rootPrivilegedKeyHex": hex::encode(&*k.root_privileged),
            "recoveryKeyHex": hex::encode(&*k.recovery_key),
            "profileIdHex": hex::encode(&p.id),
            "profileName": p.name,
            "profilePrimaryKeyHex": hex::encode(&*xor(&k.root_primary, &p.primary_pad)),
            "passwordKdf": { "algorithm": "pbkdf2-sha512", "iterations": PBKDF2_NUM_ROUNDS, "memoryKiB": 131_072, "parallelism": 1, "hashLength": 32 },
        },
        "tokenHex": hex::encode(token.to_bytes()),
    });
    println!(
        "RUST-VECTOR {}",
        serde_json::to_string_pretty(&out).unwrap()
    );
}
