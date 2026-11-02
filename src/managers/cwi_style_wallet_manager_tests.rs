//! The CWI manager's tests: profiles, and the v3 key file and the v1
//! migration witnesses (#560).

use super::*;
use crate::managers::ump_token::{KeyFile, UmpToken};
use bsv_rs::wallet::ProtoWallet;

#[test]
fn test_profile_new() {
    let profile = Profile::new("Test".to_string());
    assert_eq!(profile.name, "Test");
    assert_eq!(profile.id.len(), PROFILE_ID_LENGTH);
    assert_eq!(profile.primary_pad.len(), PRIMARY_PAD_LENGTH);
    assert_eq!(profile.privileged_pad.len(), PRIVILEGED_PAD_LENGTH);
}

#[test]
fn test_profile_salts_differ() {
    // Each is an OS draw (#191), one for the id and one for each pad.
    let before = crate::rng::os_draws();
    let (a, b) = (Profile::new("A".into()), Profile::new("A".into()));
    assert_eq!(crate::rng::os_draws() - before, 6);
    assert_eq!(a.id.len(), PROFILE_ID_LENGTH);
    assert_ne!(a.id, b.id);
    assert_ne!(a.primary_pad, b.primary_pad);
}

#[test]
fn test_profile_derive_primary_key() {
    let profile = Profile::new("Test".to_string());
    let derived_key = vec![0x55u8; 32];
    let primary_key = profile.derive_primary_key(&derived_key);

    // primary_key = primary_pad XOR derived_key
    for i in 0..32 {
        assert_eq!(primary_key[i], profile.primary_pad[i] ^ derived_key[i]);
    }
}

#[test]
fn test_profile_serialization() {
    let profile = Profile::new("Test".to_string());
    let json = serde_json::to_string(&profile).unwrap();
    let parsed: Profile = serde_json::from_str(&json).unwrap();

    assert_eq!(profile.name, parsed.name);
    assert_eq!(profile.id, parsed.id);
    assert_eq!(profile.primary_pad, parsed.primary_pad);
    assert_eq!(profile.privileged_pad, parsed.privileged_pad);
}

#[test]
fn test_profile_roundtrip() {
    let primary_key = vec![0x42u8; 32];
    let derived_key = vec![0x55u8; 32];

    let profile = Profile::from_primary_key("Test".to_string(), &primary_key, &derived_key);
    let recovered = profile.derive_primary_key(&derived_key);

    assert_eq!(primary_key, recovered);
}

#[test]
fn test_profile_backward_compat_deserialization() {
    // Simulate old format without privileged_pad
    let json = r#"{"name":"Test","id":"AAAAAAAAAAAAAAAAAAAAAA==","primaryPad":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","createdAt":"2024-01-01T00:00:00Z","updatedAt":"2024-01-01T00:00:00Z"}"#;
    let parsed: Profile = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.name, "Test");
    assert!(parsed.privileged_pad.is_empty());
}

#[test]
fn test_config_default() {
    let config = CWIStyleWalletManagerConfig::default();
    assert_eq!(config.password_rounds, DEFAULT_PASSWORD_ROUNDS);
    assert!(config.use_pbkdf2);
}

/// A manager whose builder records the key it is handed.
fn manager() -> (CWIStyleWalletManager, Arc<std::sync::Mutex<Vec<Vec<u8>>>>) {
    let seen: Arc<std::sync::Mutex<Vec<Vec<u8>>>> = Arc::default();
    let log = seen.clone();
    let builder: CWIWalletBuilder = Arc::new(move |key, _pkm| {
        log.lock().unwrap().push(key.clone());
        Box::pin(async move {
            let wallet = ProtoWallet::new(Some(PrivateKey::from_bytes(&key)?));
            Ok(Arc::new(wallet) as Arc<dyn WalletInterface + Send + Sync>)
        })
    });
    let m = CWIStyleWalletManager::new(
        "admin.test".into(),
        builder,
        CWIStyleWalletManagerConfig::default(),
    );
    (m, seen)
}

fn v1_fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("../../test_vectors/keys/cwi-key-file-v1.json")).unwrap()
}

fn hex_field(v: &serde_json::Value, key: &str) -> Vec<u8> {
    hex::decode(v[key].as_str().unwrap()).unwrap()
}

/// A key file in its own scratch directory, removed with it on drop.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Some(dir) = self.0.parent() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn scratch_file(name: &str, bytes: &[u8]) -> Scratch {
    let dir =
        std::env::temp_dir().join(format!("cwi-560-{}", hex::encode(generate_random_bytes(6))));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    Scratch(path)
}

fn only_token(file: &[u8]) -> (KeyFile, UmpToken) {
    let file: KeyFile = serde_json::from_slice(file).unwrap();
    assert_eq!(file.key_file_version, 3);
    let ump = file.profiles[0].ump.as_ref().expect("a v3 record");
    let token = UmpToken::from_bytes(&ump.token).unwrap();
    (file, token)
}

/// The fixture's profile id, its v1 key, its v1 file and its password.
fn v1_parts() -> (Vec<u8>, Vec<u8>, Vec<u8>, String) {
    let v = v1_fixture();
    let password = v["password"].as_str().unwrap().to_string();
    let (id, key) = (
        hex_field(&v, "profileIdHex"),
        hex_field(&v, "profileKeyHex"),
    );
    (id, key, hex_field(&v, "fileHex"), password)
}

/// A manager on the key file at `path`, its privileged key provided.
async fn opened(path: &Path) -> (CWIStyleWalletManager, Arc<std::sync::Mutex<Vec<Vec<u8>>>>) {
    let (m, seen) = manager();
    m.open_key_file(path).await.unwrap();
    m.provide_privileged_key_manager(PrivateKey::random()).await;
    (m, seen)
}

/// The profile key a key file's only token opens to with `password`.
fn token_key(file: &[u8], password: &str) -> Result<Zeroizing<Vec<u8>>> {
    let (file, token) = only_token(file);
    let presentation = &file.profiles[0].ump.as_ref().unwrap().presentation_key;
    let opened = ump_token::open_token(&token, presentation, password)?;
    Ok(ump_token::xor(
        &opened.root_primary,
        &opened.profiles[0].primary_pad,
    ))
}

#[tokio::test]
async fn a_new_profile_carries_a_random_32_byte_salt_and_ump_version_3() {
    let (m, seen) = manager();
    m.provide_privileged_key_manager(PrivateKey::random()).await;
    let before = crate::rng::os_draws();
    let profile = m.create_profile("Work", "pw").await.unwrap();
    // id and two pads, the root, the salt, presentation, recovery, privileged.
    assert_eq!(crate::rng::os_draws() - before, 8);
    let (file, token) = only_token(&m.save_snapshot().await.unwrap());
    assert_eq!(file.profiles[0].profile.id, profile.id);
    assert_eq!(token.password_salt.len(), 32);
    assert_ne!(token.password_salt[..16], profile.id[..]);
    assert_eq!(token.ump_version, Some(3));
    assert_eq!(token.password_kdf, Some(PasswordKdf::pbkdf2(7777)));
    let other = m.create_profile("Home", "pw").await.unwrap();
    let both: KeyFile = serde_json::from_slice(&m.save_snapshot().await.unwrap()).unwrap();
    let salts: Vec<Vec<u8>> = both
        .profiles
        .iter()
        .map(|r| {
            UmpToken::from_bytes(&r.ump.as_ref().unwrap().token)
                .unwrap()
                .password_salt
        })
        .collect();
    assert_ne!(salts[0], salts[1]);
    m.switch_profile(&profile.id, "pw").await.unwrap();
    m.switch_profile(&other.id, "pw").await.unwrap();
    assert_eq!(seen.lock().unwrap().len(), 2);
    assert!(matches!(
        m.switch_profile(&profile.id, "wrong").await,
        Err(Error::AccessDenied(_))
    ));
}

#[tokio::test]
async fn a_v1_file_gains_a_token_and_a_later_unlock_that_opens_it_drops_v1() {
    let (id, key, v1, password) = v1_parts();
    let scratch = scratch_file("wallet.keys", &v1);
    let path = scratch.0.as_path();
    let (m, seen) = opened(path).await;
    assert_eq!(m.get_profiles().await[0].name, "Legacy");
    m.switch_profile(&id, &password).await.unwrap();
    assert_eq!(
        seen.lock().unwrap()[0],
        key,
        "v1 opens at the profile-id salt"
    );

    let both = std::fs::read(path).unwrap();
    let (file, token) = only_token(&both);
    let v1_pad = &file.profiles[0].v1.as_ref().expect("v1 kept").primary_pad;
    assert_eq!(
        ump_token::xor(
            v1_pad,
            &ump_token::pbkdf2_sha512(&password, &id, 7777).unwrap()
        )
        .to_vec(),
        key
    );
    assert_eq!(token.password_salt.len(), 32);
    assert_ne!(token.password_salt[..16], id[..]);
    assert_eq!(*token_key(&both, &password).unwrap(), key);
    let siblings = std::fs::read_dir(path.parent().unwrap()).unwrap().count();
    assert_eq!(siblings, 1, "the sibling was renamed over the file");

    let (again, seen_again) = opened(path).await;
    again.switch_profile(&id, &password).await.unwrap();
    assert_eq!(
        seen_again.lock().unwrap()[0],
        key,
        "the token opens to the same key"
    );
    let alone = std::fs::read(path).unwrap();
    let (file, token) = only_token(&alone);
    assert!(
        file.profiles[0].v1.is_none(),
        "the token's GCM tag dropped v1"
    );
    assert!(!String::from_utf8_lossy(&alone).contains("\"v1\""));
    assert_eq!(token.password_salt.len(), 32);
    assert_ne!(token.password_salt[..16], id[..]);
    assert_eq!(again.recovery_key(&id, &password).await.unwrap().len(), 32);
    again.switch_profile(&id, &password).await.unwrap();
    assert_eq!(
        std::fs::read(path).unwrap(),
        alone,
        "a token alone writes nothing"
    );
}

#[tokio::test]
async fn a_wrong_password_at_the_migrating_unlock_keeps_v1_and_the_right_one_still_opens() {
    let (id, key, v1, password) = v1_parts();
    let scratch = scratch_file("typo.keys", &v1);
    let path = scratch.0.as_path();
    let (m, seen) = opened(path).await;
    m.switch_profile(&id, "a typo").await.unwrap();
    assert_ne!(seen.lock().unwrap()[0], key, "v1 has no password check");
    let typo = std::fs::read(path).unwrap();
    let (file, _) = only_token(&typo);
    assert!(file.profiles[0].v1.is_some(), "the v1 record is intact");
    assert!(
        token_key(&typo, &password).is_err(),
        "the token holds the typo's root"
    );

    m.switch_profile(&id, &password).await.unwrap();
    assert_eq!(
        seen.lock().unwrap()[1],
        key,
        "the right password opens through v1"
    );
    let resealed = m.save_snapshot().await.unwrap();
    assert_eq!(
        *token_key(&resealed, &password).unwrap(),
        key,
        "the token now carries the right root"
    );
    assert_eq!(
        std::fs::read(path).unwrap(),
        typo,
        "a token that refused writes nothing"
    );

    m.switch_profile(&id, &password).await.unwrap();
    assert_eq!(seen.lock().unwrap()[2], key);
    let alone = std::fs::read(path).unwrap();
    let (file, token) = only_token(&alone);
    assert!(
        file.profiles[0].v1.is_none(),
        "the second right unlock drops v1"
    );
    assert_eq!(token.password_salt.len(), 32);
    assert_ne!(token.password_salt[..16], id[..]);
    let (fresh, seen_fresh) = opened(path).await;
    fresh.switch_profile(&id, &password).await.unwrap();
    assert_eq!(seen_fresh.lock().unwrap()[0], key);
}

#[tokio::test]
async fn a_wrong_second_unlock_changes_nothing() {
    let (id, key, v1, password) = v1_parts();
    let scratch = scratch_file("second.keys", &v1);
    let path = scratch.0.as_path();
    opened(path)
        .await
        .0
        .switch_profile(&id, &password)
        .await
        .unwrap();
    let both = std::fs::read(path).unwrap();
    let (m, _) = opened(path).await;
    m.switch_profile(&id, "not the password").await.unwrap();
    assert_eq!(
        std::fs::read(path).unwrap(),
        both,
        "the file is byte-identical"
    );
    let (n, seen) = opened(path).await;
    n.switch_profile(&id, &password).await.unwrap();
    assert_eq!(
        seen.lock().unwrap()[0],
        key,
        "the right password still opens the token"
    );
    assert!(only_token(&std::fs::read(path).unwrap()).0.profiles[0]
        .v1
        .is_none());
}

#[tokio::test]
async fn a_migrated_file_derives_at_its_new_salt_alone() {
    let (id, key, v1, password) = v1_parts();
    let scratch = scratch_file("resalt.keys", &v1);
    let path = scratch.0.as_path();
    for _ in 0..2 {
        opened(path)
            .await
            .0
            .switch_profile(&id, &password)
            .await
            .unwrap();
    }
    let bytes = std::fs::read(path).unwrap();
    let (file, token) = only_token(&bytes);
    assert!(file.profiles[0].v1.is_none());
    let record = file.profiles[0].ump.as_ref().unwrap();
    let at_id = ump_token::pbkdf2_sha512(&password, &id, 7777).unwrap();
    let opens = |k: &[u8], c: &[u8]| SymmetricKey::from_bytes(k).unwrap().decrypt(c).is_ok();
    let blobs = [
        &token.password_presentation_primary,
        &token.password_recovery_primary,
        &token.presentation_recovery_primary,
        &token.password_primary_privileged,
        &token.presentation_recovery_privileged,
        token.profiles_encrypted.as_ref().unwrap(),
    ];
    for k in [&*at_id, &*ump_token::xor(&record.presentation_key, &at_id)] {
        assert!(
            blobs.iter().all(|c| !opens(k, c)),
            "the profile-id salt opens nothing"
        );
    }
    let clear_pad = &file.profiles[0].profile.primary_pad;
    assert_ne!(
        ump_token::xor(clear_pad, &at_id).to_vec(),
        key,
        "nor does the clear pad"
    );
    let at_salt = ump_token::pbkdf2_sha512(&password, &token.password_salt, 7777).unwrap();
    let pres_pass = ump_token::xor(&record.presentation_key, &at_salt);
    assert!(
        opens(&pres_pass, &token.password_presentation_primary),
        "the new salt opens it"
    );
    assert_eq!(
        *token_key(&bytes, &password).unwrap(),
        key,
        "the v1 primary key, recovered"
    );
}

#[tokio::test]
async fn a_failed_unlock_leaves_the_file_byte_identical() {
    let (id, _, v1, password) = v1_parts();
    let scratch = scratch_file("v1.keys", &v1);
    let path = scratch.0.as_path();
    let (m, _) = manager();
    m.open_key_file(path).await.unwrap();
    let no_pkm = m.switch_profile(&id, &password).await;
    assert!(matches!(no_pkm, Err(Error::AuthenticationRequired)));
    assert_eq!(std::fs::read(path).unwrap(), v1, "the migrating step");

    m.provide_privileged_key_manager(PrivateKey::random()).await;
    m.switch_profile(&id, &password).await.unwrap();
    let both = std::fs::read(path).unwrap();
    m.destroy().await;
    let no_pkm = m.switch_profile(&id, &password).await;
    assert!(matches!(no_pkm, Err(Error::AuthenticationRequired)));
    assert_eq!(std::fs::read(path).unwrap(), both, "the dropping step");

    m.provide_privileged_key_manager(PrivateKey::random()).await;
    m.switch_profile(&id, &password).await.unwrap();
    let alone = std::fs::read(path).unwrap();
    let wrong = m.switch_profile(&id, "not the password").await;
    assert!(matches!(wrong, Err(Error::AccessDenied(_))));
    assert_eq!(std::fs::read(path).unwrap(), alone, "a token alone");
}

#[tokio::test]
async fn a_profile_replaced_or_deleted_during_its_unlock_refuses_the_commit() {
    let (id, _, v1, password) = v1_parts();
    let scratch = scratch_file("race.keys", &v1);
    let path = scratch.0.as_path();
    let (m, _) = opened(path).await;
    let read = m.entry(&id).await.unwrap();
    let (_, next) = m.unlock_entry(&read, &password).unwrap();
    let (next, write) = next.unwrap();
    assert!(write);
    m.delete_profile(&id).await.unwrap();
    let refused = m.commit_entry(&read, next.clone(), write).await;
    assert!(matches!(refused, Err(Error::InvalidOperation(_))));
    assert!(
        m.get_profiles().await.is_empty(),
        "the deleted profile stays deleted"
    );
    assert_eq!(std::fs::read(path).unwrap(), v1, "nothing written");

    m.load_snapshot(&v1).await.unwrap();
    m.commit_entry(&read, next, write).await.unwrap();
    assert!(only_token(&std::fs::read(path).unwrap()).0.profiles[0]
        .v1
        .is_some());
}

#[tokio::test]
async fn opening_a_key_file_removes_a_stale_write_sibling() {
    let (_, _, v1, _) = v1_parts();
    let scratch = scratch_file("w.keys", &v1);
    let dir = scratch.0.parent().unwrap();
    let stale = dir.join("w.keys.tmp-0123456789abcdef");
    let other = dir.join("w.keys.tmp-not-ours");
    std::fs::write(&stale, b"key material").unwrap();
    std::fs::write(&other, b"kept").unwrap();
    let (m, _) = manager();
    m.open_key_file(&scratch.0).await.unwrap();
    assert!(!stale.exists(), "the interrupted write's sibling is gone");
    assert!(other.exists(), "a name not of the write's shape is left");
}

#[tokio::test]
async fn exports_carry_the_token_and_a_v1_export_reads_as_v1() {
    let (m, seen) = manager();
    m.provide_privileged_key_manager(PrivateKey::random()).await;
    let p = m.create_profile("Work", "pw").await.unwrap();
    let blob = m.export_profile(&p.id).await.unwrap();
    let (n, seen_n) = manager();
    n.provide_privileged_key_manager(PrivateKey::random()).await;
    n.import_profile(&blob, "pw").await.unwrap();
    m.switch_profile(&p.id, "pw").await.unwrap();
    n.switch_profile(&p.id, "pw").await.unwrap();
    assert_eq!(seen.lock().unwrap()[0], seen_n.lock().unwrap()[0]);
    let flat = serde_json::to_vec(&p).unwrap();
    assert!(Entry::from_record(serde_json::from_slice(&flat).unwrap())
        .unwrap()
        .ump
        .is_none());
}

#[tokio::test]
async fn a_reference_written_token_opens_through_switch_profile() {
    use base64::Engine;
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../test_vectors/keys/ump-token-reference.json"
    ))
    .unwrap();
    let b64 = |key: &str, path: &[&str]| {
        let s = path.iter().fold(&v, |v, k| &v[*k])[key].as_str().unwrap();
        base64::engine::general_purpose::STANDARD.encode(hex::decode(s).unwrap())
    };
    let record = serde_json::json!({
        "name": "Work", "id": b64("profileIdHex", &["expected"]),
        "primaryPad": "", "createdAt": "2026-11-02T00:00:00Z", "updatedAt": "2026-11-02T00:00:00Z",
        "ump": { "presentationKey": b64("presentationKeyHex", &["inputs"]), "token": b64("tokenV3Hex", &[]) },
    });
    let scratch = scratch_file("ref.keys", b"");
    std::fs::remove_file(&scratch.0).unwrap();
    let (m, seen) = opened(&scratch.0).await;
    let profile = m
        .import_profile_json(record.to_string().as_bytes())
        .await
        .unwrap();
    m.save_key_file().await.unwrap();
    let saved = std::fs::read(&scratch.0).unwrap();
    let password = v["inputs"]["password"].as_str().unwrap();
    m.switch_profile(&profile.id, password).await.unwrap();
    let want = hex::decode(v["expected"]["profilePrimaryKeyHex"].as_str().unwrap()).unwrap();
    assert_eq!(
        seen.lock().unwrap()[0],
        want,
        "the pad is read from the token, not the clear index"
    );
    let wrong = m.switch_profile(&profile.id, "not the password").await;
    assert!(matches!(wrong, Err(Error::AccessDenied(_))));
    assert_eq!(std::fs::read(&scratch.0).unwrap(), saved, "untouched");
    let (_, token) = only_token(&saved);
    assert_eq!(
        token.to_bytes(),
        hex::decode(v["tokenV3Hex"].as_str().unwrap()).unwrap()
    );
}
