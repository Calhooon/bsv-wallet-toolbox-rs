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
    let m = CWIStyleWalletManager::new("admin.test".into(), builder, Default::default());
    (m, seen)
}

fn v1_fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("../../test_vectors/keys/cwi-key-file-v1.json")).unwrap()
}

fn hex_field(v: &serde_json::Value, key: &str) -> Vec<u8> {
    hex::decode(v[key].as_str().unwrap()).unwrap()
}

fn scratch_file(name: &str, bytes: &[u8]) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("cwi-560-{}", hex::encode(generate_random_bytes(6))));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

fn only_token(file: &[u8]) -> (KeyFile, UmpToken) {
    let file: KeyFile = serde_json::from_slice(file).unwrap();
    assert_eq!(file.key_file_version, 3);
    let ump = file.profiles[0].ump.as_ref().expect("a v3 record");
    let token = UmpToken::from_bytes(&ump.token).unwrap();
    (file, token)
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
async fn a_v1_file_opens_and_its_unlock_rewrites_it_as_v3_with_a_fresh_salt() {
    let v = v1_fixture();
    let (id, key, v1) = (
        hex_field(&v, "profileIdHex"),
        hex_field(&v, "profileKeyHex"),
        hex_field(&v, "fileHex"),
    );
    let password = v["password"].as_str().unwrap();
    let path = scratch_file("wallet.keys", &v1);
    let (m, seen) = manager();
    m.open_key_file(&path).await.unwrap();
    assert_eq!(m.get_profiles().await[0].name, "Legacy");
    m.provide_privileged_key_manager(PrivateKey::random()).await;
    m.switch_profile(&id, password).await.unwrap();
    assert_eq!(
        seen.lock().unwrap()[0],
        key,
        "v1 opens with the profile-id salt"
    );

    let after = std::fs::read(&path).unwrap();
    assert_ne!(after, v1);
    let (_, token) = only_token(&after);
    assert_eq!(token.password_salt.len(), 32);
    assert_ne!(token.password_salt[..16], id[..]);
    assert_eq!(token.ump_version, Some(3));
    let siblings = std::fs::read_dir(path.parent().unwrap()).unwrap().count();
    assert_eq!(siblings, 1, "the sibling was renamed over the file");

    let (again, seen_again) = manager();
    again.open_key_file(&path).await.unwrap();
    again
        .provide_privileged_key_manager(PrivateKey::random())
        .await;
    again.switch_profile(&id, password).await.unwrap();
    assert_eq!(
        seen_again.lock().unwrap()[0],
        key,
        "the key material still decrypts"
    );
    assert_eq!(again.recovery_key(&id, password).await.unwrap().len(), 32);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        after,
        "a v3 unlock writes nothing"
    );
}

#[tokio::test]
async fn a_failed_unlock_leaves_the_file_byte_identical() {
    let v = v1_fixture();
    let (id, v1) = (hex_field(&v, "profileIdHex"), hex_field(&v, "fileHex"));
    let password = v["password"].as_str().unwrap();
    let path = scratch_file("v1.keys", &v1);
    let (m, _) = manager();
    m.open_key_file(&path).await.unwrap();
    let no_pkm = m.switch_profile(&id, password).await;
    assert!(matches!(no_pkm, Err(Error::AuthenticationRequired)));
    assert_eq!(std::fs::read(&path).unwrap(), v1);

    m.provide_privileged_key_manager(PrivateKey::random()).await;
    m.switch_profile(&id, password).await.unwrap();
    let v3 = std::fs::read(&path).unwrap();
    let wrong = m.switch_profile(&id, "not the password").await;
    assert!(matches!(wrong, Err(Error::AccessDenied(_))));
    assert_eq!(std::fs::read(&path).unwrap(), v3);
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
