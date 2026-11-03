//! The CWI manager's tests: profiles, and the v3 key file and the v1
//! migration witnesses (#560).

use super::*;
use crate::managers::ump_token::{KdfAlgorithm, KeyFile, UmpToken};
use bsv_rs::wallet::ProtoWallet;

#[path = "cwi_kdf_permits_tests.rs"]
mod kdf_permits;

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
    manager_with(CWIStyleWalletManagerConfig::default(), || {})
}

/// A manager at `config` whose builder runs `on_build`, then records the key.
fn manager_with(
    config: CWIStyleWalletManagerConfig,
    on_build: impl Fn() + Send + Sync + 'static,
) -> (CWIStyleWalletManager, Arc<std::sync::Mutex<Vec<Vec<u8>>>>) {
    let seen: Arc<std::sync::Mutex<Vec<Vec<u8>>>> = Arc::default();
    let log = seen.clone();
    let builder: CWIWalletBuilder = Arc::new(move |key, _pkm| {
        on_build();
        log.lock().unwrap().push(key.clone());
        Box::pin(async move {
            let wallet = ProtoWallet::new(Some(PrivateKey::from_bytes(&key)?));
            Ok(Arc::new(wallet) as Arc<dyn WalletInterface + Send + Sync>)
        })
    });
    let m = CWIStyleWalletManager::new("admin.test".into(), builder, config);
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
    // id and two pads and the root here; the salt, presentation, recovery
    // and privileged keys with the KDF on the blocking pool (#624), counted
    // through `seal` on this thread.
    assert_eq!(crate::rng::os_draws() - before, 4);
    let (sealed, root) = (Profile::new("Seal".into()), Zeroizing::new(vec![7; 32]));
    let before = crate::rng::os_draws();
    CWIStyleWalletManager::seal(sealed, root, "pw").unwrap();
    assert_eq!(crate::rng::os_draws() - before, 4);
    let (file, token) = only_token(&m.save_snapshot().await.unwrap());
    assert_eq!(file.profiles[0].profile.id, profile.id);
    assert_eq!(token.password_salt.len(), 32);
    assert_ne!(token.password_salt[..16], profile.id[..]);
    assert_eq!(token.ump_version, Some(3));
    let kdf = token.password_kdf.clone().unwrap();
    assert_eq!(
        kdf,
        PasswordKdf::argon2id(),
        "the reference's default (#593)"
    );
    assert_eq!(kdf.algorithm, KdfAlgorithm::Argon2id);
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

/// The v1 record's pad as the key file holds it.
fn v1_pad(file: &[u8]) -> Option<Vec<u8>> {
    let (file, _) = only_token(file);
    let v1 = file.profiles[0].v1.as_ref();
    v1.map(|v| v.primary_pad.to_vec())
}

/// (w1) A typo at the migrating unlock, then the right password: the token on
/// disk is re-sealed under the right one beside the intact v1 record; the
/// right password again opens the token and the file holds it alone.
#[tokio::test]
async fn a_typo_at_the_migrating_unlock_is_resealed_on_disk_by_the_right_password() {
    let (id, key, v1, password) = v1_parts();
    let scratch = scratch_file("typo.keys", &v1);
    let path = scratch.0.as_path();
    let (m, seen) = opened(path).await;
    m.switch_profile(&id, "a typo").await.unwrap();
    assert_ne!(seen.lock().unwrap()[0], key, "v1 has no password check");
    let typo = std::fs::read(path).unwrap();
    let pad = v1_pad(&typo).expect("the v1 record is intact");
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
    let resealed = std::fs::read(path).unwrap();
    assert_eq!(
        *token_key(&resealed, &password).unwrap(),
        key,
        "the token on disk carries the right root"
    );
    assert!(token_key(&resealed, "a typo").is_err());
    assert_eq!(v1_pad(&resealed), Some(pad), "the v1 record is intact");

    m.switch_profile(&id, &password).await.unwrap();
    assert_eq!(seen.lock().unwrap()[2], key);
    let alone = std::fs::read(path).unwrap();
    assert_eq!(v1_pad(&alone), None, "the token opened, so v1 left");
    assert!(!String::from_utf8_lossy(&alone).contains("\"v1\""));
    let (_, token) = only_token(&alone);
    assert_eq!(token.password_salt.len(), 32);
    assert_ne!(token.password_salt[..16], id[..]);
    let (fresh, seen_fresh) = opened(path).await;
    fresh.switch_profile(&id, &password).await.unwrap();
    assert_eq!(seen_fresh.lock().unwrap()[0], key);
}

/// (w2) A wrong password while the v1 record is present re-seals the token on
/// disk under it, the v1 record's bytes and the rest of the file unchanged;
/// the right password then opens through v1 and re-seals.
#[tokio::test]
async fn a_wrong_password_beside_v1_reseals_the_token_and_changes_nothing_else() {
    let (id, key, v1, password) = v1_parts();
    let scratch = scratch_file("second.keys", &v1);
    let path = scratch.0.as_path();
    let (m, _) = opened(path).await;
    m.switch_profile(&id, &password).await.unwrap();
    let both = std::fs::read(path).unwrap();
    let (m, seen) = opened(path).await;
    m.switch_profile(&id, "not the password").await.unwrap();
    let wrong = std::fs::read(path).unwrap();
    assert!(
        token_key(&wrong, "not the password").is_ok(),
        "the token on disk is sealed under the wrong password"
    );
    assert!(token_key(&wrong, &password).is_err());
    let (before, _) = only_token(&both);
    let (after, _) = only_token(&wrong);
    let (b, a) = (&before.profiles[0], &after.profiles[0]);
    assert_eq!(
        a.v1.as_ref().unwrap().primary_pad,
        b.v1.as_ref().unwrap().primary_pad,
        "the v1 record's bytes"
    );
    assert_eq!(
        (&a.profile.name, &a.profile.id, &a.profile.privileged_pad),
        (&b.profile.name, &b.profile.id, &b.profile.privileged_pad)
    );
    assert_eq!(
        (a.profile.created_at, a.profile.updated_at),
        (b.profile.created_at, b.profile.updated_at)
    );
    assert_eq!(after.default_profile_id, before.default_profile_id);
    assert_eq!(after.profiles.len(), 1);

    m.switch_profile(&id, &password).await.unwrap();
    assert_eq!(
        seen.lock().unwrap()[1],
        key,
        "the right one opens through v1"
    );
    let right = std::fs::read(path).unwrap();
    assert_eq!(*token_key(&right, &password).unwrap(), key, "and re-seals");
    assert_eq!(v1_pad(&right), v1_pad(&both));
}

/// (w3) The loss condition, as it is (design 4.3): the same wrong password at
/// two consecutive unlocks seals the token under it, then opens that token,
/// which drops the v1 record; the right password is refused afterwards. At
/// the migrating unlock and after a right one alike.
#[tokio::test]
async fn the_loss_condition_the_same_wrong_password_twice_drops_v1_and_the_right_one_is_refused() {
    let (id, key, v1, password) = v1_parts();
    for right_first in [false, true] {
        let scratch = scratch_file("loss.keys", &v1);
        let path = scratch.0.as_path();
        let (m, seen) = opened(path).await;
        if right_first {
            m.switch_profile(&id, &password).await.unwrap();
        }
        m.switch_profile(&id, "same wrong").await.unwrap();
        assert!(v1_pad(&std::fs::read(path).unwrap()).is_some(), "v1 kept");
        m.switch_profile(&id, "same wrong").await.unwrap();
        let lost = std::fs::read(path).unwrap();
        assert_eq!(v1_pad(&lost), None, "the wrong token opened, so v1 left");
        assert!(token_key(&lost, "same wrong").is_ok());
        assert!(seen
            .lock()
            .unwrap()
            .iter()
            .skip(usize::from(right_first))
            .all(|k| *k != key));
        let refused = m.switch_profile(&id, &password).await;
        assert!(matches!(refused, Err(Error::AccessDenied(_))));
        let (n, _) = opened(path).await;
        let refused = n.switch_profile(&id, &password).await;
        assert!(
            matches!(refused, Err(Error::AccessDenied(_))),
            "and after a restart"
        );
    }
}

/// (w4) Two manager instances over one key file, each opening it before its
/// unlock, as two processes in turn: typo, right, right completes the
/// migration and the recovered primary key is the v1 primary.
#[tokio::test]
async fn typo_right_right_across_two_instances_completes_the_migration() {
    let (id, key, v1, password) = v1_parts();
    let scratch = scratch_file("two.keys", &v1);
    let path = scratch.0.as_path();
    let ((a, seen_a), (b, seen_b)) = (manager(), manager());
    for m in [&a, &b] {
        m.provide_privileged_key_manager(PrivateKey::random()).await;
    }
    for (m, pw) in [
        (&a, "a typo"),
        (&b, password.as_str()),
        (&a, password.as_str()),
    ] {
        m.open_key_file(path).await.unwrap();
        m.switch_profile(&id, pw).await.unwrap();
    }
    assert_eq!(seen_b.lock().unwrap()[0], key, "b opened through v1");
    assert_eq!(seen_a.lock().unwrap()[1], key, "a opened b's token");
    let done = std::fs::read(path).unwrap();
    assert_eq!(v1_pad(&done), None, "the token alone");
    assert_eq!(*token_key(&done, &password).unwrap(), key, "the v1 primary");
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
    let kdf = token.password_kdf.as_ref();
    assert_eq!(
        kdf,
        Some(&PasswordKdf::argon2id()),
        "the migration writes argon2id"
    );
    let at_salt = ump_token::derive_password_key(&token.password_salt, kdf, &password).unwrap();
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
    let (_, next) = CWIStyleWalletManager::unlock_entry(&m.config, &read, &password).unwrap();
    let next = next.unwrap();
    m.delete_profile(&id).await.unwrap();
    let refused = m.commit_entry(&read, next.clone()).await;
    assert!(matches!(refused, Err(Error::InvalidOperation(_))));
    assert!(
        m.get_profiles().await.is_empty(),
        "the deleted profile stays deleted"
    );
    assert_eq!(std::fs::read(path).unwrap(), v1, "nothing written");

    m.load_snapshot(&v1).await.unwrap();
    m.commit_entry(&read, next).await.unwrap();
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

/// A planted sibling of the write's shape that is a symlink to a file outside
/// the directory is removed as a link: the target is byte-identical.
#[cfg(unix)]
#[tokio::test]
async fn a_symlink_sibling_is_removed_as_a_link_and_its_target_untouched() {
    let (_, _, v1, _) = v1_parts();
    let scratch = scratch_file("l.keys", &v1);
    let outside = scratch_file("outside", b"not the manager's to zero");
    let link = scratch.0.with_file_name("l.keys.tmp-0123456789abcdef");
    std::os::unix::fs::symlink(&outside.0, &link).unwrap();
    let (m, _) = manager();
    m.open_key_file(&scratch.0).await.unwrap();
    assert_eq!(m.get_profiles().await.len(), 1, "the open proceeds");
    assert_eq!(
        std::fs::read(&outside.0).unwrap(),
        b"not the manager's to zero"
    );
    assert!(
        std::fs::symlink_metadata(&link).is_err(),
        "the link is gone"
    );
}

/// A planted sibling of the write's shape that is a hard link to a file outside
/// the directory (a regular file sharing the target's inode) is unlinked by
/// name and never written: the target is byte-identical.
#[cfg(unix)]
#[tokio::test]
async fn a_hard_link_sibling_is_unlinked_and_its_target_untouched() {
    let (_, _, v1, _) = v1_parts();
    let scratch = scratch_file("h.keys", &v1);
    let outside = scratch_file("outside", b"not the manager's to zero");
    let link = scratch.0.with_file_name("h.keys.tmp-fedcba9876543210");
    std::fs::hard_link(&outside.0, &link).unwrap();
    let (m, _) = manager();
    m.open_key_file(&scratch.0).await.unwrap();
    assert_eq!(m.get_profiles().await.len(), 1, "the open proceeds");
    assert_eq!(
        std::fs::read(&outside.0).unwrap(),
        b"not the manager's to zero"
    );
    assert!(!link.exists(), "the sibling's name is gone");
}

type SiblingHook = Box<dyn FnOnce(&Path)>;

// Per thread, so a witness links its own sibling while other tests open key files.
thread_local! {
    static AFTER_OPEN: std::cell::Cell<Option<SiblingHook>> = const { std::cell::Cell::new(None) };
}

/// The cleanup's seam (#616): this thread's hook, once, between a sibling's open and fstat.
pub(super) fn after_sibling_open(sibling: &Path) {
    AFTER_OPEN.with(|hook| hook.take().map(|hook| hook(sibling)));
}

/// A sibling of one link at the scan's read, hard-linked to an outside file
/// after the open and before the fstat, is unlinked unzeroed: the outside
/// file is byte-identical (#616; the second link-count check of #609).
#[cfg(unix)]
#[tokio::test]
async fn a_link_made_after_the_sibling_open_is_unlinked_unzeroed() {
    let (_, _, v1, _) = v1_parts();
    let (scratch, outside) = (scratch_file("o.keys", &v1), scratch_file("outside", b""));
    std::fs::remove_file(&outside.0).unwrap();
    let stale = scratch.0.with_file_name("o.keys.tmp-0123456789abcdef");
    std::fs::write(&stale, b"not the manager's to zero").unwrap();
    let target = outside.0.clone();
    let link: SiblingHook = Box::new(move |s| std::fs::hard_link(s, &target).unwrap());
    AFTER_OPEN.with(|hook| hook.set(Some(link)));
    let (m, _) = manager();
    m.open_key_file(&scratch.0).await.unwrap();
    assert_eq!(
        std::fs::read(&outside.0).unwrap(),
        b"not the manager's to zero"
    );
    assert!(!stale.exists(), "the sibling's name is gone");
}

/// A planted sibling of the write's shape the process cannot open for write
/// (mode 0400, as another uid's leftover would be) is unlinked unzeroed and
/// the open proceeds (#609).
#[cfg(unix)]
#[tokio::test]
async fn a_sibling_the_process_cannot_open_is_unlinked_and_the_open_proceeds() {
    use std::os::unix::fs::PermissionsExt;
    let (_, _, v1, _) = v1_parts();
    let scratch = scratch_file("r.keys", &v1);
    let stale = scratch.0.with_file_name("r.keys.tmp-0123456789abcdef");
    std::fs::write(&stale, b"key material").unwrap();
    std::fs::set_permissions(&stale, std::fs::Permissions::from_mode(0o400)).unwrap();
    assert!(
        std::fs::OpenOptions::new()
            .write(true)
            .open(&stale)
            .is_err(),
        "the plant: the sibling refuses a write open"
    );
    let (m, _) = manager();
    m.open_key_file(&scratch.0).await.unwrap();
    assert_eq!(m.get_profiles().await.len(), 1, "the profiles load");
    assert!(!stale.exists(), "the sibling's name is gone");
}

/// A key file whose directory is absent is no profiles, as an absent file is;
/// the stale-sibling scan is skipped, and a save then fails at the write.
#[tokio::test]
async fn a_key_file_in_an_absent_directory_is_no_profiles() {
    let scratch = scratch_file("x", b"");
    let path = scratch.0.with_file_name("absent").join("wallet.keys");
    let (m, _) = manager();
    m.open_key_file(&path).await.unwrap();
    assert!(m.get_profiles().await.is_empty());
    assert!(!path.parent().unwrap().exists(), "nothing is created");
    assert!(
        m.save_key_file().await.is_err(),
        "the write needs the directory"
    );
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
    let pbkdf2 = include_str!("../../test_vectors/keys/ump-token-reference.json");
    a_reference_token_opens_through_switch_profile(pbkdf2).await;
}

#[tokio::test]
async fn a_reference_token_at_its_default_kdf_opens_through_switch_profile() {
    let argon2id = include_str!("../../test_vectors/keys/ump-token-reference-argon2id.json");
    a_reference_token_opens_through_switch_profile(argon2id).await;
}

async fn a_reference_token_opens_through_switch_profile(vector: &str) {
    use base64::Engine;
    let v: serde_json::Value = serde_json::from_str(vector).unwrap();
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
    // (w5) Nor does (ii) touch it: wrong, wrong, right, right in another instance.
    let (n, _) = opened(&scratch.0).await;
    for pw in ["not the password", "not the password", password, password] {
        let _ = n.switch_profile(&profile.id, pw).await;
    }
    assert_eq!(
        std::fs::read(&scratch.0).unwrap(),
        saved,
        "untouched by (ii)"
    );
    let (_, token) = only_token(&saved);
    assert_eq!(
        token.to_bytes(),
        hex::decode(v["tokenV3Hex"].as_str().unwrap()).unwrap()
    );
}

/// Runs `task` beside a 20 ms sleep on the runtime's one worker: an inline
/// argon2id holds that worker until the task ends, so the sleep wakes to a
/// finished task; on the blocking pool the sleep wakes while it derives.
async fn alongside_a_timer<T: Send + 'static>(
    task: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let task = tokio::spawn(task);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        !task.is_finished(),
        "the KDF held the runtime's only worker (#624)"
    );
    task.await.unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn a_password_kdf_leaves_a_current_thread_runtime_free_for_other_tasks() {
    let (m, seen) = manager();
    m.provide_privileged_key_manager(PrivateKey::random()).await;
    let m = Arc::new(m);
    let (a, b, c) = (m.clone(), m.clone(), m.clone());
    let profile = alongside_a_timer(async move { a.create_profile("Work", "pw").await })
        .await
        .unwrap();
    let id = profile.id.clone();
    alongside_a_timer(async move { b.switch_profile(&id, "pw").await })
        .await
        .unwrap();
    assert_eq!(seen.lock().unwrap().len(), 1, "derive, then build");
    assert!(m.is_authenticated().await);
    let key = alongside_a_timer(async move { c.recovery_key(&profile.id, "pw").await })
        .await
        .unwrap();
    assert_eq!(key.len(), 32);
}
