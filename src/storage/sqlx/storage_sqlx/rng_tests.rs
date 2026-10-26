use super::StorageSqlx;
use crate::storage::sqlx::StorageSqlxOptions;

const SEED: [u8; 32] = [42; 32];

fn draw(storage: &StorageSqlx) -> [u8; 32] {
    let mut bytes = [0; 32];
    storage._rng.fill_bytes(&mut bytes);
    bytes
}

async fn seeded_storage() -> StorageSqlx {
    StorageSqlx::new_with_options(
        "sqlite::memory:",
        StorageSqlxOptions {
            rng_seed: Some(SEED),
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn seeded_storage_clones_advance_one_shared_stream() {
    let storage = seeded_storage().await;
    let cloned = storage.clone();
    let reference = seeded_storage().await;
    assert_eq!(draw(&storage), draw(&reference));
    assert_eq!(draw(&cloned), draw(&reference));
    assert_eq!(draw(&storage), draw(&reference));
}

#[tokio::test]
async fn seeded_open_and_new_options_start_the_same_stream() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("seeded.sqlite");
    let opened = StorageSqlx::open_with_options(
        path.to_str().unwrap(),
        StorageSqlxOptions {
            rng_seed: Some(SEED),
        },
    )
    .await
    .unwrap();
    let memory = seeded_storage().await;
    assert_eq!(draw(&opened), draw(&memory));
}

#[tokio::test]
async fn default_storage_options_use_independent_entropy() {
    assert_eq!(StorageSqlxOptions::default().rng_seed, None);
    let left = StorageSqlx::in_memory().await.unwrap();
    let right = StorageSqlx::new_with_options("sqlite::memory:", StorageSqlxOptions::default())
        .await
        .unwrap();
    assert_ne!(draw(&left), draw(&right));
}
