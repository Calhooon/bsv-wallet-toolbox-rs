use super::{RandomSource, WalletOptions};

#[test]
fn wallet_seed_defaults_to_os_entropy_and_options_remain_cloneable() {
    let options = WalletOptions::default();
    assert_eq!(options.clone().rng_seed, None);
    assert!(format!("{options:?}").contains("rng_seed"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn wallet_constructors_keep_the_seed_on_a_separate_stream() {
    use super::Wallet;
    use crate::services::{mock::MockWalletServices, Chain};
    use crate::storage::WalletStorageWriter;

    let mut wallets = Vec::new();
    for with_chain in [false, true] {
        let storage = crate::StorageSqlx::in_memory().await.unwrap();
        storage.migrate("rng-test", "fixture").await.unwrap();
        storage.make_available().await.unwrap();
        let options = WalletOptions {
            rng_seed: Some([42; 32]),
            ..WalletOptions::default()
        };
        let services = MockWalletServices::new();
        let wallet = if with_chain {
            Wallet::with_chain(None, storage, services, options, Chain::Test).await
        } else {
            Wallet::with_options(None, storage, services, options).await
        }
        .unwrap();
        assert_eq!(wallet.options().rng_seed, Some([42; 32]));
        wallets.push(wallet);
    }
    let mut a = [0; 32];
    let mut b = [0; 32];
    wallets[0]._rng.fill_bytes(&mut a);
    wallets[1]._rng.fill_bytes(&mut b);
    assert_eq!(a, b);
}

#[test]
fn seeded_sources_repeat_the_stream_and_advance() {
    let left = RandomSource::new(Some([42; 32]));
    let right = RandomSource::new(Some([42; 32]));
    let mut previous = [0; 64];
    for index in 0..3 {
        let mut a = [0; 64];
        let mut b = [0; 64];
        left.fill_bytes(&mut a);
        right.fill_bytes(&mut b);
        assert_eq!(a, b);
        if index > 0 {
            assert_ne!(a, previous);
        }
        previous = a;
    }
}

#[test]
fn distinct_seeds_and_os_entropy_give_distinct_streams() {
    let sources = [
        RandomSource::new(Some([1; 32])),
        RandomSource::new(Some([2; 32])),
        RandomSource::new(None),
        RandomSource::new(None),
    ];
    let mut outputs = [[0; 32]; 4];
    for (source, output) in sources.iter().zip(&mut outputs) {
        source.fill_bytes(output);
    }
    for (index, output) in outputs.iter().enumerate() {
        assert!(!outputs[..index].contains(output));
    }
}
