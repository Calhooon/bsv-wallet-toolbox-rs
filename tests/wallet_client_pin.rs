//! #419 item 3: shared WalletClient calls and the WalletInterface adapter.

use std::sync::Arc;

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{GetPublicKeyArgs, ProtoWallet, WalletClient, WalletInterface};

#[tokio::test]
async fn wallet_clients_are_shared_wallet_interfaces() {
    let key = PrivateKey::from_hex(&format!("{:064x}", 42)).unwrap();
    let expected = key.public_key().to_hex();
    let client = Arc::new(WalletClient::with_substrate(
        Arc::new(ProtoWallet::new(Some(key))),
        None,
    ));
    let args = GetPublicKeyArgs {
        identity_key: true,
        ..Default::default()
    };
    let clients = [Arc::clone(&client), Arc::clone(&client)];
    let calls = clients
        .iter()
        .map(|client| client.get_public_key(args.clone()));
    for result in futures::future::join_all(calls).await {
        assert_eq!(result.unwrap().public_key, expected);
    }
    let shared: Arc<dyn WalletInterface> = client;
    assert_eq!(
        shared.get_public_key(args, None).await.unwrap().public_key,
        expected
    );
}
