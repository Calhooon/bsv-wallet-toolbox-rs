//! #488 surface 6 (bsv-rs #379): the wallet's createHmac and verifyHmac go
//! through bsv-rs's `ProtoWallet` (`proto_create_hmac`, `proto_verify_hmac`),
//! so at the beta.4 pin the wallet issues the amended BRC-56 minimal-length
//! key's tag and verifies both forms, the 32-byte-key tag as the legacy form.
//! At beta.3 it issued the 32-byte-key tag. The rows are bsv-rs's
//! `tests/vectors/wallet_hmac_key_379.json` at v0.9.0-beta.4 (recorded from
//! ts-stack PR 1423's test table): the two keys with a leading zero octet.

use super::*;
use crate::services::mock::MockWalletServices;
use crate::storage::WalletStorageWriter;
use crate::StorageSqlx;
use bsv_rs::primitives::{from_hex, to_hex};

const ROOT_KEY: &str = "f0efba88c117ca2614554bb676d743f413822a31e3c8d70cfcc5fff119d20a07";
const COUNTERPARTY: &str = "03458d551018414b8fe1adbcef2d0afc17e1074bfab3fdde3e1b0750bdde58a9a7";
/// "BRC-56 HMAC test"
const DATA: &str = "4252432d353620484d41432074657374";
const ORIGINATOR: &str = "hmac.test";

/// (counterparty, keyID, the minimal-length key's tag, the 32-byte key's tag)
const ROWS: [(Option<&str>, &str, &str, &str); 2] = [
    (
        None,
        "k95",
        "3a968ebaa25eaa4b1cfad138f96105bb1eb738defc066d01e37b32595900d48b",
        "054a0fc6a3b0e21b31dc627fb1d645f2eea4127dff71eac95b1f71b7cb13c787",
    ),
    (
        Some(COUNTERPARTY),
        "k497",
        "3a5ea2bf5bcc9bc1461041737384bf258b0a3e5a475fb281a19c2972cdb1cff6",
        "aa39467c48a116ffb00fd7b56ff158bd9af37d1298df2f031c0125458c30f574",
    ),
];

async fn wallet() -> Wallet<StorageSqlx, MockWalletServices> {
    let root = PrivateKey::from_hex(ROOT_KEY).unwrap();
    let identity = root.public_key().to_hex();
    let storage = StorageSqlx::in_memory().await.unwrap();
    storage.migrate("hmac-key-tests", &identity).await.unwrap();
    storage.make_available().await.unwrap();
    let services = MockWalletServices::new();
    Wallet::new(Some(root), storage, services).await.unwrap()
}

fn counterparty(row: Option<&str>) -> Counterparty {
    match row {
        None => Counterparty::Self_,
        Some(key) => Counterparty::Other(PublicKey::from_hex(key).unwrap()),
    }
}

fn protocol() -> Protocol {
    Protocol::new(SecurityLevel::Counterparty, "hmac key vectors")
}

#[tokio::test]
async fn the_wallet_issues_the_minimal_length_key_tag() {
    let wallet = wallet().await;
    for (party, key_id, minimal, legacy) in ROWS {
        let args = CreateHmacArgs {
            protocol_id: protocol(),
            key_id: key_id.into(),
            counterparty: Some(counterparty(party)),
            data: from_hex(DATA).unwrap(),
            ..Default::default()
        };
        let result = wallet.create_hmac(args, Some(ORIGINATOR)).await.unwrap();
        let tag = to_hex(&result.hmac);
        assert_eq!(tag, minimal, "{key_id}");
        assert_ne!(tag, legacy, "{key_id}");
    }
}

#[tokio::test]
async fn the_wallet_verifies_both_tags() {
    let wallet = wallet().await;
    for (party, key_id, minimal, legacy) in ROWS {
        for tag in [minimal, legacy] {
            let args = VerifyHmacArgs {
                hmac: from_hex(tag).unwrap().try_into().unwrap(),
                data: from_hex(DATA).unwrap(),
                protocol_id: protocol(),
                key_id: key_id.into(),
                counterparty: Some(counterparty(party)),
                ..Default::default()
            };
            let verdict = wallet.verify_hmac(args, Some(ORIGINATOR)).await.unwrap();
            assert!(verdict.valid, "{key_id} {tag}");
        }
    }
}
