//! #430: relinquishCertificate requires one stored certificate at the wallet door.

#![cfg(feature = "sqlite")]

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{RelinquishCertificateArgs, WalletInterface};
use bsv_rs::{Error, WalletRefusalClass};
use bsv_wallet_toolbox_rs::services::mock::MockWalletServices;
use bsv_wallet_toolbox_rs::storage::entities::TableCertificate;
use bsv_wallet_toolbox_rs::storage::AuthId;
use bsv_wallet_toolbox_rs::{StorageSqlx, Wallet, WalletStorageWriter};

const CERT_TYPE: &str = "dHlwZQ==";
const SERIAL: &str = "c2VyaWFs";
const REFUSAL: &str = "Result must exist and be unique.";

struct Fixture {
    wallet: Wallet<StorageSqlx, MockWalletServices>,
    storage: StorageSqlx,
    auth: AuthId,
}

impl Fixture {
    async fn new() -> Self {
        let storage = StorageSqlx::in_memory().await.unwrap();
        storage
            .migrate("relinquish-certificate", &"0".repeat(64))
            .await
            .unwrap();
        storage.make_available().await.unwrap();
        let key = PrivateKey::from_hex(&format!("{:064x}", 37)).unwrap();
        let identity = key.public_key().to_hex();
        let (user, _) = storage.find_or_insert_user(&identity).await.unwrap();
        let auth = AuthId::with_user_id(&identity, user.user_id);
        let wallet = Wallet::new(Some(key), storage.clone(), MockWalletServices::new())
            .await
            .unwrap();
        Self {
            wallet,
            storage,
            auth,
        }
    }

    fn args(&self) -> RelinquishCertificateArgs {
        RelinquishCertificateArgs {
            certificate_type: CERT_TYPE.into(),
            certifier: self.auth.identity_key.clone(),
            serial_number: SERIAL.into(),
        }
    }

    fn certificate(&self) -> TableCertificate {
        TableCertificate {
            certificate_id: 0,
            user_id: self.auth.user_id.unwrap(),
            cert_type: CERT_TYPE.into(),
            serial_number: SERIAL.into(),
            certifier: self.auth.identity_key.clone(),
            subject: self.auth.identity_key.clone(),
            verifier: None,
            revocation_outpoint: format!("{}.0", "0".repeat(64)),
            signature: "3044".into(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    async fn rows(&self) -> Vec<(i64, i64, String, String, String, i64, String)> {
        sqlx::query_as(
            "SELECT certificate_id, user_id, type, certifier, serial_number, is_deleted, \
             updated_at FROM certificates ORDER BY certificate_id",
        )
        .fetch_all(self.storage.pool())
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn relinquish_certificate_unknown_serial_is_bad_request_and_writes_nothing() {
    let fixture = Fixture::new().await;
    fixture
        .storage
        .insert_certificate(&fixture.auth, fixture.certificate())
        .await
        .unwrap();
    let before = fixture.rows().await;
    let mut args = fixture.args();
    args.serial_number = "dW5rbm93bg==".into();

    let error = WalletInterface::relinquish_certificate(&fixture.wallet, args, None)
        .await
        .expect_err("an unknown serial must be refused");

    assert!(matches!(
        &error,
        Error::WalletRefusal {
            class: WalletRefusalClass::BadRequest,
            message,
        } if message == REFUSAL
    ));
    assert_eq!(error.to_string(), REFUSAL);
    assert_eq!(fixture.rows().await, before);
}

#[tokio::test]
async fn relinquish_certificate_known_serial_only_deletes_the_matching_tuple() {
    let fixture = Fixture::new().await;
    let target = fixture
        .storage
        .insert_certificate(&fixture.auth, fixture.certificate())
        .await
        .unwrap();
    for field in ["type", "certifier", "serial", "user"] {
        let mut certificate = fixture.certificate();
        let mut auth = fixture.auth.clone();
        match field {
            "type" => certificate.cert_type = "b3RoZXI=".into(),
            "certifier" => certificate.certifier = "other-certifier".into(),
            "serial" => certificate.serial_number = "b3RoZXI=".into(),
            "user" => {
                let (user, _) = fixture
                    .storage
                    .find_or_insert_user("other-user")
                    .await
                    .unwrap();
                auth = AuthId::with_user_id("other-user", user.user_id);
                certificate.user_id = user.user_id;
            }
            _ => unreachable!(),
        }
        fixture
            .storage
            .insert_certificate(&auth, certificate)
            .await
            .unwrap();
    }
    let mut expected = fixture.rows().await;
    let result = WalletInterface::relinquish_certificate(&fixture.wallet, fixture.args(), None)
        .await
        .unwrap();
    assert!(result.relinquished);
    let actual = fixture.rows().await;
    let changed = actual.iter().find(|row| row.0 == target).unwrap();
    assert_eq!(changed.5, 1);
    let expected_target = expected.iter_mut().find(|row| row.0 == target).unwrap();
    expected_target.5 = 1;
    expected_target.6.clone_from(&changed.6);
    assert_eq!(actual, expected);

    // The reference's find includes deleted rows, so repeating the call still succeeds.
    assert!(
        WalletInterface::relinquish_certificate(&fixture.wallet, fixture.args(), None)
            .await
            .unwrap()
            .relinquished
    );
}

#[tokio::test]
async fn relinquish_certificate_duplicate_tuple_is_unreachable_with_the_schema() {
    let fixture = Fixture::new().await;
    fixture
        .storage
        .insert_certificate(&fixture.auth, fixture.certificate())
        .await
        .unwrap();
    let before = fixture.rows().await;
    let error = fixture
        .storage
        .insert_certificate(&fixture.auth, fixture.certificate())
        .await
        .unwrap_err();
    // migrations/001_initial.sql:72 has UNIQUE(user_id, type, certifier, serial_number).
    assert!(
        error.to_string().contains("UNIQUE constraint failed"),
        "{error}"
    );
    assert_eq!(fixture.rows().await, before);
}
