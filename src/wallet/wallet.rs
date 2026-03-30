//! Wallet Implementation
//!
//! This module provides the main `Wallet` struct that implements the full
//! `WalletInterface` trait from `bsv_rs::wallet`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::primitives::PublicKey;
use bsv_rs::wallet::KeyDeriverApi;
use bsv_rs::wallet::{
    interface::{
        RevealCounterpartyKeyLinkageArgs as InterfaceRevealCounterpartyArgs,
        RevealSpecificKeyLinkageArgs as InterfaceRevealSpecificArgs,
    },
    AbortActionArgs, AbortActionResult, AcquireCertificateArgs, AuthenticatedResult, Counterparty,
    CreateActionArgs, CreateActionResult, CreateHmacArgs, CreateHmacResult, CreateSignatureArgs,
    CreateSignatureResult, DecryptArgs, DecryptResult, DiscoverByAttributesArgs,
    DiscoverByIdentityKeyArgs, DiscoverCertificatesResult, EncryptArgs, EncryptResult,
    GetHeaderArgs, GetHeaderResult, GetHeightResult, GetNetworkResult, GetPublicKeyArgs,
    GetPublicKeyResult, GetVersionResult, InternalizeActionArgs, InternalizeActionResult,
    KeyLinkageResult, ListActionsArgs, ListActionsResult, ListCertificatesArgs,
    ListCertificatesResult, ListOutputsArgs, ListOutputsResult, Network, Outpoint, ProtoWallet,
    Protocol, ProveCertificateArgs, ProveCertificateResult, RelinquishCertificateArgs,
    RelinquishCertificateResult, RelinquishOutputArgs, RelinquishOutputResult,
    RevealCounterpartyKeyLinkageResult, RevealSpecificKeyLinkageResult, SecurityLevel,
    SignActionArgs, SignActionResult, SignableTransaction, VerifyHmacArgs, VerifyHmacResult,
    VerifySignatureArgs, VerifySignatureResult, WalletCertificate, WalletInterface,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::services::{Chain, WalletServices};
use crate::storage::entities::{TableCertificate, TableCertificateField, TransactionStatus};
use crate::storage::sqlx::{classify_broadcast_results, BroadcastOutcome};
use crate::storage::{AuthId, FindOutputsArgs, StorageProcessActionArgs, WalletStorageProvider};
use crate::wallet::lookup::OverlayLookupResolver;

use super::signer::{SignerInput, WalletSigner};

// =============================================================================
// Constants
// =============================================================================

/// Wallet version string
const WALLET_VERSION: &str = "bsv-wallet-toolbox-0.1.0";

/// Default TTL for pending transactions (24 hours)
const PENDING_TRANSACTION_TTL_SECS: i64 = 24 * 60 * 60;

// =============================================================================
// PrivilegedKeyManager
// =============================================================================

/// Trait for privileged key management operations.
/// Used for two-factor authentication where crypto operations at SecurityLevel >= 2
/// are routed through a separate key manager.
#[async_trait]
pub trait PrivilegedKeyManager: Send + Sync {
    async fn get_public_key(
        &self,
        args: GetPublicKeyArgs,
        originator: &str,
    ) -> std::result::Result<GetPublicKeyResult, bsv_rs::Error>;
    async fn encrypt(
        &self,
        args: EncryptArgs,
        originator: &str,
    ) -> std::result::Result<EncryptResult, bsv_rs::Error>;
    async fn decrypt(
        &self,
        args: DecryptArgs,
        originator: &str,
    ) -> std::result::Result<DecryptResult, bsv_rs::Error>;
    async fn create_hmac(
        &self,
        args: CreateHmacArgs,
        originator: &str,
    ) -> std::result::Result<CreateHmacResult, bsv_rs::Error>;
    async fn verify_hmac(
        &self,
        args: VerifyHmacArgs,
        originator: &str,
    ) -> std::result::Result<VerifyHmacResult, bsv_rs::Error>;
    async fn create_signature(
        &self,
        args: CreateSignatureArgs,
        originator: &str,
    ) -> std::result::Result<CreateSignatureResult, bsv_rs::Error>;
    async fn verify_signature(
        &self,
        args: VerifySignatureArgs,
        originator: &str,
    ) -> std::result::Result<VerifySignatureResult, bsv_rs::Error>;
}

// =============================================================================
// WalletLogger
// =============================================================================

/// Wallet operation logger for debugging and diagnostics.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct WalletLogger {
    pub indent: u32,
    pub logs: Vec<WalletLoggerLog>,
    pub is_origin: bool,
    pub is_error: bool,
}

/// A single log entry.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WalletLoggerLog {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub level: String,
    pub message: String,
    pub indent: u32,
}

#[allow(dead_code)]
impl WalletLogger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn group(&mut self, name: &str) {
        self.log("info", name);
        self.indent += 1;
    }

    pub fn group_end(&mut self) {
        if self.indent > 0 {
            self.indent -= 1;
        }
    }

    pub fn log(&mut self, level: &str, message: &str) {
        self.logs.push(WalletLoggerLog {
            timestamp: chrono::Utc::now(),
            level: level.to_string(),
            message: message.to_string(),
            indent: self.indent,
        });
    }

    pub fn error(&mut self, message: &str) {
        self.is_error = true;
        self.log("error", message);
    }

    pub fn to_log_string(&self) -> String {
        self.logs
            .iter()
            .map(|l| {
                let indent_str = "  ".repeat(l.indent as usize);
                format!(
                    "[{}] {}{}: {}",
                    l.timestamp.format("%H:%M:%S"),
                    indent_str,
                    l.level,
                    l.message
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

// =============================================================================
// PendingTransaction
// =============================================================================

/// A transaction awaiting signature.
///
/// When `create_action` is called with `sign_and_process = false`, the unsigned
/// transaction is cached here for later signing via `sign_action`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingTransaction {
    /// Unique reference for this pending transaction
    pub reference: String,
    /// The unsigned transaction bytes
    pub raw_tx: Vec<u8>,
    /// Input metadata for signing
    pub inputs: Vec<SignerInput>,
    /// BEEF data for the inputs (needed for broadcasting)
    pub input_beef: Option<Vec<u8>>,
    /// The original create_action args (for options like no_send, delayed)
    pub is_no_send: bool,
    /// Whether delayed broadcast was requested
    pub is_delayed: bool,
    /// Send with txids
    pub send_with: Vec<String>,
    /// When this pending transaction was created
    pub created_at: DateTime<Utc>,
}

// =============================================================================
// Balance and UTXO Types
// =============================================================================

/// Summary of wallet balance and associated UTXOs.
///
/// Returned by [`Wallet::balance`] and [`Wallet::balance_and_utxos`].
/// Mirrors the TypeScript `WalletBalance` type from `@bsv/wallet-toolbox`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletBalance {
    /// Total satoshis across all spendable outputs in the default basket.
    pub total: u64,
    /// Individual UTXOs (only populated by `balance_and_utxos`, empty for `balance`).
    pub utxos: Vec<UtxoInfo>,
}

/// Information about a single spendable UTXO.
///
/// Mirrors the TypeScript `{ satoshis, outpoint }` shape from `WalletBalance.utxos`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UtxoInfo {
    /// Satoshi value of this output.
    pub satoshis: u64,
    /// Outpoint in `"txid.vout"` format.
    pub outpoint: String,
}

// =============================================================================
// WalletOptions
// =============================================================================

/// Configuration options for the Wallet.
#[derive(Debug, Clone)]
pub struct WalletOptions {
    /// If true, signable transactions will include source transaction for each input,
    /// including those that do not require signature and those that were also contained
    /// in the inputBEEF. Default: true
    pub include_all_source_transactions: bool,

    /// If true, txids that are known to the wallet's party beef do not need to be
    /// returned from storage. Default: false
    pub auto_known_txids: bool,

    /// Controls behavior of input BEEF validation. If "known", input transactions may
    /// omit supporting validity proof data for all TXIDs known to this wallet.
    /// Default: Some("known")
    pub trust_self: Option<String>,
}

impl Default for WalletOptions {
    fn default() -> Self {
        Self {
            include_all_source_transactions: true,
            auto_known_txids: false,
            trust_self: Some("known".to_string()),
        }
    }
}

// =============================================================================
// Wallet
// =============================================================================

/// A full wallet implementation combining storage, services, and cryptographic operations.
///
/// The `Wallet` struct implements the complete `WalletInterface` trait, providing:
/// - Transaction creation, signing, and broadcasting
/// - UTXO management and tracking
/// - Certificate storage and verification
/// - Cryptographic operations (signing, encryption, HMAC)
///
/// # Type Parameters
///
/// - `S`: Storage backend implementing `WalletStorageProvider`
/// - `V`: Services backend implementing `WalletServices`
///
/// # Example
///
/// ```rust,ignore
/// use bsv_wallet_toolbox_rs::{Wallet, StorageSqlx, Services};
/// use bsv_rs::primitives::PrivateKey;
///
/// let storage = StorageSqlx::open("wallet.db").await?;
/// let services = Services::mainnet();
/// let wallet = Wallet::new(Some(PrivateKey::random()), storage, services).await?;
/// ```
pub struct Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync,
    V: WalletServices + Send + Sync,
{
    /// ProtoWallet for cryptographic operations
    proto_wallet: ProtoWallet,

    /// Storage backend for persistent state
    storage: Arc<S>,

    /// Services backend for blockchain interaction
    services: Arc<V>,

    /// The wallet's identity public key (hex string)
    identity_key: String,

    /// Network (mainnet or testnet)
    chain: Chain,

    /// Configuration options
    options: WalletOptions,

    /// Wallet signer for transaction signing
    signer: WalletSigner,

    /// Cache for pending unsigned transactions awaiting signature
    pending_transactions: Arc<RwLock<HashMap<String, PendingTransaction>>>,

    /// The user's ID in the storage system (for AuthId)
    user_id: i64,

    /// Optional privileged key manager for SecurityLevel >= 2 operations.
    /// Used for two-factor authentication where crypto operations are routed
    /// through a separate key manager.
    privileged_key_manager: Option<Arc<dyn PrivilegedKeyManager>>,

    /// Optional permissions manager for BRC-98/99 permission enforcement.
    /// When set, wallet operations will check permissions before proceeding.
    permissions_manager: Option<Arc<crate::managers::WalletPermissionsManager>>,
}

impl<S, V> Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync + 'static,
    V: WalletServices + Send + Sync + 'static,
{
    /// Creates a new Wallet instance.
    ///
    /// # Arguments
    ///
    /// * `root_key` - The root private key for key derivation. If None, uses the "anyone" key.
    /// * `storage` - Storage backend implementing `WalletStorageProvider`.
    /// * `services` - Services backend implementing `WalletServices`.
    ///
    /// # Returns
    ///
    /// A new `Wallet` instance wrapped in `Result`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let wallet = Wallet::new(Some(root_key), storage, services).await?;
    /// ```
    pub async fn new(root_key: Option<PrivateKey>, storage: S, services: V) -> Result<Self> {
        Self::with_options(root_key, storage, services, WalletOptions::default()).await
    }

    /// Creates a new Wallet instance with custom options.
    ///
    /// # Arguments
    ///
    /// * `root_key` - The root private key for key derivation. If None, uses the "anyone" key.
    /// * `storage` - Storage backend implementing `WalletStorageProvider`.
    /// * `services` - Services backend implementing `WalletServices`.
    /// * `options` - Configuration options.
    ///
    /// # Returns
    ///
    /// A new `Wallet` instance wrapped in `Result`.
    pub async fn with_options(
        root_key: Option<PrivateKey>,
        storage: S,
        services: V,
        options: WalletOptions,
    ) -> Result<Self> {
        Self::with_chain(root_key, storage, services, options, Chain::Main).await
    }

    /// Creates a new Wallet instance with specified chain.
    ///
    /// # Arguments
    ///
    /// * `root_key` - The root private key for key derivation. If None, uses the "anyone" key.
    /// * `storage` - Storage backend implementing `WalletStorageProvider`.
    /// * `services` - Services backend implementing `WalletServices`.
    /// * `options` - Configuration options.
    /// * `chain` - The blockchain network (mainnet or testnet).
    ///
    /// # Returns
    ///
    /// A new `Wallet` instance wrapped in `Result`.
    pub async fn with_chain(
        root_key: Option<PrivateKey>,
        storage: S,
        services: V,
        options: WalletOptions,
        chain: Chain,
    ) -> Result<Self> {
        // Create the ProtoWallet for cryptographic operations
        let proto_wallet = ProtoWallet::new(root_key.clone());

        // Get the identity key
        let identity_key = proto_wallet.identity_key_hex();

        // Create the wallet signer
        let signer = WalletSigner::new(root_key);

        // Ensure storage is available
        if !storage.is_available() {
            return Err(Error::StorageNotAvailable);
        }

        // Ensure the user exists in storage and get user_id
        let (user, _is_new) = storage.find_or_insert_user(&identity_key).await?;

        Ok(Self {
            proto_wallet,
            storage: Arc::new(storage),
            services: Arc::new(services),
            identity_key,
            chain,
            options,
            signer,
            pending_transactions: Arc::new(RwLock::new(HashMap::new())),
            user_id: user.user_id,
            privileged_key_manager: None,
            permissions_manager: None,
        })
    }

    /// Returns the wallet's identity public key as a hex string.
    pub fn identity_key(&self) -> &str {
        &self.identity_key
    }

    /// Returns the network this wallet is configured for.
    pub fn chain(&self) -> Chain {
        self.chain
    }

    /// Returns a reference to the storage backend.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Returns a reference to the services backend.
    pub fn services(&self) -> &V {
        &self.services
    }

    /// Returns the wallet options.
    pub fn options(&self) -> &WalletOptions {
        &self.options
    }

    /// Creates an AuthId for the current user.
    fn auth(&self) -> AuthId {
        AuthId::with_user_id(&self.identity_key, self.user_id)
    }

    // =========================================================================
    // WS4.2 - Privileged Key Manager
    // =========================================================================

    /// Sets the privileged key manager for SecurityLevel >= 2 operations.
    ///
    /// When set, cryptographic operations at elevated security levels will be
    /// routed through this separate key manager for two-factor authentication.
    pub fn set_privileged_key_manager(&mut self, manager: Arc<dyn PrivilegedKeyManager>) {
        self.privileged_key_manager = Some(manager);
    }

    /// Returns a reference to the privileged key manager, if set.
    pub fn privileged_key_manager(&self) -> Option<&Arc<dyn PrivilegedKeyManager>> {
        self.privileged_key_manager.as_ref()
    }

    // =========================================================================
    // WS4.3 - Permissions Manager (BRC-98/99)
    // =========================================================================

    /// Sets the permissions manager for BRC-98/99 permission enforcement.
    ///
    /// When set, wallet operations will check permissions before proceeding.
    /// This enables protocol access control (DPACP), basket access control (DBAP),
    /// certificate access control (DCAP), and spending authorization (DSAP).
    pub fn set_permissions_manager(
        &mut self,
        manager: Arc<crate::managers::WalletPermissionsManager>,
    ) {
        self.permissions_manager = Some(manager);
    }

    /// Returns a reference to the permissions manager, if set.
    pub fn permissions_manager(&self) -> Option<&Arc<crate::managers::WalletPermissionsManager>> {
        self.permissions_manager.as_ref()
    }

    /// Checks protocol permission if a permissions manager is set.
    /// Returns Ok(()) if no manager is set or if permission is granted.
    async fn check_protocol_permission(
        &self,
        originator: &str,
        protocol: &Protocol,
        counterparty: Option<&str>,
        usage_type: crate::managers::PermissionUsageType,
    ) -> Result<()> {
        if let Some(ref pm) = self.permissions_manager {
            pm.ensure_protocol_permission(
                originator,
                false, // not privileged by default
                protocol,
                counterparty,
                None,
                usage_type,
            )
            .await?;
        }
        Ok(())
    }

    /// Checks basket access permission if a permissions manager is set.
    async fn check_basket_permission(
        &self,
        originator: &str,
        basket: &str,
        usage_type: crate::managers::BasketUsageType,
    ) -> Result<()> {
        if let Some(ref pm) = self.permissions_manager {
            pm.ensure_basket_access(originator, basket, None, usage_type)
                .await?;
        }
        Ok(())
    }

    /// Checks spending permission if a permissions manager is set.
    async fn check_spending_permission(&self, originator: &str, satoshis: u64) -> Result<()> {
        if let Some(ref pm) = self.permissions_manager {
            pm.ensure_spending_permission(originator, satoshis, None)
                .await?;
        }
        Ok(())
    }

    /// Checks certificate access permission if a permissions manager is set.
    async fn check_certificate_permission(
        &self,
        originator: &str,
        privileged: bool,
        verifier: &str,
        cert_type: &str,
        fields: &[String],
        usage_type: crate::managers::CertificateUsageType,
    ) -> Result<()> {
        if let Some(ref pm) = self.permissions_manager {
            pm.ensure_certificate_access(
                originator, privileged, verifier, cert_type, fields, None, usage_type,
            )
            .await?;
        }
        Ok(())
    }

    // =========================================================================
    // Balance and Sweep helpers (P3-07)
    // =========================================================================

    /// Returns the total spendable balance in satoshis from the default basket.
    pub async fn balance(&self) -> Result<WalletBalance> {
        let auth = self.auth();
        let mut total: u64 = 0;
        let mut offset = 0u32;
        let limit = 1000u32;

        loop {
            let args = ListOutputsArgs {
                basket: "default".to_string(),
                tags: None,
                tag_query_mode: None,
                include: None,
                include_custom_instructions: None,
                include_tags: None,
                include_labels: None,
                limit: Some(limit),
                offset: Some(offset as i32),
                seek_permission: None,
            };

            let result = self.storage.list_outputs(&auth, args).await?;

            for output in &result.outputs {
                if output.spendable {
                    total = total.saturating_add(output.satoshis);
                }
            }

            if result.outputs.len() < limit as usize {
                break;
            }
            offset += limit;
        }

        Ok(WalletBalance {
            total,
            utxos: Vec::new(),
        })
    }

    /// Returns the total spendable balance and individual UTXOs from the default basket.
    pub async fn balance_and_utxos(&self) -> Result<WalletBalance> {
        let auth = self.auth();
        let mut total: u64 = 0;
        let mut utxos = Vec::new();
        let mut offset = 0u32;
        let limit = 1000u32;

        loop {
            let args = ListOutputsArgs {
                basket: "default".to_string(),
                tags: None,
                tag_query_mode: None,
                include: None,
                include_custom_instructions: None,
                include_tags: None,
                include_labels: None,
                limit: Some(limit),
                offset: Some(offset as i32),
                seek_permission: None,
            };

            let result = self.storage.list_outputs(&auth, args).await?;

            for output in &result.outputs {
                if output.spendable {
                    total = total.saturating_add(output.satoshis);
                    utxos.push(UtxoInfo {
                        satoshis: output.satoshis,
                        outpoint: output.outpoint.to_string(),
                    });
                }
            }

            if result.outputs.len() < limit as usize {
                break;
            }
            offset += limit;
        }

        Ok(WalletBalance { total, utxos })
    }

    /// Sweeps all funds from this wallet to a target BSV address.
    pub async fn sweep_to_address(&self, address: &str) -> Result<CreateActionResult> {
        use bsv_rs::wallet::{CreateActionInput, CreateActionOutput};

        let balance_result = self.balance_and_utxos().await?;

        if balance_result.total == 0 {
            return Err(Error::InsufficientFunds {
                needed: 1,
                available: 0,
            });
        }

        // Build inputs from all UTXOs
        let mut inputs: Vec<CreateActionInput> = Vec::new();
        for utxo in &balance_result.utxos {
            let outpoint = Outpoint::from_string(&utxo.outpoint)
                .map_err(|e| Error::ValidationError(format!("Invalid outpoint: {}", e)))?;
            inputs.push(CreateActionInput {
                outpoint,
                input_description: "sweep".to_string(),
                unlocking_script: None,
                unlocking_script_length: Some(108), // P2PKH estimate
                sequence_number: None,
            });
        }

        let locking_script = address_to_p2pkh_script(address)?;

        let outputs = vec![CreateActionOutput {
            locking_script,
            satoshis: balance_result.total,
            output_description: format!("sweep to {}", address),
            basket: None,
            custom_instructions: None,
            tags: None,
        }];

        let args = CreateActionArgs {
            description: format!("Sweep {} satoshis to {}", balance_result.total, address),
            input_beef: None,
            inputs: Some(inputs),
            outputs: Some(outputs),
            lock_time: None,
            version: None,
            labels: None,
            options: None,
        };

        let result = WalletInterface::create_action(self, args, "sweep")
            .await
            .map_err(|e| Error::TransactionError(e.to_string()))?;

        Ok(result)
    }

    // =========================================================================
    // WS4.1 - list_failed_actions
    // =========================================================================

    /// Lists actions (transactions) with Failed status.
    ///
    /// Queries the storage for outputs associated with transactions that have
    /// a Failed status, then returns the unique txids of those transactions.
    ///
    /// # Returns
    ///
    /// A vector of txid strings for transactions with Failed status.
    pub async fn list_failed_actions(&self) -> Result<Vec<String>> {
        let auth = self.auth();
        let args = FindOutputsArgs {
            user_id: Some(self.user_id),
            tx_status: Some(vec![TransactionStatus::Failed]),
            ..Default::default()
        };
        let outputs = self.storage.find_outputs(&auth, args).await?;
        let txids: Vec<String> = outputs
            .into_iter()
            .map(|o| o.txid)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        Ok(txids)
    }

    // =========================================================================
    // WS4.3 - Helper Methods
    // =========================================================================

    /// Destroys the wallet's storage, deleting all persisted data.
    ///
    /// This delegates to the storage backend's `destroy()` method.
    /// After calling this, the wallet should not be used further.
    pub async fn destroy(&self) -> Result<()> {
        self.storage.destroy().await
    }

    /// Returns the wallet's identity key as an owned String.
    ///
    /// This is a convenience method that clones the identity key.
    /// For a borrowed reference, use `identity_key()`.
    pub fn get_identity_key(&self) -> String {
        self.identity_key.clone()
    }

    /// Returns a list of known transaction IDs from storage.
    ///
    /// Queries storage for outputs associated with transactions that have
    /// been broadcast or confirmed (Completed, Unproven, Sending statuses),
    /// and returns their unique txids.
    pub async fn get_known_txids(&self) -> Result<Vec<String>> {
        let auth = self.auth();
        let args = FindOutputsArgs {
            user_id: Some(self.user_id),
            tx_status: Some(vec![
                TransactionStatus::Completed,
                TransactionStatus::Unproven,
                TransactionStatus::Sending,
            ]),
            ..Default::default()
        };
        let outputs = self.storage.find_outputs(&auth, args).await?;
        let txids: Vec<String> = outputs
            .into_iter()
            .map(|o| o.txid)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        Ok(txids)
    }

    /// Returns the storage identity key and storage name from settings.
    ///
    /// # Returns
    ///
    /// A tuple of `(storage_identity_key, storage_name)`.
    pub fn get_storage_identity(&self) -> (String, String) {
        let settings = self.storage.get_settings();
        (
            settings.storage_identity_key.clone(),
            settings.storage_name.clone(),
        )
    }

    /// Lists actions (transactions) with NoSend status.
    ///
    /// Queries the storage for outputs associated with transactions that have
    /// a NoSend status, then returns the unique txids of those transactions.
    ///
    /// # Returns
    ///
    /// A vector of txid strings for transactions with NoSend status.
    pub async fn list_no_send_actions(&self) -> Result<Vec<String>> {
        let auth = self.auth();
        let args = FindOutputsArgs {
            user_id: Some(self.user_id),
            tx_status: Some(vec![TransactionStatus::NoSend]),
            ..Default::default()
        };
        let outputs = self.storage.find_outputs(&auth, args).await?;
        let txids: Vec<String> = outputs
            .into_iter()
            .map(|o| o.txid)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        Ok(txids)
    }

    /// Calls ProtoWallet.get_public_key.
    fn proto_get_public_key(
        &self,
        args: GetPublicKeyArgs,
    ) -> std::result::Result<GetPublicKeyResult, bsv_rs::Error> {
        // ProtoWallet uses the same types that are re-exported
        use bsv_rs::wallet::GetPublicKeyArgs as ProtoGetPublicKeyArgs;

        let proto_args = ProtoGetPublicKeyArgs {
            identity_key: args.identity_key,
            protocol_id: args.protocol_id,
            key_id: args.key_id,
            counterparty: args.counterparty,
            for_self: args.for_self,
        };

        let result = self.proto_wallet.get_public_key(proto_args)?;

        Ok(GetPublicKeyResult {
            public_key: result.public_key,
        })
    }

    /// Calls ProtoWallet.encrypt.
    fn proto_encrypt(
        &self,
        args: EncryptArgs,
    ) -> std::result::Result<EncryptResult, bsv_rs::Error> {
        use bsv_rs::wallet::EncryptArgs as ProtoEncryptArgs;

        let proto_args = ProtoEncryptArgs {
            plaintext: args.plaintext,
            protocol_id: args.protocol_id,
            key_id: args.key_id,
            counterparty: args.counterparty,
        };

        let result = self.proto_wallet.encrypt(proto_args)?;

        Ok(EncryptResult {
            ciphertext: result.ciphertext,
        })
    }

    /// Calls ProtoWallet.decrypt.
    fn proto_decrypt(
        &self,
        args: DecryptArgs,
    ) -> std::result::Result<DecryptResult, bsv_rs::Error> {
        use bsv_rs::wallet::DecryptArgs as ProtoDecryptArgs;

        let proto_args = ProtoDecryptArgs {
            ciphertext: args.ciphertext,
            protocol_id: args.protocol_id,
            key_id: args.key_id,
            counterparty: args.counterparty,
        };

        let result = self.proto_wallet.decrypt(proto_args)?;

        Ok(DecryptResult {
            plaintext: result.plaintext,
        })
    }

    /// Calls ProtoWallet.create_hmac.
    fn proto_create_hmac(
        &self,
        args: CreateHmacArgs,
    ) -> std::result::Result<CreateHmacResult, bsv_rs::Error> {
        use bsv_rs::wallet::CreateHmacArgs as ProtoCreateHmacArgs;

        let proto_args = ProtoCreateHmacArgs {
            data: args.data,
            protocol_id: args.protocol_id,
            key_id: args.key_id,
            counterparty: args.counterparty,
        };

        let result = self.proto_wallet.create_hmac(proto_args)?;

        Ok(CreateHmacResult { hmac: result.hmac })
    }

    /// Calls ProtoWallet.verify_hmac.
    fn proto_verify_hmac(
        &self,
        args: VerifyHmacArgs,
    ) -> std::result::Result<VerifyHmacResult, bsv_rs::Error> {
        use bsv_rs::wallet::VerifyHmacArgs as ProtoVerifyHmacArgs;

        let proto_args = ProtoVerifyHmacArgs {
            data: args.data,
            hmac: args.hmac,
            protocol_id: args.protocol_id,
            key_id: args.key_id,
            counterparty: args.counterparty,
        };

        let result = self.proto_wallet.verify_hmac(proto_args)?;

        Ok(VerifyHmacResult {
            valid: result.valid,
        })
    }

    /// Calls ProtoWallet.create_signature.
    fn proto_create_signature(
        &self,
        args: CreateSignatureArgs,
    ) -> std::result::Result<CreateSignatureResult, bsv_rs::Error> {
        use bsv_rs::wallet::CreateSignatureArgs as ProtoCreateSignatureArgs;

        let proto_args = ProtoCreateSignatureArgs {
            data: args.data,
            hash_to_directly_sign: args.hash_to_directly_sign,
            protocol_id: args.protocol_id,
            key_id: args.key_id,
            counterparty: args.counterparty,
        };

        let result = self.proto_wallet.create_signature(proto_args)?;

        Ok(CreateSignatureResult {
            signature: result.signature,
        })
    }

    /// Calls ProtoWallet.verify_signature.
    fn proto_verify_signature(
        &self,
        args: VerifySignatureArgs,
    ) -> std::result::Result<VerifySignatureResult, bsv_rs::Error> {
        use bsv_rs::wallet::VerifySignatureArgs as ProtoVerifySignatureArgs;

        let proto_args = ProtoVerifySignatureArgs {
            data: args.data,
            hash_to_directly_verify: args.hash_to_directly_verify,
            signature: args.signature,
            protocol_id: args.protocol_id,
            key_id: args.key_id,
            counterparty: args.counterparty,
            for_self: args.for_self,
        };

        let result = self.proto_wallet.verify_signature(proto_args)?;

        Ok(VerifySignatureResult {
            valid: result.valid,
        })
    }
}

// =============================================================================
// WalletInterface Implementation
// =============================================================================

#[async_trait]
impl<S, V> WalletInterface for Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync + 'static,
    V: WalletServices + Send + Sync + 'static,
{
    // =========================================================================
    // Key Operations (delegated to ProtoWallet)
    // =========================================================================

    async fn get_public_key(
        &self,
        args: GetPublicKeyArgs,
        originator: &str,
    ) -> bsv_rs::Result<GetPublicKeyResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        // BRC-98/99: Check protocol permission for public key revelation
        if let Some(ref protocol_id) = args.protocol_id {
            self.check_protocol_permission(
                originator,
                protocol_id,
                None,
                crate::managers::PermissionUsageType::PublicKey,
            )
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        }
        self.proto_get_public_key(args)
    }

    async fn encrypt(&self, args: EncryptArgs, originator: &str) -> bsv_rs::Result<EncryptResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        // BRC-98/99: Check protocol permission for encryption
        self.check_protocol_permission(
            originator,
            &args.protocol_id,
            None,
            crate::managers::PermissionUsageType::Encrypting,
        )
        .await
        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        self.proto_encrypt(args)
    }

    async fn decrypt(&self, args: DecryptArgs, originator: &str) -> bsv_rs::Result<DecryptResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        // BRC-98/99: Check protocol permission for decryption
        self.check_protocol_permission(
            originator,
            &args.protocol_id,
            None,
            crate::managers::PermissionUsageType::Encrypting,
        )
        .await
        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        self.proto_decrypt(args)
    }

    async fn create_hmac(
        &self,
        args: CreateHmacArgs,
        originator: &str,
    ) -> bsv_rs::Result<CreateHmacResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        // BRC-98/99: Check protocol permission for HMAC
        self.check_protocol_permission(
            originator,
            &args.protocol_id,
            None,
            crate::managers::PermissionUsageType::Hmac,
        )
        .await
        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        self.proto_create_hmac(args)
    }

    async fn verify_hmac(
        &self,
        args: VerifyHmacArgs,
        originator: &str,
    ) -> bsv_rs::Result<VerifyHmacResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        // BRC-98/99: Check protocol permission for HMAC verification
        self.check_protocol_permission(
            originator,
            &args.protocol_id,
            None,
            crate::managers::PermissionUsageType::Hmac,
        )
        .await
        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        self.proto_verify_hmac(args)
    }

    async fn create_signature(
        &self,
        args: CreateSignatureArgs,
        originator: &str,
    ) -> bsv_rs::Result<CreateSignatureResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        // BRC-98/99: Check protocol permission for signing
        self.check_protocol_permission(
            originator,
            &args.protocol_id,
            None,
            crate::managers::PermissionUsageType::Signing,
        )
        .await
        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        self.proto_create_signature(args)
    }

    async fn verify_signature(
        &self,
        args: VerifySignatureArgs,
        originator: &str,
    ) -> bsv_rs::Result<VerifySignatureResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
        self.proto_verify_signature(args)
    }

    async fn reveal_counterparty_key_linkage(
        &self,
        args: InterfaceRevealCounterpartyArgs,
        originator: &str,
    ) -> bsv_rs::Result<RevealCounterpartyKeyLinkageResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Convert interface args to proto_wallet args
        use bsv_rs::wallet::RevealCounterpartyKeyLinkageArgs as ProtoRevealCounterpartyArgs;

        let proto_args = ProtoRevealCounterpartyArgs {
            counterparty: args.counterparty.clone(),
            verifier: args.verifier.clone(),
        };

        let result = self
            .proto_wallet
            .reveal_counterparty_key_linkage(proto_args)?;

        // Parse the prover hex string back to PublicKey
        let prover = PublicKey::from_hex(&result.prover)
            .map_err(|e| bsv_rs::Error::WalletError(format!("Invalid prover key: {}", e)))?;

        // Parse the counterparty hex string back to PublicKey
        let counterparty_key = PublicKey::from_hex(&result.counterparty)
            .map_err(|e| bsv_rs::Error::WalletError(format!("Invalid counterparty key: {}", e)))?;

        Ok(RevealCounterpartyKeyLinkageResult {
            linkage: KeyLinkageResult {
                encrypted_linkage: result.encrypted_linkage,
                encrypted_linkage_proof: result.encrypted_linkage_proof,
                prover,
                verifier: args.verifier,
                counterparty: counterparty_key,
            },
            revelation_time: result.revelation_time,
        })
    }

    async fn reveal_specific_key_linkage(
        &self,
        args: InterfaceRevealSpecificArgs,
        originator: &str,
    ) -> bsv_rs::Result<RevealSpecificKeyLinkageResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Convert interface args to proto_wallet args
        use bsv_rs::wallet::{
            Counterparty, RevealSpecificKeyLinkageArgs as ProtoRevealSpecificArgs,
        };

        let proto_args = ProtoRevealSpecificArgs {
            counterparty: args.counterparty.clone(),
            verifier: args.verifier.clone(),
            protocol_id: args.protocol_id.clone(),
            key_id: args.key_id.clone(),
        };

        let result = self.proto_wallet.reveal_specific_key_linkage(proto_args)?;

        // Parse the prover hex string back to PublicKey
        let prover = PublicKey::from_hex(&result.prover)
            .map_err(|e| bsv_rs::Error::WalletError(format!("Invalid prover key: {}", e)))?;

        // Counterparty may be "self" or "anyone" - handle gracefully
        let counterparty_key = match &args.counterparty {
            Counterparty::Self_ | Counterparty::Anyone => self.proto_wallet.identity_key(),
            Counterparty::Other(pk) => pk.clone(),
        };

        Ok(RevealSpecificKeyLinkageResult {
            linkage: KeyLinkageResult {
                encrypted_linkage: result.encrypted_linkage,
                encrypted_linkage_proof: result.encrypted_linkage_proof,
                prover,
                verifier: args.verifier,
                counterparty: counterparty_key,
            },
            protocol: result.protocol_id,
            key_id: result.key_id,
            proof_type: result.proof_type,
        })
    }

    // =========================================================================
    // Action Operations (coordinated with storage and services)
    // =========================================================================

    async fn create_action(
        &self,
        args: CreateActionArgs,
        originator: &str,
    ) -> bsv_rs::Result<CreateActionResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // BRC-98/99: Check spending permission based on total output satoshis
        if self.permissions_manager.is_some() {
            let total_satoshis: u64 = args
                .outputs
                .as_ref()
                .map(|outputs| outputs.iter().map(|o| o.satoshis).sum())
                .unwrap_or(0);

            if total_satoshis > 0 {
                self.check_spending_permission(originator, total_satoshis)
                    .await
                    .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
            }

            // Also check basket access for any outputs that target specific baskets
            if let Some(ref outputs) = args.outputs {
                for output in outputs {
                    if let Some(ref basket) = output.basket {
                        self.check_basket_permission(
                            originator,
                            basket,
                            crate::managers::BasketUsageType::Insertion,
                        )
                        .await
                        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
                    }
                }
            }
        }

        let auth = self.auth();

        // Call storage to create the action
        let storage_result = self
            .storage
            .create_action(&auth, args.clone())
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Check if we should sign and process immediately
        let sign_and_process = args
            .options
            .as_ref()
            .and_then(|o| o.sign_and_process)
            .unwrap_or(true);

        let no_send = args
            .options
            .as_ref()
            .and_then(|o| o.no_send)
            .unwrap_or(false);

        let accept_delayed_broadcast = args
            .options
            .as_ref()
            .and_then(|o| o.accept_delayed_broadcast)
            .unwrap_or(false);

        // If sign_and_process is true and we have inputs to sign, sign them
        if sign_and_process && !storage_result.inputs.is_empty() {
            // Build the unsigned transaction from storage result
            // Pass the key_deriver so we can compute locking scripts for change outputs
            let unsigned_tx =
                build_unsigned_transaction(&storage_result, Some(self.proto_wallet.key_deriver()))
                    .map_err(|e| {
                        bsv_rs::Error::WalletError(format!("Failed to build transaction: {}", e))
                    })?;

            // Debug: Log storage result outputs
            for (i, output) in storage_result.outputs.iter().enumerate() {
                tracing::debug!(
                    vout = output.vout,
                    satoshis = output.satoshis,
                    locking_script = %output.locking_script,
                    locking_script_len = output.locking_script.len(),
                    provided_by = ?output.provided_by,
                    purpose = ?output.purpose,
                    derivation_suffix = ?output.derivation_suffix,
                    "Storage output {}", i
                );
            }

            // Convert storage inputs to signer inputs
            let signer_inputs: Vec<SignerInput> = storage_result
                .inputs
                .iter()
                .map(|input| SignerInput {
                    vin: input.vin,
                    source_txid: input.source_txid.clone(),
                    source_vout: input.source_vout,
                    satoshis: input.source_satoshis,
                    source_locking_script: Some(
                        hex::decode(&input.source_locking_script).unwrap_or_default(),
                    ),
                    unlocking_script: None,
                    derivation_prefix: input.derivation_prefix.clone(),
                    derivation_suffix: input.derivation_suffix.clone(),
                    sender_identity_key: input.sender_identity_key.clone(),
                })
                .collect();

            // Sign the transaction using the wallet signer
            let signed_tx = match self.signer.sign_transaction(
                &unsigned_tx,
                &signer_inputs,
                &self.proto_wallet,
            ) {
                Ok(tx) => tx,
                Err(e) => {
                    // Signing failed - abort the transaction to release locked UTXOs
                    tracing::error!(reference = %storage_result.reference, error = %e, "Signing failed in create_action, aborting transaction to release UTXOs");
                    if let Err(abort_err) = self
                        .storage
                        .abort_action(
                            &auth,
                            bsv_rs::wallet::AbortActionArgs {
                                reference: storage_result.reference.clone(),
                            },
                        )
                        .await
                    {
                        tracing::error!(reference = %storage_result.reference, error = %abort_err, "Failed to abort transaction after signing error");
                    }
                    return Err(bsv_rs::Error::WalletError(format!(
                        "Signing failed: {}. Transaction aborted and UTXOs released.",
                        e
                    )));
                }
            };

            // Compute txid from signed transaction
            let txid = compute_txid(&signed_tx);

            tracing::debug!(
                txid = %txid,
                raw_tx_hex = %hex::encode(&signed_tx),
                raw_tx_len = signed_tx.len(),
                "Sending processAction with signed transaction"
            );

            // Process the signed transaction
            let process_args = StorageProcessActionArgs {
                is_new_tx: true,
                is_send_with: args
                    .options
                    .as_ref()
                    .and_then(|o| o.send_with.as_ref())
                    .map(|s| !s.is_empty())
                    .unwrap_or(false),
                is_no_send: no_send,
                is_delayed: accept_delayed_broadcast,
                txid: Some(txid.clone()),
                raw_tx: Some(signed_tx.clone()),
                reference: Some(storage_result.reference.clone()),
                send_with: args
                    .options
                    .as_ref()
                    .and_then(|o| o.send_with.clone())
                    .map(|txids| txids.iter().map(hex::encode).collect())
                    .unwrap_or_default(),
            };

            let process_result = match self.storage.process_action(&auth, process_args).await {
                Ok(result) => result,
                Err(e) => {
                    // Processing failed - abort the transaction to release locked UTXOs
                    tracing::error!(reference = %storage_result.reference, error = %e, "Process action failed in create_action, aborting transaction to release UTXOs");
                    if let Err(abort_err) = self
                        .storage
                        .abort_action(
                            &auth,
                            bsv_rs::wallet::AbortActionArgs {
                                reference: storage_result.reference.clone(),
                            },
                        )
                        .await
                    {
                        tracing::error!(reference = %storage_result.reference, error = %abort_err, "Failed to abort transaction after process error");
                    }
                    return Err(bsv_rs::Error::WalletError(format!(
                        "Process action failed: {}. Transaction aborted and UTXOs released.",
                        e
                    )));
                }
            };

            // Log the process result for debugging
            tracing::debug!(
                send_with_results = ?process_result.send_with_results,
                not_delayed_results = ?process_result.not_delayed_results,
                "processAction returned"
            );

            // Build the full BEEF (signed tx + ancestor proofs) for broadcasting and returning to caller
            let full_beef_bytes = if let Some(ref input_beef_bytes) = storage_result.input_beef {
                match bsv_rs::transaction::Beef::from_binary(input_beef_bytes) {
                    Ok(mut beef) => {
                        beef.merge_raw_tx(signed_tx.clone(), None);

                        // Force V1 for ARC compatibility
                        let can_downgrade = beef.txs.iter().all(|tx| !tx.is_txid_only());
                        if can_downgrade && beef.version != bsv_rs::transaction::BEEF_V1 {
                            beef.version = bsv_rs::transaction::BEEF_V1;
                        }

                        Some(beef.to_binary())
                    }
                    Err(e) => {
                        tracing::error!(txid = %txid, error = %e, "Failed to parse input BEEF");
                        None
                    }
                }
            } else {
                None
            };

            // Broadcast the transaction if not no_send and not delayed
            // BUG-002/BUG-003 FIX: Update status AFTER broadcast attempt, not before
            if !no_send && !accept_delayed_broadcast {
                let broadcast_outcome = if let Some(ref beef_bytes) = full_beef_bytes {
                    let txid_strings = vec![txid.clone()];
                    // DEBUG: Validate BEEF structure and dump to file
                    match bsv_rs::transaction::Beef::from_binary(beef_bytes) {
                        Ok(parsed) => {
                            let dump_path = format!("/tmp/beef-{}.hex", &txid[..8]);
                            let _ = std::fs::write(&dump_path, hex::encode(beef_bytes));
                            tracing::info!(
                                txid = %txid,
                                beef_version = parsed.version,
                                num_bumps = parsed.bumps.len(),
                                num_txs = parsed.txs.len(),
                                beef_len = beef_bytes.len(),
                                dump_path = %dump_path,
                                "BEEF dumped for analysis"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                txid = %txid,
                                error = %e,
                                "BEEF pre-broadcast validation FAILED"
                            );
                        }
                    }
                    tracing::debug!(
                        txid = %txid,
                        beef_len = beef_bytes.len(),
                        "Broadcasting transaction via post_beef (with signed tx)"
                    );
                    match self.services.post_beef(beef_bytes, &txid_strings).await {
                        Ok(results) => {
                            let outcome = classify_broadcast_results(&results);
                            match &outcome {
                                BroadcastOutcome::Success => {
                                    tracing::info!(txid = %txid, "Transaction broadcast successfully");
                                }
                                BroadcastOutcome::ServiceError { details } => {
                                    tracing::warn!(
                                        txid = %txid,
                                        errors = ?details,
                                        "Broadcast returned service errors — will retry via SendWaitingTask"
                                    );
                                }
                                BroadcastOutcome::DoubleSpend { details, .. } => {
                                    tracing::error!(
                                        txid = %txid,
                                        errors = ?details,
                                        "Broadcast detected double spend"
                                    );
                                }
                                BroadcastOutcome::InvalidTx { details } => {
                                    tracing::error!(
                                        txid = %txid,
                                        errors = ?details,
                                        "Broadcast rejected — transaction invalid"
                                    );
                                }
                            }
                            outcome
                        }
                        Err(e) => {
                            // Network/service error is transient — will retry
                            tracing::warn!(txid = %txid, error = %e, "Broadcast service error — will retry");
                            BroadcastOutcome::ServiceError {
                                details: vec![e.to_string()],
                            }
                        }
                    }
                } else {
                    tracing::warn!(txid = %txid, "No BEEF available for broadcast");
                    BroadcastOutcome::InvalidTx {
                        details: vec!["No BEEF available for broadcast".to_string()],
                    }
                };

                // Update transaction status based on classified broadcast outcome
                if let Err(e) = self
                    .storage
                    .update_transaction_status_after_broadcast(&txid, &broadcast_outcome)
                    .await
                {
                    tracing::error!(txid = %txid, error = %e, "Failed to update transaction status after broadcast");
                }

                // On permanent failure, return an error with details
                // On service error (transient), the tx stays in 'sending' for retry — not an error
                match &broadcast_outcome {
                    BroadcastOutcome::DoubleSpend { .. } | BroadcastOutcome::InvalidTx { .. } => {
                        return Err(bsv_rs::Error::WalletError(
                            broadcast_outcome
                                .error_message(&txid)
                                .unwrap_or_else(|| format!(
                                    "Transaction broadcast failed for txid {}. Transaction marked as failed and inputs restored.",
                                    txid
                                )),
                        ));
                    }
                    _ => {} // Success or ServiceError — continue
                }
            }

            // Convert txid string to [u8; 32]
            let txid_bytes = hex::decode(&txid)
                .map_err(|e| bsv_rs::Error::WalletError(format!("Invalid txid: {}", e)))?;
            let mut txid_array = [0u8; 32];
            txid_array.copy_from_slice(&txid_bytes);

            return Ok(CreateActionResult {
                txid: Some(txid_array),
                tx: Some(signed_tx),
                no_send_change: storage_result.no_send_change_output_vouts.map(|vouts| {
                    vouts
                        .into_iter()
                        .map(|v| Outpoint {
                            txid: txid_array,
                            vout: v,
                        })
                        .collect()
                }),
                send_with_results: process_result.send_with_results.map(|results| {
                    results
                        .into_iter()
                        .map(|r| {
                            let mut txid = [0u8; 32];
                            if let Ok(bytes) = hex::decode(&r.txid) {
                                if bytes.len() == 32 {
                                    txid.copy_from_slice(&bytes);
                                }
                            }
                            bsv_rs::wallet::SendWithResult {
                                txid,
                                status: match r.status.as_str() {
                                    "unproven" => bsv_rs::wallet::SendWithResultStatus::Unproven,
                                    "sending" => bsv_rs::wallet::SendWithResultStatus::Sending,
                                    _ => bsv_rs::wallet::SendWithResultStatus::Failed,
                                },
                            }
                        })
                        .collect()
                }),
                signable_transaction: None,
                input_type: None,
                inputs: None,
                reference_number: None,
                beef: full_beef_bytes,
            });
        }

        // Return the result with signable transaction for external signing
        // Build transaction before consuming storage_result fields
        // Pass the key_deriver so we can compute locking scripts for change outputs
        let unsigned_tx =
            build_unsigned_transaction(&storage_result, Some(self.proto_wallet.key_deriver()))
                .map_err(|e| {
                    bsv_rs::Error::WalletError(format!("Failed to build transaction: {}", e))
                })?;
        let reference = storage_result.reference.clone();
        let reference_bytes = reference.clone().into_bytes();

        // Convert storage inputs to signer inputs for caching
        let signer_inputs: Vec<SignerInput> = storage_result
            .inputs
            .iter()
            .map(|input| SignerInput {
                vin: input.vin,
                source_txid: input.source_txid.clone(),
                source_vout: input.source_vout,
                satoshis: input.source_satoshis,
                source_locking_script: Some(
                    hex::decode(&input.source_locking_script).unwrap_or_default(),
                ),
                unlocking_script: None,
                derivation_prefix: input.derivation_prefix.clone(),
                derivation_suffix: input.derivation_suffix.clone(),
                sender_identity_key: input.sender_identity_key.clone(),
            })
            .collect();

        // Cache the pending transaction for later signing via sign_action
        let pending_tx = PendingTransaction {
            reference: reference.clone(),
            raw_tx: unsigned_tx.clone(),
            inputs: signer_inputs,
            input_beef: storage_result.input_beef.clone(),
            is_no_send: no_send,
            is_delayed: accept_delayed_broadcast,
            send_with: args
                .options
                .as_ref()
                .and_then(|o| o.send_with.clone())
                .map(|txids| txids.iter().map(hex::encode).collect())
                .unwrap_or_default(),
            created_at: Utc::now(),
        };

        // Store in the pending transactions cache
        {
            let mut cache = self.pending_transactions.write().await;
            // Clean up expired pending transactions while we have the lock
            let cutoff = Utc::now() - chrono::Duration::seconds(PENDING_TRANSACTION_TTL_SECS);
            cache.retain(|_, tx| tx.created_at > cutoff);
            // Add the new pending transaction
            cache.insert(reference.clone(), pending_tx);
        }

        Ok(CreateActionResult {
            txid: None,
            tx: None,
            no_send_change: storage_result.no_send_change_output_vouts.map(|vouts| {
                vouts
                    .into_iter()
                    .map(|v| Outpoint {
                        txid: [0u8; 32], // No txid yet for unsigned transaction
                        vout: v,
                    })
                    .collect()
            }),
            send_with_results: None,
            signable_transaction: Some(SignableTransaction {
                tx: unsigned_tx,
                reference: reference_bytes,
            }),
            input_type: None,
            inputs: None,
            reference_number: None,
            beef: None, // Not available until signed
        })
    }

    async fn sign_action(
        &self,
        args: SignActionArgs,
        originator: &str,
    ) -> bsv_rs::Result<SignActionResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Get the reference from args (it's already a String)
        let reference = args.reference;

        if reference.is_empty() {
            return Err(bsv_rs::Error::WalletError(
                "Missing reference argument for sign action".to_string(),
            ));
        }

        // Look up the pending transaction from cache
        let pending_tx = {
            let cache = self.pending_transactions.read().await;
            cache.get(&reference).cloned()
        };

        let pending_tx = pending_tx.ok_or_else(|| {
            bsv_rs::Error::WalletError(format!(
                "No pending transaction found for reference: {}",
                reference
            ))
        })?;

        // Check if the pending transaction has expired
        let cutoff = Utc::now() - chrono::Duration::seconds(PENDING_TRANSACTION_TTL_SECS);
        if pending_tx.created_at < cutoff {
            // Remove expired transaction from cache
            let mut cache = self.pending_transactions.write().await;
            cache.remove(&reference);
            return Err(bsv_rs::Error::WalletError(
                "Pending transaction has expired".to_string(),
            ));
        }

        // Merge any client-provided unlocking scripts from args.spends
        let mut inputs = pending_tx.inputs.clone();
        for (vin, spend) in &args.spends {
            if let Some(input) = inputs.iter_mut().find(|i| i.vin == *vin) {
                if !spend.unlocking_script.is_empty() {
                    input.unlocking_script = Some(spend.unlocking_script.clone());
                }
            }
        }

        // Sign the transaction using the wallet signer
        let signed_tx = match self.signer.sign_transaction(
            &pending_tx.raw_tx,
            &inputs,
            &self.proto_wallet,
        ) {
            Ok(tx) => tx,
            Err(e) => {
                // Signing failed - abort the transaction to release locked UTXOs
                tracing::error!(reference = %reference, error = %e, "Signing failed, aborting transaction to release UTXOs");
                let auth = self.auth();
                if let Err(abort_err) = self
                    .storage
                    .abort_action(
                        &auth,
                        bsv_rs::wallet::AbortActionArgs {
                            reference: reference.clone(),
                        },
                    )
                    .await
                {
                    tracing::error!(reference = %reference, error = %abort_err, "Failed to abort transaction after signing error");
                }
                return Err(bsv_rs::Error::WalletError(format!(
                    "Signing failed: {}. Transaction aborted and UTXOs released.",
                    e
                )));
            }
        };

        // Compute txid from signed transaction
        let txid = compute_txid(&signed_tx);

        // Determine options from sign_action args, falling back to cached values
        let is_no_send = args
            .options
            .as_ref()
            .and_then(|o| o.no_send)
            .unwrap_or(pending_tx.is_no_send);

        let is_delayed = args
            .options
            .as_ref()
            .and_then(|o| o.accept_delayed_broadcast)
            .unwrap_or(pending_tx.is_delayed);

        let send_with = args
            .options
            .as_ref()
            .and_then(|o| o.send_with.clone())
            .map(|txids| txids.iter().map(hex::encode).collect())
            .unwrap_or_else(|| pending_tx.send_with.clone());

        let is_send_with = !send_with.is_empty();

        // Process the signed transaction
        let auth = self.auth();
        let process_args = StorageProcessActionArgs {
            is_new_tx: true,
            is_send_with,
            is_no_send,
            is_delayed,
            txid: Some(txid.clone()),
            raw_tx: Some(signed_tx.clone()),
            reference: Some(reference.clone()),
            send_with,
        };

        let process_result = match self.storage.process_action(&auth, process_args).await {
            Ok(result) => result,
            Err(e) => {
                // Processing failed - abort the transaction to release locked UTXOs
                tracing::error!(reference = %reference, error = %e, "Process action failed, aborting transaction to release UTXOs");
                if let Err(abort_err) = self
                    .storage
                    .abort_action(
                        &auth,
                        bsv_rs::wallet::AbortActionArgs {
                            reference: reference.clone(),
                        },
                    )
                    .await
                {
                    tracing::error!(reference = %reference, error = %abort_err, "Failed to abort transaction after process error");
                }
                return Err(bsv_rs::Error::WalletError(format!(
                    "Process action failed: {}. Transaction aborted and UTXOs released.",
                    e
                )));
            }
        };

        // If not no_send and not delayed, broadcast the transaction
        // BUG-002/BUG-003 FIX: Update status AFTER broadcast attempt, not before
        if !is_no_send && !is_delayed {
            let broadcast_outcome = if let Some(ref input_beef_bytes) = pending_tx.input_beef {
                // Create a new BEEF that includes both the signed tx and its ancestors
                match bsv_rs::transaction::Beef::from_binary(input_beef_bytes) {
                    Ok(mut broadcast_beef) => {
                        broadcast_beef.merge_raw_tx(signed_tx.clone(), None);
                        let beef_bytes = broadcast_beef.to_binary();
                        let txid_strings = vec![txid.clone()];
                        match self.services.post_beef(&beef_bytes, &txid_strings).await {
                            Ok(results) => {
                                let outcome = classify_broadcast_results(&results);
                                match &outcome {
                                    BroadcastOutcome::Success => {
                                        tracing::info!(txid = %txid, "Transaction broadcast successfully (sign_action)");
                                    }
                                    BroadcastOutcome::ServiceError { details } => {
                                        tracing::warn!(
                                            txid = %txid,
                                            errors = ?details,
                                            "Broadcast returned service errors — will retry (sign_action)"
                                        );
                                    }
                                    BroadcastOutcome::DoubleSpend { details, .. } => {
                                        tracing::error!(txid = %txid, errors = ?details, "Double spend detected (sign_action)");
                                    }
                                    BroadcastOutcome::InvalidTx { details } => {
                                        tracing::error!(txid = %txid, errors = ?details, "Transaction rejected (sign_action)");
                                    }
                                }
                                outcome
                            }
                            Err(e) => {
                                tracing::warn!(txid = %txid, error = %e, "Broadcast service error — will retry (sign_action)");
                                BroadcastOutcome::ServiceError {
                                    details: vec![e.to_string()],
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(txid = %txid, error = %e, "Failed to parse input BEEF (sign_action)");
                        BroadcastOutcome::InvalidTx {
                            details: vec![format!("Failed to parse input BEEF: {}", e)],
                        }
                    }
                }
            } else {
                tracing::warn!(txid = %txid, "No input_beef available for broadcast (sign_action)");
                BroadcastOutcome::InvalidTx {
                    details: vec!["No input_beef available for broadcast".to_string()],
                }
            };

            // Update transaction status based on classified broadcast outcome
            if let Err(e) = self
                .storage
                .update_transaction_status_after_broadcast(&txid, &broadcast_outcome)
                .await
            {
                tracing::error!(txid = %txid, error = %e, "Failed to update transaction status after broadcast");
            }

            // On permanent failure, return an error with details
            match &broadcast_outcome {
                BroadcastOutcome::DoubleSpend { .. } | BroadcastOutcome::InvalidTx { .. } => {
                    return Err(bsv_rs::Error::WalletError(
                        broadcast_outcome
                            .error_message(&txid)
                            .unwrap_or_else(|| format!(
                                "Transaction broadcast failed for txid {}. Transaction marked as failed and inputs restored.",
                                txid
                            )),
                    ));
                }
                _ => {} // Success or ServiceError — continue
            }
        }

        // Remove from pending transactions cache on success
        {
            let mut cache = self.pending_transactions.write().await;
            cache.remove(&reference);
        }

        // Convert txid string to [u8; 32]
        let txid_bytes = hex::decode(&txid)
            .map_err(|e| bsv_rs::Error::WalletError(format!("Invalid txid: {}", e)))?;
        let mut txid_array = [0u8; 32];
        txid_array.copy_from_slice(&txid_bytes);

        Ok(SignActionResult {
            txid: Some(txid_array),
            tx: Some(signed_tx),
            send_with_results: process_result.send_with_results.map(|results| {
                results
                    .into_iter()
                    .map(|r| {
                        let mut result_txid = [0u8; 32];
                        if let Ok(bytes) = hex::decode(&r.txid) {
                            if bytes.len() == 32 {
                                result_txid.copy_from_slice(&bytes);
                            }
                        }
                        bsv_rs::wallet::SendWithResult {
                            txid: result_txid,
                            status: match r.status.as_str() {
                                "unproven" => bsv_rs::wallet::SendWithResultStatus::Unproven,
                                "sending" => bsv_rs::wallet::SendWithResultStatus::Sending,
                                _ => bsv_rs::wallet::SendWithResultStatus::Failed,
                            },
                        }
                    })
                    .collect()
            }),
        })
    }

    async fn abort_action(
        &self,
        args: AbortActionArgs,
        originator: &str,
    ) -> bsv_rs::Result<AbortActionResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let result = self
            .storage
            .abort_action(&auth, args)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        Ok(result)
    }

    async fn list_actions(
        &self,
        args: ListActionsArgs,
        originator: &str,
    ) -> bsv_rs::Result<ListActionsResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let result = self
            .storage
            .list_actions(&auth, args)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        Ok(result)
    }

    async fn internalize_action(
        &self,
        args: InternalizeActionArgs,
        originator: &str,
    ) -> bsv_rs::Result<InternalizeActionResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Validate wallet payment outputs have correct BRC-29 derivation parameters
        for output in &args.outputs {
            if let Some(ref payment) = output.payment_remittance {
                // Validate derivation parameters are present and non-empty
                if payment.derivation_prefix.is_empty() || payment.derivation_suffix.is_empty() {
                    return Err(bsv_rs::Error::WalletError(
                        "Wallet payment outputs require non-empty derivation_prefix and derivation_suffix".to_string()
                    ));
                }
                tracing::debug!(
                    output_index = output.output_index,
                    derivation_prefix = %payment.derivation_prefix,
                    derivation_suffix = %payment.derivation_suffix,
                    "Validated BRC-29 derivation parameters for wallet payment output"
                );
            }
        }

        // Validate derivation metadata matches locking scripts (death spiral prevention).
        // If the derived key doesn't match the script, this UTXO can never be signed.
        if let Ok(tx) = bsv_rs::transaction::Transaction::from_beef(&args.tx, None) {
            for output in &args.outputs {
                if let Some(ref payment) = output.payment_remittance {
                    let vout = output.output_index as usize;
                    if vout >= tx.outputs.len() {
                        return Err(bsv_rs::Error::WalletError(format!(
                            "output_index {} exceeds transaction output count {}",
                            vout,
                            tx.outputs.len()
                        )));
                    }
                    let script = tx.outputs[vout].locking_script.as_script().to_binary();
                    // Only validate P2PKH (25 bytes: OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG)
                    if script.len() == 25
                        && script[0] == 0x76
                        && script[1] == 0xa9
                        && script[2] == 0x14
                        && script[23] == 0x88
                        && script[24] == 0xac
                    {
                        let counterparty = if !payment.sender_identity_key.is_empty() {
                            Counterparty::Other(
                                PublicKey::from_hex(&payment.sender_identity_key).map_err(|e| {
                                    bsv_rs::Error::WalletError(format!(
                                        "Invalid sender_identity_key: {}",
                                        e
                                    ))
                                })?,
                            )
                        } else {
                            Counterparty::Self_
                        };
                        let protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
                        let key_id = format!(
                            "{} {}",
                            payment.derivation_prefix, payment.derivation_suffix
                        );
                        let derived = self
                            .proto_wallet
                            .key_deriver()
                            .derive_public_key(&protocol, &key_id, &counterparty, true)
                            .map_err(|e| {
                                bsv_rs::Error::WalletError(format!(
                                    "BRC-29 key derivation failed: {}",
                                    e
                                ))
                            })?;
                        if derived.hash160()[..] != script[3..23] {
                            return Err(bsv_rs::Error::WalletError(format!(
                                "Derivation mismatch for output {}: derived {} != script {}",
                                vout,
                                hex::encode(derived.hash160()),
                                hex::encode(&script[3..23])
                            )));
                        }
                    }
                }
            }
        }

        let auth = self.auth();

        // Save a copy of the BEEF bytes before args is moved into storage.
        // We need these to broadcast the transaction after it's committed to DB.
        let beef_bytes = args.tx.clone();

        let result = self
            .storage
            .internalize_action(&auth, args)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // After the transaction is committed to DB, attempt immediate broadcast
        // for new unproven transactions (not merges, not already proven).
        // This mirrors the TypeScript `shareReqsWithWorld()` call which broadcasts
        // via `services.postBeef()` after internalizing.
        // Broadcast failure is non-fatal: the transaction is already persisted,
        // and the background proof monitor task will eventually retry.
        if !result.is_merge && result.send_with_results.is_none() {
            let txid = result.txid.clone();
            let txid_strings = vec![txid.clone()];
            tracing::debug!(
                txid = %txid,
                beef_len = beef_bytes.len(),
                "Broadcasting internalized transaction via post_beef"
            );
            let broadcast_failed = match self.services.post_beef(&beef_bytes, &txid_strings).await {
                Ok(results) => {
                    let any_success = results.iter().any(|r| r.status == "success");
                    if any_success {
                        tracing::info!(
                            txid = %txid,
                            "Internalized transaction broadcast successfully"
                        );
                        false
                    } else {
                        let errors: Vec<_> = results
                            .iter()
                            .filter(|r| r.status != "success")
                            .map(|r| format!("{}: {}", r.name, r.status))
                            .collect();
                        tracing::warn!(
                            txid = %txid,
                            errors = ?errors,
                            "Internalized transaction broadcast returned no successes"
                        );
                        true
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        txid = %txid,
                        error = %e,
                        "Internalized transaction broadcast failed"
                    );
                    true
                }
            };

            // Broadcast failure is non-fatal for internalized transactions.
            // The sender (e.g., x402 server) already broadcast the tx — our
            // broadcast is just a courtesy to speed up confirmation. The outputs
            // are valid and should remain spendable. The background proof monitor
            // will eventually confirm the tx or detect a genuine double-spend.
            //
            // Previously this called mark_internalized_tx_failed() which set
            // spendable=0 and status='failed', permanently killing valid refund
            // outputs. This caused UTXO starvation on wallets with high x402
            // payment volume (e.g., agent wallets).
            if broadcast_failed {
                tracing::info!(
                    txid = %txid,
                    "Internalized tx broadcast failed — outputs kept spendable (sender already broadcast)"
                );
            }
        }

        Ok(InternalizeActionResult {
            accepted: result.base.accepted,
        })
    }

    // =========================================================================
    // Output Operations (delegated to storage)
    // =========================================================================

    async fn list_outputs(
        &self,
        args: ListOutputsArgs,
        originator: &str,
    ) -> bsv_rs::Result<ListOutputsResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // BRC-98/99: Check basket access permission for listing
        self.check_basket_permission(
            originator,
            &args.basket,
            crate::managers::BasketUsageType::Listing,
        )
        .await
        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let result = self
            .storage
            .list_outputs(&auth, args)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        Ok(result)
    }

    async fn relinquish_output(
        &self,
        args: RelinquishOutputArgs,
        originator: &str,
    ) -> bsv_rs::Result<RelinquishOutputResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // BRC-98/99: Check basket access permission for removal
        self.check_basket_permission(
            originator,
            &args.basket,
            crate::managers::BasketUsageType::Removal,
        )
        .await
        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let _result = self
            .storage
            .relinquish_output(&auth, args)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        Ok(RelinquishOutputResult { relinquished: true })
    }

    // =========================================================================
    // Certificate Operations (delegated to storage)
    // =========================================================================

    async fn acquire_certificate(
        &self,
        args: AcquireCertificateArgs,
        originator: &str,
    ) -> bsv_rs::Result<WalletCertificate> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Route based on acquisition protocol:
        // - Direct: certificate data is already provided in args, store directly
        // - Issuance: use BRC-104 HTTP communication with certifier service

        match args.acquisition_protocol {
            bsv_rs::wallet::AcquisitionProtocol::Direct => {
                // Direct certificate storage
                tracing::debug!("Acquiring certificate via direct protocol");
                let auth = self.auth();

                // Build the certificate from args
                let mut certificate = build_wallet_certificate_from_args(&args)?;

                // Set the subject to the wallet's identity key (matching issuance behavior)
                certificate.subject = self.identity_key.clone();

                // Determine the verifier from the keyring_revealer argument
                let verifier = match &args.keyring_revealer {
                    Some(bsv_rs::wallet::KeyringRevealer::Certifier) => {
                        Some(args.certifier.clone())
                    }
                    Some(bsv_rs::wallet::KeyringRevealer::PublicKey(pk)) => Some(pk.to_hex()),
                    None => None,
                };

                let now = chrono::Utc::now();

                // Convert to TableCertificate for storage
                let table_cert = TableCertificate {
                    certificate_id: 0, // Will be assigned by storage
                    user_id: self.user_id,
                    cert_type: certificate.certificate_type.clone(),
                    serial_number: certificate.serial_number.clone(),
                    certifier: certificate.certifier.clone(),
                    subject: certificate.subject.clone(),
                    verifier,
                    revocation_outpoint: certificate.revocation_outpoint.clone(),
                    signature: certificate.signature.clone(),
                    created_at: now,
                    updated_at: now,
                };

                // Persist the certificate
                let cert_id = self
                    .storage
                    .insert_certificate(&auth, table_cert)
                    .await
                    .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

                // Persist certificate fields with their master keys from the keyring
                let keyring = args.keyring_for_subject.as_ref();
                for (field_name, field_value) in &certificate.fields {
                    let master_key = keyring
                        .and_then(|kr| kr.get(field_name))
                        .cloned()
                        .unwrap_or_default();

                    let field = TableCertificateField {
                        certificate_field_id: 0, // Will be assigned by storage
                        certificate_id: cert_id,
                        user_id: self.user_id,
                        field_name: field_name.clone(),
                        field_value: field_value.clone(),
                        master_key,
                        created_at: now,
                        updated_at: now,
                    };

                    self.storage
                        .insert_certificate_field(&auth, field)
                        .await
                        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;
                }

                Ok(certificate)
            }
            bsv_rs::wallet::AcquisitionProtocol::Issuance => {
                // Use the certificate issuance module (BRC-104 protocol)
                tracing::debug!("Acquiring certificate via issuance protocol");
                let auth = self.auth();

                super::certificate_issuance::acquire_certificate_issuance(
                    self,
                    self.storage.as_ref(),
                    &auth,
                    args,
                    &self.identity_key,
                    originator,
                )
                .await
                .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))
            }
        }
    }

    async fn list_certificates(
        &self,
        args: ListCertificatesArgs,
        originator: &str,
    ) -> bsv_rs::Result<ListCertificatesResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let result = self
            .storage
            .list_certificates(&auth, args)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        Ok(result)
    }

    async fn prove_certificate(
        &self,
        args: ProveCertificateArgs,
        originator: &str,
    ) -> bsv_rs::Result<ProveCertificateResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // BRC-98/99: Check certificate disclosure permission
        self.check_certificate_permission(
            originator,
            args.privileged.unwrap_or(false),
            &args.verifier,
            &args.certificate.certificate_type,
            &args.fields_to_reveal,
            crate::managers::CertificateUsageType::Disclosure,
        )
        .await
        .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        // Build list_certificates args to find the certificate matching the provided certificate
        // We query by all identifying fields to ensure a unique match
        let list_args = ListCertificatesArgs {
            certifiers: vec![args.certificate.certifier.clone()],
            types: vec![args.certificate.certificate_type.clone()],
            limit: Some(2), // Request 2 to detect multiple matches
            offset: Some(0),
            privileged: args.privileged,
            privileged_reason: args.privileged_reason.clone(),
        };

        let list_result = self
            .storage
            .list_certificates(&auth, list_args)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Filter by serial number, subject, revocation outpoint, and signature
        let matching_certs: Vec<_> = list_result
            .certificates
            .into_iter()
            .filter(|cert| {
                cert.certificate.serial_number == args.certificate.serial_number
                    && cert.certificate.subject == args.certificate.subject
                    && cert.certificate.revocation_outpoint == args.certificate.revocation_outpoint
                    && cert.certificate.signature == args.certificate.signature
            })
            .collect();

        // Ensure exactly one certificate matches
        if matching_certs.is_empty() {
            return Err(bsv_rs::Error::WalletError(
                "Certificate not found with the provided arguments".to_string(),
            ));
        }
        if matching_certs.len() > 1 {
            return Err(bsv_rs::Error::WalletError(
                "Multiple certificates match the provided arguments, expected unique match"
                    .to_string(),
            ));
        }

        let storage_cert = &matching_certs[0];

        // Get the master keyring from storage
        let master_keyring = storage_cert.keyring.as_ref().ok_or_else(|| {
            bsv_rs::Error::WalletError(
                "Certificate does not have a master keyring stored".to_string(),
            )
        })?;

        // Determine which fields to reveal
        // If fields_to_reveal is empty, reveal all fields
        let fields_to_reveal: Vec<String> = if args.fields_to_reveal.is_empty() {
            storage_cert.certificate.fields.keys().cloned().collect()
        } else {
            args.fields_to_reveal.clone()
        };

        // Parse the verifier public key
        let verifier = PublicKey::from_hex(&args.verifier).map_err(|e| {
            bsv_rs::Error::WalletError(format!("Invalid verifier public key: {}", e))
        })?;

        // Parse the certifier public key
        let certifier = PublicKey::from_hex(&storage_cert.certificate.certifier).map_err(|e| {
            bsv_rs::Error::WalletError(format!("Invalid certifier public key: {}", e))
        })?;

        // Create keyring for verifier
        // This follows the TypeScript/Go implementation:
        // 1. For each field to reveal, decrypt the master key
        // 2. Re-encrypt the symmetric key for the verifier
        let keyring_for_verifier = create_keyring_for_verifier(
            &self.proto_wallet,
            &certifier,
            &verifier,
            &storage_cert.certificate.fields,
            &fields_to_reveal,
            master_keyring,
            &storage_cert.certificate.serial_number,
            originator,
        )?;

        Ok(ProveCertificateResult {
            keyring_for_verifier,
            certificate: None,
            verifier: None,
        })
    }

    async fn relinquish_certificate(
        &self,
        args: RelinquishCertificateArgs,
        originator: &str,
    ) -> bsv_rs::Result<RelinquishCertificateResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let _result = self
            .storage
            .relinquish_certificate(&auth, args)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        Ok(RelinquishCertificateResult { relinquished: true })
    }

    // =========================================================================
    // Discovery Operations
    // =========================================================================

    async fn discover_by_identity_key(
        &self,
        args: DiscoverByIdentityKeyArgs,
        originator: &str,
    ) -> bsv_rs::Result<DiscoverCertificatesResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let auth = self.auth();
        let identity_key_hex = &args.identity_key;

        // 1. Local discovery: query certificates from storage where subject matches
        let find_args = crate::storage::FindCertificatesArgs {
            base: crate::storage::FindSincePagedArgs {
                paged: Some(crate::storage::Paged {
                    limit: args.limit,
                    offset: args.offset,
                }),
                ..Default::default()
            },
            include_fields: Some(true),
            ..Default::default()
        };

        let certs = self
            .storage
            .find_certificates(&auth, find_args)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let local_certs: Vec<bsv_rs::wallet::IdentityCertificate> = certs
            .into_iter()
            .filter(|cert| cert.subject == *identity_key_hex)
            .map(|cert| bsv_rs::wallet::IdentityCertificate {
                certificate: WalletCertificate {
                    certificate_type: cert.cert_type.clone(),
                    serial_number: cert.serial_number.clone(),
                    subject: cert.subject.clone(),
                    certifier: cert.certifier.clone(),
                    revocation_outpoint: cert.revocation_outpoint.clone(),
                    fields: std::collections::HashMap::new(),
                    signature: cert.signature.clone(),
                },
                decrypted_fields: None,
                publicly_revealed_keyring: None,
                certifier_info: None,
            })
            .collect();

        // 2. Overlay discovery: query ls_identity via SLAP resolution
        let resolver = match self.chain {
            Chain::Main => crate::wallet::lookup::HttpLookupResolver::mainnet(),
            Chain::Test => crate::wallet::lookup::HttpLookupResolver::testnet(),
        };

        let overlay_certs = match resolver.lookup_by_identity_key(identity_key_hex).await {
            Ok(certs) => certs
                .into_iter()
                .map(|c| c.to_identity_certificate())
                .collect::<Vec<_>>(),
            Err(e) => {
                tracing::debug!("Overlay lookup failed (using local only): {}", e);
                vec![]
            }
        };

        // 3. Merge and deduplicate
        let mut all_certs = local_certs;
        all_certs.extend(overlay_certs);
        let certificates = crate::wallet::lookup::dedup_certificates(all_certs);

        Ok(DiscoverCertificatesResult {
            total_certificates: certificates.len() as u32,
            certificates,
        })
    }

    async fn discover_by_attributes(
        &self,
        args: DiscoverByAttributesArgs,
        originator: &str,
    ) -> bsv_rs::Result<DiscoverCertificatesResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Overlay discovery: query ls_identity via SLAP resolution
        let resolver = match self.chain {
            Chain::Main => crate::wallet::lookup::HttpLookupResolver::mainnet(),
            Chain::Test => crate::wallet::lookup::HttpLookupResolver::testnet(),
        };

        let certificates = match resolver.lookup_by_attributes(&args.attributes).await {
            Ok(certs) => certs
                .into_iter()
                .map(|c| c.to_identity_certificate())
                .collect::<Vec<_>>(),
            Err(e) => {
                tracing::debug!("Overlay attribute lookup failed: {}", e);
                vec![]
            }
        };

        Ok(DiscoverCertificatesResult {
            total_certificates: certificates.len() as u32,
            certificates,
        })
    }

    // =========================================================================
    // Chain/Status Operations
    // =========================================================================

    async fn is_authenticated(&self, originator: &str) -> bsv_rs::Result<AuthenticatedResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Wallet is always authenticated (it has a key)
        Ok(AuthenticatedResult {
            authenticated: true,
        })
    }

    async fn wait_for_authentication(
        &self,
        originator: &str,
    ) -> bsv_rs::Result<AuthenticatedResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // Wallet is always authenticated
        Ok(AuthenticatedResult {
            authenticated: true,
        })
    }

    async fn get_height(&self, originator: &str) -> bsv_rs::Result<GetHeightResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let height = self
            .services
            .get_height()
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        Ok(GetHeightResult { height })
    }

    async fn get_header_for_height(
        &self,
        args: GetHeaderArgs,
        originator: &str,
    ) -> bsv_rs::Result<GetHeaderResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let header_bytes = self
            .services
            .get_header_for_height(args.height)
            .await
            .map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        // GetHeaderResult expects header as hex string
        Ok(GetHeaderResult {
            header: hex::encode(&header_bytes),
        })
    }

    async fn get_network(&self, originator: &str) -> bsv_rs::Result<GetNetworkResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        let network = match self.chain {
            Chain::Main => Network::Mainnet,
            Chain::Test => Network::Testnet,
        };

        Ok(GetNetworkResult { network })
    }

    async fn get_version(&self, originator: &str) -> bsv_rs::Result<GetVersionResult> {
        validate_originator(originator).map_err(|e| bsv_rs::Error::WalletError(e.to_string()))?;

        Ok(GetVersionResult {
            version: WALLET_VERSION.to_string(),
        })
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Validates the originator string.
fn validate_originator(originator: &str) -> Result<()> {
    if originator.is_empty() {
        return Err(Error::ValidationError(
            "originator cannot be empty".to_string(),
        ));
    }
    // The originator should be a valid domain or identifier
    // For now, just check it's not empty and doesn't contain invalid characters
    if originator.len() > 253 {
        return Err(Error::ValidationError(
            "originator exceeds maximum length".to_string(),
        ));
    }
    Ok(())
}

/// Computes the txid (double SHA256, reversed) from raw transaction bytes.
fn compute_txid(raw_tx: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let hash1 = Sha256::digest(raw_tx);
    let hash2 = Sha256::digest(hash1);
    let mut reversed = hash2.to_vec();
    reversed.reverse();
    hex::encode(reversed)
}

/// Builds an unsigned transaction from StorageCreateActionResult.
///
/// If `key_deriver` is provided, it will be used to compute locking scripts
/// for change outputs that have a derivation_suffix but no locking_script.
fn build_unsigned_transaction(
    result: &crate::storage::StorageCreateActionResult,
    key_deriver: Option<&dyn bsv_rs::wallet::KeyDeriverApi>,
) -> Result<Vec<u8>> {
    let mut tx = Vec::new();

    // Version
    tx.extend_from_slice(&result.version.to_le_bytes());

    // Sort inputs by vin
    let mut sorted_inputs: Vec<_> = result.inputs.iter().collect();
    sorted_inputs.sort_by_key(|i| i.vin);

    // Input count
    write_varint(&mut tx, sorted_inputs.len() as u64);

    // Inputs
    for input in &sorted_inputs {
        // Previous txid - needs to be reversed from display format to transaction format
        let mut txid_bytes = hex::decode(&input.source_txid)
            .map_err(|e| Error::TransactionError(format!("Invalid txid: {}", e)))?;
        txid_bytes.reverse(); // Reverse from display format to internal format
        tx.extend_from_slice(&txid_bytes);

        // Previous vout
        tx.extend_from_slice(&input.source_vout.to_le_bytes());

        // Script (empty for unsigned)
        tx.push(0);

        // Sequence
        tx.extend_from_slice(&0xfffffffe_u32.to_le_bytes());
    }

    // Sort outputs by vout
    let mut sorted_outputs: Vec<_> = result.outputs.iter().collect();
    sorted_outputs.sort_by_key(|o| o.vout);

    // Output count
    write_varint(&mut tx, sorted_outputs.len() as u64);

    // Outputs
    for output in &sorted_outputs {
        // Satoshis
        tx.extend_from_slice(&output.satoshis.to_le_bytes());

        // Get or compute locking script
        let script = if output.locking_script.is_empty() {
            // Need to derive the locking script from derivation info
            // Change outputs use BRC-29 protocol with Counterparty::Self_
            if let (Some(key_deriver), Some(suffix)) = (key_deriver, &output.derivation_suffix) {
                use bsv_rs::wallet::{Protocol, SecurityLevel};

                // BRC-29 protocol: security level 2, protocol name "3241645161d8"
                let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
                // Key ID has a SPACE between prefix and suffix
                let key_id = format!("{} {}", result.derivation_prefix, suffix);

                // Derive the public key using BRC-29 protocol
                let pubkey = key_deriver
                    .derive_private_key(
                        &brc29_protocol,
                        &key_id,
                        &bsv_rs::wallet::Counterparty::Self_,
                    )
                    .map_err(|e| {
                        Error::TransactionError(format!("Failed to derive change key: {}", e))
                    })?
                    .public_key();

                // Create P2PKH locking script using SDK's ScriptTemplate trait
                use bsv_rs::script::template::ScriptTemplate;
                bsv_rs::script::templates::P2PKH::new()
                    .lock(&pubkey.hash160())
                    .map_err(|e| {
                        Error::TransactionError(format!("Failed to create P2PKH script: {}", e))
                    })?
                    .to_binary()
            } else {
                // Can't derive - use empty (will likely fail validation)
                tracing::warn!(
                    vout = output.vout,
                    "Output has empty locking script and no derivation info"
                );
                Vec::new()
            }
        } else {
            hex::decode(&output.locking_script)
                .map_err(|e| Error::TransactionError(format!("Invalid locking script: {}", e)))?
        };

        write_varint(&mut tx, script.len() as u64);
        tx.extend_from_slice(&script);
    }

    // Locktime
    tx.extend_from_slice(&result.lock_time.to_le_bytes());

    Ok(tx)
}

/// Writes a varint to the output buffer.
fn write_varint(output: &mut Vec<u8>, value: u64) {
    if value < 0xfd {
        output.push(value as u8);
    } else if value <= 0xffff {
        output.push(0xfd);
        output.extend_from_slice(&(value as u16).to_le_bytes());
    } else if value <= 0xffffffff {
        output.push(0xfe);
        output.extend_from_slice(&(value as u32).to_le_bytes());
    } else {
        output.push(0xff);
        output.extend_from_slice(&value.to_le_bytes());
    }
}

/// Builds a WalletCertificate from AcquireCertificateArgs.
fn build_wallet_certificate_from_args(
    args: &AcquireCertificateArgs,
) -> bsv_rs::Result<WalletCertificate> {
    let serial_number = args.serial_number.clone().ok_or_else(|| {
        bsv_rs::Error::WalletError("serial_number required for direct acquisition".to_string())
    })?;

    let revocation_outpoint = args.revocation_outpoint.clone().ok_or_else(|| {
        bsv_rs::Error::WalletError(
            "revocation_outpoint required for direct acquisition".to_string(),
        )
    })?;

    let signature = args.signature.clone().ok_or_else(|| {
        bsv_rs::Error::WalletError("signature required for direct acquisition".to_string())
    })?;

    Ok(WalletCertificate {
        certificate_type: args.certificate_type.clone(),
        subject: String::new(), // Will be set by storage based on identity key
        serial_number,
        certifier: args.certifier.clone(),
        revocation_outpoint,
        signature,
        fields: args.fields.clone(),
    })
}

/// Protocol for certificate field encryption (BRC-52/53).
const CERTIFICATE_FIELD_ENCRYPTION_PROTOCOL: &str = "certificate field encryption";

/// Creates a keyring for a verifier, enabling them to decrypt specific certificate fields.
///
/// This function follows the TypeScript/Go implementation:
/// 1. For each field to reveal, decrypt the master symmetric key using the master keyring
/// 2. Re-encrypt the symmetric key for the verifier with a verifiable key ID
///
/// # Arguments
/// * `subject_wallet` - The subject's wallet (implements encryption/decryption)
/// * `certifier` - The certifier's public key
/// * `verifier` - The verifier's public key who will receive the keyring
/// * `fields` - All encrypted field values from the certificate (base64 encoded)
/// * `fields_to_reveal` - Field names to include in the verifier keyring
/// * `master_keyring` - The master keyring containing encrypted symmetric keys (base64 encoded)
/// * `serial_number` - Certificate serial number (base64 encoded)
/// * `_originator` - Application originator string (unused by ProtoWallet but kept for API consistency)
///
/// # Returns
/// A keyring for the verifier: field_name -> base64 encoded encrypted symmetric key
#[allow(clippy::too_many_arguments)]
fn create_keyring_for_verifier(
    subject_wallet: &ProtoWallet,
    certifier: &PublicKey,
    verifier: &PublicKey,
    fields: &HashMap<String, String>,
    fields_to_reveal: &[String],
    master_keyring: &HashMap<String, String>,
    serial_number: &str,
    _originator: &str,
) -> bsv_rs::Result<HashMap<String, String>> {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
    use bsv_rs::wallet::{DecryptArgs as ProtoDecryptArgs, EncryptArgs as ProtoEncryptArgs};

    // Validate master keyring is not empty
    if master_keyring.is_empty() {
        return Err(bsv_rs::Error::WalletError(
            "Master keyring is empty - cannot create keyring for verifier".to_string(),
        ));
    }

    let protocol = Protocol::new(
        SecurityLevel::Counterparty,
        CERTIFICATE_FIELD_ENCRYPTION_PROTOCOL,
    );

    let mut keyring_for_verifier = HashMap::new();

    for field_name in fields_to_reveal {
        // Verify field exists in the certificate
        if !fields.contains_key(field_name) {
            return Err(bsv_rs::Error::WalletError(format!(
                "Field '{}' not found in certificate - fields_to_reveal must be a subset of certificate fields",
                field_name
            )));
        }

        // Get the master key for this field (base64 encoded encrypted symmetric key)
        let master_key_base64 = master_keyring.get(field_name).ok_or_else(|| {
            bsv_rs::Error::WalletError(format!(
                "Field '{}' not found in master keyring",
                field_name
            ))
        })?;

        // Decode master key from base64
        let master_key_ciphertext = BASE64.decode(master_key_base64).map_err(|e| {
            bsv_rs::Error::WalletError(format!(
                "Failed to decode master key for field '{}': {}",
                field_name, e
            ))
        })?;

        // Decrypt the master symmetric key using the certifier as counterparty
        // Key ID for master is just the field name
        let decrypted = subject_wallet.decrypt(ProtoDecryptArgs {
            ciphertext: master_key_ciphertext,
            protocol_id: protocol.clone(),
            key_id: field_name.clone(),
            counterparty: Some(Counterparty::Other(certifier.clone())),
        })?;

        // The decrypted plaintext is the symmetric key bytes
        let symmetric_key = decrypted.plaintext;

        // Re-encrypt the symmetric key for the verifier
        // Key ID for verifiable is: "{serial_number} {field_name}"
        let verifiable_key_id = format!("{} {}", serial_number, field_name);

        let re_encrypted = subject_wallet.encrypt(ProtoEncryptArgs {
            plaintext: symmetric_key,
            protocol_id: protocol.clone(),
            key_id: verifiable_key_id,
            counterparty: Some(Counterparty::Other(verifier.clone())),
        })?;

        // Encode the re-encrypted symmetric key as base64
        let encrypted_base64 = BASE64.encode(&re_encrypted.ciphertext);
        keyring_for_verifier.insert(field_name.clone(), encrypted_base64);
    }

    Ok(keyring_for_verifier)
}

// =============================================================================
// Address Helpers
// =============================================================================

/// Decodes a Base58Check BSV address into a P2PKH locking script.
fn address_to_p2pkh_script(address: &str) -> Result<Vec<u8>> {
    use sha2::{Digest, Sha256};

    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    let mut result = [0u8; 25];
    for &ch in address.as_bytes() {
        let mut carry = ALPHABET.iter().position(|&c| c == ch).ok_or_else(|| {
            Error::ValidationError(format!("Invalid Base58 character: {}", ch as char))
        })? as u32;

        for byte in result.iter_mut().rev() {
            carry += 58 * (*byte as u32);
            *byte = (carry % 256) as u8;
            carry /= 256;
        }

        if carry != 0 {
            return Err(Error::ValidationError("Address too long".to_string()));
        }
    }

    let leading_zeros = address.bytes().take_while(|&b| b == b'1').count();
    let first_nonzero = result.iter().position(|&b| b != 0).unwrap_or(result.len());
    if first_nonzero < leading_zeros {
        return Err(Error::ValidationError(
            "Invalid address encoding".to_string(),
        ));
    }

    let decoded = &result[(first_nonzero - leading_zeros)..];

    if decoded.len() != 25 {
        return Err(Error::ValidationError(format!(
            "Invalid address length: expected 25, got {}",
            decoded.len()
        )));
    }

    let payload = &decoded[..21];
    let checksum = &decoded[21..25];
    let hash1 = Sha256::digest(payload);
    let hash2 = Sha256::digest(hash1);
    if &hash2[..4] != checksum {
        return Err(Error::ValidationError(
            "Invalid address checksum".to_string(),
        ));
    }

    let version = decoded[0];
    if version != 0x00 && version != 0x6f {
        return Err(Error::ValidationError(format!(
            "Unsupported address version: 0x{:02x}",
            version
        )));
    }

    let pubkey_hash = &decoded[1..21];

    let mut script = Vec::with_capacity(25);
    script.push(0x76); // OP_DUP
    script.push(0xa9); // OP_HASH160
    script.push(0x14); // PUSH 20 bytes
    script.extend_from_slice(pubkey_hash);
    script.push(0x88); // OP_EQUALVERIFY
    script.push(0xac); // OP_CHECKSIG

    Ok(script)
}

// =============================================================================
// Debug Implementation
// =============================================================================

impl<S, V> std::fmt::Debug for Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync,
    V: WalletServices + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wallet")
            .field("identity_key", &self.identity_key)
            .field("chain", &self.chain)
            .field(
                "has_privileged_key_manager",
                &self.privileged_key_manager.is_some(),
            )
            .finish_non_exhaustive()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_transaction_struct() {
        // Test that PendingTransaction can be created
        let pending = PendingTransaction {
            reference: "test-ref-123".to_string(),
            raw_tx: vec![0x01, 0x00, 0x00, 0x00],
            inputs: vec![SignerInput {
                vin: 0,
                source_txid: "0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
                source_vout: 0,
                satoshis: 50000,
                source_locking_script: Some(vec![0x76, 0xa9]),
                unlocking_script: None,
                derivation_prefix: Some("1".to_string()),
                derivation_suffix: Some("suffix".to_string()),
                sender_identity_key: None,
            }],
            input_beef: Some(vec![0xBE, 0xEF]),
            is_no_send: false,
            is_delayed: false,
            send_with: vec![],
            created_at: Utc::now(),
        };

        assert_eq!(pending.reference, "test-ref-123");
        assert_eq!(pending.inputs.len(), 1);
        assert_eq!(pending.inputs[0].vin, 0);
        assert!(!pending.is_no_send);
    }

    #[test]
    fn test_pending_transaction_ttl_constant() {
        // TTL should be 24 hours in seconds
        assert_eq!(PENDING_TRANSACTION_TTL_SECS, 24 * 60 * 60);
        assert_eq!(PENDING_TRANSACTION_TTL_SECS, 86400);
    }

    #[test]
    fn test_pending_transaction_clone() {
        let pending = PendingTransaction {
            reference: "clone-test".to_string(),
            raw_tx: vec![0x01, 0x02],
            inputs: vec![],
            input_beef: None,
            is_no_send: true,
            is_delayed: true,
            send_with: vec!["txid1".to_string()],
            created_at: Utc::now(),
        };

        let cloned = pending.clone();
        assert_eq!(cloned.reference, pending.reference);
        assert_eq!(cloned.is_no_send, pending.is_no_send);
        assert_eq!(cloned.is_delayed, pending.is_delayed);
        assert_eq!(cloned.send_with, pending.send_with);
    }

    #[test]
    fn test_pending_transaction_with_send_with() {
        let pending = PendingTransaction {
            reference: "sendwith-test".to_string(),
            raw_tx: vec![],
            inputs: vec![],
            input_beef: None,
            is_no_send: false,
            is_delayed: false,
            send_with: vec!["abc123".to_string(), "def456".to_string()],
            created_at: Utc::now(),
        };

        assert_eq!(pending.send_with.len(), 2);
        assert_eq!(pending.send_with[0], "abc123");
    }

    #[test]
    fn test_pending_transaction_debug() {
        let pending = PendingTransaction {
            reference: "debug-test".to_string(),
            raw_tx: vec![0x01],
            inputs: vec![],
            input_beef: None,
            is_no_send: false,
            is_delayed: false,
            send_with: vec![],
            created_at: Utc::now(),
        };

        // Should be Debug-printable
        let debug_str = format!("{:?}", pending);
        assert!(debug_str.contains("debug-test"));
    }

    #[test]
    fn test_compute_txid() {
        // Test vector: a simple transaction
        let raw_tx = hex::decode(
            "01000000010000000000000000000000000000000000000000000000000000000000000000\
             ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a\
             2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781\
             e62294721166bf621e73a82cbf2342c858eeac00000000",
        )
        .unwrap();

        let txid = compute_txid(&raw_tx);
        // This is the coinbase transaction from block 1
        assert_eq!(
            txid,
            "0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098"
        );
    }

    #[test]
    fn test_validate_originator() {
        // Valid originator
        assert!(validate_originator("app.example.com").is_ok());

        // Empty originator
        assert!(validate_originator("").is_err());

        // Too long originator
        let long_originator = "a".repeat(254);
        assert!(validate_originator(&long_originator).is_err());
    }

    #[test]
    fn test_wallet_options_default() {
        let options = WalletOptions::default();
        assert!(options.include_all_source_transactions);
        assert!(!options.auto_known_txids);
        assert_eq!(options.trust_self, Some("known".to_string()));
    }

    #[test]
    fn test_write_varint() {
        let mut buf = Vec::new();

        // Single byte
        write_varint(&mut buf, 0);
        assert_eq!(buf, vec![0x00]);

        buf.clear();
        write_varint(&mut buf, 252);
        assert_eq!(buf, vec![0xfc]);

        // Two bytes
        buf.clear();
        write_varint(&mut buf, 253);
        assert_eq!(buf, vec![0xfd, 0xfd, 0x00]);
    }

    // Tests for create_keyring_for_verifier helper function
    mod prove_certificate_tests {
        use super::*;
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
        use bsv_rs::primitives::PrivateKey;

        #[test]
        fn test_create_keyring_empty_master_keyring_fails() {
            let subject_key = PrivateKey::random();
            let certifier_key = PrivateKey::random();
            let verifier_key = PrivateKey::random();
            let subject_wallet = ProtoWallet::new(Some(subject_key));

            let fields: HashMap<String, String> =
                HashMap::from([("name".to_string(), BASE64.encode("encrypted_name"))]);
            let master_keyring: HashMap<String, String> = HashMap::new(); // Empty
            let fields_to_reveal = vec!["name".to_string()];

            let result = create_keyring_for_verifier(
                &subject_wallet,
                &certifier_key.public_key(),
                &verifier_key.public_key(),
                &fields,
                &fields_to_reveal,
                &master_keyring,
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", // 32-byte serial
                "test.app.com",
            );

            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("Master keyring is empty"));
        }

        #[test]
        fn test_create_keyring_field_not_in_certificate_fails() {
            let subject_key = PrivateKey::random();
            let certifier_key = PrivateKey::random();
            let verifier_key = PrivateKey::random();
            let subject_wallet = ProtoWallet::new(Some(subject_key));

            let fields: HashMap<String, String> =
                HashMap::from([("name".to_string(), BASE64.encode("encrypted_name"))]);
            let master_keyring: HashMap<String, String> = HashMap::from([
                ("name".to_string(), BASE64.encode("encrypted_key")),
                ("email".to_string(), BASE64.encode("encrypted_key2")),
            ]);
            // Request a field that doesn't exist in certificate
            let fields_to_reveal = vec!["nonexistent".to_string()];

            let result = create_keyring_for_verifier(
                &subject_wallet,
                &certifier_key.public_key(),
                &verifier_key.public_key(),
                &fields,
                &fields_to_reveal,
                &master_keyring,
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                "test.app.com",
            );

            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("not found in certificate"));
        }

        #[test]
        fn test_create_keyring_field_not_in_master_keyring_fails() {
            let subject_key = PrivateKey::random();
            let certifier_key = PrivateKey::random();
            let verifier_key = PrivateKey::random();
            let subject_wallet = ProtoWallet::new(Some(subject_key));

            let fields: HashMap<String, String> = HashMap::from([
                ("name".to_string(), BASE64.encode("encrypted_name")),
                ("email".to_string(), BASE64.encode("encrypted_email")),
            ]);
            // Master keyring missing 'email' key
            let master_keyring: HashMap<String, String> =
                HashMap::from([("name".to_string(), BASE64.encode("encrypted_key"))]);
            let fields_to_reveal = vec!["email".to_string()];

            let result = create_keyring_for_verifier(
                &subject_wallet,
                &certifier_key.public_key(),
                &verifier_key.public_key(),
                &fields,
                &fields_to_reveal,
                &master_keyring,
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                "test.app.com",
            );

            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("not found in master keyring"));
        }

        #[test]
        fn test_create_keyring_invalid_base64_master_key_fails() {
            let subject_key = PrivateKey::random();
            let certifier_key = PrivateKey::random();
            let verifier_key = PrivateKey::random();
            let subject_wallet = ProtoWallet::new(Some(subject_key));

            let fields: HashMap<String, String> =
                HashMap::from([("name".to_string(), BASE64.encode("encrypted_name"))]);
            // Invalid base64
            let master_keyring: HashMap<String, String> =
                HashMap::from([("name".to_string(), "not-valid-base64!!!".to_string())]);
            let fields_to_reveal = vec!["name".to_string()];

            let result = create_keyring_for_verifier(
                &subject_wallet,
                &certifier_key.public_key(),
                &verifier_key.public_key(),
                &fields,
                &fields_to_reveal,
                &master_keyring,
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                "test.app.com",
            );

            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("Failed to decode master key"));
        }

        #[test]
        fn test_certificate_field_encryption_protocol_constant() {
            assert_eq!(
                CERTIFICATE_FIELD_ENCRYPTION_PROTOCOL,
                "certificate field encryption"
            );
        }

        /// End-to-end test for keyring creation that verifies the complete encryption flow:
        /// 1. Create a master keyring by encrypting symmetric keys from subject to certifier
        /// 2. Create fields encrypted with those symmetric keys
        /// 3. Call create_keyring_for_verifier to re-encrypt for verifier
        /// 4. Verify the verifier can decrypt the field using the new keyring
        #[test]
        fn test_create_keyring_end_to_end() {
            use bsv_rs::primitives::{PrivateKey, SymmetricKey};
            use bsv_rs::wallet::{
                DecryptArgs as ProtoDecryptArgs, EncryptArgs as ProtoEncryptArgs,
            };

            // Setup keys for subject, certifier, and verifier
            let subject_key = PrivateKey::random();
            let certifier_key = PrivateKey::random();
            let verifier_key = PrivateKey::random();

            let subject_wallet = ProtoWallet::new(Some(subject_key.clone()));
            let certifier_wallet = ProtoWallet::new(Some(certifier_key.clone()));
            let verifier_wallet = ProtoWallet::new(Some(verifier_key.clone()));

            // Create protocol for certificate field encryption
            let protocol = Protocol::new(
                SecurityLevel::Counterparty,
                CERTIFICATE_FIELD_ENCRYPTION_PROTOCOL,
            );

            // Serial number for the certificate (base64 encoded 32 bytes)
            let serial_bytes = [42u8; 32];
            let serial_number = BASE64.encode(serial_bytes);

            // Step 1: Create a symmetric key for the field "name"
            let field_name = "name";
            let field_plaintext = "Alice Smith";
            let symmetric_key = SymmetricKey::random();

            // Step 2: Encrypt the field value using the symmetric key
            let encrypted_field_value = symmetric_key.encrypt(field_plaintext.as_bytes()).unwrap();
            let encrypted_field_base64 = BASE64.encode(&encrypted_field_value);

            // Step 3: Create the master keyring by encrypting the symmetric key
            // The certifier encrypts the symmetric key for the subject
            let master_key_encrypted = certifier_wallet
                .encrypt(ProtoEncryptArgs {
                    plaintext: symmetric_key.as_bytes().to_vec(),
                    protocol_id: protocol.clone(),
                    key_id: field_name.to_string(),
                    counterparty: Some(Counterparty::Other(subject_key.public_key())),
                })
                .unwrap();
            let master_key_base64 = BASE64.encode(&master_key_encrypted.ciphertext);

            // Build the structures
            let fields: HashMap<String, String> =
                HashMap::from([(field_name.to_string(), encrypted_field_base64.clone())]);
            let master_keyring: HashMap<String, String> =
                HashMap::from([(field_name.to_string(), master_key_base64)]);
            let fields_to_reveal = vec![field_name.to_string()];

            // Step 4: Call create_keyring_for_verifier
            // This should decrypt the master key and re-encrypt for the verifier
            let keyring_for_verifier = create_keyring_for_verifier(
                &subject_wallet,
                &certifier_key.public_key(),
                &verifier_key.public_key(),
                &fields,
                &fields_to_reveal,
                &master_keyring,
                &serial_number,
                "test.app.com",
            )
            .expect("create_keyring_for_verifier should succeed");

            // Verify we got a keyring for the field
            assert_eq!(keyring_for_verifier.len(), 1);
            assert!(keyring_for_verifier.contains_key(field_name));

            // Step 5: Verify the verifier can use the keyring to decrypt the field
            let verifier_encrypted_key = keyring_for_verifier.get(field_name).unwrap();
            let verifier_encrypted_key_bytes = BASE64.decode(verifier_encrypted_key).unwrap();

            // Verifier decrypts the symmetric key using their wallet
            let verifiable_key_id = format!("{} {}", serial_number, field_name);
            let decrypted_key_result = verifier_wallet
                .decrypt(ProtoDecryptArgs {
                    ciphertext: verifier_encrypted_key_bytes,
                    protocol_id: protocol.clone(),
                    key_id: verifiable_key_id,
                    counterparty: Some(Counterparty::Other(subject_key.public_key())),
                })
                .expect("Verifier should be able to decrypt the symmetric key");

            // Reconstruct the symmetric key
            let key_bytes: [u8; 32] = decrypted_key_result
                .plaintext
                .as_slice()
                .try_into()
                .expect("Key should be 32 bytes");
            let recovered_symmetric_key = SymmetricKey::from_bytes(&key_bytes)
                .expect("Should be able to reconstruct symmetric key");

            // Use the symmetric key to decrypt the field value
            let encrypted_field_bytes = BASE64.decode(&encrypted_field_base64).unwrap();
            let decrypted_field = recovered_symmetric_key
                .decrypt(&encrypted_field_bytes)
                .expect("Field decryption should succeed");

            // Verify we got back the original plaintext
            let decrypted_str = String::from_utf8(decrypted_field).unwrap();
            assert_eq!(decrypted_str, field_plaintext);
        }

        /// Test that multiple fields can be revealed correctly
        #[test]
        fn test_create_keyring_multiple_fields() {
            use bsv_rs::primitives::{PrivateKey, SymmetricKey};
            use bsv_rs::wallet::EncryptArgs as ProtoEncryptArgs;

            let subject_key = PrivateKey::random();
            let certifier_key = PrivateKey::random();
            let verifier_key = PrivateKey::random();

            let subject_wallet = ProtoWallet::new(Some(subject_key.clone()));
            let certifier_wallet = ProtoWallet::new(Some(certifier_key.clone()));

            let protocol = Protocol::new(
                SecurityLevel::Counterparty,
                CERTIFICATE_FIELD_ENCRYPTION_PROTOCOL,
            );

            let serial_number = BASE64.encode([1u8; 32]);

            // Create multiple fields
            let field_names = ["name", "email", "organization"];
            let mut fields: HashMap<String, String> = HashMap::new();
            let mut master_keyring: HashMap<String, String> = HashMap::new();

            for field_name in &field_names {
                let symmetric_key = SymmetricKey::random();
                let encrypted_value = symmetric_key.encrypt(b"test value").unwrap();
                fields.insert(field_name.to_string(), BASE64.encode(&encrypted_value));

                let encrypted_key = certifier_wallet
                    .encrypt(ProtoEncryptArgs {
                        plaintext: symmetric_key.as_bytes().to_vec(),
                        protocol_id: protocol.clone(),
                        key_id: field_name.to_string(),
                        counterparty: Some(Counterparty::Other(subject_key.public_key())),
                    })
                    .unwrap();
                master_keyring.insert(
                    field_name.to_string(),
                    BASE64.encode(&encrypted_key.ciphertext),
                );
            }

            // Only reveal name and email, not organization
            let fields_to_reveal = vec!["name".to_string(), "email".to_string()];

            let keyring = create_keyring_for_verifier(
                &subject_wallet,
                &certifier_key.public_key(),
                &verifier_key.public_key(),
                &fields,
                &fields_to_reveal,
                &master_keyring,
                &serial_number,
                "test.app.com",
            )
            .expect("Should succeed");

            // Verify only the requested fields are in the keyring
            assert_eq!(keyring.len(), 2);
            assert!(keyring.contains_key("name"));
            assert!(keyring.contains_key("email"));
            assert!(!keyring.contains_key("organization"));
        }
    }

    // =========================================================================
    // Balance and Sweep tests
    // =========================================================================

    #[test]
    fn test_wallet_balance_struct() {
        let balance = WalletBalance {
            total: 100_000,
            utxos: vec![
                UtxoInfo {
                    satoshis: 60_000,
                    outpoint: "abc123.0".to_string(),
                },
                UtxoInfo {
                    satoshis: 40_000,
                    outpoint: "def456.1".to_string(),
                },
            ],
        };
        assert_eq!(balance.total, 100_000);
        assert_eq!(balance.utxos.len(), 2);
        assert_eq!(balance.utxos[0].satoshis, 60_000);
        assert_eq!(balance.utxos[1].outpoint, "def456.1");
    }

    #[test]
    fn test_utxo_info_struct() {
        let utxo = UtxoInfo {
            satoshis: 50_000,
            outpoint: "aabb.0".to_string(),
        };
        assert_eq!(utxo.satoshis, 50_000);
        assert_eq!(utxo.outpoint, "aabb.0");
    }

    #[test]
    fn test_wallet_balance_empty() {
        let balance = WalletBalance {
            total: 0,
            utxos: Vec::new(),
        };
        assert_eq!(balance.total, 0);
        assert!(balance.utxos.is_empty());
    }

    #[test]
    fn test_wallet_balance_serialization() {
        let balance = WalletBalance {
            total: 12345,
            utxos: vec![UtxoInfo {
                satoshis: 12345,
                outpoint: "txid.0".to_string(),
            }],
        };
        let json = serde_json::to_string(&balance).expect("serialize");
        assert!(json.contains("\"total\":12345"));
        assert!(json.contains("\"satoshis\":12345"));
        assert!(json.contains("\"outpoint\":\"txid.0\""));
    }

    #[test]
    fn test_address_to_p2pkh_script_mainnet() {
        let script =
            address_to_p2pkh_script("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa").expect("valid address");
        assert_eq!(script.len(), 25);
        assert_eq!(script[0], 0x76); // OP_DUP
        assert_eq!(script[1], 0xa9); // OP_HASH160
        assert_eq!(script[2], 0x14); // PUSH 20 bytes
        assert_eq!(script[23], 0x88); // OP_EQUALVERIFY
        assert_eq!(script[24], 0xac); // OP_CHECKSIG
    }

    #[test]
    fn test_address_to_p2pkh_script_invalid() {
        assert!(address_to_p2pkh_script("0OIl").is_err());
        assert!(address_to_p2pkh_script("1").is_err());
    }

    // =========================================================================
    // Direct certificate acquisition conversion tests
    // =========================================================================

    #[test]
    fn test_build_wallet_certificate_from_args_direct() {
        use bsv_rs::wallet::AcquisitionProtocol;

        let mut fields = HashMap::new();
        fields.insert("name".to_string(), "encrypted_name_value".to_string());
        fields.insert("email".to_string(), "encrypted_email_value".to_string());

        let args = AcquireCertificateArgs {
            certificate_type: "dGVzdC1jZXJ0LXR5cGU=".to_string(),
            certifier: "02".to_string() + &"ab".repeat(32),
            acquisition_protocol: AcquisitionProtocol::Direct,
            fields: fields.clone(),
            serial_number: Some("c2VyaWFsLW51bWJlcg==".to_string()),
            revocation_outpoint: Some("abc123.0".to_string()),
            signature: Some("deadbeef".to_string()),
            certifier_url: None,
            keyring_revealer: None,
            keyring_for_subject: None,
            privileged: None,
            privileged_reason: None,
        };

        let cert = build_wallet_certificate_from_args(&args).expect("should build certificate");

        assert_eq!(cert.certificate_type, "dGVzdC1jZXJ0LXR5cGU=");
        assert_eq!(cert.serial_number, "c2VyaWFsLW51bWJlcg==");
        assert_eq!(cert.revocation_outpoint, "abc123.0");
        assert_eq!(cert.signature, "deadbeef");
        assert_eq!(cert.fields.len(), 2);
        assert_eq!(cert.fields.get("name").unwrap(), "encrypted_name_value");
        assert_eq!(cert.fields.get("email").unwrap(), "encrypted_email_value");
    }

    #[test]
    fn test_build_wallet_certificate_from_args_missing_serial_number() {
        use bsv_rs::wallet::AcquisitionProtocol;

        let args = AcquireCertificateArgs {
            certificate_type: "dGVzdA==".to_string(),
            certifier: "02".to_string() + &"ab".repeat(32),
            acquisition_protocol: AcquisitionProtocol::Direct,
            fields: HashMap::new(),
            serial_number: None,
            revocation_outpoint: Some("abc.0".to_string()),
            signature: Some("sig".to_string()),
            certifier_url: None,
            keyring_revealer: None,
            keyring_for_subject: None,
            privileged: None,
            privileged_reason: None,
        };

        let result = build_wallet_certificate_from_args(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_direct_certificate_table_conversion() {
        // Verify that a WalletCertificate can be correctly mapped to TableCertificate
        // and TableCertificateField entries (the conversion pattern used in acquire_certificate)

        let identity_key = "02".to_string() + &"cc".repeat(32);
        let user_id = 42i64;

        let certificate = WalletCertificate {
            certificate_type: "dGVzdA==".to_string(),
            serial_number: "c2VyaWFs".to_string(),
            subject: identity_key.clone(),
            certifier: "02".to_string() + &"ab".repeat(32),
            revocation_outpoint: "txid.0".to_string(),
            signature: "deadbeef".to_string(),
            fields: HashMap::from([
                ("name".to_string(), "enc_name".to_string()),
                ("email".to_string(), "enc_email".to_string()),
            ]),
        };

        let keyring: HashMap<String, String> = HashMap::from([
            ("name".to_string(), "master_key_name".to_string()),
            ("email".to_string(), "master_key_email".to_string()),
        ]);

        let now = chrono::Utc::now();

        // Build table cert (same pattern as acquire_certificate direct path)
        let table_cert = TableCertificate {
            certificate_id: 0,
            user_id,
            cert_type: certificate.certificate_type.clone(),
            serial_number: certificate.serial_number.clone(),
            certifier: certificate.certifier.clone(),
            subject: certificate.subject.clone(),
            verifier: Some(certificate.certifier.clone()),
            revocation_outpoint: certificate.revocation_outpoint.clone(),
            signature: certificate.signature.clone(),
            created_at: now,
            updated_at: now,
        };

        assert_eq!(table_cert.cert_type, "dGVzdA==");
        assert_eq!(table_cert.serial_number, "c2VyaWFs");
        assert_eq!(table_cert.subject, identity_key);
        assert_eq!(table_cert.user_id, 42);

        // Build table fields (same pattern as acquire_certificate direct path)
        let cert_id = 99i64;
        let mut table_fields: Vec<TableCertificateField> = Vec::new();
        for (field_name, field_value) in &certificate.fields {
            let master_key = Some(&keyring)
                .and_then(|kr| kr.get(field_name))
                .cloned()
                .unwrap_or_default();
            table_fields.push(TableCertificateField {
                certificate_field_id: 0,
                certificate_id: cert_id,
                user_id,
                field_name: field_name.clone(),
                field_value: field_value.clone(),
                master_key,
                created_at: now,
                updated_at: now,
            });
        }

        assert_eq!(table_fields.len(), 2);
        for f in &table_fields {
            assert_eq!(f.certificate_id, 99);
            assert_eq!(f.user_id, 42);
            assert!(!f.master_key.is_empty());
            if f.field_name == "name" {
                assert_eq!(f.field_value, "enc_name");
                assert_eq!(f.master_key, "master_key_name");
            } else if f.field_name == "email" {
                assert_eq!(f.field_value, "enc_email");
                assert_eq!(f.master_key, "master_key_email");
            }
        }
    }

    #[test]
    fn test_direct_certificate_field_without_keyring() {
        // When no keyring_for_subject is provided, master_key should be empty string

        let now = chrono::Utc::now();
        let keyring: Option<&HashMap<String, String>> = None;

        let master_key = keyring
            .and_then(|kr| kr.get("name"))
            .cloned()
            .unwrap_or_default();

        let field = TableCertificateField {
            certificate_field_id: 0,
            certificate_id: 1,
            user_id: 1,
            field_name: "name".to_string(),
            field_value: "enc_value".to_string(),
            master_key,
            created_at: now,
            updated_at: now,
        };

        assert_eq!(field.master_key, "");
    }
}
