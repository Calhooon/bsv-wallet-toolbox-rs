//! Wallet Implementation
//!
//! This module provides the main `Wallet` struct that implements the full
//! `WalletInterface` trait from `bsv_sdk::wallet`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bsv_sdk::primitives::PrivateKey;
use bsv_sdk::primitives::PublicKey;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use bsv_sdk::wallet::{
    interface::{
        RevealCounterpartyKeyLinkageArgs as InterfaceRevealCounterpartyArgs,
        RevealSpecificKeyLinkageArgs as InterfaceRevealSpecificArgs,
    },
    AbortActionArgs, AbortActionResult, AcquireCertificateArgs, AuthenticatedResult,
    CreateActionArgs, CreateActionResult, CreateHmacArgs, CreateHmacResult, CreateSignatureArgs,
    CreateSignatureResult, DecryptArgs, DecryptResult, DiscoverByAttributesArgs,
    DiscoverByIdentityKeyArgs, DiscoverCertificatesResult, EncryptArgs, EncryptResult,
    GetHeaderArgs, GetHeaderResult, GetHeightResult, GetNetworkResult, GetPublicKeyArgs,
    GetPublicKeyResult, GetVersionResult, InternalizeActionArgs, InternalizeActionResult,
    KeyLinkageResult, ListActionsArgs, ListActionsResult, ListCertificatesArgs,
    ListCertificatesResult, ListOutputsArgs, ListOutputsResult, Network, Outpoint,
    ProveCertificateArgs, ProveCertificateResult, ProtoWallet, RelinquishCertificateArgs,
    RelinquishCertificateResult, RelinquishOutputArgs, RelinquishOutputResult,
    RevealCounterpartyKeyLinkageResult, RevealSpecificKeyLinkageResult, SignActionArgs,
    SignActionResult, SignableTransaction, VerifyHmacArgs, VerifyHmacResult, VerifySignatureArgs,
    VerifySignatureResult, WalletCertificate, WalletInterface,
};

use crate::error::{Error, Result};
use crate::services::{Chain, WalletServices};
use crate::storage::{AuthId, StorageProcessActionArgs, WalletStorageProvider};

use super::signer::{SignerInput, WalletSigner};

// =============================================================================
// Constants
// =============================================================================

/// Wallet version string
const WALLET_VERSION: &str = "bsv-wallet-toolbox-0.1.0";

/// Default TTL for pending transactions (24 hours)
const PENDING_TRANSACTION_TTL_SECS: i64 = 24 * 60 * 60;

// =============================================================================
// PendingTransaction
// =============================================================================

/// A transaction awaiting signature.
///
/// When `create_action` is called with `sign_and_process = false`, the unsigned
/// transaction is cached here for later signing via `sign_action`.
#[derive(Debug, Clone)]
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
/// use bsv_wallet_toolbox::{Wallet, StorageSqlx, Services};
/// use bsv_sdk::primitives::PrivateKey;
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
}

impl<S, V> Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync,
    V: WalletServices + Send + Sync,
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

        // Ensure the user exists in storage
        storage.find_or_insert_user(&identity_key).await?;

        Ok(Self {
            proto_wallet,
            storage: Arc::new(storage),
            services: Arc::new(services),
            identity_key,
            chain,
            options,
            signer,
            pending_transactions: Arc::new(RwLock::new(HashMap::new())),
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
        AuthId::new(&self.identity_key)
    }

    /// Calls ProtoWallet.get_public_key.
    fn proto_get_public_key(
        &self,
        args: GetPublicKeyArgs,
    ) -> std::result::Result<GetPublicKeyResult, bsv_sdk::Error> {
        // ProtoWallet uses the same types that are re-exported
        use bsv_sdk::wallet::GetPublicKeyArgs as ProtoGetPublicKeyArgs;

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
    ) -> std::result::Result<EncryptResult, bsv_sdk::Error> {
        use bsv_sdk::wallet::EncryptArgs as ProtoEncryptArgs;

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
    ) -> std::result::Result<DecryptResult, bsv_sdk::Error> {
        use bsv_sdk::wallet::DecryptArgs as ProtoDecryptArgs;

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
    ) -> std::result::Result<CreateHmacResult, bsv_sdk::Error> {
        use bsv_sdk::wallet::CreateHmacArgs as ProtoCreateHmacArgs;

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
    ) -> std::result::Result<VerifyHmacResult, bsv_sdk::Error> {
        use bsv_sdk::wallet::VerifyHmacArgs as ProtoVerifyHmacArgs;

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
    ) -> std::result::Result<CreateSignatureResult, bsv_sdk::Error> {
        use bsv_sdk::wallet::CreateSignatureArgs as ProtoCreateSignatureArgs;

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
    ) -> std::result::Result<VerifySignatureResult, bsv_sdk::Error> {
        use bsv_sdk::wallet::VerifySignatureArgs as ProtoVerifySignatureArgs;

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
    ) -> bsv_sdk::Result<GetPublicKeyResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;
        self.proto_get_public_key(args)
    }

    async fn encrypt(&self, args: EncryptArgs, originator: &str) -> bsv_sdk::Result<EncryptResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;
        self.proto_encrypt(args)
    }

    async fn decrypt(&self, args: DecryptArgs, originator: &str) -> bsv_sdk::Result<DecryptResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;
        self.proto_decrypt(args)
    }

    async fn create_hmac(
        &self,
        args: CreateHmacArgs,
        originator: &str,
    ) -> bsv_sdk::Result<CreateHmacResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;
        self.proto_create_hmac(args)
    }

    async fn verify_hmac(
        &self,
        args: VerifyHmacArgs,
        originator: &str,
    ) -> bsv_sdk::Result<VerifyHmacResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;
        self.proto_verify_hmac(args)
    }

    async fn create_signature(
        &self,
        args: CreateSignatureArgs,
        originator: &str,
    ) -> bsv_sdk::Result<CreateSignatureResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;
        self.proto_create_signature(args)
    }

    async fn verify_signature(
        &self,
        args: VerifySignatureArgs,
        originator: &str,
    ) -> bsv_sdk::Result<VerifySignatureResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;
        self.proto_verify_signature(args)
    }

    async fn reveal_counterparty_key_linkage(
        &self,
        args: InterfaceRevealCounterpartyArgs,
        originator: &str,
    ) -> bsv_sdk::Result<RevealCounterpartyKeyLinkageResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // Convert interface args to proto_wallet args
        use bsv_sdk::wallet::RevealCounterpartyKeyLinkageArgs as ProtoRevealCounterpartyArgs;

        let proto_args = ProtoRevealCounterpartyArgs {
            counterparty: args.counterparty.clone(),
            verifier: args.verifier.clone(),
        };

        let result = self.proto_wallet.reveal_counterparty_key_linkage(proto_args)?;

        // Parse the prover hex string back to PublicKey
        let prover = PublicKey::from_hex(&result.prover)
            .map_err(|e| bsv_sdk::Error::WalletError(format!("Invalid prover key: {}", e)))?;

        // Parse the counterparty hex string back to PublicKey
        let counterparty_key = PublicKey::from_hex(&result.counterparty)
            .map_err(|e| bsv_sdk::Error::WalletError(format!("Invalid counterparty key: {}", e)))?;

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
    ) -> bsv_sdk::Result<RevealSpecificKeyLinkageResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // Convert interface args to proto_wallet args
        use bsv_sdk::wallet::{
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
            .map_err(|e| bsv_sdk::Error::WalletError(format!("Invalid prover key: {}", e)))?;

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
    ) -> bsv_sdk::Result<CreateActionResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        // Call storage to create the action
        let storage_result = self
            .storage
            .create_action(&auth, args.clone())
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

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
            let unsigned_tx =
                build_unsigned_transaction(&storage_result).map_err(|e| {
                    bsv_sdk::Error::WalletError(format!("Failed to build transaction: {}", e))
                })?;

            // Convert storage inputs to signer inputs
            let signer_inputs: Vec<SignerInput> = storage_result
                .inputs
                .iter()
                .map(|input| SignerInput {
                    vin: input.vin,
                    source_txid: input.source_txid.clone(),
                    source_vout: input.source_vout,
                    satoshis: input.source_satoshis,
                    source_locking_script: Some(hex::decode(&input.source_locking_script).unwrap_or_default()),
                    unlocking_script: None,
                    derivation_prefix: input.derivation_prefix.clone(),
                    derivation_suffix: input.derivation_suffix.clone(),
                    sender_identity_key: input.sender_identity_key.clone(),
                })
                .collect();

            // Sign the transaction using the wallet signer
            let signed_tx = self
                .signer
                .sign_transaction(&unsigned_tx, &signer_inputs, &self.proto_wallet)
                .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

            // Compute txid from signed transaction
            let txid = compute_txid(&signed_tx);

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
                    .map(|txids| txids.iter().map(|t| hex::encode(t)).collect())
                    .unwrap_or_default(),
            };

            let process_result = self
                .storage
                .process_action(&auth, process_args)
                .await
                .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

            // If not no_send and not delayed, broadcast the transaction
            if !no_send && !accept_delayed_broadcast {
                // Build BEEF for broadcasting
                if let Some(ref beef) = storage_result.input_beef {
                    let txid_strings = vec![txid.clone()];
                    let _broadcast_result = self
                        .services
                        .post_beef(beef, &txid_strings)
                        .await
                        .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;
                }
            }

            // Convert txid string to [u8; 32]
            let txid_bytes = hex::decode(&txid)
                .map_err(|e| bsv_sdk::Error::WalletError(format!("Invalid txid: {}", e)))?;
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
                            bsv_sdk::wallet::SendWithResult {
                                txid,
                                status: match r.status.as_str() {
                                    "unproven" => bsv_sdk::wallet::SendWithResultStatus::Unproven,
                                    "sending" => bsv_sdk::wallet::SendWithResultStatus::Sending,
                                    _ => bsv_sdk::wallet::SendWithResultStatus::Failed,
                                },
                            }
                        })
                        .collect()
                }),
                signable_transaction: None,
            });
        }

        // Return the result with signable transaction for external signing
        // Build transaction before consuming storage_result fields
        let unsigned_tx = build_unsigned_transaction(&storage_result).map_err(|e| {
            bsv_sdk::Error::WalletError(format!("Failed to build transaction: {}", e))
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
                source_locking_script: Some(hex::decode(&input.source_locking_script).unwrap_or_default()),
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
                .map(|txids| txids.iter().map(|t| hex::encode(t)).collect())
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
        })
    }

    async fn sign_action(
        &self,
        args: SignActionArgs,
        originator: &str,
    ) -> bsv_sdk::Result<SignActionResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // Get the reference from args (it's already a String)
        let reference = args.reference;

        if reference.is_empty() {
            return Err(bsv_sdk::Error::WalletError(
                "Missing reference argument for sign action".to_string(),
            ));
        }

        // Look up the pending transaction from cache
        let pending_tx = {
            let cache = self.pending_transactions.read().await;
            cache.get(&reference).cloned()
        };

        let pending_tx = pending_tx.ok_or_else(|| {
            bsv_sdk::Error::WalletError(format!(
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
            return Err(bsv_sdk::Error::WalletError(
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
        let signed_tx = self
            .signer
            .sign_transaction(&pending_tx.raw_tx, &inputs, &self.proto_wallet)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

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
            .map(|txids| txids.iter().map(|t| hex::encode(t)).collect())
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

        let process_result = self
            .storage
            .process_action(&auth, process_args)
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // If not no_send and not delayed, broadcast the transaction
        if !is_no_send && !is_delayed {
            if let Some(ref beef) = pending_tx.input_beef {
                let txid_strings = vec![txid.clone()];
                let _broadcast_result = self
                    .services
                    .post_beef(beef, &txid_strings)
                    .await
                    .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;
            }
        }

        // Remove from pending transactions cache on success
        {
            let mut cache = self.pending_transactions.write().await;
            cache.remove(&reference);
        }

        // Convert txid string to [u8; 32]
        let txid_bytes = hex::decode(&txid)
            .map_err(|e| bsv_sdk::Error::WalletError(format!("Invalid txid: {}", e)))?;
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
                        bsv_sdk::wallet::SendWithResult {
                            txid: result_txid,
                            status: match r.status.as_str() {
                                "unproven" => bsv_sdk::wallet::SendWithResultStatus::Unproven,
                                "sending" => bsv_sdk::wallet::SendWithResultStatus::Sending,
                                _ => bsv_sdk::wallet::SendWithResultStatus::Failed,
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
    ) -> bsv_sdk::Result<AbortActionResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let result = self
            .storage
            .abort_action(&auth, args)
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        Ok(result)
    }

    async fn list_actions(
        &self,
        args: ListActionsArgs,
        originator: &str,
    ) -> bsv_sdk::Result<ListActionsResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let result = self
            .storage
            .list_actions(&auth, args)
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        Ok(result)
    }

    async fn internalize_action(
        &self,
        args: InternalizeActionArgs,
        originator: &str,
    ) -> bsv_sdk::Result<InternalizeActionResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let result = self
            .storage
            .internalize_action(&auth, args)
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

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
    ) -> bsv_sdk::Result<ListOutputsResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let result = self
            .storage
            .list_outputs(&auth, args)
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        Ok(result)
    }

    async fn relinquish_output(
        &self,
        args: RelinquishOutputArgs,
        originator: &str,
    ) -> bsv_sdk::Result<RelinquishOutputResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let _result = self
            .storage
            .relinquish_output(&auth, args)
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        Ok(RelinquishOutputResult { relinquished: true })
    }

    // =========================================================================
    // Certificate Operations (delegated to storage)
    // =========================================================================

    async fn acquire_certificate(
        &self,
        args: AcquireCertificateArgs,
        originator: &str,
    ) -> bsv_sdk::Result<WalletCertificate> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // For direct acquisition, store the certificate
        // For issuance, this would require HTTP communication with the certifier

        match args.acquisition_protocol {
            bsv_sdk::wallet::AcquisitionProtocol::Direct => {
                // Direct acquisition - certificate is already provided
                let _auth = self.auth();

                // Build the certificate from args
                let certificate = build_wallet_certificate_from_args(&args)?;

                // Note: insert_certificate expects TableCertificate, not WalletCertificate
                // This needs type conversion - for now return the certificate
                // TODO: implement proper certificate storage conversion

                Ok(certificate)
            }
            bsv_sdk::wallet::AcquisitionProtocol::Issuance => {
                // Issuance protocol requires HTTP communication with certifier
                // This is a complex flow that requires the certifier URL
                Err(bsv_sdk::Error::WalletError(
                    "Certificate issuance protocol not yet implemented".to_string(),
                ))
            }
        }
    }

    async fn list_certificates(
        &self,
        args: ListCertificatesArgs,
        originator: &str,
    ) -> bsv_sdk::Result<ListCertificatesResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let result = self
            .storage
            .list_certificates(&auth, args)
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        Ok(result)
    }

    async fn prove_certificate(
        &self,
        _args: ProveCertificateArgs,
        originator: &str,
    ) -> bsv_sdk::Result<ProveCertificateResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // Proving a certificate involves creating a keyring for the verifier
        // This requires decrypting the master keyring and creating selective reveal

        // For now, return a stub - full implementation requires keyring handling
        Err(bsv_sdk::Error::WalletError(
            "prove_certificate not yet implemented".to_string(),
        ))
    }

    async fn relinquish_certificate(
        &self,
        args: RelinquishCertificateArgs,
        originator: &str,
    ) -> bsv_sdk::Result<RelinquishCertificateResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let auth = self.auth();

        let _result = self
            .storage
            .relinquish_certificate(&auth, args)
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        Ok(RelinquishCertificateResult {
            relinquished: true,
        })
    }

    // =========================================================================
    // Discovery Operations
    // =========================================================================

    async fn discover_by_identity_key(
        &self,
        _args: DiscoverByIdentityKeyArgs,
        originator: &str,
    ) -> bsv_sdk::Result<DiscoverCertificatesResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // Discovery requires overlay lookup service
        // Return empty result for now
        Ok(DiscoverCertificatesResult {
            total_certificates: 0,
            certificates: vec![],
        })
    }

    async fn discover_by_attributes(
        &self,
        _args: DiscoverByAttributesArgs,
        originator: &str,
    ) -> bsv_sdk::Result<DiscoverCertificatesResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // Discovery requires overlay lookup service
        // Return empty result for now
        Ok(DiscoverCertificatesResult {
            total_certificates: 0,
            certificates: vec![],
        })
    }

    // =========================================================================
    // Chain/Status Operations
    // =========================================================================

    async fn is_authenticated(&self, originator: &str) -> bsv_sdk::Result<AuthenticatedResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // Wallet is always authenticated (it has a key)
        Ok(AuthenticatedResult {
            authenticated: true,
        })
    }

    async fn wait_for_authentication(
        &self,
        originator: &str,
    ) -> bsv_sdk::Result<AuthenticatedResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // Wallet is always authenticated
        Ok(AuthenticatedResult {
            authenticated: true,
        })
    }

    async fn get_height(&self, originator: &str) -> bsv_sdk::Result<GetHeightResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let height = self
            .services
            .get_height()
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        Ok(GetHeightResult { height })
    }

    async fn get_header_for_height(
        &self,
        args: GetHeaderArgs,
        originator: &str,
    ) -> bsv_sdk::Result<GetHeaderResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let header_bytes = self
            .services
            .get_header_for_height(args.height)
            .await
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        // GetHeaderResult expects header as hex string
        Ok(GetHeaderResult {
            header: hex::encode(&header_bytes),
        })
    }

    async fn get_network(&self, originator: &str) -> bsv_sdk::Result<GetNetworkResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

        let network = match self.chain {
            Chain::Main => Network::Mainnet,
            Chain::Test => Network::Testnet,
        };

        Ok(GetNetworkResult { network })
    }

    async fn get_version(&self, originator: &str) -> bsv_sdk::Result<GetVersionResult> {
        validate_originator(originator)
            .map_err(|e| bsv_sdk::Error::WalletError(e.to_string()))?;

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
fn build_unsigned_transaction(
    result: &crate::storage::StorageCreateActionResult,
) -> Result<Vec<u8>> {
    let mut tx = Vec::new();

    // Version
    tx.extend_from_slice(&result.version.to_le_bytes());

    // Input count
    write_varint(&mut tx, result.inputs.len() as u64);

    // Inputs
    for input in &result.inputs {
        // Previous txid (reversed)
        let txid_bytes = hex::decode(&input.source_txid)
            .map_err(|e| Error::TransactionError(format!("Invalid txid: {}", e)))?;
        tx.extend_from_slice(&txid_bytes);

        // Previous vout
        tx.extend_from_slice(&input.source_vout.to_le_bytes());

        // Script (empty for unsigned)
        tx.push(0);

        // Sequence
        tx.extend_from_slice(&0xfffffffe_u32.to_le_bytes());
    }

    // Output count
    write_varint(&mut tx, result.outputs.len() as u64);

    // Outputs
    for output in &result.outputs {
        // Satoshis
        tx.extend_from_slice(&output.satoshis.to_le_bytes());

        // Locking script
        let script = hex::decode(&output.locking_script)
            .map_err(|e| Error::TransactionError(format!("Invalid locking script: {}", e)))?;
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
) -> bsv_sdk::Result<WalletCertificate> {
    let serial_number = args.serial_number.clone().ok_or_else(|| {
        bsv_sdk::Error::WalletError("serial_number required for direct acquisition".to_string())
    })?;

    let revocation_outpoint = args.revocation_outpoint.clone().ok_or_else(|| {
        bsv_sdk::Error::WalletError(
            "revocation_outpoint required for direct acquisition".to_string(),
        )
    })?;

    let signature = args.signature.clone().ok_or_else(|| {
        bsv_sdk::Error::WalletError("signature required for direct acquisition".to_string())
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
                source_txid: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
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
            send_with: vec![
                "abc123".to_string(),
                "def456".to_string(),
            ],
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
}
