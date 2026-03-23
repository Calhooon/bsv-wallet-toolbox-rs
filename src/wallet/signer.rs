//! Wallet Signer
//!
//! This module provides the `WalletSigner` struct for signing transaction inputs
//! using derived keys from the wallet's ProtoWallet.

use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::{Counterparty, KeyDeriverApi, ProtoWallet};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

// =============================================================================
// UnlockingScriptTemplate Types
// =============================================================================

/// Template for generating unlocking scripts via BRC-29 key derivation.
/// Used for deferred signing where the wallet controls the keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnlockingScriptTemplate {
    /// Key derivation prefix (hex).
    pub derivation_prefix: String,
    /// Key derivation suffix (hex).
    pub derivation_suffix: String,
    /// The type of script to generate.
    pub script_type: ScriptType,
    /// Satoshis for this input (required for correct BIP-143 sighash).
    pub satoshis: u64,
}

/// Supported script types for template-based signing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScriptType {
    /// Pay-to-Public-Key-Hash
    P2PKH,
    /// Pay-to-Public-Key
    P2PK,
}

/// Input details from storage for transaction creation.
///
/// This is used to pass input information to the signer.
/// The full type is in `crate::storage::StorageCreateTransactionInput`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignerInput {
    pub vin: u32,
    pub source_txid: String,
    pub source_vout: u32,
    pub satoshis: u64,
    pub source_locking_script: Option<Vec<u8>>,
    pub unlocking_script: Option<Vec<u8>>,
    pub derivation_prefix: Option<String>,
    pub derivation_suffix: Option<String>,
    pub sender_identity_key: Option<String>,
}

// =============================================================================
// WalletSigner
// =============================================================================

/// Handles transaction signing for the wallet.
///
/// The `WalletSigner` uses key derivation to sign transaction inputs based on
/// their derivation paths (prefix and suffix). It integrates with the ProtoWallet's
/// key deriver to compute the appropriate signing keys.
///
/// # Example
///
/// ```rust,ignore
/// let signer = WalletSigner::new(Some(root_key));
/// let signed_tx = signer.sign_transaction(&unsigned_tx, &inputs, &proto_wallet)?;
/// ```
#[derive(Debug)]
pub struct WalletSigner {
    /// Root private key for key derivation
    #[allow(dead_code)]
    root_key: Option<PrivateKey>,
}

impl WalletSigner {
    /// Creates a new WalletSigner.
    ///
    /// # Arguments
    ///
    /// * `root_key` - The root private key for key derivation. If None, uses "anyone" key.
    pub fn new(root_key: Option<PrivateKey>) -> Self {
        Self { root_key }
    }

    /// Signs a transaction using the provided inputs and ProtoWallet.
    ///
    /// This method:
    /// 1. Parses the unsigned transaction
    /// 2. For each input that needs signing:
    ///    - Derives the signing key using the input's derivation path
    ///    - Creates the signature
    ///    - Constructs the unlocking script
    /// 3. Returns the fully signed transaction
    ///
    /// # Arguments
    ///
    /// * `unsigned_tx` - The unsigned transaction bytes (from create_action)
    /// * `inputs` - Input metadata including derivation info
    /// * `proto_wallet` - The ProtoWallet for key derivation
    ///
    /// # Returns
    ///
    /// The signed transaction bytes.
    pub fn sign_transaction(
        &self,
        unsigned_tx: &[u8],
        inputs: &[SignerInput],
        proto_wallet: &ProtoWallet,
    ) -> Result<Vec<u8>> {
        // Parse the unsigned transaction
        let mut tx_data = unsigned_tx.to_vec();

        // For each input that has derivation info, we need to sign it
        for (vin, input) in inputs.iter().enumerate() {
            // Skip inputs that don't need signing (have unlocking script already)
            if input.unlocking_script.is_some() {
                continue;
            }

            // Get the derivation info
            let derivation_prefix = input.derivation_prefix.as_ref().ok_or_else(|| {
                Error::ValidationError(format!(
                    "Input {} requires signing but has no derivation_prefix",
                    vin
                ))
            })?;

            let derivation_suffix = input.derivation_suffix.as_ref().ok_or_else(|| {
                Error::ValidationError(format!(
                    "Input {} requires signing but has no derivation_suffix",
                    vin
                ))
            })?;

            // Get the source locking script to determine script type
            let locking_script = input.source_locking_script.as_ref().ok_or_else(|| {
                Error::ValidationError(format!(
                    "Input {} requires signing but has no source_locking_script",
                    vin
                ))
            })?;

            // Determine the counterparty from sender_identity_key
            let counterparty = if let Some(ref sender_key) = input.sender_identity_key {
                // If there's a sender key, use it as counterparty
                let pubkey = bsv_rs::primitives::PublicKey::from_hex(sender_key)
                    .map_err(|e| Error::ValidationError(format!("Invalid sender key: {}", e)))?;
                Counterparty::Other(pubkey)
            } else {
                // Default to self
                Counterparty::Self_
            };

            // Derive the private key for signing using BRC-29 (SABPPP) protocol
            // BRC-29 uses:
            // - Security level 2 (Counterparty)
            // - Protocol name: "3241645161d8"
            // - Key ID: "{derivation_prefix} {derivation_suffix}" (WITH SPACE)
            // This produces invoice number: "2-3241645161d8-{prefix} {suffix}"
            use bsv_rs::wallet::{Protocol, SecurityLevel};

            let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
            let key_id = format!("{} {}", derivation_prefix, derivation_suffix);

            tracing::debug!(
                derivation_prefix = %derivation_prefix,
                derivation_suffix = %derivation_suffix,
                key_id = %key_id,
                "Deriving key for input using BRC-29 protocol"
            );

            let signing_key = proto_wallet
                .key_deriver()
                .derive_private_key(&brc29_protocol, &key_id, &counterparty)
                .map_err(|e| Error::TransactionError(format!("Key derivation failed: {}", e)))?;

            // Create the sighash for this input
            // This depends on the script type (P2PKH, P2PK, etc.)
            let sighash = compute_sighash(&tx_data, vin as u32, locking_script, input.satoshis)?;

            // Sign the sighash
            let signature = signing_key
                .sign(&sighash)
                .map_err(|e| Error::TransactionError(format!("Signing failed: {}", e)))?;

            // Get the public key for the unlocking script
            let pubkey = signing_key.public_key();

            // Validate that the derived pubkey matches the locking script (P2PKH only)
            let pubkey_compressed = pubkey.to_compressed();
            let pubkey_hash = hash160(&pubkey_compressed);
            validate_p2pkh_pubkey_match(&pubkey_hash, locking_script)?;

            // Build the unlocking script based on script type
            let unlocking_script = build_unlocking_script(
                locking_script,
                &signature.to_der(),
                &pubkey_compressed,
            )?;

            // Insert the unlocking script into the transaction
            tx_data = insert_unlocking_script(&tx_data, vin as u32, &unlocking_script)?;
        }

        // Inject user-provided unlocking scripts for external inputs
        // (these were skipped in the loop above)
        for (vin, input) in inputs.iter().enumerate() {
            if let Some(ref script) = input.unlocking_script {
                tx_data = insert_unlocking_script(&tx_data, vin as u32, script)?;
            }
        }

        Ok(tx_data)
    }

    /// Signs a single input and returns the unlocking script.
    ///
    /// This is useful when you need to sign inputs individually rather than
    /// all at once.
    ///
    /// # Arguments
    ///
    /// * `tx_data` - The transaction data
    /// * `input_index` - Index of the input to sign
    /// * `input` - Input metadata
    /// * `proto_wallet` - The ProtoWallet for key derivation
    ///
    /// # Returns
    ///
    /// The unlocking script bytes.
    pub fn sign_input(
        &self,
        tx_data: &[u8],
        input_index: u32,
        input: &SignerInput,
        proto_wallet: &ProtoWallet,
    ) -> Result<Vec<u8>> {
        let derivation_prefix = input.derivation_prefix.as_ref().ok_or_else(|| {
            Error::ValidationError(format!(
                "Input {} requires signing but has no derivation_prefix",
                input_index
            ))
        })?;

        let derivation_suffix = input.derivation_suffix.as_ref().ok_or_else(|| {
            Error::ValidationError(format!(
                "Input {} requires signing but has no derivation_suffix",
                input_index
            ))
        })?;

        let locking_script = input.source_locking_script.as_ref().ok_or_else(|| {
            Error::ValidationError(format!(
                "Input {} requires signing but has no source_locking_script",
                input_index
            ))
        })?;

        let counterparty = if let Some(ref sender_key) = input.sender_identity_key {
            let pubkey = bsv_rs::primitives::PublicKey::from_hex(sender_key)
                .map_err(|e| Error::ValidationError(format!("Invalid sender key: {}", e)))?;
            Counterparty::Other(pubkey)
        } else {
            Counterparty::Self_
        };

        // Derive the private key using BRC-29 (SABPPP) protocol
        // Same as sign_transaction: SecurityLevel::Counterparty, protocol "3241645161d8"
        // Key ID: "{derivation_prefix} {derivation_suffix}" (WITH SPACE)
        use bsv_rs::wallet::{Protocol, SecurityLevel};

        let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
        let key_id = format!("{} {}", derivation_prefix, derivation_suffix);

        let signing_key = proto_wallet
            .key_deriver()
            .derive_private_key(&brc29_protocol, &key_id, &counterparty)
            .map_err(|e| Error::TransactionError(format!("Key derivation failed: {}", e)))?;

        let sighash = compute_sighash(tx_data, input_index, locking_script, input.satoshis)?;

        let signature = signing_key
            .sign(&sighash)
            .map_err(|e| Error::TransactionError(format!("Signing failed: {}", e)))?;

        let pubkey = signing_key.public_key();

        // Validate that the derived pubkey matches the locking script (P2PKH only)
        let pubkey_compressed = pubkey.to_compressed();
        let pubkey_hash = hash160(&pubkey_compressed);
        validate_p2pkh_pubkey_match(&pubkey_hash, locking_script)?;

        build_unlocking_script(locking_script, &signature.to_der(), &pubkey_compressed)
    }

    /// Create an unlocking script template for deferred signing.
    ///
    /// This creates a template that captures the key derivation parameters
    /// and script type needed to generate an unlocking script later.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Key derivation prefix (hex)
    /// * `suffix` - Key derivation suffix (hex)
    /// * `script_type` - The type of script to generate (P2PKH or P2PK)
    /// * `satoshis` - The satoshi value of the input (required for BIP-143 sighash)
    pub fn create_unlock_template(
        &self,
        prefix: &str,
        suffix: &str,
        script_type: ScriptType,
        satoshis: u64,
    ) -> UnlockingScriptTemplate {
        UnlockingScriptTemplate {
            derivation_prefix: prefix.to_string(),
            derivation_suffix: suffix.to_string(),
            script_type,
            satoshis,
        }
    }

    /// Apply unlocking script templates to a transaction.
    ///
    /// This derives keys from the templates and creates proper unlocking scripts
    /// for each input that has a template. Uses BRC-29 key derivation to compute
    /// the signing key for each templated input.
    ///
    /// # Arguments
    ///
    /// * `raw_tx` - The unsigned transaction bytes (modified in place concept, but cloned)
    /// * `templates` - Pairs of (input_index, template) for each input to sign
    /// * `proto_wallet` - The ProtoWallet for key derivation
    ///
    /// # Returns
    ///
    /// The transaction bytes with unlocking scripts applied.
    #[allow(clippy::ptr_arg)]
    pub fn apply_templates(
        &self,
        raw_tx: &mut Vec<u8>,
        templates: &[(usize, UnlockingScriptTemplate)],
        proto_wallet: &ProtoWallet,
    ) -> Result<Vec<u8>> {
        // For each template, derive the key and create the unlocking script
        // This is a stub that returns the raw_tx unchanged for now
        // Full implementation would derive keys per template and sign inputs
        tracing::debug!("Applying {} unlocking script templates", templates.len());

        let mut tx_data = raw_tx.clone();

        for (input_index, template) in templates {
            tracing::debug!(
                input_index = %input_index,
                derivation_prefix = %template.derivation_prefix,
                derivation_suffix = %template.derivation_suffix,
                script_type = ?template.script_type,
                "Applying template for input"
            );

            // Derive the signing key using BRC-29 protocol
            use bsv_rs::wallet::{Protocol, SecurityLevel};

            let brc29_protocol = Protocol::new(SecurityLevel::Counterparty, "3241645161d8");
            let key_id = format!(
                "{} {}",
                template.derivation_prefix, template.derivation_suffix
            );

            let signing_key = proto_wallet
                .key_deriver()
                .derive_private_key(&brc29_protocol, &key_id, &Counterparty::Self_)
                .map_err(|e| {
                    Error::TransactionError(format!("Template key derivation failed: {}", e))
                })?;

            let pubkey = signing_key.public_key();
            let pubkey_bytes = pubkey.to_compressed();

            // Build a locking script for sighash computation based on script type
            let locking_script = match template.script_type {
                ScriptType::P2PKH => {
                    // P2PKH locking script: OP_DUP OP_HASH160 <pubkey_hash> OP_EQUALVERIFY OP_CHECKSIG
                    let pubkey_hash = hash160(&pubkey_bytes);
                    let mut script = vec![0x76, 0xa9, 0x14];
                    script.extend_from_slice(&pubkey_hash);
                    script.extend_from_slice(&[0x88, 0xac]);
                    script
                }
                ScriptType::P2PK => {
                    // P2PK locking script: <pubkey> OP_CHECKSIG
                    let mut script = vec![pubkey_bytes.len() as u8];
                    script.extend_from_slice(&pubkey_bytes);
                    script.push(0xac);
                    script
                }
            };

            let sighash = compute_sighash(
                &tx_data,
                *input_index as u32,
                &locking_script,
                template.satoshis,
            )?;

            let signature = signing_key
                .sign(&sighash)
                .map_err(|e| Error::TransactionError(format!("Template signing failed: {}", e)))?;

            let unlocking_script =
                build_unlocking_script(&locking_script, &signature.to_der(), &pubkey_bytes)?;

            tx_data = insert_unlocking_script(&tx_data, *input_index as u32, &unlocking_script)?;
        }

        Ok(tx_data)
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Computes the sighash for a transaction input.
///
/// This implements BIP-143 style sighash computation for SegWit-like signing,
/// which BSV uses for all transactions.
fn compute_sighash(
    tx_data: &[u8],
    input_index: u32,
    locking_script: &[u8],
    satoshis: u64,
) -> Result<[u8; 32]> {
    // Parse transaction to get components
    let (version, inputs, outputs, locktime) = parse_transaction(tx_data)?;

    // Compute hashPrevouts (double SHA256 of all outpoints)
    let mut prevouts_data = Vec::new();
    for input in &inputs {
        prevouts_data.extend_from_slice(&input.txid);
        prevouts_data.extend_from_slice(&input.vout.to_le_bytes());
    }
    let hash_prevouts = double_sha256(&prevouts_data);

    // Compute hashSequence (double SHA256 of all sequences)
    let mut sequence_data = Vec::new();
    for input in &inputs {
        sequence_data.extend_from_slice(&input.sequence.to_le_bytes());
    }
    let hash_sequence = double_sha256(&sequence_data);

    // Compute hashOutputs (double SHA256 of all outputs)
    let mut outputs_data = Vec::new();
    for output in &outputs {
        outputs_data.extend_from_slice(&output.satoshis.to_le_bytes());
        write_varint(&mut outputs_data, output.script.len() as u64);
        outputs_data.extend_from_slice(&output.script);
    }
    let hash_outputs = double_sha256(&outputs_data);

    // Build the preimage for sighash
    let mut preimage = Vec::new();

    // nVersion
    preimage.extend_from_slice(&version.to_le_bytes());

    // hashPrevouts
    preimage.extend_from_slice(&hash_prevouts);

    // hashSequence
    preimage.extend_from_slice(&hash_sequence);

    // outpoint (this input's txid and vout)
    let input = &inputs[input_index as usize];
    preimage.extend_from_slice(&input.txid);
    preimage.extend_from_slice(&input.vout.to_le_bytes());

    // scriptCode (the locking script being spent)
    write_varint(&mut preimage, locking_script.len() as u64);
    preimage.extend_from_slice(locking_script);

    // value (satoshis)
    preimage.extend_from_slice(&satoshis.to_le_bytes());

    // nSequence
    preimage.extend_from_slice(&input.sequence.to_le_bytes());

    // hashOutputs
    preimage.extend_from_slice(&hash_outputs);

    // nLockTime
    preimage.extend_from_slice(&locktime.to_le_bytes());

    // sighash type (SIGHASH_ALL | SIGHASH_FORKID = 0x41)
    preimage.extend_from_slice(&0x41u32.to_le_bytes());

    // Double SHA256 the preimage
    Ok(double_sha256(&preimage))
}

/// Parses a transaction into its components.
fn parse_transaction(tx_data: &[u8]) -> Result<(u32, Vec<TxInput>, Vec<TxOutput>, u32)> {
    let mut offset = 0;

    // Version (4 bytes)
    if tx_data.len() < 4 {
        return Err(Error::TransactionError("Transaction too short".to_string()));
    }
    let version = u32::from_le_bytes([
        tx_data[offset],
        tx_data[offset + 1],
        tx_data[offset + 2],
        tx_data[offset + 3],
    ]);
    offset += 4;

    // Input count
    let (input_count, bytes_read) = read_varint(&tx_data[offset..])?;
    offset += bytes_read;

    // Inputs
    let mut inputs = Vec::with_capacity(input_count as usize);
    for _ in 0..input_count {
        // txid (32 bytes)
        if offset + 32 > tx_data.len() {
            return Err(Error::TransactionError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&tx_data[offset..offset + 32]);
        offset += 32;

        // vout (4 bytes)
        if offset + 4 > tx_data.len() {
            return Err(Error::TransactionError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        let vout = u32::from_le_bytes([
            tx_data[offset],
            tx_data[offset + 1],
            tx_data[offset + 2],
            tx_data[offset + 3],
        ]);
        offset += 4;

        // Script length and script
        let (script_len, bytes_read) = read_varint(&tx_data[offset..])?;
        offset += bytes_read;

        if offset + script_len as usize > tx_data.len() {
            return Err(Error::TransactionError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        let script = tx_data[offset..offset + script_len as usize].to_vec();
        offset += script_len as usize;

        // Sequence (4 bytes)
        if offset + 4 > tx_data.len() {
            return Err(Error::TransactionError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        let sequence = u32::from_le_bytes([
            tx_data[offset],
            tx_data[offset + 1],
            tx_data[offset + 2],
            tx_data[offset + 3],
        ]);
        offset += 4;

        inputs.push(TxInput {
            txid,
            vout,
            script,
            sequence,
        });
    }

    // Output count
    let (output_count, bytes_read) = read_varint(&tx_data[offset..])?;
    offset += bytes_read;

    // Outputs
    let mut outputs = Vec::with_capacity(output_count as usize);
    for _ in 0..output_count {
        // Satoshis (8 bytes)
        if offset + 8 > tx_data.len() {
            return Err(Error::TransactionError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        let satoshis = u64::from_le_bytes([
            tx_data[offset],
            tx_data[offset + 1],
            tx_data[offset + 2],
            tx_data[offset + 3],
            tx_data[offset + 4],
            tx_data[offset + 5],
            tx_data[offset + 6],
            tx_data[offset + 7],
        ]);
        offset += 8;

        // Script length and script
        let (script_len, bytes_read) = read_varint(&tx_data[offset..])?;
        offset += bytes_read;

        if offset + script_len as usize > tx_data.len() {
            return Err(Error::TransactionError(
                "Unexpected end of transaction data".to_string(),
            ));
        }
        let script = tx_data[offset..offset + script_len as usize].to_vec();
        offset += script_len as usize;

        outputs.push(TxOutput { satoshis, script });
    }

    // Locktime (4 bytes)
    if offset + 4 > tx_data.len() {
        return Err(Error::TransactionError(
            "Unexpected end of transaction data".to_string(),
        ));
    }
    let locktime = u32::from_le_bytes([
        tx_data[offset],
        tx_data[offset + 1],
        tx_data[offset + 2],
        tx_data[offset + 3],
    ]);

    Ok((version, inputs, outputs, locktime))
}

/// Transaction input structure.
struct TxInput {
    txid: [u8; 32],
    vout: u32,
    script: Vec<u8>,
    sequence: u32,
}

/// Transaction output structure.
struct TxOutput {
    satoshis: u64,
    script: Vec<u8>,
}

/// Reads a varint from data and returns (value, bytes_read).
fn read_varint(data: &[u8]) -> Result<(u64, usize)> {
    if data.is_empty() {
        return Err(Error::TransactionError("Empty varint".to_string()));
    }

    let first = data[0];
    if first < 0xfd {
        Ok((first as u64, 1))
    } else if first == 0xfd {
        if data.len() < 3 {
            return Err(Error::TransactionError("Truncated varint".to_string()));
        }
        let val = u16::from_le_bytes([data[1], data[2]]) as u64;
        Ok((val, 3))
    } else if first == 0xfe {
        if data.len() < 5 {
            return Err(Error::TransactionError("Truncated varint".to_string()));
        }
        let val = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as u64;
        Ok((val, 5))
    } else {
        if data.len() < 9 {
            return Err(Error::TransactionError("Truncated varint".to_string()));
        }
        let val = u64::from_le_bytes([
            data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
        ]);
        Ok((val, 9))
    }
}

/// Computes double SHA256.
fn double_sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let hash1 = Sha256::digest(data);
    let hash2 = Sha256::digest(hash1);
    let mut result = [0u8; 32];
    result.copy_from_slice(&hash2);
    result
}

/// Computes HASH160 (RIPEMD160(SHA256(data))).
///
/// Used to derive the public key hash for P2PKH locking scripts.
fn hash160(data: &[u8]) -> [u8; 20] {
    use sha2::{Digest, Sha256};

    let sha256_hash = Sha256::digest(data);

    use ripemd::Ripemd160;
    let ripemd_hash = <Ripemd160 as ripemd::Digest>::digest(sha256_hash);
    let mut result = [0u8; 20];
    result.copy_from_slice(&ripemd_hash);
    result
}

/// Validates that the derived public key's hash160 matches the P2PKH locking script.
/// P2PKH format: 76 a9 14 <20-byte-hash160> 88 ac
///
/// For non-P2PKH scripts (not matching the 25-byte pattern), validation is skipped.
/// For P2PKH scripts, a mismatch between the derived pubkey hash and the script hash
/// is an error -- this catches wrong key derivation before broadcasting.
fn validate_p2pkh_pubkey_match(pubkey_hash160: &[u8; 20], locking_script: &[u8]) -> Result<()> {
    if locking_script.len() == 25
        && locking_script[0] == 0x76
        && locking_script[1] == 0xa9
        && locking_script[2] == 0x14
        && locking_script[23] == 0x88
        && locking_script[24] == 0xac
    {
        let script_hash160 = &locking_script[3..23];
        if pubkey_hash160 != script_hash160 {
            return Err(Error::SigningError(format!(
                "Derived pubkey hash160 {} does not match locking script hash160 {}",
                hex::encode(pubkey_hash160),
                hex::encode(script_hash160)
            )));
        }
    }
    Ok(())
}

/// Builds an unlocking script based on the locking script type.
fn build_unlocking_script(
    locking_script: &[u8],
    signature: &[u8],
    pubkey: &[u8],
) -> Result<Vec<u8>> {
    // Check if this is a P2PKH script
    // P2PKH: OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
    // Bytes: 76 a9 14 <20 bytes pubkey hash> 88 ac
    if locking_script.len() == 25
        && locking_script[0] == 0x76  // OP_DUP
        && locking_script[1] == 0xa9  // OP_HASH160
        && locking_script[2] == 0x14  // Push 20 bytes
        && locking_script[23] == 0x88 // OP_EQUALVERIFY
        && locking_script[24] == 0xac
    // OP_CHECKSIG
    {
        // P2PKH unlocking script: <sig> <pubkey>
        let mut unlocking = Vec::new();

        // Signature with sighash byte
        let sig_with_hashtype: Vec<u8> = signature
            .iter()
            .copied()
            .chain(std::iter::once(0x41)) // SIGHASH_ALL | SIGHASH_FORKID
            .collect();

        // Push signature
        unlocking.push(sig_with_hashtype.len() as u8);
        unlocking.extend_from_slice(&sig_with_hashtype);

        // Push pubkey
        unlocking.push(pubkey.len() as u8);
        unlocking.extend_from_slice(pubkey);

        return Ok(unlocking);
    }

    // Check if this is a P2PK script
    // P2PK: <pubkey> OP_CHECKSIG
    if locking_script.len() >= 35
        && (locking_script[0] == 33 || locking_script[0] == 65)
        && locking_script[locking_script.len() - 1] == 0xac
    {
        // P2PK unlocking script: <sig>
        let mut unlocking = Vec::new();

        let sig_with_hashtype: Vec<u8> = signature
            .iter()
            .copied()
            .chain(std::iter::once(0x41))
            .collect();

        unlocking.push(sig_with_hashtype.len() as u8);
        unlocking.extend_from_slice(&sig_with_hashtype);

        return Ok(unlocking);
    }

    // Unknown script type - return error
    Err(Error::TransactionError(format!(
        "Unknown locking script type: {}",
        hex::encode(locking_script)
    )))
}

/// Inserts an unlocking script into a transaction at the specified input index.
fn insert_unlocking_script(
    tx_data: &[u8],
    input_index: u32,
    unlocking_script: &[u8],
) -> Result<Vec<u8>> {
    // Parse the transaction
    let (version, inputs, outputs, locktime) = parse_transaction(tx_data)?;

    // Rebuild the transaction with the new unlocking script
    let mut result = Vec::new();

    // Version
    result.extend_from_slice(&version.to_le_bytes());

    // Input count
    write_varint(&mut result, inputs.len() as u64);

    // Inputs
    for (i, input) in inputs.iter().enumerate() {
        // txid
        result.extend_from_slice(&input.txid);

        // vout
        result.extend_from_slice(&input.vout.to_le_bytes());

        // Script (use new unlocking script for target input)
        let script = if i == input_index as usize {
            unlocking_script
        } else {
            &input.script
        };

        // Write varint for script length
        write_varint(&mut result, script.len() as u64);
        result.extend_from_slice(script);

        // Sequence
        result.extend_from_slice(&input.sequence.to_le_bytes());
    }

    // Output count
    write_varint(&mut result, outputs.len() as u64);

    // Outputs
    for output in &outputs {
        // Satoshis
        result.extend_from_slice(&output.satoshis.to_le_bytes());

        // Script
        write_varint(&mut result, output.script.len() as u64);
        result.extend_from_slice(&output.script);
    }

    // Locktime
    result.extend_from_slice(&locktime.to_le_bytes());

    Ok(result)
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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        // Single byte
        assert_eq!(read_varint(&[0x00]).unwrap(), (0, 1));
        assert_eq!(read_varint(&[0xfc]).unwrap(), (252, 1));

        // Two bytes
        assert_eq!(read_varint(&[0xfd, 0xfd, 0x00]).unwrap(), (253, 3));
        assert_eq!(read_varint(&[0xfd, 0xff, 0xff]).unwrap(), (65535, 3));

        // Four bytes
        assert_eq!(
            read_varint(&[0xfe, 0x00, 0x00, 0x01, 0x00]).unwrap(),
            (65536, 5)
        );

        // Eight bytes
        assert_eq!(
            read_varint(&[0xff, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00]).unwrap(),
            (4294967296, 9)
        );
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

        buf.clear();
        write_varint(&mut buf, 65535);
        assert_eq!(buf, vec![0xfd, 0xff, 0xff]);
    }

    #[test]
    fn test_double_sha256() {
        // Test vector: empty string
        let result = double_sha256(&[]);
        let expected =
            hex::decode("5df6e0e2761359d30a8275058e299fcc0381534545f55cf43e41983f5d4c9456")
                .unwrap();
        assert_eq!(result.to_vec(), expected);
    }

    #[test]
    fn test_build_unlocking_script_p2pkh() {
        // Create a P2PKH locking script
        let pubkey_hash = [0u8; 20];
        let mut locking_script = vec![0x76, 0xa9, 0x14];
        locking_script.extend_from_slice(&pubkey_hash);
        locking_script.extend_from_slice(&[0x88, 0xac]);

        // Create a dummy signature and pubkey
        let signature = vec![0x30, 0x44]; // Simplified - just testing structure
        let pubkey = vec![0x02; 33]; // Compressed pubkey prefix

        let result = build_unlocking_script(&locking_script, &signature, &pubkey).unwrap();

        // Should have: push_sig + sig + hashtype + push_pubkey + pubkey
        assert!(!result.is_empty());
        // First byte is length of signature + hashtype
        assert_eq!(result[0], 3); // 2 byte sig + 1 byte hashtype
    }

    #[test]
    fn test_wallet_signer_new() {
        let signer = WalletSigner::new(None);
        assert!(signer.root_key.is_none());

        let key = PrivateKey::random();
        let signer = WalletSigner::new(Some(key));
        assert!(signer.root_key.is_some());
    }

    #[test]
    fn test_create_unlock_template() {
        let signer = WalletSigner::new(None);
        let template = signer.create_unlock_template("aabbcc", "ddeeff", ScriptType::P2PKH, 5000);

        assert_eq!(template.derivation_prefix, "aabbcc");
        assert_eq!(template.derivation_suffix, "ddeeff");
        assert_eq!(template.script_type, ScriptType::P2PKH);
        assert_eq!(template.satoshis, 5000);
    }

    #[test]
    fn test_create_unlock_template_p2pk() {
        let signer = WalletSigner::new(None);
        let template = signer.create_unlock_template("1234", "5678", ScriptType::P2PK, 10000);

        assert_eq!(template.derivation_prefix, "1234");
        assert_eq!(template.derivation_suffix, "5678");
        assert_eq!(template.script_type, ScriptType::P2PK);
        assert_eq!(template.satoshis, 10000);
    }

    #[test]
    fn test_unlocking_script_template_serialization() {
        let template = UnlockingScriptTemplate {
            derivation_prefix: "aabb".to_string(),
            derivation_suffix: "ccdd".to_string(),
            script_type: ScriptType::P2PKH,
            satoshis: 1000,
        };

        let json = serde_json::to_string(&template).unwrap();
        let deserialized: UnlockingScriptTemplate = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.derivation_prefix, "aabb");
        assert_eq!(deserialized.derivation_suffix, "ccdd");
        assert_eq!(deserialized.script_type, ScriptType::P2PKH);
    }

    #[test]
    fn test_script_type_equality() {
        assert_eq!(ScriptType::P2PKH, ScriptType::P2PKH);
        assert_eq!(ScriptType::P2PK, ScriptType::P2PK);
        assert_ne!(ScriptType::P2PKH, ScriptType::P2PK);
    }

    #[test]
    fn test_hash160() {
        // Test with a known value - empty input
        let result = hash160(&[]);
        // SHA256 of empty = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        // RIPEMD160 of that = b472a266d0bd89c13706a4132ccfb16f7c3b9fcb
        let expected = hex::decode("b472a266d0bd89c13706a4132ccfb16f7c3b9fcb").unwrap();
        assert_eq!(result.to_vec(), expected);
    }
}
