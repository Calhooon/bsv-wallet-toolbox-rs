# Rust Wallet Toolbox

> Rust implementation of the BSV wallet toolbox, providing storage and services for BSV wallets

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]()
[![Tests](https://img.shields.io/badge/tests-185%20passing-brightgreen)]()
[![License](https://img.shields.io/badge/license-MIT-blue)]()

## Overview

This crate provides the infrastructure layer for BSV wallets, ported from the TypeScript [@bsv/wallet-toolbox](https://github.com/bsv-blockchain/wallet-toolbox) and Go [go-wallet-toolbox](https://github.com/bsv-blockchain/go-wallet-toolbox) implementations.

It builds on top of [bsv-sdk](https://github.com/bsv-blockchain/bsv-sdk-rust) which provides:
- Cryptographic primitives (ECDSA, SHA256, AES-256-GCM)
- BRC-42 key derivation
- Transaction building and signing
- BEEF/MerklePath SPV proofs
- WalletInterface trait (28 methods)

## Features

### Storage Layer

Multiple storage backends for wallet state persistence:

- **StorageSqlx** - Local SQLite database with full CRUD operations
- **StorageClient** - Remote storage via JSON-RPC to `storage.babbage.systems`

```rust
use bsv_wallet_toolbox::storage::{StorageSqlx, WalletStorageProvider};

// Local SQLite storage
let storage = StorageSqlx::open("wallet.db").await?;
storage.migrate("my-wallet", &identity_key).await?;
let settings = storage.make_available().await?;

// Create a transaction
let result = storage.create_action(&auth, args).await?;
```

### Services Layer

Blockchain service integration with automatic failover:

- **WhatsOnChain** - UTXO queries, Merkle paths, broadcasting
- **ARC** - Transaction broadcasting (TAAL, GorillaPool)
- **Bitails** - Alternative blockchain data provider

```rust
use bsv_wallet_toolbox::services::{Services, WalletServices};

let services = Services::mainnet();
let height = services.get_height().await?;
let raw_tx = services.get_raw_tx("txid...").await?;
let result = services.post_beef(&beef, &txids).await?;
```

### Chaintracks

Block header tracking and chain state management:

- **Bulk Ingestors** - Download historical headers from CDN or WhatsOnChain
- **Live Ingestors** - Track new blocks via polling or WebSocket
- **Memory Storage** - In-memory header storage with reorg handling

```rust
use bsv_wallet_toolbox::chaintracks::{Chaintracks, Chain};
use bsv_wallet_toolbox::chaintracks::ingestors::BulkCdnIngestor;

let ingestor = BulkCdnIngestor::new(Chain::Main);
let headers = ingestor.get_headers(HeightRange::new(0, 1000)).await?;
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
bsv-wallet-toolbox-rs = "0.1"
```

## Database Schema

The storage layer uses 18 tables matching the TypeScript/Go implementations:

| Table | Purpose |
|-------|---------|
| `users` | User identity tracking |
| `settings` | Storage configuration |
| `transactions` | Transaction records |
| `outputs` | UTXOs and spent outputs |
| `output_baskets` | Output organization |
| `output_tags` / `output_tags_map` | Output labeling |
| `tx_labels` / `tx_labels_map` | Transaction labeling |
| `proven_txs` | Merkle-proven transactions |
| `proven_tx_reqs` | Pending proof requests |
| `certificates` / `certificate_fields` | Identity certificates |
| `sync_states` | Multi-storage sync state |
| `commissions` | Fee tracking |
| `monitor_events` | System events |

## API Reference

### Storage Traits

```rust
// Read operations
trait WalletStorageReader {
    fn is_available(&self) -> bool;
    fn get_settings(&self) -> &TableSettings;
    async fn find_outputs(&self, auth: &AuthId, args: FindOutputsArgs) -> Result<Vec<TableOutput>>;
    async fn list_actions(&self, auth: &AuthId, args: ListActionsArgs) -> Result<ListActionsResult>;
    async fn list_outputs(&self, auth: &AuthId, args: ListOutputsArgs) -> Result<ListOutputsResult>;
    // ...
}

// Write operations
trait WalletStorageWriter: WalletStorageReader {
    async fn make_available(&self) -> Result<TableSettings>;
    async fn migrate(&self, name: &str, key: &str) -> Result<String>;
    async fn create_action(&self, auth: &AuthId, args: CreateActionArgs) -> Result<StorageCreateActionResult>;
    // ...
}

// Sync operations
trait WalletStorageSync: WalletStorageWriter {
    async fn get_sync_chunk(&self, args: RequestSyncChunkArgs) -> Result<SyncChunk>;
    async fn process_sync_chunk(&self, args: RequestSyncChunkArgs, chunk: SyncChunk) -> Result<ProcessSyncChunkResult>;
    // ...
}
```

### Services Trait

```rust
trait WalletServices {
    async fn get_height(&self) -> Result<u32>;
    async fn get_header_for_height(&self, height: u32) -> Result<BlockHeader>;
    async fn get_raw_tx(&self, txid: &str) -> Result<Vec<u8>>;
    async fn get_merkle_path(&self, txid: &str) -> Result<MerklePath>;
    async fn post_beef(&self, beef: &[u8], txids: &[String]) -> Result<PostBeefResult>;
    async fn get_utxo_status(&self, txid: &str, vout: u32) -> Result<UtxoStatus>;
    async fn get_script_hash_history(&self, hash: &str) -> Result<Vec<ScriptHistoryEntry>>;
    async fn get_bsv_exchange_rate(&self) -> Result<f64>;
}
```

## Testing

```bash
# Run all tests
cargo test

# Run library tests only (faster)
cargo test --lib

# Run specific module tests
cargo test storage::sqlx
cargo test services
cargo test chaintracks

# Run with output
cargo test -- --nocapture
```

## Project Status

| Component | Status | Tests |
|-----------|--------|-------|
| Storage Traits | ✅ Complete | - |
| Entity Definitions | ✅ Complete | - |
| StorageSqlx | ✅ Complete | 36 |
| StorageClient | ✅ Complete | 52 |
| Services Layer | ✅ Complete | 36 |
| Chaintracks | ✅ Complete | 102 |
| **Wallet** | 🚧 In Progress | - |
| **Monitor** | 📋 Planned | - |

**Total: 185 tests passing**

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         rust-wallet-toolbox                              │
├─────────────────────────────────────────────────────────────────────────┤
│  Wallet (implements WalletInterface with full storage/services)          │
├───────────────┬──────────────────────┬───────────────┬──────────────────┤
│  WalletSigner │ WalletStorageManager │   Services    │     Monitor      │
├───────────────┴──────────────────────┴───────────────┴──────────────────┤
│  Storage Providers: StorageSqlx (SQLite) | StorageClient (Remote)        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            bsv-sdk (rust)                                │
│  primitives | script | transaction | wallet (ProtoWallet, KeyDeriver)    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Related Projects

- [bsv-sdk](https://github.com/bsv-blockchain/bsv-sdk-rust) - Core BSV SDK for Rust
- [@bsv/wallet-toolbox](https://github.com/bsv-blockchain/wallet-toolbox) - TypeScript implementation
- [go-wallet-toolbox](https://github.com/bsv-blockchain/go-wallet-toolbox) - Go implementation

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! Please read the [PLAN.md](PLAN.md) for the migration roadmap and [HANDOFF.md](HANDOFF.md) for current session status.
