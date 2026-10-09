# Changelog

## [0.5.0] - 2026-10-09

Rule 28: a third-party chain explorer is a break-glass read. The primary truth of the chain is headers and merkle proofs (the header service, a BEEF's own BUMPs, our own storage). Every explorer request in the crate was judged by one test, what header, proof or own-index answer we already hold for the question: where one exists the request is removed; where none can exist the request stays, named at the site, with the explorers as each other's fallback (a rotating start, a negative only from a second provider, "could not look" never "nothing there"). The numbering is the fix list's. No limit changed.

### Removed, breaking (the deletions)

1. `get_height` asks no explorer. The tip height is the header service's tip header's height (`chaintracks_url`), then the Block Header Service (`bhs_url`), then an error. With neither configured `get_height`, and so a block-height `n_lock_time_is_final`, is an error instead of WhatsOnChain's or Bitails' number. `WhatsOnChain::get_chain_info`, `WocChainInfo` and `Bitails::current_height` are removed.
2. The script hash history is a chain scan and is no longer built by default. `WalletServices::get_script_hash_history`, the `Services` provider collection, `WhatsOnChain::get_script_hash_history` (and its confirmed and unconfirmed halves) and `Bitails::get_script_hash_history` exist only under the new cargo feature `break-glass-script-history` (off by default, not part of `full`). Under the feature the trait method has a default body. `ServicesCallHistory::get_script_hash_history` is `None` without the feature. The result types stay.
3. The Bitails tip, header and root reads are removed: `Bitails::get_current_height`, `get_latest_block`, `get_header_by_height`, `is_valid_root_for_height`. `Bitails::get_status_for_txids` no longer reads the tip to count confirmations: a transaction Bitails places in a block is reported `mined` at `depth` 1, and a tip outage can no longer fail the status read.
4. The embedded header store's four explorer ingestors are removed: `BulkCdnIngestor`, `BulkWocIngestor`, `LivePollingIngestor`, `LiveWebSocketIngestor`, their option and wire types, and the `chaintracks::ingestor` module. They stored headers from WhatsOnChain and a CDN with no proof of work, difficulty, checkpoint or ancestry check. The store, its storage backends and the `BulkIngestor` and `LiveIngestor` traits stay; the store has no source in this crate and is documented as not a source of truth. The dependencies `tokio-tungstenite` and `url` go with the ingestors.

### Changed, breaking (the fallback shape of what stays)

5. An output's spend is asked of two explorers. `get_utxo_status` gains Bitails as a second provider, rotates its starting provider per call, returns a positive from the first provider that gives it, returns a negative only when both give it, and answers `status: "error"` with `is_utxo: None` for anything else. `WalletServices::is_utxo` returns `UtxoVerdict` (`Unspent`, `Spent`, `Unknown`) instead of `Result<bool>`: an outage is `Unknown`, where it was `false` or an error that four callers read as spent. `UtxoVerdict` now lives in `services` (its `storage` re-exports stay) and gains `from_status` and `is_unspent`. `abort_abandoned` schedules the locked-input re-check when it could not look.
6. A break-glass merkle root needs both explorers. Under `break_glass_explorer_headers`, with the header service giving no answer, `FallbackChainTracker` asks WhatsOnChain and Bitails: `true` when both name the asked root (the only explorer answer that is cached), `false` when both name one other root, an error otherwise. `with_break_glass_woc` and `break_glass_woc` are replaced by `with_break_glass_explorers` and `break_glass_explorers`.
7. The break-glass header by hash (`hash_to_header`) rotates its starting explorer, falls through to the other on a fault, answers `NotFound` only when both have no such header, and takes an answer only when its fields hash to the hash asked for. Bitails is read at `block/{hash}`, which carries the height; before, its header came back at height 0.
8. `get_merkle_path` fetches and returns no proof when no header service is configured. Before, the root check was skipped and the provider's proof was returned unchecked. The answer is `merkle_path: None` with a fault note, never "not mined".
9. `GetRawTxResult` gains `could_not_look` (and `is_not_found()`). With no bytes, "not found" is every explorer answering "no such transaction"; one that could not look makes the absence unknown. Before, a fault followed by a 404 came back with no error.

### Changed

10. Every remaining explorer request carries its reason at the site: the question, why no header, proof or own index answers it, and that it is a break-glass read. The broadcasts (a write) and the two price reads (not a chain question) are named as outside the rule's test.

### Upgrading

- Configure a header service (`ServicesOptions::with_chaintracks_url`). Without one, `get_height` is an error and `get_merkle_path` returns no proof.
- A `WalletServices` implementation changes `is_utxo` to return `UtxoVerdict`, and drops `get_script_hash_history` unless it builds with `break-glass-script-history`.
- A literal `GetRawTxResult { .. }` adds `could_not_look: false`.
- A caller of `FallbackChainTracker::with_break_glass_woc` passes the Bitails API base as well.
- A host that fed the embedded header store from the removed ingestors points at a header service instead.
- No stored data changes and there is no migration.

### Not verified against a live service

- The Bitails unspent route (`scripthash/{hash}/unspent`) and its field names are not in the TypeScript reference, which asks WhatsOnChain alone, and were exercised against a local fixture only. If the live shape differs, Bitails answers "could not look", no negative is confirmed, and locked inputs stay locked and are asked about again; nothing is released on it.

(bsv-stack-lean Rule 28, `docs/p0/rule-28-explorer-calls.md` section 1.)

## [0.4.2] - 2026-10-09

### Changed

- The `bsv-rs` floor moves from `0.3.20` to `0.3.35`, the latest on crates.io. A consumer's lock can no longer hold this crate on a `bsv-rs` older than 0.3.35. No code changed and no limit changed.

## [0.4.1] - 2026-10-08

### Fixed

- One broadcaster's refusal is final only when no other source holds the transaction. An Arcade `REJECTED` (or conflict) pushed on the status stream now goes through `MonitorStorage::mark_transaction_rejected_by`: when another broadcaster accepted the transaction (a bare acceptance does not overrule a conflict), or the network holds it by a status source or another broadcaster's own status read, nothing is failed and the transaction stays for the proof task. The 2026-10-07 oversize post (Arcade 460 "missing input source data", GorillaPool ARC accepted, mined at 970030) was failed with its 47,377-sat change hidden.
- The auto-unfail canary asks every source: the status sources, then each configured broadcaster's own status read (`WalletServices::get_broadcaster_statuses`, new, default empty) and a merkle path the chain tracker accepted, not only the source that refused the transaction.
- The proof attempt backstop reaches every req the proof task checks. `synchronize_transaction_statuses` counted an attempt only for a req the status sources called mined, so a broadcast transaction that no source holds and no broadcaster refused stayed `unproven` with attempts 0, its change spendable and its inputs locked, forever. Now a status pass that finds no proof is an attempt, as in the TypeScript `TaskCheckForProofs`, and once `attempts` is greater than the reference's limit (`unprovenAttemptsLimitMain` 144, `unprovenAttemptsLimitTest` 10) the req goes `invalid` and the transaction `failed` through the release rule (`retire_undeliverable_tx`: one more live status read, its own outputs unspendable, each input released only on its own `is_utxo`). A req a source still holds is never written off by the count alone: the status sources say known or mined, or the broadcast memory has a seen or mined row no older than 2 hours. A status source that cannot answer, a closed proof gate and a `sending` req count nothing.
- A req the status sources call mined but that no merkle path arrives for is no longer set `invalid` at 144 attempts with its transaction left `unproven`; it keeps counting and waits for its proof.
- A written-off req restarts at attempts 0, so the auto-unfail canary asks about it hourly (it reads `attempts` as its own counter and would have asked daily).

### Tests

- Pinned: `create_action` marks a caller-named input that storage holds (a pf head in its basket) `spent_by` with the wallet's change, and `list_actions` lists both inputs from storage. This was already the behavior; a caller input storage does not hold (known only from `inputBEEF`) has no row to mark, as in the TypeScript toolbox.

(Calgooon/zanaadu-v2#357, #368.)

## [0.4.0] - 2026-10-08

### Changed, breaking

- No tracker, no proof: with no chain tracker attached, every path that stores a merkle proof refuses it (`ProofIngestOutcome::TrackerUnavailable`); proofs already stored unchecked are checked once on the next read and demoted if the tracker refutes them (a new table records the checks, created on open). The BEEF builder's own proof fetch and compaction follow the same rule.
- No explorer in the proof path: WhatsOnChain is no longer asked for headers unless `break_glass_explorer_headers` is set (off by default, every call logged at warn), and never overrules the configured header service.
- One verdict per Arcade client: `txStatus` decides on a 2xx (REJECTED is a failure, an unknown word an invalid response, IMMUTABLE serves the proof, 476 is retryable, 466 is a conflict), and the reorg words `reorg_reanchor` and `reorg_unmined` schedule a re-check instead of changing a status.

(bsv-stack-lean #35, #48, #49.)

