# Changelog

## [0.7.0] - 2026-10-09

The 0.3 line of bsv-rs leaves the wallet: the crate depends on bsv-rs 0.4.1, whose streaming BEEF reader refuses a BEEF only for invalid bytes and never for its size or its counts. The wallet takes the same posture at its own doors: a valid BEEF is never refused, or cut short, for its size or its counts, anywhere in the crate; a refusal is for invalid bytes only, and names the byte and the kind.

### Changed, breaking

1. bsv-rs 0.4.1 (was 0.3.35). bsv-rs types are in this crate's public API (the re-exported `bsv_rs::wallet` arguments and results, `Error::SdkError(bsv_rs::Error)`, the `ChainTracker` and `Beef` parameters), so a caller moves its own bsv-rs to 0.4 with this release. The crate never named `BeefLimits` or `from_binary_with_limits`, so the limits' lost verdict moves nothing here.
2. `Error` gains `InvalidBeef { offset, kind }` (`Error` is not `#[non_exhaustive]`; an exhaustive `match` needs the arm). `kind` is bsv-rs's `transaction::Kind`.
3. A stranger's BEEF is read by bsv-rs's `BeefStream` before the in-memory parse, at `internalizeAction` (`tx`) and at `createAction` (`inputBEEF`), through the new `storage::sqlx::refuse_invalid_beef_bytes`. Invalid bytes are `Error::InvalidBeef` with the offset and the kind, where 0.6.0 gave a `ValidationError` with the parser's text and no offset. A transaction with no input is now refused at both doors (`NoInputs`, the node's rule): 0.6.0 internalized one when no chain tracker was set. The graph and the roots stay with each door's own verification.
4. The ancestor walk that builds a BEEF (`createAction`'s input BEEF, the broadcast rebuild, `listOutputs` with entire transactions) reaches the proven anchor at any depth. 0.6.0 skipped every unproven ancestor past depth 12, after the reference's `maxRecursionDepth` (which throws there), and the BEEF it built then missed those ancestors.

### Named, not routed around

- The two doors receive the BEEF as one byte vector (the `WalletInterface` argument, and over JSON-RPC one request body), and the in-memory `Beef` holds it whole after the stream has read it: memory is linear in the BEEF, never a refusal.
- A stored `input_beef` and a `raw_tx` are each one SQLite row, held whole when read; a sync chunk carries at least one row whole however large (`max_rough_size` pages, it never refuses).
- `MAX_PROOF_FETCHES_PER_WALK` (8) bounds the merkle-path requests of one walk; past it an ancestor rides unproven and the walk goes on, so the BEEF stays valid, only larger.

### Upgrading

- Move the caller's bsv-rs to 0.4 (0.4.1 or later) together with this crate.
- A `match` on `Error` adds `InvalidBeef`. A caller that read `ValidationError("Failed to parse AtomicBEEF: ...")` or `ValidationError("inputBEEF: invalid BEEF format: ...")` for bad bytes now reads `InvalidBeef`.
- A BEEF with an input-less transaction, internalized by 0.6.0 without a chain tracker, is refused. A transaction with no input is invalid by the node's rule; whether any sender emits one is not known.
- No stored data changes and nothing is migrated. Rollback: pin 0.6.0 (and bsv-rs 0.3.35).

(bsv-stack-lean `docs/charters/beef-of-any-size.md`; `docs/p0/align-toolbox.md`.)

## [0.6.0] - 2026-10-09

Rule 28, the second pass, from the owner's two rulings on 0.5.0's open questions: the wallet keeps no header store of its own, the header service is the one headers machine; and a positive unspent answer from an explorer is a hint and never a verdict, the wallet's shared storage is the verdict for its own devices' spends, and a stranger's spend of our output becomes a chain fact only by the spending transaction's merkle proof checked against our headers. No limit changed.

### Removed, breaking

1. The embedded header store is deleted: the `chaintracks` module (`Chaintracks`, `ChaintracksOptions`, `ChaintracksClient`, `ChaintracksManagement`, `ChaintracksInfo`, `ChaintracksStorage` and its memory and SQLite backends, `BulkIngestor`, `LiveIngestor`, `BaseBlockHeader`, `LiveBlockHeader`, `HeightRange`, `InsertHeaderResult`), its root re-exports and the `chaintracks_demo` example. Nothing in the crate built the store, and it checked no proof of work, difficulty rule, checkpoint or ancestry. Every header question goes to the header service (`chaintracks_url`, then `bhs_url`) and is an error without one. `Chain` is now defined in `services` (`bsv_wallet_toolbox_rs::Chain` and `services::Chain` are unchanged paths; `chaintracks::Chain` is gone).

### Changed, breaking

2. A stranger's spend is `Spent` only by proof. `WalletServices::is_utxo` answers `UtxoVerdict::Spent` only when a provider names the spending transaction, that transaction's own bytes have an input that is the outpoint, and `get_merkle_path` returns its path (which it does only after the root met the header service's header). Two explorers agreeing the outpoint is not in the unspent set, which was `Spent` in 0.5.0, is the new `UtxoVerdict::SpentHint`; so is a named spender whose bytes name the outpoint with no proof held. With no header service no spender is asked for and nothing is `Spent`. `UtxoVerdict::from_status` maps an explorer's negative to `SpentHint`. New: `WhatsOnChain::get_spender` (the `tx/{txid}/{vout}/spent` route, the explorer as the courier of a name that is then checked).
3. Storage writes a spend only from a proof. The terminal value of `locked_input_checks.last_verdict` is `spent-proven` (`LOCKED_VERDICT_SPENT_PROVEN`), written only for `Spent`. A hint is `spent-hint` and stays on the re-check backoff. `LockedInputVerdict` gains `SpentHint` and `LockedInputReport` gains `spent_hints`. The chain-knowledge read that made a two-explorer negative terminal is removed from `recheck_locked_inputs` and the poisoned-chain retire (one status request fewer per negative). `utxo_verdict` delegates to `is_utxo`.
4. "Unspent" from any provider is a hint, and the type says so: `UtxoVerdict::Unspent` is renamed `UtxoVerdict::UnspentHint`, `is_unspent()` is `is_unspent_hint()`. Behavior is unchanged: one provider's positive gives the answer and a locked input of a failed transaction is released on it.
5. `MockWalletServices::is_utxo` answers a negative as `SpentHint`; `MockWalletServicesBuilder::spends_are_proven(true)` makes it `Spent`. With no `is_utxo_response` configured the mock derives the answer from `get_utxo_status_response`.

### Changed

6. The cadence of the stranger's-spend check is named in one place, `services::cadence`, in the rule's words: which outpoints are asked about, how often per outpoint (`stranger_spend_recheck_minutes`: 1 minute doubling to 64), when it ends, the pace within a pass (`STRANGER_SPEND_LOOKUP_PACE`), and who starts a pass. No number changed. `locked_input_backoff_minutes` and `LOCKED_INPUT_BACKOFF_CAP_MINUTES` stay exported, defined there.

### Upgrading

- A caller of the `chaintracks` module points at a header service (`ServicesOptions::with_chaintracks_url`); there is no replacement in this crate.
- A `match` on `UtxoVerdict` renames `Unspent` to `UnspentHint` and adds a `SpentHint` arm. Treat `SpentHint` as "keep locked, ask again"; it is not a spend.
- A `WalletServices` implementation returns `Spent` from `is_utxo` only when it holds the proof described above.
- A reader of `LockedInputReport` adds `spent_hints`; a reader of `locked_input_checks.last_verdict` reads `spent-proven` as the terminal value.
- Stored data, no migration: a `locked_input_checks` row whose `last_verdict` is `spent` (written by 0.5.0 or earlier on two explorers' agreement) is no longer terminal. It is due at once and is decided again: released on an unspent hint, terminal on a proof, otherwise back on the backoff (at most one lookup per row per 64 minutes). Rolling back to 0.5.0 reads `spent-proven` and `spent-hint` as non-terminal and re-checks them; nothing is unreadable.

### Not verified against a live service

- The WhatsOnChain `tx/{txid}/{vout}/spent` route and its `txid` field were exercised against a local fixture only (the shape is the one bsv-wallet-cli reads). If the live shape differs no spender is ever named, no spend is ever `Spent`, and a spent locked input stays locked on the 64-minute backoff; nothing is released on it.
- The Bitails unspent route of 0.5.0 stays unverified, as recorded there.

(bsv-stack-lean `NORTH-STAR.md`, Rulings, 2026-10-09; `docs/p0/rule-28-toolbox-2.md`.)

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

