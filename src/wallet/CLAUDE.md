# src/wallet/ - Wallet Implementation
> Full WalletInterface implementation combining storage, services, and cryptography

## Overview

`Wallet::process_action(StorageProcessActionArgs, &str)` posts the references or txids in `send_with` without `spend_lock`; `create_action_offering` with `no_send` builds, and its one-call form releases that lock before posting (#66).

With `returnTXIDOnly`, an immediate `create_action` post reads the untrimmed
`input_beef` from the committed `proven_tx_reqs` row by txid (#466).
`processAction` moved it there from the transaction row; the door's response
deliberately carries none. The wallet merges the signed transaction and keeps
its ancestor closure before posting. This uses the existing storage reader,
with no new result field. The reference stores `inputBEEF: storageBeef`
(`ts-stack@edf6e03`, `storage/methods/createAction.ts:725`); its signer passes
the signed bytes to `processAction`, whose immediate path reads the stored
request and calls `attemptToPostReqsToNetwork` (`processAction.ts:315-349`).

The `signAndProcess` answer omits `tx` under `returnTXIDOnly`, including with
`noSend` or `acceptDelayedBroadcast` (#467). It keeps `txid`, `noSendChange`
and `sendWithResults`. The field is already optional; with the option false
or absent the response is unchanged. The reference sets `r.txid`, maps
`r.noSendChange`, assigns `r.tx` only inside `if (!vargs.options.returnTXIDOnly)`,
and then sets `r.sendWithResults` (`signer/methods/createAction.ts:101-142`).
Its storage helper returns `undefined` under the option
(`storage/methods/createAction.ts:1725-1726`).

`create_action` refuses an empty caller `unlocking_script` or a zero `unlocking_script_length` with `Error::ValidationError` before BEEF parsing or storage (#212), at each input in `create_args`, ahead of its length match (#416); the sqlx door meets the same texts. The texts name the parameters bare: `The unlockingScript parameter must be hexadecimal string.` is the reference's byte for byte (`ts-stack@edf6e03` `validationHelpers.ts:522-530,595,758` and `WERR_INVALID_PARAMETER.ts:14`); `The unlockingScriptLength parameter must be a positive integer.` is the port's own in the reference's sentence shape. Hosts reserving a caller input must omit its script and supply a positive length.

For a caller-named stored input, the create door checks change, then the presence of a locking script and integer satoshis, then the live spendability check, including under `options.no_send = true` (#245). In `ts-stack@edf6e03` `packages/wallet/wallet-toolbox/src/storage/methods/createAction.ts`, the `:982` gate is disabled by default at `:61`; `markKnownInputsSpent` (`:399-436`, called at `:503`) decides. The port re-reads the stored output under the write lock before marking it spent. A spender with a txid requires double-spend review regardless of its status, including failed; it answers `Error::ReviewActions` (#264, the owner's ruling 18.4), the reference's `WERR_REVIEW_ACTIONS` text `Undelayed createAction or signAction results require review.` with one `doubleSpend` `ReviewActionResult` whose `competing_txs` names the spender (`:503-509`); its `txid` is all zeros for the reference's `''`, and `competing_beef` is absent. Through `WalletInterface` it is bsv-rs's `Error::WalletReviewActions`. A spender without a txid falls through to the output's `spendable` flag. Without a competing txid, an unspendable row uses `Error::ValidationError` with the live `:420-423` text: `The inputs[N] parameter must be spendable output. output <txid>:<vout> appears to have been spent (spendable=false).` Refusal preserves the original spender through a later abort. A pruned script can still be recovered from stored transaction bytes; zero satoshis and an empty stored script count as present. Missing data uses `Error::ValidationError` with `The <txid>.<vout> parameter must be output with valid lockingScript and satoshis`, without a trailing period (`:979-980`, `sdk/WERR_errors.ts:105`). The change guard and its `:972-976` text remain, and the failed-creator guard still applies.

`internalize_action` runs `internalize_args::validate_internalize_action_args` after originator validation and before the spend lock or BRC-29 derivation check (#376). The module is declared without a feature gate in `mod.rs`; the sqlx door calls the same validator at step 0. Outputs and their remittances, customInstructions and tags precede the description, labels bound, each normalized label and the BRC-177 incoming refusal (`ts-stack@edf6e03` `validationHelpers.ts:1052-1054`, `Wallet.ts:1245-1252`). `Error::ValidationError` still crosses the boundary as `WalletRefusal` of class `InvalidParameter`, with `Validation error: ` in its message. The module uses the `label.rs` normalization module, declared without a feature gate beside it; sqlx imports both, so local and remote storage use the same checks. The ordering and unchanged storage are covered in `nosend_label_tests.rs`; `tests/storage_wire.rs` also pins the messages through a wallet over remote storage. The validator returns each wallet payment's sender as the key it parsed from the trimmed value (#453), and the BRC-29 check derives from that key, as the reference's signer derives from `vargs` (`signer/methods/internalizeAction.ts:56,86`): a sender with surrounding whitespace is accepted with the unpadded sender's derivation, where it was refused with `Invalid sender_identity_key: ...`. That refusal is gone; a 66-hex value off the curve is refused earlier, by the validator's `a valid compressed secp256k1 public key`. Witness: `padded_sender_on_p2pkh_derives_from_the_validated_sender`.

`create_action` runs `create_args::validate_create_action_args` after originator validation and ahead of its own work: the spend lock, the permission checks, the trust-self default and every storage call (#416). It is the module the sqlx door calls where it ran its checks, declared without a feature gate in `mod.rs` beside `internalize_args`, whose description, label and array-bound helpers it shares, so a `remote`-only build runs it too. Its order is `validateCreateActionArgs`'s (`ts-stack@edf6e03` `validationHelpers.ts:755-763`: description, inputs, outputs, labels, options), then the BRC-177 grammar and the capability refusal (`Wallet.ts:1154`, then 1161-1165), every text unchanged. Both doors match a BRC-177 module label with `label::is_brc177_no_send_label`, the prefix and its `startsWith` in one place (#363). `Error::ValidationError` crosses the boundary as `InvalidParameter` and the capability's `Error::InvalidOperation` as `InvalidOperation`. Before #416 a wallet over `StorageClient` handed these requests to the remote store, and the caller-input script texts of #212 answered ahead of the description. Witnesses: `create_nosend_labels_refused_before_remote_storage`, `create_description_refused_before_input_script` and `create_input_script_refused_before_output` (`tests/fixtures/internalize_order.rs`, through `tests/storage_wire.rs`), whose storage mock sees no `createAction`.

This module provides the main `Wallet<S, V>` struct that implements the complete `WalletInterface` trait from `bsv_sdk::wallet`. The Wallet orchestrates three components: `ProtoWallet` for cryptographic operations (key derivation, signing, encryption), a storage backend for persistent state (UTXOs, transactions, certificates), and a services backend for blockchain interaction (broadcasting, merkle proofs, chain height).

## Error boundary (#404, #185)

`From<crate::Error> for bsv_rs::Error` preserves a request refusal as
`WalletRefusal { class, message }`. A host matches the class, never its words.
`message` is the existing toolbox Display, including its prefix. The SDK
displays the message alone; the former generic `wallet error: ` wrapper is
absent. No toolbox variant or argument type changes.

For example, `ValidationError("bad input")` formerly displayed through the
SDK as `wallet error: Validation error: bad input`; it now displays as
`Validation error: bad input`. Every row below loses exactly the same
`wallet error: ` prefix, with the payload preserved byte for byte. The two
originator Display assertions reflect that move; the other legacy refusal
checks keep their existing payload text and assert the class beside it.

The reference is `ts-stack@edf6e03`,
`packages/wallet/wallet-toolbox/src/sdk/WERR_errors.ts`; the typed classes are
from `bsv-rs =0.9.0-beta.2`'s `src/error.rs`.

| Toolbox variant | `WalletRefusalClass` | Reference class | Message / Display |
|---|---|---|---|
| `ValidationError(detail)` | `InvalidParameter` | `WERR_INVALID_PARAMETER` | `Validation error: {detail}` |
| Direct acquire proof (`ValidationError`, #374) | `InvalidParameter` | `WERR_INVALID_PARAMETER` (`Wallet.ts:716-728`) | `Validation error: The args parameter must be valid encrypted and signed certificate and keyring from revealer. WERR_UNKNOWN: <message>` |
| `InvalidArgument(detail)` | `InvalidParameter` | `WERR_INVALID_PARAMETER` | `Invalid argument: {detail}` |
| `InvalidBeef { offset, kind }` | `InvalidParameter` | `WERR_INVALID_PARAMETER` | `Invalid BEEF at byte {offset}: {kind:?}` |
| `SpendRefused { offset, txid, input, why }` | `InvalidParameter` | `WERR_INVALID_PARAMETER` | `spend refused at offset {offset}, input {input or none} of {txid}: {why}` |
| `NotFound { entity, id }` | `InvalidParameter` | `WERR_INVALID_PARAMETER` (nearest) | `Entity not found: {entity} with id {id}` |
| `UserNotFound(detail)` | `InvalidParameter` | `WERR_INVALID_PARAMETER` (nearest) | `User not found: {detail}` |
| `InvalidOperation(detail)` | `InvalidOperation` | `WERR_INVALID_OPERATION` | `Invalid operation: {detail}` |
| `InvalidTransactionStatus(detail)` | `InvalidOperation` | `WERR_INVALID_OPERATION` (`processAction.ts:477-478`; nearest for port-only guards) | `Invalid transaction status: {detail}` |
| `UnspendableCoin { outpoint, missing }` | `InvalidOperation` | `WERR_INVALID_OPERATION` (nearest) | `Unspendable coin: {outpoint}: its stored ancestry is incomplete, ancestor(s) not stored: [{missing joined with commas}]; createAction fetches none, and the wallet's other coins do not cover the action` |
| `FailedAncestor { txid }` | `InvalidOperation` | `WERR_INVALID_OPERATION` (nearest) | `Failed ancestor: {txid} is failed in this wallet; nothing built on it is assembled or broadcast` |
| `Duplicate { entity, id }` | `InvalidOperation` | `WERR_INVALID_OPERATION` (nearest) | `Duplicate entity: {entity} with id {id}` |
| `AccessDenied(detail)` | `Unauthorized` | `WERR_UNAUTHORIZED` | `Access denied: {detail}` |
| `AuthenticationRequired` | `Unauthorized` | `WERR_UNAUTHORIZED` | `Authentication required` |
| `InvalidIdentityKey(detail)` | `InvalidPublicKey` | `WERR_INVALID_PUBLIC_KEY` | `Invalid identity key: {detail}` |
| `InsufficientFunds { needed, available }` | `InsufficientFunds` | `WERR_INSUFFICIENT_FUNDS` | `Insufficient funds: need {needed}, have {available}` |
| `StorageNotAvailable` | `NotActive` | `WERR_NOT_ACTIVE` (nearest) | `Storage not available` |

Neither reference supplies dedicated not-found, duplicate, unspendable-coin,
failed-ancestor or storage-not-available classes. The nearest classes describe
an invalid requested identifier or an operation unavailable in the wallet's
state. They do not provide distinct host status codes for those subcases.
`InvalidBeef` and `SpendRefused` are argument refusals, not separate BEEF classes.

Faults are bsv-rs's typed `WalletFault { family, message }` (#439, since the
beta.4 re-pin), the message the toolbox Display unchanged; its display and
its wire frame (code 1) equal the `WalletError` of the same message, byte for
byte, so no host text moves. `WalletFaultFamily` is non-exhaustive: a host
match carries a wildcard arm.

| Family | Toolbox variants |
|---|---|
| `Transport` | `NetworkError`, `HttpError` |
| `Service` | `ServiceError`, `BroadcastFailed`, `NoServicesAvailable`, `RateLimited`, `NoExplorerAnswered` |
| `Storage` | `StorageError`, `DatabaseError`, `SqlxError`, `MigrationError`, `SyncError`, `SyncConflict`, `LockTimeout` |
| `Internal` | `TransactionError`, `SigningError`, `JsonError`, `IoError`, `Internal`, and the wallet's 28 direct sites below (`internal_fault`) |

These variants do not establish a classed request refusal. A provider
outage must not become a refusal by matching a phrase in its detail.

`StoreBusy` keeps `WalletBusy { detail, retry_after_ms: None }` for every
`StoreBusyReason`, and `ReviewActions` keeps `WalletReviewActions` with its
results (`WERR_REVIEW_ACTIONS`). `SdkError` passes its SDK error through,
including an existing refusal or transport fault.

The SDK's binary wallet wire frames only classes with `wire_code()`:
`InvalidParameter` uses 6 and `InsufficientFunds` uses 7. `ReviewActions`
continues to use 5. Other mapped classes have no binary code and the SDK
frames them as code 1, `Wallet operation failed`; its HTTP JSON client can
read their named class. The storage JSON-RPC fixtures exercise a separate
protocol and require no changes for this boundary conversion.

The authenticated storage client's response-wait timeout carries an SDK
`TransportFault` with `MayHaveBeenReceived`, no status and the original
`Request timed out` detail. Storage-level hosts receive
`Error::SdkError(bsv_rs::Error::TransportFault { .. })` instead of
`Error::NetworkError("Request timed out")`. Its SDK Display is now
`transport fault MAY_HAVE_BEEN_RECEIVED: Request timed out`, formerly
`wallet error: Network error: Request timed out`. The response-channel closure
remains a local `NetworkError`; it supplies no remote outcome.

BEEF verification retains a `ChainTrackerError::TransportFault` as
`Error::SdkError`, so the internalize door and proof helpers preserve its
class, status, detail and rate-limit wait. SDK Display loses the former
`wallet error: Service error: ChainTracker verification failed: ` prefix.
Missing mining context and untyped tracker errors keep the service-fault text.
The service-factory lookup in the internalize door still wraps errors from a
custom `WalletServices::get_chain_tracker`; built-in Services only reports
missing configuration there. That custom-provider erasure remains under #185.

Three of the four reference/status guards in `validate_transaction_for_abort`
use `Error::into_invalid_parameter`: it wraps the variant's unchanged Display
in `SdkError(WalletRefusal { class: InvalidParameter, message })`. This is a
site-specific override, not a change to every `InvalidTransactionStatus`.
Storage-level hosts receive that SDK wrapper where these guards returned
`Error::InvalidTransactionStatus(text)`. The fourth, the unproven-status
guard, carries the same class and text in the acceptance record below.

An abort refused because acceptance is stored carries bsv-rs's typed record
(#439, the owner's 20.4 (a)): `Error::SdkError(bsv_rs::Error::AbortAccepted(
Box<AbortAcceptance>))` with `reference` (the caller's), `class` (the
guard's own), `message` (the guard's whole Display), and `txid`, `status` and
`accepting_word` when the guard knows them (`Error::abort_accepted`). The
unproven-status guard keeps `InvalidParameter` (wire code 6), its status
`unproven`; the shared stored-acceptance guard (`refuse_if_accepted`'s text)
keeps `InvalidOperation` (wire code 1, `Wallet operation failed`), its
accepting word named. The record displays the guard's text at the storage
level and through the SDK, exactly as the `WalletRefusal` of the same class
and message, and never reaches the wire. Retirement guards keep
`Error::InvalidTransactionStatus(text)` (`InvalidOperation` at the wallet
boundary) and build no record.
The `SdkError` Display is transparent for an SDK `WalletRefusal`, so the
storage caller sees the same message too; other SDK errors retain their
`SDK error: ` prefix.
`StorageProvider.ts:716-720` raises `WERR_INVALID_PARAMETER` for non-outgoing
and unabortable actions. The process door instead raises
`WERR_INVALID_OPERATION` (`storage/methods/processAction.ts:477-478`). The
port-only missing-txid, proven-descendant, stored-acceptance, unknown-status
and spent-output guards retain the nearest `InvalidOperation` judgement.
The reference's on-chain abort answers `aborted: false`
(`StorageProvider.ts:878-889`); a dedicated abort-on-chain variant remains
an owner's public API decision under #185.

The direct SDK guards below carry classed refusals too, with their
message bytes unchanged. Hosts matching `WalletError` at these doors must
read `WalletRefusalClass`; SDK Display loses the generic `wallet error: `
prefix, as for the variant mapping above:

- `sign_action`'s empty reference is `InvalidParameter`
  (`validationHelpers.ts:439-441,861`). Missing pending data is
  `NotImplemented`, the reference's `WERR_NOT_IMPLEMENTED`
  (`signer/methods/signAction.ts:57-60`). The port's expired-cache refusal
  uses that missing-session class as a judgement: the reference has no TTL.
- `internalize_action`'s output index and derivation mismatch are
  `InvalidParameter` (`signer/methods/internalizeAction.ts:57-58,88-89`).
  Its invalid-sender guard is gone (#453): the validator's sender reaches
  the derivation already parsed.
- `prove_certificate`'s absent and ambiguous certificate matches are
  `InvalidParameter` (`signer/methods/proveCertificate.ts:28-29`).

The remaining direct constructors are faults or the SDK reference's plain
`Error`/`TypeError` paths. They carry no WERR class to preserve. Since the
beta.4 re-pin they are `WalletFault` of family `Internal` (`internal_fault`,
the former `WalletError` with the same text). This table accounts for every
retained site in `wallet.rs` (line numbers as of beta.3):

| Site in `wallet.rs` | Fault and reason for an `Internal` fault |
|---|---|
| `create_action_at`, signing and processing (1390, 1402) | The wallet's atomic build failed and rolled back; not a caller-argument guard. |
| `create_action_at`, missing broadcast BEEF (1635) | The wallet failed to assemble its own broadcast bytes. |
| `create_action_at`, txid decode (1670) | The wallet produced a malformed txid. |
| `create_action_at`, unsigned build (1724) | Local transaction assembly failed. |
| `create_action_at`, reference decode (1729) | Storage returned malformed reference bytes, not the caller's reference. |
| `create_action_at`, BEEF parse/encode (1829, 1840) | Parsing stored ancestry or encoding the wallet's result failed. |
| Key-linkage methods (2015, 2019, 2056) | A key returned by the SDK failed to parse. |
| `sign_action`, sequence patch (2185) | The wallet's cached transaction could not be patched after the caller input was validated. |
| `sign_action`, signing/processing (2218, 2284) | Local signing or processing failed; compensation text is retained. A processing refusal `Error::SpendRefused` (#450) is not wrapped: it keeps its class, `InvalidParameter`, and the action is not aborted. |
| `sign_action`, broadcast preparation (2379) | Cached ancestry could not produce the wallet's broadcast bytes. |
| `sign_action`, txid decode (2415) | The signed transaction's own txid failed to decode. |
| `sign_action`, BEEF parse/encode (2433, 2443) | Parsing cached ancestry or encoding the wallet's result failed. |
| `internalize_action`, key derivation (2420) | The SDK failed to derive a key from the validated sender. |
| `prove_certificate`, missing stored keyring (2925) | Incomplete stored certificate data; `MasterCertificate.ts:35-37,350` raises plain `Error` for a missing record. |
| Field name helper (3469) | The SDK reference raises plain `Error`, `MasterCertificate.ts:23-29`. |
| Reveal array limit (3478) | The SDK reference raises plain `TypeError`, `MasterCertificate.ts:125-129`. |
| Duplicate reveal name (3486) | The SDK reference raises plain `Error`, `MasterCertificate.ts:138-140`. |
| Record entry limit (3499) | The SDK reference raises plain `Error`, `MasterCertificate.ts:43-45`. |
| Field/keyring base64 helpers (3519, 3522) | The SDK reference raises plain `Error`, `MasterCertificate.ts:63-73,351-364`. |
| Serial base64/length helpers (3859, 3864, 3869) | The SDK reference raises plain `Error`, `MasterCertificate.ts:63-78,365`. |
| Field recovery (3875) | Decryption/UTF-8 recovery failed; the reference catches these as plain `Error`, `MasterCertificate.ts:573-622`. |
| Missing reveal field (3888) | The SDK reference raises plain `Error`, `MasterCertificate.ts:370-379`. |

`direct_refusal_tests.rs` checks the direct sign, abort and certificate
lookups; `nosend_label_tests.rs` checks the internalize guards. The BEEF
proof-helper tests exercise all three transport classes, status and detail.
For #185, the custom `get_chain_tracker` factory erasure and the local
response-channel closure remain; neither is reclassified by matching text.

## ProtoWallet refusal words at beta.3 (#419 item 4)

The delegated signature and HMAC methods validate their arguments before
cryptography (bsv-rs #415). These messages are `WalletRefusal` with class
`InvalidParameter`, displayed without a prefix. Valid cryptographic misses
retain `wallet error: Signature is not valid` and `wallet error: HMAC is not valid`.

| Condition | Before beta.3 | At beta.3 |
|---|---|---|
| Noncanonical signature | `invalid signature: DER too short`, `invalid signature: Expected sequence tag 0x30`, or the DER parser's other words | `The signature parameter must be a canonical DER-encoded ECDSA signature` |
| Signature over 72 bytes | DER parser error or crypto miss | `The signature parameter must be at most 72 bytes` |
| Missing signing data/hash; both supplied | `wallet error: data or hashToDirectlySign must be provided`; both accepted | `The data, hashToDirectlySign parameter must be exactly one value` |
| Missing verification data/hash; both supplied | `wallet error: data or hashToDirectlyVerify must be provided`; both accepted | `The data, hashToDirectlyVerify parameter must be exactly one value` |
| Protocol name below 5 / above 400 bytes | `protocol validation error: protocol name must be at least 5 characters` / `protocol validation error: protocol name must be 400 characters or less` | `The protocolID[1] parameter must be at least 5 length.` / `The protocolID[1] parameter must be no more than 400 length.` |
| Empty / oversized key ID | `protocol validation error: key ID must be at least 1 character` / `protocol validation error: key ID must be 800 characters or less` | `The keyID parameter must be at least 1 length.` / `The keyID parameter must be no more than 800 length.` |
| Privileged operation without a reason | Ignored | `The privilegedReason parameter must be a value when 'privileged' is true` |
| Reason below 5 / above 50 bytes | Ignored | `The privilegedReason parameter must be at least 5 length.` / `The privilegedReason parameter must be no more than 50 length.` |
| Data over 268435456 bytes | Processed | `The data parameter must be at most 268435456 bytes` |

`tests/proto_validator_pin.rs` exercises the delegated methods and their
class and message together. The table's large data bound is the upstream
entry's reading; the toolbox witnesses do not allocate that input.

The same checks reach `get_public_key`, `encrypt`, `decrypt` and the
inherent linkage methods at beta.3 (bsv-rs #420). The identity-key branch
now checks supplied arguments too. Missing `protocolID` changes from
`wallet error: protocolID is required when identityKey is false` to
`The protocolID parameter must be a [securityLevel, protocolName] tuple`;
missing `keyID` changes from `wallet error: keyID is required when identityKey is false`
to `The keyID parameter must be a string`. An empty `keyID` uses the
length refusal in the table. `Counterparty::from_hex` now refuses a valid
uncompressed key with `The counterparty parameter must be no more than 66 length.`
where beta.2 parsed and accepted it. Oversized plaintext and ciphertext
use `The plaintext parameter must be at most 268435456 bytes` and
`The ciphertext parameter must be at most 268435456 bytes` where beta.2
processed them. The public-key, cipher and encoded-counterparty cases are
also pinned in `tests/proto_validator_pin.rs`.

Both linkage methods delegate through ProtoWallet's `WalletInterface`
adapter, retaining `privileged` and `privilegedReason` for its beta.3
validator. The inherent argument types omit those fields. Missing, short
and long reasons use the same three refusal texts above, before any
linkage cryptography; the returned counterparty keys remain compressed.

## HMAC and the result rows at beta.4 (#488)

`create_hmac` and `verify_hmac` delegate to bsv-rs's `ProtoWallet`, so since
the beta.4 re-pin (bsv-rs #379) the wallet issues the amended BRC-56 tag,
keyed by the ECDH X coordinate without its leading zero octets, and accepts
both that tag and the 32-byte-key tag (the named, counted legacy form). At
beta.3 it issued the 32-byte-key tag and refused the minimal-length one with
`HmacMinimalLengthKey`. Witness: `hmac_key_tests.rs` (bsv-rs's two
leading-zero vectors).

A JSON or wire client of the wallet (bsv-rs's `HttpWalletJson` and
`WalletWireTransceiver`, the CLI's daemon among the hosts) applies the
reference client's result rows since beta.4 (bsv-rs #487, #495). The
wallet's answers read back through `HttpWalletJson` against a loopback mock:
`discovery_rows_tests.rs` (the nine discovery rows; an overlay entry the
reference drops, `OverlayCertificate::is_reference_record`, is dropped from
the answer, so an unsigned one, an empty keyring value or a value
`Certificate.verify()` refuses before it verifies (an empty or non-canonical
DER signature, a malformed outpoint, type, serial or field name; D28) no
longer reaches R74 or R80; since #495 the door also verifies each overlay
signature and decrypts the fields, see Discovery trust) and `result_rows_tests.rs` (R86's keyring,
the `createSignature` data form, and CLI #371's unsigned input recorded in
today's words).

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Wallet<S, V>                            │
│  (Implements WalletInterface with full storage and services)    │
├─────────────────────────────────────────────────────────────────┤
│  ProtoWallet     │    Storage (S)     │    Services (V)         │
│  - Key derivation│    - UTXOs         │    - Broadcasting       │
│  - Signing       │    - Transactions  │    - Merkle proofs      │
│  - Encryption    │    - Certificates  │    - UTXO status        │
│  - HMAC          │    - Labels/Tags   │    - Chain height       │
├──────────────────┴───────────────────┴──────────────────────────┤
│                       WalletSigner                              │
│  - BIP-143 sighash computation                                  │
│  - P2PKH / P2PK unlocking script generation                     │
│  - Key derivation via BRC-29 protocol + counterparty            │
│  - Unlocking script templates for deferred signing              │
├─────────────────────────────────────────────────────────────────┤
│                    PendingTransaction Cache                     │
│  - Caches unsigned transactions for deferred signing            │
│  - 24-hour TTL with automatic cleanup                           │
├─────────────────────────────────────────────────────────────────┤
│              BRC-98/99 Permission Enforcement                   │
│  - Protocol access control (DPACP)                              │
│  - Basket access control (DBAP)                                 │
│  - Certificate access control (DCAP)                            │
│  - Spending authorization (DSAP)                                │
├─────────────────────────────────────────────────────────────────┤
│              Balance & Sweep Helpers (P3-07)                    │
│  - balance() / balance_and_utxos() for UTXO summaries           │
│  - sweep_to_address() for full-wallet sweeps                    │
├─────────────────────────────────────────────────────────────────┤
│                  Certificate Issuance Protocol                  │
│  - BRC-104 mutual auth with certifiers via bsv-rs Peer (#315)   │
│  - Field encryption and master keyring creation                 │
│  - HMAC-based serial number verification                        │
├─────────────────────────────────────────────────────────────────┤
│              Overlay Service Discovery (lookup.rs)              │
│  - OverlayLookupResolver trait for SLAP/SHIP queries            │
│  - HttpLookupResolver with endpoint failover                    │
│  - BEEF/PushDrop certificate parsing                            │
├─────────────────────────────────────────────────────────────────┤
│              Scaffolding Types                                  │
│  - PrivilegedKeyManager trait (two-factor auth, wired in)       │
│  - LookupResolver (legacy stub in wallet.rs)                    │
│  - WalletLogger (operation diagnostics)                         │
└─────────────────────────────────────────────────────────────────┘
```

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | ~102 | Module declaration with documentation, exports `Wallet`, `WalletOptions`, `WalletSigner`, `PendingTransaction`, `SignerInput`, `ScriptType`, `UnlockingScriptTemplate`, `WalletBalance`, `UtxoInfo`, `HttpLookupResolver`, `OverlayCertificate`, `OverlayLookupResolver` |
| `wallet.rs` | ~3547 | Main `Wallet<S, V>` struct implementing `WalletInterface` with 28 methods, plus `PendingTransaction`, `WalletBalance`, `UtxoInfo`, `PrivilegedKeyManager`, `LookupResolver`, `WalletLogger`, balance/sweep helpers, permission checks, and helper functions |
| `signer.rs` | ~993 | `WalletSigner` for transaction signing with BIP-143 sighash, `UnlockingScriptTemplate` for deferred signing, transaction parsing, and script generation |
| `certificate_issuance.rs` | ~1430 | Certificate issuance protocol implementation (BRC-104) for acquiring certificates from certifier services |
| `certificate_issuance_auth.rs` | ~130 | The issuance request over BRC-104 mutual auth: a Peer per call over the wallet's `ProtoWallet`, the answer verified and its sender returned (#315) |
| `direct_acquire.rs` | ~210 | Direct `acquire_certificate` argument validation and normalization, signature verification and keyring decryption proof before storage (#311, #374) |
| `label.rs` | ~110 | Label normalization as validateLabel leaves a label, ECMAScript's trim and lowercase, for both doors and list filters (#379, #392), and the BRC-177 module-label matcher both doors call (#363); declared without a feature gate, the sqlx folder imports it as `label` (#420); `normalize_label` and `trim_ecmascript` are public since #505, re-exported as `bsv_wallet_toolbox_rs::wallet::{normalize_label, trim_ecmascript}`, the one definition of the reference's label rule for every host (bsv-wallet-cli#376's spend gate); `is_ecmascript_whitespace` stays private |
| `discovery_search.rs` | ~320 | The reference's attribute matcher (`identityUtils.ts:140-280`): search normalization, legacy `/gi` literals, the five-class tokenizer and stop words, the NFD and Mn fold (#474) |
| `lookup.rs` | ~760 | Overlay service discovery: `OverlayLookupResolver` trait, `HttpLookupResolver` implementation, SLAP host parsing, deduplication |
| `lookup_decode.rs` | ~175 | The overlay output read as the reference's `decodeIdentityOutput` (`decode_identity_output`), and the door's signature check and decryption (`OverlayCertificate::verified_and_decrypted`) (#495) |
| `lookup_evidence.rs` | ~360 | Each overlay output's chain evidence verified with the wallet's tracker before its token is read and fenced after the certificate's crypto, the reference's `IdentityEvidenceVerifier` and its limits (`overlay_certificates`, #502) |
| `lookup_record.rs` | ~230 | The reference's bounds on the overlay record (`certificateRecord`, `is_reference_record`) |
| `lookup_fixture.rs` | ~130 | Test only: the reference's identity record, its keyring for `anyone` and its token, as `publicly_reveal_attributes` builds them |

## Key Exports

### Wallet Struct

```rust
pub struct Wallet<S, V>
where
    S: WalletStorageProvider + Send + Sync,
    V: WalletServices + Send + Sync,
{
    proto_wallet: ProtoWallet,
    storage: Arc<S>,
    services: Arc<V>,
    identity_key: String,
    chain: Chain,
    options: WalletOptions,
    signer: WalletSigner,
    pending_transactions: Arc<RwLock<HashMap<String, PendingSignAction>>>,
    user_id: i64,
    privileged_key_manager: Option<Arc<dyn PrivilegedKeyManager>>,
    permissions_manager: Option<Arc<WalletPermissionsManager>>,
}
```

Generic wallet implementation parameterized by:
- `S` - Storage backend (e.g., `StorageSqlx`, `StorageClient`)
- `V` - Services backend (e.g., `Services`)

The `user_id` field is resolved during construction via `storage.find_or_insert_user()` and used to build `AuthId` for all storage calls.

The `privileged_key_manager` field is optional; when set via `set_privileged_key_manager()`, crypto operations at SecurityLevel >= 2 are routed through it for two-factor authentication.

The `permissions_manager` field is optional; when set via `set_permissions_manager()`, wallet operations check BRC-98/99 permissions (protocol, basket, certificate, spending) before proceeding.

### PendingTransaction

```rust
pub struct PendingTransaction {
    pub reference: String,              // Unique reference for this pending transaction
    pub raw_tx: Vec<u8>,                // The unsigned transaction bytes
    pub inputs: Vec<SignerInput>,       // Input metadata for signing
    pub input_beef: Option<Vec<u8>>,    // BEEF data for broadcasting
    pub is_no_send: bool,               // Whether to skip broadcast
    pub is_delayed: bool,               // Whether delayed broadcast was requested
    pub send_with: Vec<String>,         // TXIDs to send with
    pub created_at: DateTime<Utc>,      // When this pending transaction was created
}
```

Cached transaction awaiting signature via `sign_action`. Created when `create_action` is called with `sign_and_process = false`. Expires after 24 hours (configurable via `PENDING_TRANSACTION_TTL_SECS`).

`SignableTransaction.reference` holds the storage reference's decoded bytes: 12 random bytes, not the UTF-8 bytes of its base64 text (#285). Base64-encode that field for `SignActionArgs.reference`; the pending cache and storage RPC use the original base64 string. A malformed base64 storage reference fails the deferred create result before caching with `WalletError("Invalid storage reference: ...")`.

The binary wallet wire carries the raw bytes after their varint length. Since the `bsv-rs =0.9.0-beta.2` pin (bsv-rs edfc535) direct JSON serde writes this field as base64, the reference's JSON string, which is the stored reference text byte for byte; at beta.1 it was hex.

### WalletBalance & UtxoInfo

```rust
pub struct WalletBalance {
    pub total: u64,              // Total satoshis across spendable outputs
    pub utxos: Vec<UtxoInfo>,    // Individual UTXOs (empty for balance(), populated for balance_and_utxos())
}

pub struct UtxoInfo {
    pub satoshis: u64,           // Satoshi value of this output
    pub outpoint: String,        // Outpoint in "txid.vout" format
}
```

Returned by `balance()` and `balance_and_utxos()`. Both use `#[serde(rename_all = "camelCase")]` for cross-SDK wire compatibility.

### WalletOptions

```rust
pub struct WalletOptions {
    pub script_verifier: Option<Arc<dyn SpendVerifier>>, // Default: None
    pub rng_seed: Option<[u8; 32]>,              // Default: None (OS entropy); separate from the storage seed
    pub include_all_source_transactions: bool,  // Default: true
    pub auto_known_txids: bool,                 // Default: false
    pub trust_self: Option<String>,             // Default: Some("known")
    pub settings_manager: Option<Arc<WalletSettingsManager>>, // Default: None
}
```

`SpendVerifier`, `SpendRequest` and `SpendVerificationContext` are exported
at the crate root beside `WalletOptions`. The methods are synchronous where
`SpendVerifierInterface.ts:13-43` at ts-stack@edf6e03 returns promises.
`create_args.rs` validates argument shape, and `signer.rs` constructs signatures;
neither evaluates spending scripts. The wallet field has no in-process reader:
the port's signer check runs in storage, where the reference's runs in the wallet.
`Wallet` receives an existing generic storage provider and has no verifier
configuration hook on that trait. Set `StorageSqlxOptions.script_verifier` when
constructing the store to use the host backend at the live internalize door,
the createAction caller-inputBEEF pre-check, and own transaction checks at
createAction signing and signAction. Both options default to `None`; unselected
inputs and absent verifiers keep the bsv-rs interpreter.

Configuration for wallet behavior:
- `include_all_source_transactions` - Include source transactions for all inputs in signable transactions
- `auto_known_txids` - TXIDs known to wallet's party beef don't need to be returned from storage
- `trust_self` - Input BEEF validation behavior ("known" = trust wallet's known TXIDs)
- `settings_manager` - Shared host trust settings, read on every discovery call; absent means Main defaults or Test defaults for Test and Regtest

### WalletSigner

```rust
pub struct WalletSigner {
    root_key: Option<PrivateKey>,
}
```

Handles transaction signing using key derivation from `ProtoWallet`. The wallet authorizes automatic signing only for storage-allocated inputs, using their derivation prefix/suffix and BRC-29 protocol (`"3241645161d8"` at SecurityLevel::Counterparty). Caller-named inputs use the caller's unlocking scripts, even when their stored outputs carry derivation metadata.

**Methods:**
- `new(root_key: Option<PrivateKey>)` - Create a new signer (uses "anyone" key if None)
- `sign_transaction(&self, unsigned_tx, inputs, proto_wallet) -> Result<Vec<u8>>` - Sign inputs without unlocking scripts using their derivation metadata, and insert supplied scripts; the wallet checks signing authority before calling it
- `sign_input(&self, tx_data, input_index, input, proto_wallet) -> Result<Vec<u8>>` - Sign a single input, returns unlocking script bytes
- `create_unlock_template(&self, prefix, suffix, script_type, satoshis) -> UnlockingScriptTemplate` - Create a template for deferred signing
- `apply_templates(&self, raw_tx, templates, proto_wallet) -> Result<Vec<u8>>` - Apply unlocking script templates to sign inputs

### UnlockingScriptTemplate & ScriptType

```rust
pub struct UnlockingScriptTemplate {
    pub derivation_prefix: String,
    pub derivation_suffix: String,
    pub script_type: ScriptType,
    pub satoshis: u64,
}

pub enum ScriptType {
    P2PKH,
    P2PK,
}
```

Templates for deferred signing where the wallet controls the keys. Captures BRC-29 key derivation parameters, script type, and satoshi value (required for BIP-143 sighash) for later unlocking script generation.

### SignerInput

```rust
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
```

Input metadata required for signing. Passed from storage results to the signer.

## Scaffolding Types (wallet.rs)

### PrivilegedKeyManager Trait

```rust
#[async_trait]
pub trait PrivilegedKeyManager: Send + Sync {
    async fn get_public_key(...) -> Result<GetPublicKeyResult>;
    async fn encrypt(...) -> Result<EncryptResult>;
    async fn decrypt(...) -> Result<DecryptResult>;
    async fn create_hmac(...) -> Result<CreateHmacResult>;
    async fn verify_hmac(...) -> Result<VerifyHmacResult>;
    async fn create_signature(...) -> Result<CreateSignatureResult>;
    async fn verify_signature(...) -> Result<VerifySignatureResult>;
}
```

For two-factor authentication where crypto operations at SecurityLevel >= 2 are routed through a separate key manager. The wallet stores an optional `Arc<dyn PrivilegedKeyManager>` and provides `set_privileged_key_manager()` / `privileged_key_manager()` accessors.

### LookupResolver (legacy stub)

Overlay certificate discovery via HTTP in wallet.rs. Has `new(hosts)` and `query(attributes)` methods. Returns empty results with a `tracing::debug!` log. Superseded by the `OverlayLookupResolver` trait in `lookup.rs`.

### WalletLogger

Structured operation logger with indented groups, timestamps, and log levels. Provides `group()`/`group_end()`, `log()`, `error()`, and `to_log_string()` methods.

## Overlay Service Discovery (lookup.rs)

The `lookup.rs` module provides trait-based overlay service lookups for `discover_by_identity_key` and `discover_by_attributes`. Exported via `mod.rs` as `HttpLookupResolver`, `OverlayCertificate`, and `OverlayLookupResolver`.

### OverlayCertificate

Wire format for certificates returned from overlay SLAP/SHIP services. Fields: `type_id`, `serial_number`, `subject`, `certifier`, `revocation_outpoint`, `fields`, `keyring`, `signature`, `decrypted_fields`. The overlay record is the leading eight; `decrypted_fields` is filled by the discovery door from the keyring's decryption, never from the wire (a record carrying `decryptedFields` is dropped by the parser) nor from a resolver (#495, #494). Provides:
- `to_identity_certificate()` - Convert to SDK's `IdentityCertificate` format
- `dedup_key()` - Returns `(type_id, serial_number)` tuple for deduplication

The beta.3 pin uses `IndexMap<String, String>` at these public sites (bsv-rs #385/#394). Insertion order follows the reference's `Object.keys` order for non-integer keys; integer-like keys retain the upstream ordering difference noted below.

| Public site | Reason for `IndexMap` |
|---|---|
| `OverlayCertificate.fields` | Copies directly into bsv-rs's `WalletCertificate.fields`, whose type is `IndexMap`, retaining certificate field order. |
| `OverlayCertificate.keyring` | Copies directly into bsv-rs's `IdentityCertificate.publicly_revealed_keyring`, whose type is `IndexMap`, retaining keyring order. |
| `OverlayCertificate.decrypted_fields` | Copies directly into bsv-rs's `IdentityCertificate.decrypted_fields`, whose type is `IndexMap`, retaining decrypted field order. |
| `OverlayLookupResolver::lookup_by_attributes` | Receives bsv-rs's `DiscoverByAttributesArgs.attributes`, whose type is `IndexMap`, retaining attribute order through lookup JSON serialization. |

### OverlayLookupResolver Trait

```rust
#[async_trait]
pub trait OverlayLookupResolver: Send + Sync {
    async fn lookup_by_identity_key(&self, identity_key: &str, certifiers: &[String]) -> Result<Vec<OutputListItem>>;
    async fn lookup_by_attributes(&self, attributes: &IndexMap<String, String>, certifiers: &[String]) -> Result<Vec<OutputListItem>>;
}
```

Since #502 (api-break) a resolver answers the overlay's outputs, each with
its BEEF and index (bsv-rs's `OutputListItem`, the items of an
`output-list` answer), never certificates: the door verifies each output's
chain evidence and reads its token (Discovery trust, below).

The wallet supplies the sorted trusted keys on both calls. Implementors add
the `certifiers` parameter; the CLI implements no resolver today (#347).
`HttpLookupResolver` includes the array in both query bodies. Discovery still
applies the wallet's trust settings to the returned certificates.

### HttpLookupResolver

Default implementation that queries overlay `/lookup` endpoints via HTTP POST. Features:
- `new(endpoint)` / `with_endpoints(endpoints)` - Single or multi-endpoint
- `for_network(preset)` / `mainnet()` / `testnet()` - Network preset constructors
- Endpoint failover: tries each endpoint in order, skips on error
- Answers the outputs of the overlay's `output-list` answer as it read them (`answer_outputs`); the door verifies and decodes them
- Default hosts: `https://lookup.babbage.systems` (mainnet), `https://staging-lookup.babbage.systems` (testnet)
- 10-second request timeout per endpoint

### Helper Functions

- `answer_outputs(answer)` - The outputs of an `output-list` answer; any other answer has none
- `overlay_certificates(outputs, tracker, chain)` - The door's reading of a resolver's outputs: each output's chain evidence, its token, its certificate, the fence (`lookup_evidence.rs`, #502)
- `decode_identity_output(script)` - The reference's checks on the token: two fields in their bounds, five chunks with each push opcode its length asks and `OP_2DROP`, the record's bounds, the locking key and the field signature (#495)
- `dedup_certificates(certs)` - Deduplicate by `(type_id, serial_number)`, keeping first occurrence

## WalletInterface Methods

The `Wallet` implements all 28 methods from `WalletInterface`:

### Cryptographic Operations (delegated to ProtoWallet)

| Method | Description |
|--------|-------------|
| `get_public_key` | Get identity or derived public key |
| `encrypt` / `decrypt` | AES-GCM encryption with derived keys |
| `create_hmac` / `verify_hmac` | HMAC-SHA256 operations |
| `create_signature` / `verify_signature` | ECDSA signatures |
| `reveal_counterparty_key_linkage` | Reveal key linkage to verifier for counterparty |
| `reveal_specific_key_linkage` | Reveal specific protocol/key_id linkage |

### Action Operations (coordinated with storage and services)

| Method | Description |
|--------|-------------|
| `create_action` | Create Bitcoin transaction with automatic or deferred signing/broadcast |
| `sign_action` | Sign previously created transaction from pending cache |
| `abort_action` | Cancel transaction in progress |
| `list_actions` | List transactions matching labels |
| `internalize_action` | Import external transaction into wallet (validates BRC-29 derivation params) |

### Output Operations (delegated to storage)

| Method | Description |
|--------|-------------|
| `list_outputs` | List spendable outputs in a basket |
| `relinquish_output` | Remove output from basket tracking |

### Certificate Operations (delegated to storage)

| Method | Description |
|--------|-------------|
| `acquire_certificate` | Acquire identity certificate (direct or issuance protocol); direct acquisition validates and normalizes arguments, verifies the signature and proves the keyring by decrypting every field before storing the encrypted values (#291, #311, #374) |
| `list_certificates` | List certificates by certifier/type |
| `prove_certificate` | Prove certificate fields to verifier by creating a verifier-specific keyring |
| `relinquish_certificate` | Remove certificate from wallet |

### Discovery Operations

| Method | Description |
|--------|-------------|
| `discover_by_identity_key` | Merge stored and overlay certificates, bind the subject, apply current trust settings, then page |
| `discover_by_attributes` | Query the overlay with trusted keys, keep the certificates whose decrypted fields match the attributes, apply current trust settings, then page |

### Chain/Status Operations

| Method | Description |
|--------|-------------|
| `is_authenticated` | Always returns true (wallet has key) |
| `wait_for_authentication` | Always returns true |
| `get_height` | Current blockchain height from services |
| `get_header_for_height` | Block header at height from services |
| `get_network` | Network (mainnet/testnet) |
| `get_version` | Returns "bsv-wallet-toolbox-0.1.0" |

## Additional Wallet Methods (beyond WalletInterface)

| Method | Description |
|--------|-------------|
| `set_privileged_key_manager(manager)` | Set the privileged key manager for 2FA |
| `privileged_key_manager()` | Get reference to privileged key manager |
| `set_permissions_manager(manager)` | Set BRC-98/99 permissions manager |
| `permissions_manager()` | Get reference to permissions manager |
| `balance()` | Total spendable satoshis from default basket (paginated) |
| `balance_and_utxos()` | Total + individual `UtxoInfo` list from default basket |
| `sweep_to_address(address)` | Sweep all funds to a BSV address via `create_action` |
| `list_failed_actions()` | List txids with Failed status |
| `list_no_send_actions()` | List txids with NoSend status |
| `get_known_txids()` | List txids with Completed/Unproven/Sending status |
| `destroy()` | Destroy wallet storage (delegates to `storage.destroy()`) |
| `get_identity_key()` | Identity key as owned String |
| `get_storage_identity()` | Returns `(storage_identity_key, storage_name)` tuple |

### Permission Check Helpers (internal)

When `permissions_manager` is set, operations automatically check permissions:
- `check_protocol_permission(originator, protocol, counterparty, usage_type)` - DPACP enforcement
- `check_basket_permission(originator, basket, usage_type)` - DBAP enforcement
- `check_spending_permission(originator, satoshis)` - DSAP enforcement
- `check_certificate_permission(originator, privileged, verifier, cert_type, fields, usage_type)` - DCAP enforcement

All check helpers return `Ok(())` if no permissions manager is set.

## Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `WALLET_VERSION` | "bsv-wallet-toolbox-0.1.0" | Version string returned by `get_version` |
| `PENDING_TRANSACTION_TTL_SECS` | 86400 (24 hours) | TTL for pending transactions in cache |
| `CERTIFICATE_FIELD_ENCRYPTION_PROTOCOL` | "certificate field encryption" | Protocol for BRC-52/53 field encryption |

## Transaction Signing Flow

### Immediate Signing (sign_and_process = true, default)

```
CreateActionArgs → storage.create_action_at_door(args, CreateActionDoor { offer, must_spend, sign_and_process })
    ├─ one write transaction: the create step's rows
    │     → the signer closure: build_unsigned_transaction(result, key_deriver)
    │                           → WalletSigner.sign_transaction()
    │     → processAction's writes → ONE commit
    → services.post_beef() (if not no_send/delayed)
    → storage.update_transaction_status_after_broadcast() (a failure is logged, never the call's error)
    → CreateActionResult { txid, tx unless returnTXIDOnly, ... }
```

For each storage-allocated input: derive the signing key from the BRC-29 protocol/counterparty, compute the BIP-143 sighash, sign and build the unlocking script. Caller-named inputs retain their supplied scripts.

**The signed transaction's word (#450).** The wallet's own transaction is judged at its signing, as the reference's signer judges it (`signer/methods/createAction.ts:120`, before `processAction` at :138; `signAction.ts:90`, after it at :76; ts-stack@edf6e03). Inside the door, after the signer and before processAction's writes, each input whose source bytes are at hand (the untrimmed input BEEF, before `knownTxids` trims the caller's copy, or the caller's as supplied; else the store) runs under the node's word (`beef_verification::verify_own`, the #422 seam); an input without source bytes is skipped (`verifyUnlockScripts.ts:316-321`). A failing input refuses the call with bsv-rs's `Error::SpendRefused`, `spend refused at offset N, input I of <txid>: <why>` (for example `The signature must use SIGHASH_FORKID.`), class `InvalidParameter`, before the post: nothing written, nothing posted. `signAction` meets the same refusal from `process_action_internal`, before its writer and the post (the reference's comes after its post; `docs/ARCHITECTURE.md` D18); the wallet does not abort the action for it, so the action stays unsigned, its inputs reserved and its pending transaction cached, and the caller may sign again. The word reads the header tip before it judges, the spend's era at tip+1; a tracker with no mining tip, or a tip read that faults, no longer holds the call (#464, the keeper's clause of 2026-10-29): bsv-rs's constants name no activation height still ahead, so the spend's word is the latest era, post-Chronicle, the coin's bits from its proof height, as the reference reads no tip (`verifyUnlockScripts.ts:331`), and the hold `Service error: ChainTracker verification failed: the header service supplied no mining tip` returns only if the constants carry an activation not yet reached (`create_judges_its_own_input_at_the_latest_era_without_a_tip`, `sign_action_judges_its_own_input_at_the_latest_era_without_a_tip`). The details and the word's limits are in `src/storage/sqlx/CLAUDE.md` (The wallet's own signed transaction).

**Error recovery:** a refusal inside the door (the create step, the signer, processAction's writes) rolls the one transaction back: nothing was written, so a busy is `WalletBusy` and any other refusal keeps its text (`Signing failed: ...`, `Process action failed: ...`). After the broadcast a store fault is logged with the marker `post_broadcast_status_write_failed` and the call answers its success with the txid; the monitor's send-waiting task heals the row (bsv-wallet-cli #44). The same rule after any committed write: `abort_action` on a broadcast transaction answers `{ aborted: true }` when the descendants' retirement after its commit fails (marker `post_commit_retire_failed`; the review-status pass retires them). Coins held only by other calls in flight are `WalletBusy` (`Store busy: coins held by calls in flight`), never `Insufficient funds`. `create_action_offering(args, originator, offer, must_spend)` is the same call with the change coins limited to the host's offered output ids. `must_spend: &[i64]` (#353, bsv-wallet-cli #313; the CLI's `--from` wraps it) names output ids the action spends ahead of the pick: each counts toward the target and is signed as a picked coin (storage-provided change, the wallet's derivation; one path for the door's signer, `sign_action` and the door bench), the pick funds the remainder and the fee from the rest of the offer, change returns as always. Each must be wallet-managed change inside the offer, the user's and unspent, and a received coin whose transaction is not `completed` needs the action's `underReceivedRule` label (R1 (d)); any other is refused by name (`Validation error: mustSpend[i]: output ...`: `is outside the offer`, `is named twice`, `is not this user's`, `is not wallet-managed change`, `is spent or reserved`, `is a received coin whose transaction is <status>, not completed; the action does not name underReceivedRule`, `is not spendable: its transaction is <status>`), nothing written. Every `mustSpend[i]` text is the port's own: the reference names a transaction's inputs through `inputs` alone, and the offering call with its list is the port's composition. `&[]` is the offer alone; `inputs` and the served `create_action` do not move.

### Deferred Signing (sign_and_process = false or a named input lacks a script)

```
CreateActionArgs → storage.create_action() → build_unsigned_transaction()
    → Cache in pending_transactions → CreateActionResult { signable_transaction }

Later via sign_action:
SignActionArgs → Lookup from cache → Merge and require caller unlocking scripts
    → WalletSigner.sign_transaction() → storage.process_action()
    → services.post_beef() → storage.update_transaction_status_after_broadcast()
    → SignActionResult { txid, tx, ... }
```

The private cache retains which inputs storage allocated (`provided_by = "storage"`), separately from the public `PendingTransaction` data. A caller-named input is never authorized for wallet signing. Its non-empty script from `create_action` or `sign_action` is preserved. This port refuses a missing or empty named-input script before signing or processing, using the reference's `The inputs[N].unlockScript parameter must be valid.` text and mapping its `WERR_INVALID_PARAMETER` to `Error::ValidationError`; `ts-stack@edf6e03` completes and processes the action before `verifyUnlockScripts` reports a missing script's refusal, and refuses an empty script earlier, at its argument gate, as not a hexadecimal string (`validationHelpers.ts:522-530`), the same class. The WalletInterface boundary carries this as `WalletRefusal` of class `InvalidParameter`, with `Validation error: ...` as its message. The action stays unsigned and its inputs stay reserved, so the caller can supply the script on a later call. Reference: `ts-stack@edf6e03`'s `signer/methods/signAction.ts:65-90`, `buildSignableTransaction.ts:176-189`, `completeSignedTransaction.ts:15-32,50-79`, and `verifyUnlockScripts.ts:51-64`.

For #211, `sign_action` accepts spends only for caller inputs deferred with `unlockingScriptLength`, refusing storage-allocated or absent vins and replacements for scripts supplied at `create_action` with `Error::ValidationError` and the text `The args parameter must be spend does not correspond to prior input with valid unlockingScriptLength.`, without changing storage or the pending action, as in `ts-stack@edf6e03`'s `completeSignedTransaction.ts:19-24` and `WERR_errors.ts:105`.

For #216, the private pending-action cache also retains each caller's declared `unlockingScriptLength` from the create args and `sign_action` refuses a longer script before signing or writing with `Error::ValidationError`, while accepting equal or shorter scripts (`ts-stack@edf6e03` `completeSignedTransaction.ts:25-30`, whose rendered message has no trailing period).

### Broadcast Flow

When broadcasting (not no_send, not delayed):
1. Parse `input_beef` into BEEF structure
2. Merge signed transaction into BEEF (`merge_raw_tx`)
3. Downgrade BEEF V2 to V1 for ARC compatibility (if all txs have full data)
4. Broadcast via `services.post_beef()`
5. Check results: at least one provider must return "success"
6. Call `storage.update_transaction_status_after_broadcast(txid, success)` to update status
7. On broadcast failure: return error with "Transaction marked as failed and inputs restored"

### Change Output Locking Scripts

`build_unsigned_transaction` accepts an optional `KeyDeriverApi` reference. For outputs with an empty `locking_script` but a `derivation_suffix`, it derives the P2PKH locking script using BRC-29 key derivation (protocol `"3241645161d8"`, key_id `"{prefix} {suffix}"`, counterparty Self).

## Sighash Computation

The signer implements BIP-143 style sighash (required for BSV) in `signer.rs`:

```rust
fn compute_sighash(
    tx_data: &[u8],
    input_index: u32,
    locking_script: &[u8],
    satoshis: u64,
) -> Result<[u8; 32]>
```

Computes:
1. `hashPrevouts` - Double SHA256 of all outpoints (txid + vout pairs)
2. `hashSequence` - Double SHA256 of all sequences
3. `hashOutputs` - Double SHA256 of all outputs (satoshis + script for each)
4. Preimage construction: version (4 bytes) + hashPrevouts + hashSequence + outpoint (36 bytes) + scriptCode + value (8 bytes) + sequence (4 bytes) + hashOutputs + locktime (4 bytes) + sighash type (4 bytes)
5. Final hash: Double SHA256 of preimage
6. SIGHASH_ALL | SIGHASH_FORKID (0x41)

## Script Types Supported

The signer recognizes and signs (implemented in `build_unlocking_script`):

| Script Type | Detection | Unlocking Script |
|-------------|-----------|------------------|
| P2PKH | 25 bytes: `76 a9 14 <20-byte hash> 88 ac` (OP_DUP OP_HASH160 PUSH20 \<hash\> OP_EQUALVERIFY OP_CHECKSIG) | `<sig+0x41> <pubkey>` |
| P2PK | >=35 bytes, starts with pubkey length (33 or 65), ends with `ac` (OP_CHECKSIG) | `<sig+0x41>` |

Note: Signature includes SIGHASH_ALL \| SIGHASH_FORKID (0x41) appended.

## Usage

### Creating a Wallet

```rust
use bsv_wallet_toolbox::{Wallet, StorageSqlx, Services};
use bsv_sdk::primitives::PrivateKey;

let storage = StorageSqlx::open("wallet.db").await?;
storage.migrate("my-wallet", &storage_identity_key).await?;
storage.make_available().await?;

let services = Services::mainnet();
let wallet = Wallet::new(Some(PrivateKey::random()), storage, services).await?;

// Use WalletInterface methods
let result = wallet.create_action(args, "app.example.com").await?;
```

### Testnet / Custom Options

```rust
// With custom options
let wallet = Wallet::with_options(root_key, storage, services, WalletOptions {
    include_all_source_transactions: false,
    auto_known_txids: true,
    trust_self: Some("known".to_string()),
}).await?;

// With specific chain
let wallet = Wallet::with_chain(root_key, storage, services, WalletOptions::default(), Chain::Test).await?;
```

## Accessor Methods

| Method | Returns |
|--------|---------|
| `identity_key()` | `&str` - Wallet's identity public key (hex) |
| `chain()` | `Chain` - Network (Main/Test) |
| `storage()` | `&S` - Reference to storage backend |
| `services()` | `&V` - Reference to services backend |
| `options()` | `&WalletOptions` - Configuration options |
| `auth()` | `AuthId` - Creates an AuthId for the current user (internal) |

## Wallet Constructors

| Constructor | Description |
|-------------|-------------|
| `new(root_key, storage, services)` | Default options, mainnet |
| `with_options(root_key, storage, services, options)` | Custom options, mainnet |
| `with_chain(root_key, storage, services, options, chain)` | Full control over all parameters |

Initialization flow: Creates `ProtoWallet` -> gets identity key -> creates `WalletSigner` -> verifies storage is available -> ensures user exists via `find_or_insert_user` -> stores `user_id` -> sets `privileged_key_manager` and `permissions_manager` to None.

## Originator Validation

All `WalletInterface` methods take `originator: Option<&str>` (bsv-rs #329,
from the `=0.9.0-beta.2` pin), as do `Wallet::process_action` and
`Wallet::create_action_offering`. `None` is the reference's `undefined` and is
accepted (`validationHelpers.ts:1003-1004`); `Some("")` is a supplied string
and is refused by the length rule at `:1006` (#284). The permission checks
receive an absent originator as `""`.

Supplied strings are checked after ECMAScript trimming and lowercasing, using
`ts-stack@edf6e03` `packages/sdk/src/wallet/validationHelpers.ts:1005-1036`:

- The normalized string has 1 to 250 UTF-8 bytes.
- An optional port has 1 to 5 ASCII digits and is at most 65535.
- The hostname has 1 to 250 bytes; each dot-separated label has 1 to 63 bytes
  and contains lowercase ASCII letters, digits or internal hyphens.
- Single-label hostnames are accepted. Validation leaves the caller's original
  string for downstream permission checks, as the reference wallet doors do.

An empty or whitespace-only string is refused with
`The originator parameter must be at least 1 length.` inside `Validation error:`.
Other length errors name `originator`, `originator hostname` or `originator part`.
Malformed hostnames and ports use
`The originator parameter must be a canonical DNS hostname with an optional port`.

The storage client's BRC-31 peer passes a fixed internal originator,
`Some("bsv-wallet-toolbox")`.

## Implementation Notes

### Discovery trust

`discovery_trust.rs` applies the TS transform (`identityUtils.ts:313-384`):
up to 256 input certificates, trusted certifiers only, each certifier counted
once per subject, certificates deduplicated by type, serial and certifier
within that subject, the subject's combined trust threshold, then a stable
sort by descending certifier trust. Descriptions are cut at a UTF-8 boundary
to 50 bytes, trimmed as ECMAScript, and use `Trusted certifier` below 5 bytes.
An absent icon uses the reference's fallback. Pagination follows the merge
and transform, with default limit 10; `totalCertificates` is the full count.

The local identity leg is the port's extension. With a host manager, changing
its threshold or certifiers applies on the next call. With no manager the
chain defaults still discover trusted certificates. Attributes use only the
overlay, so a proxy-isolated CLI fixture needs an overlay fixture to answer
that call; host trust settings alone enable its local identity leg.

Every overlay output's chain evidence is verified before its token is read
(#502; `IdentityEvidenceVerifier.parse`, `identityUtils.ts:600-635` at
ts-stack@edf6e03). The door reads the services' chain tracker
(`overlay_tracker`, as `Wallet.ts:894-911` reads `getChainTracker()`); with
none (the services' not-configured answer, `NO_HEADER_SERVICE`) it asks no
resolver and both paths answer no overlay certificate, as `queryOverlay`
answers `[]` (`:399-400`). Any other `get_chain_tracker` error is the
discover call's answer, class and text unchanged, as the reference's
uncaught `getChainTracker()` is; the identity path's local records are not
answered alongside it (#521). Every resolver answers outputs
with their BEEF; for each (`lookup_evidence.rs`): the BEEF's transaction,
the Atomic subject or else its last, holds the output index; its
unconfirmed graph is inside the BEEF, its inputs and scripts inside
`defaultTransactionEvidenceLimits`; every BUMP root is one the tracker
holds; a transaction at its block's offset 0 is 100 blocks deep; bsv-rs's
stream verifier judges the structure, the proofs and each unproven input
under the network's `ChainParams` with the doors' 2^31 work budget per
input and the reference's 16 MiB of stack memory. A failure drops the
output; an answer past 512 outputs, 1 MiB of one BEEF or 16 MiB in all
answers none. Then the reference's decode
(#495; `identityUtils.ts:493-557`) of the token read from that
transaction's output (`decode_identity_output`): the PushDrop's two
fields, the record of 1 byte to 256 KiB and the field signature of 8 to 80,
exactly five chunks (the 33-byte key, `OP_CHECKSIG`, each field under the
push opcode its length asks, `OP_2DROP`), the record's eight properties and
bounds, the locking key equal to the one an `anyone` `ProtoWallet` derives
for the subject under `[1, 'identity']` key `1`, and the subject's field
signature over the record's bytes. Then the door, for every resolver's
entry (`OverlayCertificate::verified_and_decrypted`): bsv-rs's
`VerifiableCertificate::verify`, then `decrypt_fields` with the `anyone`
wallet, its keyring values canonical base64 of 1 to 2048 bytes, each field
opened under the certificate field protocol with the subject as
counterparty. A failure, or an empty decryption, drops the entry;
`decrypted_fields` is the decryption's. Then the fence: each root the
earlier pass held is asked again (the reference's second `verify`, `:612`),
and a second output at the same outpoint is dropped (`:700`). Before #495 the door matched the
`decrypted_fields` a resolver supplied, which no real overlay entry fills, so
attribute discovery against a real overlay found nothing (#494). No cache is kept (`docs/ARCHITECTURE.md` D30). Witnesses:
`lookup_evidence_tests.rs`, `discovery_evidence_tests.rs`,
`discovery_decrypt_tests.rs`, `lookup_decode_tests.rs`.

The attribute path binds the overlay's answer to the query before the
transform, as `Wallet.ts:1078` does: `filter_certificates_by_attributes`
(`identityUtils.ts:283-293`) keeps a certificate only when its decrypted
fields satisfy the search. Its helpers are `discovery_search.rs`, each
ported at its reference line (`:140-280`): `any` is the overlay's
all-fields search (two UTF-16 units: in-order literals; longer: complete
tokens outside `\p{Dash}`, `\p{Pattern_Syntax}`, `\p{Quotation_Mark}`,
`\p{Terminal_Punctuation}` and `\p{White_Space}` with the ASCII apostrophe
kept, stop words dropped, `-term` and `"phrase"` parts), `userName` matches
exactly after normalization, other fields as in-order literals on one line
under legacy `RegExp` `/gi` case folding (per character `to_uppercase`,
kept when the uppercase is two units or maps non-ASCII to ASCII). The text
search runs `foldIdentityText` on both sides (`:218-223`): NFD
(`unicode-normalization`), every `\p{Mn}` removed (regex `unicode-gencat`),
lowercase. An empty query answers nothing.

### Pending Transaction Cache

The wallet maintains an in-memory cache of unsigned transactions:
- Keyed by `reference` string from `StorageCreateActionResult`
- Automatically cleaned up: expired transactions (>24 hours) removed on access
- Thread-safe via `Arc<RwLock<HashMap<...>>>`

### Thread Safety

`Wallet<S, V>` uses `Arc<S>` and `Arc<V>` for storage/services. `WalletInterface` impl requires `S: WalletStorageProvider + Send + Sync + 'static` and `V: WalletServices + Send + Sync + 'static`.

### ProtoWallet Delegation

Cryptographic methods delegate to `ProtoWallet` with type conversions:
- `proto_get_public_key`, `proto_encrypt`/`proto_decrypt`, `proto_create_hmac`/`proto_verify_hmac`, `proto_create_signature`/`proto_verify_signature`
- Key linkage methods parse hex public keys back to `PublicKey` for result structs

## Helper Functions (wallet.rs)

Internal helper functions in `wallet.rs`:

| Function | Purpose |
|----------|---------|
| `validate_originator(originator: Option<&str>)` | `None` passes; a supplied string follows the pinned reference's hostname-and-port rules |
| `compute_txid(raw_tx: &[u8])` | Computes txid (double SHA256, reversed) from raw transaction |
| `build_unsigned_transaction(result, key_deriver)` | Builds unsigned transaction bytes from storage result; optionally derives P2PKH locking scripts for change outputs via BRC-29 |
| `direct_acquire::validate_acquire_direct_args(args)` | The direct acquire's argument checks (`validationHelpers.ts:1405-1439`) and the normalized certificate, verifier and keyring (#311) |
| `direct_acquire::verify_acquire_direct(proto_wallet, certificate, keyring, args)` | Builds a `MasterCertificate`, verifies its signature and decrypts all fields through the wallet's internal crypto before storage (`Wallet.ts:705-731`); no caller protocol permission is needed; discards the plaintext (#374) |
| `create_keyring_for_verifier(...)` | Creates verifier-specific keyring for selective field disclosure (BRC-52/53) |
| `address_to_p2pkh_script(address: &str)` | Decodes Base58Check BSV address to P2PKH locking script bytes (used by `sweep_to_address`) |

### Certificate Field Encryption (prove_certificate)

The `prove_certificate` method implements BRC-52/53 selective attribute disclosure:

Each field's Base64 text encodes ciphertext under an independent 32-byte
`SymmetricKey`. The master keyring holds the wallet-encrypted field key, under
protocol `[2, "certificate field encryption"]` and key ID `field_name`.
The verifier unwraps that key under `"{serial_number_base64} {field_name}"`,
Base64-decodes the field ciphertext, decrypts it with the recovered key, and
reads the plaintext as UTF-8. The keyring does not carry the field plaintext.

1. **Validate arguments** - After permission, check privilegedReason, the `certificate.fields` names (1478-1480), type, serial, certifier, subject, revocation outpoint, signature, the 100000-item array bound, requested name lengths, verifier, then the 100-name count and uniqueness before the lookup (`ts-stack@edf6e03 validationHelpers.ts:1477-1518`)
2. **Query certificate** - Finds unique certificate matching normalized args via `list_certificates`
3. **Validate keyring** - Ensures storage has master keyring for decryption
4. **Create verifier keyring** - For each field to reveal:
   - Decrypts master symmetric key using certifier as counterparty with key_id = field_name
   - Re-encrypts for verifier using key_id = "{serial_number} {field_name}"
5. **Return keyring** - Field names mapped to base64-encoded encrypted symmetric keys

**Encryption Protocol:**
- Security Level: Counterparty (level 2)
- Master Key ID: `"{field_name}"` (decryption from certifier)
- Verifiable Key ID: `"{serial_number_base64} {field_name}"` (encryption for verifier)

## Signer Helper Functions (signer.rs)

Internal helper functions in `signer.rs` (the varint, sha256d and hash160 are bsv-rs's: `bsv_rs::primitives::{from_var_int, to_var_int, sha256d, hash160}`):

| Function | Purpose |
|----------|---------|
| `compute_sighash(tx_data, input_index, locking_script, satoshis)` | Computes BIP-143 sighash for input |
| `parse_transaction(tx_data)` | Parses transaction into (version, inputs, outputs, locktime) |
| `build_unlocking_script(locking_script, signature, pubkey)` | Builds P2PKH/P2PK unlocking script |
| `insert_unlocking_script(tx_data, input_index, unlocking_script)` | Inserts unlocking script into transaction |

## Certificate Issuance Protocol (certificate_issuance.rs)

The `certificate_issuance` module implements the BRC-104 certificate issuance
protocol for acquiring identity certificates from certifier services. Its
field scheme follows `ts-stack@edf6e03`,
`MasterCertificate.ts:277-293,380-420`: draw a fresh `SymmetricKey` per field,
encrypt the plaintext with it, and wallet-encrypt the key to the certifier.
Both ciphertexts are Base64 text in the request and storage. Intake preserves
that text's bytes for the signed representation (BRC-52 `0052.md:97`, #197);
bsv-rs 0.9.0-beta.2's `set_field` takes the Base64 string as received (#261).
Decode a field's Base64 only when decrypting it.

For #283, `Wallet::acquire_certificate` selects the issuance helper and its
argument guards follow `ts-stack@edf6e03 validationHelpers.ts:1307-1349`:
privilege/reason (5-50 UTF-8 bytes), issuance protocol, direct-only serial,
signature, revocation outpoint, revealer and subject keyring, URL presence,
type, certifier, URL syntax, then fields. Empty direct-only strings pass;
even an empty subject keyring refuses. Rust's typed struct supplies the
record/boolean/string shape guarantees. The later privilege validators at
1342-1348 repeat the earlier check and cannot refuse an otherwise valid call.

The shared `validate_certificate_base64_arg` and `validate_public_key_arg`
retain #261/#271's type-before-certifier order, ECMAScript trimming and
canonical public-key text. Since the beta.6 re-pin `validate_public_key_arg`
and the internalize sender's check read the key through bsv-rs's public
`wallet::validate_public_key` (bsv-rs #579, the reference's `validatePublicKey`,
`validationHelpers.ts:185-191`), the port's copy retired; its words are kept
as the toolbox's `ValidationError`, so no verdict or text moves. The issued
certificate's keys (`certificate_key`) and the overlay record's keys
(`lookup_record.rs`) stay on `internalize_args::compressed_key`, `toBinary`'s
rule (`Certificate.ts:240-254`: untrimmed, either case), which no public
bsv-rs helper names (its `read_to_binary_key`, #585, is crate-private). The new guards share the argument error constructor
and UTF-8 length helper. URL validation requires exact whitespace, an absolute
HTTPS URL, no credentials/query/fragment and a public DNS hostname, in that
order, then removes trailing slashes. Fields permit at most 100000 entries,
reject unsafe record keys before checking names, and require 1-50 UTF-8 bytes
per name. Every argument guard runs before identity parsing, encryption or
HTTP. Both `Wallet.ts:607` (ordinary telemetry-disabled execution) and `:617`
call the same validator via `:770`.

Response guards follow `Wallet.ts:794-817`: certificate presence before nonce
presence, the server nonce helper, the serial HMAC, type, subject, certifier,
truthy revocation outpoint, field count, missing field, field value, signature,
then master-field decryption. Certificate and nonce presence errors end in `!`.
The parser retains the supplied strings; decoding for the SDK occurs only
after metadata checks, and the stored outpoint keeps its supplied text.
The signature step decodes as `Certificate.verify` does (`Certificate.ts:425-441`
over `toBinary`): type and serial through the strict decoder
(`:233,237`; `WalletByteEncoding.ts:39-52`), so a non-canonical type, admitted
by the argument gate, or a non-canonical serial, admitted by the lenient HMAC
decode at `Wallet.ts:808`, answers `Certificate verification failed`. The
outpoint follows `toBinary`'s grammar (`:260-270`): `txid` or `txid.vout`,
64 hex digits, a decimal index without leading zeros up to `0xffffffff`; a
txid-only text is vout 0 over its txid, and `unparsable` or `<txid>.07`
answers `Certificate verification failed`. The subject and certifier are
read as `toBinary` reads them (`:240-254`, `certificate_key`): `02` or `03`
and 64 hex digits, then a compressed point through `PublicKey::from_bytes`,
so a key text in one of the reference's lenient `fromString` forms that
bsv-rs's `PublicKey::from_hex` reads since 0.9.0-beta.5 (bsv-rs #498: the
odd-length string padded, a hybrid key) answers `Certificate verification
failed` as the reference's `verify` answers false; a `05` tag too (bsv-rs
#550). Only `reverify_stored_certificates` can meet such a text: at the door
the subject and certifier equal the request's validated keys ahead of it
(`a_lenient_key_text_does_not_verify_as_the_reference_refuses_it`). The
overlay record's subject and certifier are refused in those forms at the
record's own compressed-key check (`lookup_record.rs`), where the reference
drops the entry at `verify()`
(`a_lenient_subject_text_is_dropped_as_the_reference_drops_it`). So does a signature bsv-rs's
`Certificate::verify` answers `Ok(false)` to (absent, non-DER, wrong signer;
bsv-rs #299, BRC-52's derived key from #251), as `Wallet.ts:816` (#188).
`Wallet::reverify_stored_certificates` re-reads every stored certificate
through the same check once, logs each failure and answers
`StoredCertificateReverification { checked, failed }`; it deletes nothing
(the owner's word on #188). An empty master keyring answers `A MasterCertificate
must have a valid masterKeyring!` before decryption and storage
(`MasterCertificate.ts:522-524`, outside the decrypt try).

The server nonce helper matches `verifyNonce.ts:26-57`, using the requested
certifier, `[2, 'server hmac']`, and the UTF-8-decoded random part as key ID, including TextDecoder's leading-BOM removal.
As at `Wallet.ts:806`, its false return is ignored; an error propagates before
the serial HMAC. This differs from the pinned SDK nonce helper (security
level 1, a base64 key ID), so that helper cannot be substituted.

For #300, the client nonce is `createNonce.ts:21-44` as `Wallet.ts:771` calls
it: 16 random bytes, their HMAC through the wallet's `create_hmac` with
`[2, 'server hmac']`, the certifier as counterparty and the bytes'
TextDecoder text as key ID (`nonce_key_id`, shared with the server nonce
check: `from_utf8_lossy`, one U+FFFD per maximal subpart, then one leading
BOM removed), base64 of the 48 bytes. A certifier receives a 64-character
`clientNonce` without padding (it was 44, padded, with no HMAC), which
`verifyNonce(clientNonce, wallet, subject)` accepts and which joins a server
nonce as one base64 string. Tests: `certificate_issuance_nonce_tests.rs`.
Serial decoding and length errors follow the earlier Base64/HMAC validators:
the serial, then `clientNonce + serverNonce` decoded as one string
(`Wallet.ts:808-809`, so a bad server nonce character is named at its index
in the joined text), then the keyID `serverNonce + clientNonce` past 800
UTF-8 bytes (`The keyID parameter must be no more than 800 length.`,
`validationHelpers.ts:227` via `:1772`), then the hmac length
(`validationHelpers.ts:1774`);
the ordinary verifier throws `HMAC is not valid` on a bad tag. A verifier's
false return uses exactly `Invalid serialNumber` at `:814`.

Response field errors are `Fields mismatch! Objects have different numbers
of keys.`, `Missing field: <field> in certificate.fields`, and
`Invalid field! Expected: <sent>, Received: <received>`. Signature errors use
`Certificate verification failed`; decryption errors use
`Failed to decrypt all master certificate fields.` (`MasterCertificate.ts:560`).
The existing error envelope is retained; the internal identity-key parse is
unchanged.

For #315 (the owner's ruling 19.2 (a)), the issuance request goes over
BRC-104 mutual auth, as `Wallet.ts:773-795` sends it through `AuthFetch`
(design: `docs/design/issuance-mutual-auth.md`). `certificate_issuance_auth.rs`
builds a bsv-rs `Peer` per call, as the reference builds an AuthFetch per
call, over the wallet's `ProtoWallet` (the same root key; `Peer` needs an
owned wallet, so `Wallet<S, V>` gains no bound, and the port's BRC-98/99
check does not run on the handshake signatures, as the reference's Wallet
makes none). At bsv-rs `=0.9.0-beta.2` `Peer::start` is the verifying path
(bsv-rs #353): the initial response's nonce and signature, then the general
answer's session, sender and signature. The transport is the shared guarded
one (`services/providers/http_client.rs`): bsv-rs's `SimplifiedFetchTransport`
in production, its redirect refused on both paths (bsv-rs #320); under test
and `bench-support` the #182 wrapper, which checks every send and hands the
SDK the policy's client through `with_client`. The base URL is the
endpoint's origin and the request path its path (`signCertificate` is not
appended: #332). The request is an `HttpRequest`: 32 random bytes, `POST`,
the path, `content-type: application/json` only, the JSON body; it is sent
with `to_peer` and no pinned identity, as a fresh AuthFetch has none. A
general-message listener takes the answer whose leading 32 bytes are the
request id. Then, in the reference's order: the signed sender must equal the
normalized certifier, else `Invalid certifier! Expected: <certifier>,
Received: <sender hex>` (`Wallet.ts:789-791`); then the body as JSON, with no
status gate (a signed `400` reaches `No certificate received from
certifier!`). A 30 s deadline answers `Timed out waiting for authenticated
response.` (`AuthFetch.ts:298-300`). A bsv-rs transport fault keeps its
class (`Error::SdkError`); any other refusal is `Error::NetworkError` with
`Failed to send request: <bsv-rs text>`, as the storage client's. Not
ported, each its own issue: the 402 payment (#328), the certifier's
certificate request (#333), the private-address refusal (#334).

What a certifier behind the reference middleware is expected to see, per
issuance (read from `index.ts`, not yet run against it): a
`POST /.well-known/auth` `initialRequest` from the wallet's identity key,
then `POST <path>` with the six `x-bsv-auth-*` headers (version, identity
key, nonce, your nonce, signature, request id), its handler called with
`req.auth.identityKey` the wallet's key. Before #315 it saw one request
carrying `x-bsv-auth-version` with no request id, which the middleware
answers `400 ERR_AUTH_MALFORMED` (`index.ts:1099-1102`). A certifier that
read the `x-bsv-identity-key` request header stops receiving it.

What the wallet's caller sees (through `WalletInterface`, each inside
`wallet error: `):

| situation | before #315 | after |
|---|---|---|
| a reference certifier | `Network error: Certifier returned HTTP 400 Bad Request: {...ERR_AUTH_MALFORMED...}` | the certificate, expected: shown against this crate's fixture, not yet run against the reference middleware (the interop lane, the design's section 5, steps 1 to 4) |
| a certifier signing with another key | (no handshake; its header compared) | `Validation error: Invalid certifier! Expected: <certifier>, Received: <its key>` |
| a plain server answering the handshake with malformed JSON | the header compared, `Received: null` or the body's | `Network error: Failed to send request: authentication error: Unexpected token 'o', "not auth" is not valid JSON` for `not auth`, the reference's syntax words (bsv-rs #410) |
| a server answering the handshake `404` | (no handshake) | `Network error: Failed to send request: authentication error: Received HTTP 404 Not Found from <url>/.well-known/auth without valid BSV authentication (response lacked required BSV auth headers) - body preview: <body>` (bsv-rs #410) |
| an unsigned answer to the signed request | n/a | `Network error: Failed to send request: authentication error: Received HTTP <status> from <url> without valid BSV authentication (missing headers: ...)` |
| a bad answer signature | n/a | `Network error: Failed to send request: wallet error: Signature is not valid`; unchanged for a canonical signature from another key (bsv-rs #407) |
| a redirect | refused by the policy client under test, followed by reqwest in production | `Network error: Failed to send request: authentication error: Network error while sending authenticated request to <url>: fetch failed` (bsv-rs #410) |
| malformed requested-certificates header | previously discarded | `Network error: Failed to send request: authentication error: Failed to parse x-bsv-auth-requested-certificates returned by <url>: not auth. Unexpected token 'o', "not auth" is not valid JSON` for `not auth` (bsv-rs #410) |
| a non-2xx signed answer | `Network error: Certifier returned HTTP <status>: <body>` | the body read: its envelope checks name it |
| no answer within 30 s | reqwest's timeout text | `Network error: Timed out waiting for authenticated response.` |

The bsv-rs #410 witnesses pin the complete new messages, including the
existing toolbox envelope, and show that redirects and a failed handshake
send no later request. The malformed-header witness uses the signed
issuance path and refuses before the envelope is read.

At beta.3 the transport also applies bsv-rs #416's bounds before delivery.
Every refusal below has the existing `authentication error: ` prefix, then
the issuance caller's `Network error: Failed to send request: ` envelope.
`tests/brc104_bounds.rs` replays 55 recorded reference cases with mockito.

| Input | Accepted limit or mapping | Refusal payload |
|---|---|---|
| Requested-certificates header | 262,144 UTF-8 bytes | `Failed to parse x-bsv-auth-requested-certificates returned by <url>: [oversized]. header exceeds its byte limit` |
| Signature header | Nonempty even-length hexadecimal, at most 2,048 characters; `MAA=` is refused | `Failed to parse x-bsv-auth-signature returned by <url>: [invalid]. signature must be bounded even-length hexadecimal` |
| Message type | Exact `certificateRequest` remains `CertificateRequest`, formerly `General` | No refusal at the transport layer; issuance certificate-response policy remains #333 |
| Handshake / application body | 1,048,576 / 16,777,216 bytes | `Authenticated response from <url> exceeds <maximum> bytes` |
| Optional request ID | Canonical padded base64 for exactly 32 bytes; absent stays absent | `Authenticated response request ID must be canonical base64` or `Authenticated response request ID must encode exactly 32 bytes` |
| Signed headers | 512 distinct headers; names at most 1,024 bytes, values 32,768 bytes, sum 262,144 UTF-8 bytes | `Authenticated response contains an oversized signed header` or `Authenticated response signed headers exceed their limit` |

`certificate_issuance_bounds_tests.rs` drives the vector's refused cases
through this door (#423): the fixture certifier's server answers them after
its handshake, or on the handshake path, and each refusal is the payload
above, byte for byte, inside `Network error: Failed to send request: authentication error: `
as `WalletError`. Each bound has a case past it: the 2,050-character
signature past the 2,048 cap (`response_bounds.rs:47`, beside `MAA=`'s
shape), and the 33-byte request id beside the 31-byte one, both answering
`must encode exactly 32 bytes` (`:65-76`). At the door the kept type is visible: an answer typed
`certificateRequest` with no requested-certificates header reaches the Peer
as a certificate request and is refused with
`requestedCertificates must be an object.`; the vector's `CertificateRequest`
spelling stays general and meets the Peer's nonce check. The signed-header
count bound is not reachable here: the door's HTTP/1 client fails at hyper's
100-header limit, as `TransportFault` `MAY_HAVE_BEEN_RECEIVED` with
`HTTP request failed: error sending request for url (<url>)`, before 512
signed headers arrive; `tests/brc104_bounds.rs` replays it over HTTP/2.
The cases are shared with the storage client from
`tests/common/brc104_door_cases.rs`.

The requested-certificates preimage remains the keeper's stated divergence:
bsv-rs verifies the received header text; the reference verifies the parsed
object's stringified form. Whitespace and redundant escapes can therefore
give opposite verdicts. This pin does not resolve that difference; bsv-rs
#426 is outside beta.3.

Tests: `certificate_issuance_auth_tests.rs` (the design's witnesses),
`certificate_issuance_auth.rs` (the deadline), the fixture certifier
`certificate_issuance_peer_fixture.rs` (the handshake and the signed answer
as `index.ts:931-968,1005-1031` send them, and a request whose session or
signature fails refused as `index.ts:1380-1395` refuses it: `401`, none of
the six headers, `{"status":"error","code":"ERR_AUTH_FAILED","description":"Authentication failed."}`,
the handler not called; its self-check in
`certificate_issuance_peer_fixture_tests.rs`). The fixture is this crate's
reading of the middleware; the recorded shapes and the live loopback run
against the middleware itself are the interop lane's (the design's
section 5, steps 1 to 4). The #269 header witnesses are
gone with the header they read.

`certificate_issuance_guard_tests.rs` checks the representable argument guards
and their precedence. `certificate_issuance_response_tests.rs` exercises the
response order, texts, nonce calls, raw storage and the verify-time decode through
loopback HTTP. A task-scoped test transport override routes an already
validated public URL to the local fixture; production has no override.

The bsv-rs #261 fixture is pinned byte for byte under
`conformance/upstream/261-certificate-field-value/`. The four tests in
`certificate_issuance_tests.rs` cover intake bytes and reference signature
verification, the stored certificate's `prove_certificate` path, local
issuance through preparation/validation/storage/proof, and fresh keys for
equal and empty plaintexts. They implement the reference's verifier and
decrypt procedures because the pinned SDK's helpers still use the older
signature and field schemes; #199 owns that dependency update.

Hosts must re-issue every certificate produced by the toolbox's own issuance
before #203: those fields and keyrings do not decrypt with the reference, Go
or bsv-rs's corrected scheme. Stored certificates from the reference now
prove and decrypt through the toolbox.
For #222, `prove_certificate` decrypts each requested field under its recovered
master key before wrapping it for the verifier, refusing a mismatched pair with
`bsv_rs::Error::WalletError("Failed to decrypt certificate field!")` as the
reference's plain `Error` does (`ts-stack@edf6e03`,
`MasterCertificate.ts:382-393,612-622`).
For #228 and #229, this recovery restores legacy keys to 32 bytes before wrapping them for the verifier and uses the same `WalletError` for every recovery failure, including non-UTF-8 plaintext (`ts-stack@edf6e03`, `MasterCertificate.ts:573-622`, `VerifiableCertificate.ts:54-60,203`); the plaintext size bound remains with #223.
For #233, the requested name must contain 1 to 50 UTF-8 bytes and differ from
`__proto__`, `constructor`, and `prototype`. The check inside recovery remains
(`ts-stack@edf6e03`, `MasterCertificate.ts:23-29,573-574,621-622`,
`SafeRecord.ts:2-3`). For #236, the gate is here too: before the subset loop,
`create_keyring_for_verifier` checks the requested names, then the keys of
`fields` and the master keyring, then every value in `fields` and the master
keyring, in that order (`MasterCertificate.ts:348-364`). The helper's invalid names
read `Certificate field name must be a safe UTF-8 string of 1–50 bytes`;
duplicate requests read `fieldsToReveal contains duplicate field: <name>`.
Requests and maps are limited to 100 entries (`:43-45,125-142`). Non-canonical
Base64 reads `<label> <name> must use canonical base64 encoding`, and empty
values read `<label> <name> is empty, oversized, or non-canonical`, with
`Certificate field` or `Master certificate keyring field` as the label
(`:63-73,351-364`). The subset check includes the empty-value condition and
uses `Fields to reveal must be a subset of the certificate fields. Missing the "<name>" field.`
(`:370-379`); empty values are refused by the earlier value check.
The gate's plain JavaScript `Error` maps to `WalletError`, as does the
requested-array limit's `TypeError`. Empty requests with empty maps return an empty
keyring. Failures reached inside recovery keep its one sentence; the value
size caps beyond canonical Base64 remain with #223.

For #240, the helper validates the stored serial number after the map values and before the
subset loop (`ts-stack@edf6e03`, `MasterCertificate.ts:365,63-78`). It must be
canonical Base64 encoding exactly 32 bytes. Invalid encoding reads
`Certificate serial number must use canonical base64 encoding`; empty or
oversized values read `Certificate serial number is empty, oversized, or non-canonical`;
shorter nonempty values read `Certificate serial number must encode exactly 32 bytes`.
Each is a `WalletError`, following the gate's mapping of the reference's `Error`.

An empty `fields_to_reveal` is passed unchanged to the helper: it returns an
empty verifier keyring even for a populated certificate, without recovering
any field. Requested names, both maps, and the serial still pass through the
gate. The reference preserves the empty array at
`validationHelpers.ts:1506-1508` and `signer/methods/proveCertificate.ts:31-43`;
`MasterCertificate.ts:348,370-415` runs no field iterations. The serial and
empty-request tests in `wallet.rs::prove_certificate_tests::preloop_gate`
cover both the helper and the storage-backed prove call.

For #249 and #260, `prove_certificate` checks type, serial, certifier,
subject, signature, each requested name's UTF-8 length, then verifier after
permission and before lookup. This is the ported checks' order in
`ts-stack@edf6e03 validationHelpers.ts:1483-1509`, called by
`Wallet.ts:852-858` before `signer/methods/proveCertificate.ts:28`. The
validator runs in both telemetry branches (`Wallet.ts:601-617`); default
telemetry is disabled (`sdk/src/telemetry/Telemetry.ts:601-609`). All Rust
argument checks run without a feature flag. Each key is trimmed with the ECMAScript
WhiteSpace and LineTerminator set and lowercased, as `validateHexString`
does at `validationHelpers.ts:524,533`. This accepts U+FEFF padding but
retains U+0085, which fails the hexadecimal string check. The normalized
value must be 66 hex characters encoding a compressed secp256k1 key; the
refusals are the reference's
`WERR_INVALID_PARAMETER` texts (`validationHelpers.ts:185-191,522-534`),
`The <name> parameter must be <rule>` with `<name>` one of
`certificate.certifier`, `certificate.subject`, or `verifier` and `<rule>` one of `even length, not N.`,
`hexadecimal string.`, `at least 66 length.`, `no more than 66 length.` or
`a valid compressed secp256k1 public key`, as `Error::ValidationError`, so the
WalletInterface reads `Validation error: ...`. An empty request refuses the
same way. The lookup binds the normalized certifier, as
`signer/methods/proveCertificate.ts:16` does. In SQLite, `certifier IN (?)`
on an uncollated TEXT column matches the stored certifier to that value byte
for byte; the remote storage server decides its own collation. The stored
certifier is not parsed, and `create_keyring_for_verifier` receives the keys
parsed from the normalized arguments, as the reference passes the stored
certifier unparsed to `decrypt` (`MasterCertificate.ts:383-392,592-597`). Tests:
`wallet.rs::prove_certificate_tests::preloop_gate::key_gate`.

For #260, type and serial use the reference's structural Base64 check and
32-byte bound (`validationHelpers.ts:439-497,1483-1489`), with rules
`valid base64 string`, `at least 32 bytes`, or `no more than 32 bytes`.
They are trimmed before lookup; the helper still checks the stored serial's
canonical encoding at `MasterCertificate.ts:365`. Subject uses the same
compressed-key check as certifier. Signature uses the hex rules above,
without the key length bounds, then requires canonical DER with in-range
nonzero scalars (`validationHelpers.ts:536-541,1502-1505`,
`Secp256k1Validation.ts:33-50`); high S remains accepted. Its DER refusal is
`The certificate.signature parameter must be a canonical DER-encoded ECDSA signature`.
Subject and signature are normalized before matching stored rows.
The argument name gate (`validationHelpers.ts:348-355,1506-1508`) uses
`The fieldsToReveal <name> parameter must be at least 1 length.` or
`The fieldsToReveal <name> parameter must be no more than 50 length.`.
All these argument refusals use `Validation error: ` at the wallet boundary;
invalid subject or signature previously fell through to the lookup, and
empty or overlong requested names previously reached the helper's safe-name
text. The helper's own name, map and serial gates remain in their existing
order. The permission check still receives the raw verifier, just as
`wallet-toolbox/src/WalletPermissionsManager.ts:5424-5434,1659-1686` does;
that manager normalizes only the originator before checking permission.
Tests: `key_gate::arg_gate` under the same `preloop_gate` module.

For #270, `certificate.revocationOutpoint` is checked after subject and before
signature (`validationHelpers.ts:1498-1501`, its validator at 1083-1116): an
untrimmed 64-hex txid, `.`, and a canonical ASCII-digit vout, else
`The certificate.revocationOutpoint parameter must be a 32-byte txid and canonical numeric output index joined with '.'`.
A vout past 2^53 - 1 reads `The certificate.revocationOutpoint vout parameter must be a safe integer`;
one past 4294967295 reads `... vout parameter must be no more than 4294967295 length.`
(`validateInteger` at 306-324). The txid is lowercased and the lookup
matches that normalized outpoint (`signer/methods/proveCertificate.ts:18`).
Ahead of `certificate.type`, `validatePrivilege(args, false)` runs
(`validationHelpers.ts:1477`, its body at 207-222): a present
`privileged_reason` must be 5 to 50 UTF-8 bytes (212-217, via 247-255 and
348-355), `The privilegedReason parameter must be at least 5 length.` or
`... must be no more than 50 length.`; then `privileged: Some(true)` with no
reason reads
`The privilegedReason parameter must be a value when 'privileged' is true`
(218-220). The check at 1511 repeats 1477 on the same value and never fires.
After every vargs check, more
than 100 requested names read
`The fieldsToReveal parameter must be at most 100 fields` and a repeated name
`The fieldsToReveal parameter must be unique field names` (1513-1518, the
bound at 70). These precede the helper's own entry-limit and duplicate texts,
which its direct tests still assert.

For #280, after signature and ahead of the requested names, more than 100000
requested names read
`The fieldsToReveal parameter must be an array of at most 100000 items`
(`validateArray` at 105-114, its default bound at 67, called at 1506); 100000
pass it and meet the count above. No earlier helper bounds the array.
The reference's other `validateArray` callers (createAction's inputs, outputs,
labels, output tags, knownTxids and noSendChange; listCertificates' certifiers and types;
listOutputs' tags; listActions' labels) validate outside `wallet.rs` in this
port and are not bounded here; createAction's are bounded at both doors by
`create_args` (#416). `sendWith` (137) carries its own bound,
`MAXIMUM_SEND_WITH_TRANSACTIONS` (1000), and is #309's. InternalizeAction's outputs, insertion tags and labels are bounded at both wallet and storage doors by `internalize_args` (#376).

For #291, a direct `acquire_certificate` passes `revocationOutpoint` through
the same outpoint validator before storing, as `validateAcquireDirectCertificateArgs`
does at `validationHelpers.ts:1435` (called by `Wallet.ts:697` through
`validateAuthAndArgs` in both telemetry branches, 601-617): the txid is
lowercased (533) and the stored row and the returned certificate carry that
value (`signer/methods/acquireDirectCertificate.ts:23,47`). A row acquired
with an uppercase txid now proves; before, it answered
`Certificate not found with the provided arguments` under any argument. A
malformed outpoint is refused with the #270 texts under the bare name:
`The revocationOutpoint parameter must be a 32-byte txid and canonical numeric output index joined with '.'`,
`The revocationOutpoint vout parameter must be a safe integer` or
`... must be no more than 4294967295 length.`, and nothing is stored.

For #311, the direct acquire runs the whole of
`validateAcquireDirectCertificateArgs` (`validationHelpers.ts:1401-1450`)
before any write, in `direct_acquire.rs` (`validate_acquire_direct_args`), in
this order: `validatePrivilege(args, false)` (1405, as `prove_certificate`'s);
the presence of `serialNumber`, `signature` and `revocationOutpoint` (absent
or empty), `keyringRevealer` and `keyringForSubject` (absent; an empty
keyring is present), each `The <name> parameter must be valid when
acquisitionProtocol is "direct"` (1409-1430); `type` and `serialNumber` as
base64 of 32 bytes (1432-1433, `valid base64 string`, `at least 32 bytes`,
`no more than 32 bytes`); `certifier` as a compressed key (1434, the key texts
above); `revocationOutpoint` (1435); `fields` (1436, the record and name
texts below); `signature` as hex then canonical DER (1437); then the keyring
(1439, `validateKeyringForSubject` at 1248-1259): `The keyringForSubject
parameter must be an object with at most 100000 properties` or `... an object
with safe string keys`, then per name `The keyringForSubject field name
parameter must be at least 1 length.` or `... no more than 50 length.` and
`The keyringForSubject field value parameter must be valid base64 string`
(structure only, no size bound). The revealer's key (1438) is a parsed
`PublicKey` in the typed arguments and cannot fail; 1440-1446 repeat 1405.
The row stores the normalized values (type and serial trimmed, certifier and
signature lowercased) and `verifier` is the normalized certifier for
`Certifier`, else the revealer's key (`acquireDirectCertificate.ts:19`).
A map's names retain insertion order at the beta.3 pin (bsv-rs #385/#394),
closing the recorded bad-name refusals in `docs/ARCHITECTURE.md` D12.
Integer-like keys retain `IndexMap` order; bsv-rs records that residual
difference from JavaScript's numeric-key order.

For #374, after validation and before storage, the direct arm builds bsv-rs's
`MasterCertificate` from those normalized values, requires `verify()` to
succeed, then calls `decrypt_fields` through `self.proto_wallet`,
with the certificate's certifier, `privileged`, `privilegedReason` and the
originator omitted. The proof uses the subject's identity key and the SDK's
certificate field encryption protocol without a caller protocol prompt or
permission refusal, including with a permissions manager attached.
This is the reference's `Wallet.ts:705-731` proof: every
field must open with this wallet's keys. The plaintext is discarded; the
stored encrypted fields, keyring entries and verifier retain their values.
No storage write lock is held during the proof. The constructor's field and
keyring checks and `verify`'s serialized field bound apply: a field value
over 1 MiB is refused before storage.

The wrapper returns a `ValidationError`, which crosses `WalletInterface` as
`bsv_rs::Error::WalletRefusal { class: InvalidParameter, message }` for the
reference's `WERR_INVALID_PARAMETER('args', ...)`, with the exact message prefix
`Validation error: The args parameter must be valid encrypted and signed certificate and keyring from revealer. `
(`Wallet.ts:716-728`, `sdk/WERR_errors.ts:100-105`). A false signature ends in
`WERR_UNKNOWN: Certificate verification failed`, as the reference's
`WalletError.fromUnknown(new Error(...))` does. Constructor and decrypt
failures likewise use `WERR_UNKNOWN: <message>` and preserve the message
without bsv-rs's `certificate validation failed: ` Display prefix.
An undecryptable keyring ends in
`WERR_UNKNOWN: Failed to decrypt all master certificate fields.`
A 1,048,577-byte `name` field ends in
`WERR_UNKNOWN: Certificate field name must use canonical base64 encoding`.
An empty keyring with a `name` field ends in
`WERR_UNKNOWN: Master keyring must contain a value for every field. Missing or empty key for field: "name".`
Conversion faults use `WERR_UNKNOWN` with their inner message; other SDK
faults use that name and their Display text. The tests build encrypted, signed
certificates with `MasterCertificate::issue_for_subject`, the SDK equivalent
of `issueCertificateForSubject`. Tests: `direct_acquire_tests.rs`.

`prove_certificate` checks the argument's `certificate.fields` after
`validatePrivilege` and ahead of `certificate.type`
(`validationHelpers.ts:1478-1480`, `validateCertificateFields` at 1232-1241):
more than 100000 keys reads
`The fields parameter must be an object with at most 100000 properties` and a
`__proto__`, `constructor` or `prototype` key
`The fields parameter must be an object with safe string keys`
(`validateRecord` at 81-103), then each name must be 1 to 50 UTF-8 bytes,
`The field name parameter must be at least 1 length.` or
`... must be no more than 50 length.` (348-355); names are checked in sorted
order since a map keeps no insertion order. The helper's own map check on
the stored row stays (`MasterCertificate.ts:349-350`, which the reference
runs on `storageCert.fields`, `signer/methods/proveCertificate.ts:35`).

### Protocol Flow

1. **PrepareIssuanceActionData**: Create the client nonce as `createNonce` (16 random bytes and their 32-byte HMAC, 64 base64 characters), encrypt fields under independent symmetric keys, wrap the keys from subject to certifier, build JSON request body
2. **BRC-104 request**: A handshake with the certifier, then the request as a signed general message through bsv-rs's `Peer` (#315)
3. **Signed sender**: Require the answer's verified sender equal to the certifier before the body is read
4. **ParseCertificateResponse**: Require the certificate, then nonce, and retain the supplied certificate text
5. **VerifyCertificateIssuance**: Run the nonce helper, verify the serial HMAC, validate metadata and fields, verify the signature, then decrypt every master field
6. **StoreCertificate**: Save certificate to storage with encrypted fields and master keyring

### Key Types

```rust
// Request sent to certifier
pub struct ProtocolIssuanceRequest {
    pub cert_type: String,           // Certificate type (base64)
    pub client_nonce: String,        // createNonce's 48 bytes (64 base64 chars, no padding)
    pub fields: HashMap<String, String>,      // Encrypted field values
    pub master_keyring: HashMap<String, String>, // Master keyring
}

// Response from certifier
pub struct ProtocolIssuanceResponse {
    pub protocol: String,
    pub certificate: Option<CertificateResponse>,
    pub server_nonce: String,        // Server nonce (base64)
    pub timestamp: String,
    pub version: String,
}
```

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `auth::RESPONSE_DEADLINE` | 30 s | The answer's deadline (`AuthFetch.ts:90`) |
| `CERTIFICATE_ISSUANCE_PROTOCOL` | "certificate issuance" | Protocol for HMAC verification |
| `NONCE_HMAC_SIZE` | 32 | Expected HMAC/serial number size |
| `NONCE_SIZE` | 16 | The client nonce's random part in bytes (`createNonce.ts:27`) |
| `MAX_KEY_ID_BYTES` | 800 | verifyHmac's keyID bound (`validationHelpers.ts:227`) |

### Main Entry Point

```rust
pub async fn acquire_certificate_issuance<W, S>(
    wallet: &W,
    peer_wallet: ProtoWallet,   // the wallet's own, the BRC-104 Peer's signer
    storage: &S,
    auth: &AuthId,
    args: AcquireCertificateArgs,
    identity_key: &str,
    originator: &str,
) -> Result<WalletCertificate>
where
    W: WalletInterface + Send + Sync,
    S: WalletStorageProvider + Send + Sync,
```

Called by `Wallet::acquire_certificate` when `acquisition_protocol` is `Issuance`.

### HMAC Verification

The serial number is verified via HMAC:
- **Data**: `clientNonce + serverNonce` decoded as one base64 string
- **Key ID**: `serverNonce + clientNonce` (concatenated base64 strings)
- **Protocol**: "certificate issuance" at SecurityLevel::Counterparty
- **Counterparty**: The certifier's public key

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Main crate documentation
- [../storage/CLAUDE.md](../storage/CLAUDE.md) - Storage layer details, `WalletStorageProvider` trait
- [../services/CLAUDE.md](../services/CLAUDE.md) - Services layer, `WalletServices` trait
