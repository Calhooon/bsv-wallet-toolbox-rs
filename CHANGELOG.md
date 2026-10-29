# Changelog

## [Unreleased]

### Changed

- #451: The sqlx door's `build_input_beef` no longer repeats the inputBEEF
  envelope check the create validator runs ahead of it on every door path
  (#416). A refactor: nothing a host sees changes, neither a text nor an
  order; the public `storage::sqlx::refuse_invalid_beef_bytes` stays.

### Fixed

- #422: The live `internalizeAction` door judges each input of every
  unproven raw transaction, including parents, under the node's word: the
  transaction-level bits of `ScriptFlags::standard` for the spend's era with
  the `UTXO_AFTER_GENESIS` and `UTXO_AFTER_CHRONICLE` bits of the coin's era.
  A fork-ID-less original-digest signature now refuses with
  `The signature must use SIGHASH_FORKID.`. A valid version-1 `0x61` signature
  spent after Chronicle now passes instead of refusing with
  `The signature format is invalid.`, on a pre-Chronicle coin too; a spend
  before Chronicle refuses it with
  `The signature must not use SIGHASH_CHRONICLE before Chronicle.`. A coin
  proven before Chronicle keeps `OP_2MUL` disabled and `OP_SUBSTR` as
  `OP_NOP4` (`OP_NOP4 is discouraged by verification flags.`).
  Nonminimal numbers now say
  `Invalid script number: script execution error: Non-minimally encoded script number`
  instead of `non-minimally encoded script number`.
  On mainnet every spender takes the era of the block after the tracker tip,
  the block it would be mined in; a mined parent's coin takes its proven BUMP
  height's era for the UTXO bits and an unproven parent's coin that same next
  block's, against bsv-rs's activation constants.
  Pre-Genesis mainnet refuses with `The mainnet coin height is before Genesis.`.
  Testnet and regtest use the spend's era for the coin until bsv-rs #440
  supplies their activation heights. The interim second evaluation preserves
  the stream verifier's structural, proof and non-script refusals; #440's
  per-input hook will replace it.

- #436: `StorageClient` rebuilds the server's refusal class from the JSON-RPC
  error as the reference's client does (`WalletErrorFromJson`). Host class
  change for the CLI replay (CLI #347): a host behind the remote client now
  reads `WalletRefusal` with the class the local door answers (`BadRequest`,
  `InvalidParameter`, `InvalidOperation`, `InsufficientFunds` and the
  reference's other whitelisted `WERR_*` names) and `WalletReviewActions`
  with its results, where it read `WalletError` with
  `Storage error: RPC error <code>: <message>`. The text of each refusal is
  the server's message unchanged. `WERR_INTERNAL`, a name outside the
  whitelist and a malformed error keep the generic RPC text.
- #416: `Wallet::create_action` runs the create argument checks (description,
  inputs, outputs, labels, options, the BRC-177 grammar and capability
  refusal) after the originator check and ahead of its own work, as the
  reference's `validateCreateActionArgs` runs before storage; the sqlx door
  runs the same checks. No text changes; what moves in order, for the CLI's
  replay (CLI #347): through a wallet over `StorageClient`, a bad argument or
  a `p nosend ` label is refused by the wallet and the remote store sees no
  `createAction` (a well formed expiry label answers `Invalid operation:
  Active storage does not support BRC-177 noSend expiry`, class
  `InvalidOperation`); with a permissions manager, an argument refusal now
  precedes the spending and basket permission checks; a bad description, or
  an earlier input's check, beside an input's empty `unlockingScript` or zero
  `unlockingScriptLength` answers the earlier text where it answered
  `The unlockingScript parameter must be hexadecimal string.` or
  `The unlockingScriptLength parameter must be a positive integer.`; a
  duplicate input outpoint answers `inputs[i]: duplicate outpoint` after every
  input's, output's, label's and option's own check, as the reference's
  duplicate loop runs after them, where it answered ahead of a later input's
  or any output's, label's or option's refusal; a malformed `inputBEEF`
  answers `Invalid BEEF at byte {offset}: {kind}` after the options and ahead
  of the duplicate outpoint, as the reference parses the envelope between
  them, where it answered after the duplicate and only for an action with
  inputs to assemble (a create with no inputs, or `returnTXIDOnly`, now meets
  it too); a direct `StorageSqlx` create caller now meets those two refusals
  too.

- #430: `relinquishCertificate` now refuses an unknown or non-unique certificate
  before changing storage. Host TEXT-DIFF for CLI #344: success with
  `{ relinquished: true }` and storage count `0` becomes `WalletRefusal` with
  class `BadRequest` (`WERR_BAD_REQUEST`) and the exact message
  `Result must exist and be unique.`. One matching row is still soft-deleted;
  the storage result remains a count.

- #392: `createAction` and `internalizeAction` store labels after ECMAScript
  trim and lowercase, and `listActions` normalizes its label filters the same
  way. Host stored-data change for CLI #347: a label supplied as `  Label `
  now reads back as `label`, and a filter of `LABEL` matches it. Existing rows
  keep their stored spelling; an unfiltered listing still returns that spelling,
  while a label filter matches only rows equal to its normalized value.
  The stored `underReceivedRule` label also reads back as `underreceivedrule`;
  the action's opt-in check reads the supplied label after the same
  normalization (#437).
  `listActions` with a label filter that fails validateLabel (empty after the trim, or over 300 UTF-8 bytes) is refused with `Validation error: The label parameter must be at least 1 length.` or `... no more than 300 length.` (class `InvalidParameter`) where it answered zero actions: the reference's `validationHelpers.ts:1691`; a TEXT-DIFF for the CLI's replay (CLI #347).
- #437: `createAction` matches the received-coin opt-in label after
  validateLabel's trim and lowercase, exact on `underreceivedrule`, as every
  label is stored (#392). Host behaviour change for CLI #347: a label of any
  case or with surrounding whitespace (`UNDERRECEIVEDRULE`,
  `underReceivedRule `) now opts a create into the received-coin rule where
  only the exact spelling did; a label that normalizes to another string still
  does not. No refusal text moves.

- #389: A `createAction` double-spend refusal now carries `competingBeef`
  alongside `competingTxs` in its review result (CLI #347). The BEEF contains
  the stored competing transaction and its proof or ancestry, read from
  `proven_txs` and served `proven_tx_reqs` rows as the reference's
  `getProvenOrRawTx`, never `transactions.raw_tx`. Create asks no provider, so
  a competitor the stores lack yields a txid-only BEEF where the reference
  asks services before that fallback; a missing ancestor yields the same on
  both sides, since the reference's `getValidBeefForKnownTxid` throws
  `WERR_INVALID_PARAMETER` at once (`StorageProvider.ts:1239`) and the review
  falls back to txid-only. The result's own zero `txid` remains the D14
  filing (ts-stack #1436).

- #386: `createAction` shares the internalize door's `validateLabel` bound:
  1 to 300 UTF-8 bytes after ECMAScript trim and lowercase. Whitespace-only
  labels, including U+FEFF, now refuse before options and BRC-177 parsing;
  labels padded past 300 raw bytes are accepted when the normalized value fits.
  Host TEXT-DIFF for the CLI replay, with class `InvalidParameter` unchanged:
  `Validation error: label cannot be empty` becomes
  `Validation error: The label parameter must be at least 1 length.`;
  `Validation error: label exceeds maximum length of 300 characters` becomes
  `Validation error: The label parameter must be no more than 300 length.`.
- #435: `StorageSqlx::migrate` keeps an existing settings row's `chain`
  instead of rewriting it to `mainnet`, as the reference writes `chain` once,
  in the migration that inserts the row. A new row is still `mainnet`: no
  storage constructor takes a `Chain` yet, so a testnet or regtest host does
  not yet see its configured chain in settings (the shape is the owner's word).

### Changed

### CI

- #417: Check the remote-only feature set in the Clippy job; compile RNG state and methods only for their SQL storage and test consumers.
- #429: A host running the crate's tests with `remote` alone now builds and
  passes them: `cargo test --lib --tests --no-default-features --features remote`
  went from a build failure on the lib-test helper `assert_argument_refusal`
  and on `tests/rule_28_cadence_tests.rs` (both reaching items the SQL
  features export) to 974 tests passing. The helper, that assertion,
  `tests/sync_manager_tests.rs` and the three `StorageSqlx` vector modules
  carry the feature gate their consumers have; the default and `full` builds
  run the same tests as before. CI's Test Suite job runs that remote-only
  build and test step last, so a gate a remote-only host needs cannot go
  missing again.

## [0.14.0-beta.3] - 2026-10-28

### Changed

- #419: Pin both workspaces to `bsv-rs =0.9.0-beta.3`, retaining
  `full` and `http`. Host migration notes for the CLI's beta.3 pin (CLI #338):
  - Certificate fields, keyrings and attributes: `HashMap<String, String>` becomes `IndexMap<String, String>` (bsv-rs #385/#394); literals use `IndexMap::from` or `collect()`. The toolbox's `OverlayCertificate` maps and `OverlayLookupResolver::lookup_by_attributes` follow that type.
  - Attribute lookup JSON: sorted keys such as `{"alpha":"a","zeta":"z"}` become supplied insertion order such as `{"zeta":"z","alpha":"a"}`; the request retains the typed map through HTTP serialization (bsv-rs #394).
  - Direct certificate refusal order: sorted byte order becomes supplied insertion order; D12 closes for the recorded bad-name cases. Integer-like-key ordering remains the upstream residual.
  - `WalletClient` methods: `&mut self` becomes `&self`, and the client implements `WalletInterface` under `wallet`; method-path uses can become closures such as `.map(|client| client.get_version())` (bsv-rs #404). No existing toolbox call site used the client.
  - Malformed signature: `invalid signature: DER too short`, `invalid signature: Expected sequence tag 0x30`, and other DER parser words become `The signature parameter must be a canonical DER-encoded ECDSA signature` (bsv-rs #415).
  - Signature over 72 bytes: parser error or crypto miss becomes `The signature parameter must be at most 72 bytes`.
  - Missing signing data/hash, or both supplied: `wallet error: data or hashToDirectlySign must be provided`, or acceptance of both, becomes `The data, hashToDirectlySign parameter must be exactly one value`.
  - Missing verification data/hash, or both supplied: `wallet error: data or hashToDirectlyVerify must be provided`, or acceptance of both, becomes `The data, hashToDirectlyVerify parameter must be exactly one value`.
  - Short protocol name: `protocol validation error: protocol name must be at least 5 characters` becomes `The protocolID[1] parameter must be at least 5 length.`.
  - Long protocol name: `protocol validation error: protocol name must be 400 characters or less` becomes `The protocolID[1] parameter must be no more than 400 length.`.
  - Empty key ID: `protocol validation error: key ID must be at least 1 character` (or `wallet error: keyID is required when identityKey is false` for getPublicKey) becomes `The keyID parameter must be at least 1 length.`.
  - Long key ID: `protocol validation error: key ID must be 800 characters or less` becomes `The keyID parameter must be no more than 800 length.`.
  - Missing privileged reason with privilege enabled: ignored becomes `The privilegedReason parameter must be a value when 'privileged' is true`.
  - Short privileged reason, even without privilege enabled: ignored becomes `The privilegedReason parameter must be at least 5 length.`.
  - Long privileged reason, even without privilege enabled: ignored becomes `The privilegedReason parameter must be no more than 50 length.`.
  - Both linkage calls now retain privilege arguments through ProtoWallet's trait adapter: ignored missing/short/long reasons become the three preceding refusals, with class `InvalidParameter` (bsv-rs #420).
  - Signature/HMAC data above 268435456 bytes: processed becomes `The data parameter must be at most 268435456 bytes` (bsv-rs #415).
  - Plaintext above 268435456 bytes: encrypted becomes `The plaintext parameter must be at most 268435456 bytes` (bsv-rs #420).
  - Ciphertext above 268435456 bytes: `decryption failed` becomes `The ciphertext parameter must be at most 268435456 bytes` (bsv-rs #420).
  - Missing public-key protocol/key: `wallet error: protocolID is required when identityKey is false` / `wallet error: keyID is required when identityKey is false` becomes `The protocolID parameter must be a [securityLevel, protocolName] tuple` / `The keyID parameter must be a string`; supplied arguments on the identity branch are now validated (bsv-rs #420).
  - Uncompressed encoded counterparty: accepted becomes `The counterparty parameter must be no more than 66 length.` (bsv-rs #420).
  - Odd-length encoded counterparty `a`: `invalid hex string: Odd number of digits` becomes `The counterparty parameter must be even length, not 1.` (bsv-rs #420).
  - Nonhex encoded counterparty: `invalid hex string: Invalid character 'z' at position 0` becomes `The counterparty parameter must be hexadecimal string.` (bsv-rs #420).
  - Short encoded counterparty: `invalid public key: Invalid public key encoding: elliptic curve point encoding error` becomes `The counterparty parameter must be at least 66 length.` (bsv-rs #420).
  - Compressed encoded counterparty outside the curve: `invalid public key: Point not on curve` becomes `The counterparty parameter must be a valid compressed secp256k1 public key` (bsv-rs #420).
  - The preceding ProtoWallet argument refusals now carry `WalletRefusalClass::InvalidParameter` and display their message alone; genuine cryptographic misses retain their existing words.
  - Handshake HTTP 404: `Auth endpoint returned 404 Not Found: <body>` becomes `Received HTTP 404 Not Found from <url>/.well-known/auth without valid BSV authentication (response lacked required BSV auth headers) - body preview: <body>` (bsv-rs #410).
  - Malformed handshake body `not auth`: `Failed to parse auth response: expected ident at line 1 column 2 - body: not auth` becomes `Unexpected token 'o', "not auth" is not valid JSON`.
  - Redirect: `Network error while sending authenticated request to <url>: redirect refused: 307 Temporary Redirect` becomes `Network error while sending authenticated request to <url>: fetch failed`.
  - Malformed requested-certificates header `not auth`: silently discarded becomes `Failed to parse x-bsv-auth-requested-certificates returned by <url>: not auth. Unexpected token 'o', "not auth" is not valid JSON`.
  - Requested-certificates header above 262,144 UTF-8 bytes: delivered becomes `Failed to parse x-bsv-auth-requested-certificates returned by <url>: [oversized]. header exceeds its byte limit` (bsv-rs #416).
  - Signature header, including `MAA=`: permissive decoding becomes bounded even-length hex, at most 2,048 characters; refusal is `Failed to parse x-bsv-auth-signature returned by <url>: [invalid]. signature must be bounded even-length hexadecimal`.
  - Response type: `certificateRequest` delivered as `General` becomes `CertificateRequest`; issuance certificate-response policy remains #333.
  - Handshake/application body: unbounded buffering becomes 1,048,576/16,777,216 bytes; overflow is `Authenticated response from <url> exceeds <maximum> bytes`. The two `with_max_*_response_bytes` methods are additive.
  - Response request ID: permissive decode or outgoing-ID substitution becomes optional canonical base64 of exactly 32 bytes; refusals are `Authenticated response request ID must be canonical base64` and `Authenticated response request ID must encode exactly 32 bytes`; absent stays absent.
  - Signed response headers: unbounded becomes 512 names, 1,024 bytes per name, 32,768 per value, and 262,144 total UTF-8 bytes; refusals are `Authenticated response contains an oversized signed header` and `Authenticated response signed headers exceed their limit`.
  - Transport messages retain `authentication error: ` and the toolbox's `Network error: Failed to send request: ` envelope. Requested-certificates whitespace/escape preimages remain the keeper's stated divergence: raw received text here, stringified object in the reference; bsv-rs #426 is outside this pin.
  - Default script opcodes `OP_SUBSTR`, `OP_LEFT`, `OP_RIGHT`, `OP_LSHIFTNUM` and `OP_RSHIFTNUM`: treated as NOPs before Chronicle becomes execution at every transaction version (bsv-rs #397).
  - Default script opcodes `OP_VER`, `OP_VERIF`, `OP_VERNOTIF`, `OP_2MUL` and `OP_2DIV` at version 1: reserved/disabled refusals become execution with the reference's semantics (bsv-rs #397).
  - Default non-minimal number: `Invalid script number: script execution error: Non-minimally encoded script number` becomes `non-minimally encoded script number`.
  - Version-1 Chronicle signature: accepted becomes `Script evaluation error: The signature format is invalid.`; version 2 remains accepted (bsv-rs #397).
  - Default version-1 undefined hash type `0x44`: `Script evaluation error: The signature's hash type (0x44) has no defined base type.` becomes `Script evaluation error: The signature format is invalid.` (bsv-rs #417).
  - Default version-1 high-S signature: `Script evaluation error: The signature must have a low S value.` becomes `Script evaluation error: The signature format is invalid.` (bsv-rs #417).
  - Default no-fork-ID signature over the original digest: `The signature must use SIGHASH_FORKID.` becomes acceptance; the wrong digest still fails (bsv-rs #429). The live internalize verifier uses this default through `verify_stream_two_pass`, so this change reaches it. Selecting an explicit script word requires a separate ruling; no toolbox signer changes its `0x41` hash type. The mode at the door is #422 (the stack's rule: the node's word per input, one lane after the beta.3 tag; D15).
  - Default substring operands: full sign-magnitude reading becomes signed interpretation of the leading eight bytes for longer operands, clamped to the int32 range; `OP_LEFT` on `aabbcc` with length bytes `020000000000000001` changes from `OP_LEFT length (-1) must be in range [0, 3]` to the bytes `aabb` (bsv-rs #382).
  - Default negative `OP_NUM2BIN` size: `It's not currently possible to push data larger than 1073741824 bytes or negative size.` becomes `It's not currently possible to push data larger than Infinity bytes or negative size.` (bsv-rs #382).
  - Default `OP_NUM2BIN` size above INT32_MAX: `It's not currently possible to push data larger than 1073741824 bytes or negative size.` becomes `OP_NUM2BIN size exceeds the node int32 push limit.` (bsv-rs #382).
  - Default numeric left shift above INT_MAX: acceptance for zero, a resource decline, or `Maximum BigInt size exceeded` becomes `OP_LSHIFTNUM shift count exceeds INT_MAX.` (bsv-rs #383).
  - Default numeric right shift above INT_MAX: acceptance becomes `OP_RSHIFTNUM shift count exceeds INT_MAX.` (bsv-rs #383).
  - Numbers above 64 bytes in remaining interpreter error texts: decimal expansion becomes `a N-byte number`; the arithmetic working set stays outside the stack-memory budget (bsv-rs #348/#349).
  - Specific linkage: inherent `self` / `anyone` aliases become the wallet's identity / scalar-1 compressed keys; the toolbox now reads the returned key, correcting its `Anyone` result (bsv-rs #427). Both wire linkage calls are available through the transceiver (#406); existing storage JSON-RPC fixtures contain neither call.
  - Direct `HttpWalletJson` users gain the reference's success-byte normalization, including numeric-key objects and byte-valued `1.0` / `-0`; malformed success JSON uses serde's parse text instead of reqwest's decoding text (bsv-rs #365). Toolbox storage uses its own JSON-RPC reader, so none of its recorded replies change for this item.
  - Internalize output protocol `foo`: with description `x`, `Validation error: The description parameter must be at least 5 length.` becomes `Validation error: The protocol parameter must be 'basket insertion' or 'wallet payment'`; with a valid description, `Validation error: Unknown protocol: foo` becomes the same protocol refusal (bsv-rs #384). With an empty label and the same protocol, `Validation error: The label parameter must be at least 1 length.` becomes the same protocol refusal.
  - Streaming `set_memory_limit` is additive (bsv-rs #373); the toolbox still passes no memory cap. The pin introduces no new budget policy. Diagnostic allocation fixes (#348/#349) and test-only changes (#401/#407) require no additional toolbox shape changes.
  - bsv-rs #408 (HTTP JSON result-value checks), #426 (requested-certificates preimage) and #379 (HMAC issuance inversion) are not in beta.3.

- #376: `Wallet::internalize_action` validates arguments before the BRC-29
  derivation check with both local and remote storage; the sqlx door shares
  the validator. D13 is closed. Host TEXT-DIFF for the CLI replay: a valid
  remittance with a bad derivation and a `p nosend ` label changes from
  `Derivation mismatch for output <i>: derived <hash> != script <hash>` to
  `Validation error: The labels parameter must be BRC-177 noSend expiry labels only on outgoing createAction requests`.
  A labels-bound breach beside a bad derivation now reads
  `Validation error: The labels parameter must be an array of at most 100000 items`.
  Empty derivation values gain the existing validator's `Validation error: `
  prefix; a malformed or empty sender now reads
  `Validation error: The senderIdentityKey parameter must be <mustBe>` where
  a malformed sender read `Invalid sender_identity_key: <detail>` and an empty
  sender on a P2PKH output paying another key read the derivation mismatch
  text above.
  Refusals remain `WalletRefusal` with class `InvalidParameter`.
  The CLI captain must re-record these replies when taking this change.
- #412: an explicit dependents `--line` (including `cli_ref`) must name an
  existing branch; a missing branch fails without cloning and names the branch
  and repository URL in its failure row. Implicit CI lines
  retain the noted default-branch fallback. The workflow grants check accepts
  callee jobs with no permission override as inheriting the caller's grant.

- #410: the ladder replay checks that mock server locks are taken off runtime
  workers with a task-local witness; elapsed timings are diagnostics, so runner
  load alone cannot fail the lock regression check.

- #383: createAction's description refusal text now matches the reference.
  Through `WalletInterface`, `WalletRefusalClass::InvalidParameter` retains
  its class but its message changes from
  `Validation error: description length must be between 5 and 2000, got N`
  to `Validation error: The description parameter must be at least 5 length.`
  below the bound, or
  `Validation error: The description parameter must be no more than 2000 length.`
  above it. The bound remains 5 to 2000 untrimmed UTF-8 bytes. The two shared
  description vectors carry the reference text. CLI replay is a TEXT-DIFF
  until re-recorded at the updated toolbox pin.

- #404, #185: toolbox request refusals converted at the `WalletInterface`
  boundary now carry `bsv_rs::Error::WalletRefusal { class, message }`.
  Hosts that matched `WalletError(text)` must match `WalletRefusalClass`;
  invalid BEEF and spend refusals share `InvalidParameter`. The toolbox
  message is unchanged; the SDK's Display prints it without the former
  generic `wallet error: ` wrapper. Busy, review and SDK errors retain their
  existing typed forms, and other faults stay faults. The mapping and nearest
  reference classes are documented in `src/wallet/CLAUDE.md`. CLI #330 must
  take this migration with the next published toolbox beta; no version or pin
  is changed here. Direct SDK guards at signAction (empty reference, missing
  or expired pending action), internalizeAction (derivation values, output
  index, sender and derivation mismatch) and proveCertificate (absent or
  ambiguous match) now carry their reference class too, with the same message
  bytes and without the generic Display wrapper. Missing or expired sign
  sessions are `NotImplemented`; the other listed guards are `InvalidParameter`.
  The abort door's non-outgoing and unabortable-status guards are
  `InvalidParameter`; process-action status guards remain `InvalidOperation`.
  At the storage trait, those four abort guards now return
  `Error::SdkError(WalletRefusal { class: InvalidParameter, .. })` where they
  returned `Error::InvalidTransactionStatus(text)`, with the text unchanged.
  The port-only stored-acceptance guards, including retirement, retain
  `Error::InvalidTransactionStatus(text)`.
  An SDK `WalletRefusal` inside the toolbox's `SdkError` now displays its
  message directly, preserving the abort storage texts; other SDK errors
  keep their `SDK error: ` prefix.
  The remaining direct SDK errors are local faults or the reference's plain
  helper errors, documented with their reasons under #185.

- #374: Direct `Wallet::acquire_certificate` now refuses a certificate whose
  signature does not verify, a keyring this wallet cannot open, and a field
  value over bsv-rs's 1 MiB bound, before any storage write. It previously
  stored these after argument validation. The refusal is
  `bsv_rs::Error::WalletRefusal { class: InvalidParameter, message }`, with
  `message` equal to `Validation error: The args parameter must be valid encrypted and signed certificate and keyring from revealer. WERR_UNKNOWN: <message>`.
  A false signature ends in `WERR_UNKNOWN: Certificate verification failed`;
  an undecryptable keyring in `WERR_UNKNOWN: Failed to decrypt all master certificate fields.`
  A 1,048,577-byte `name` field ends in
  `WERR_UNKNOWN: Certificate field name must use canonical base64 encoding`.
  Constructor and decryption suffixes use the reference's `WERR_UNKNOWN`
  name and inner message without bsv-rs's Display prefix. The proof uses the
  wallet's internal crypto with no caller originator, so an attached
  permissions manager raises no protocol prompt or permission refusal.
  Stored encrypted fields and keyring entries retain their input values.
  Hosts, including the CLI, must
  supply signed certificates and usable keyrings on direct acquisition.

- #185: the authenticated storage client's response-wait timeout now carries
  `Error::SdkError(bsv_rs::Error::TransportFault)` with
  `MayHaveBeenReceived`, no status and the existing `Request timed out` detail.
  Storage-level hosts now receive that SDK wrapper where they received
  `Error::NetworkError("Request timed out")`.
  Through the SDK its Display moves from
  `wallet error: Network error: Request timed out` to
  `transport fault MAY_HAVE_BEEN_RECEIVED: Request timed out`.
  The internalize door and BEEF proof helpers also retain a tracker's
  `TransportFault` class, status and detail, including a rate-limit wait.
  Their SDK Display loses `wallet error: Service error: ChainTracker
  verification failed: ` and keeps the tracker's own Display. Other tracker
  errors and missing mining context retain their existing service-fault text.
  CLI #330 and other hosts read the typed fault; a response-wait timeout
  promises nothing about whether the request acted.

- CI: `release.yml`'s call of the reusable `ci.yml` grants `pull-requests: read`
  beside `contents: read`, the permission the semver job reads on a push run
  since #398; the tag v0.14.0-beta.2's release run failed at startup without it
  (a called workflow's permissions must fit the caller's), publishing nothing.

### CI

- #403: The required `locks` job checks that `release.yml` grants every permission requested by the reusable `ci.yml`, with fixture coverage for the permission levels and fallbacks.
- #405: The Dependents dispatch accepts an optional `cli_ref` branch and passes it as `--line`; an empty input keeps the branch named like this line.

## [0.14.0-beta.2] - 2026-10-28

### Changed
- #315, the owner's ruling 19.2 (a): certificate issuance speaks BRC-104
  mutual auth to the certifier through bsv-rs's `Peer`, as the reference's
  `AuthFetch` does (`Wallet.ts:773-795`). The wire moves: a
  `POST /.well-known/auth` handshake from the wallet's identity key, then the
  issuance request as a signed general message carrying the six
  `x-bsv-auth-*` headers; the answer's signature and session are verified
  before its body is read, and its signed sender is the key compared with
  the certifier. The client no longer writes `x-bsv-auth-version` or
  `x-bsv-identity-key` itself, and no longer reads an
  `x-bsv-auth-identity-key` header a server wrote. The texts that change,
  each through `WalletInterface` inside `wallet error: `: a certifier
  signing with another key is `Validation error: Invalid certifier!
  Expected: <certifier>, Received: <its key>`; a server that does not
  answer the handshake, or answers unsigned, is `Network error: Failed to
  send request: <bsv-rs's refusal>` (`Auth endpoint returned 404 ...`,
  `Failed to parse auth response: ...`, `... without valid BSV
  authentication (missing headers: ...)`, `Signature is not valid`,
  `redirect refused: <status>`; all but the missing-headers text are
  bsv-rs's own words, not the reference's: bsv-rs #407 and #410); a signed non-2xx answer is read as a body,
  so `Certifier returned HTTP <status>: <body>` is gone; no answer within
  30 s is `Network error: Timed out waiting for authenticated response.`; a
  bsv-rs transport fault keeps its class (`Error::SdkError`). For the CLI's
  captain and certifier operators: a certifier behind the reference
  middleware is expected to answer (it refused the old request as
  `400 ERR_AUTH_MALFORMED`); this crate shows it against its own fixture
  of the middleware, and the run against the middleware itself is the
  interop lane's (the design's section 5, steps 1 to 4). A certifier that read the
  `x-bsv-identity-key` request header stops receiving it. No public shape
  moves: the issuance module is private, `Wallet<S, V>` gains no bound,
  `Error` no variant.

- #199, the pin: `bsv-rs` `=0.9.0-beta.2` (was `=0.9.0-beta.1`), the
  owner's standing word of 2026-10-27, in `Cargo.toml` and in the fuzz
  workspace's `fuzz/Cargo.toml` (was a caret `0.8.0`; #243). A dependent
  that builds against this crate re-pins `bsv-rs` to the same version.
  The host note for beta.2, one line each (the CLI's captain reads this
  list):
  - Every `WalletInterface` method takes `originator: Option<&str>` (bsv-rs
    #329), and so do `Wallet::process_action`, `Wallet::create_action_offering`
    and the public `storage::client::auth` helpers (`create_nonce`,
    `sign_request`, `verify_response`, `create_auth_headers`,
    `verify_response_auth`). `None` is absence and passes; `Some("")` is
    refused with `The originator parameter must be at least 1 length.`; a
    supplied string follows the reference's hostname-and-port rule (#284,
    PR #290's texts), where it had to be non-empty and at most 253 bytes.
  - The create door's double spend (a caller-named input whose spender has a
    txid) answers the new `Error::ReviewActions`, bsv-rs's
    `Error::WalletReviewActions` through `WalletInterface`, with the
    reference's words `Undelayed createAction or signAction results require
    review.` and one `doubleSpend` result naming the competing txid; it was
    `Error::InvalidOperation` with a `doubleSpend: <txid>` suffix (#264).
  - Certificate issuance refuses a certificate bsv-rs's `verify` answers
    false to (absent, non-DER or wrong-signer signature, BRC-52's derived
    key) with `Certificate verification failed`; an unsigned or non-DER one
    passed before (#188, bsv-rs #251, #299).
  - New `Wallet::reverify_stored_certificates`, answering
    `StoredCertificateReverification { checked, failed }`: one read of every
    stored certificate through the same check, logged, nothing deleted; a
    host runs it once after taking the pin (#188, the owner's word).
  - `discover_by_identity_key` and `discover_by_attributes` answer no
    certificates: BRC-100 requires each entry's `certifierInfo` and this
    wallet holds no trust settings (bsv-rs #105).
    `OverlayCertificate::to_identity_certificate` takes the
    `IdentityCertifier`.
  - `StorageClient::set_verify_responses` is gone: bsv-rs's `Peer` verifies
    every answer (#335, bsv-rs #353); an unsigned general answer is refused
    in the reference's words (bsv-rs #354) and a redirect status before its
    body (bsv-rs #320); the verified sender becomes the cached server
    identity key.
  - The storage wire sends the reference's `StorageServer` form for
    createAction's `options.knownTxids` and `options.sendWith` (hex),
    `options.returnTXIDOnly`, internalizeAction's `tx` (a byte array) and
    relinquishCertificate's `type` (bsv-rs #105).
  - bsv-rs's JSON moved under the wallet's types: `SignableTransaction.reference`
    is base64 (edfc535), `InternalizeActionArgs.tx` a byte array,
    `IdentityCertificate`'s keyring and decrypted fields required maps,
    `Certificate.fields` base64 strings (bsv-rs #105, #261).
  - A TSC proof or BUMP with a duplicate at an even offset is read as the
    reference reads it, where it was refused (bsv-rs #167).
  - A spend-verifier verdict this crate does not read holds as
    `Error::ServiceError`, never passes (bsv-rs #240).
  - The internalize door's BEEF stream verification (`verify_stream_two_pass`,
    `beef_verification.rs`) runs scripts with no memory cap: bsv-rs #331
    removed beta.1's 32,000,000-byte cap and asks a door that verifies
    untrusted transactions to pass its own figure, which the stream readers
    take through bsv-rs #373 at beta.3 (#395); until then an untrusted BEEF's
    scripts run uncapped as the reference's default does.
  - A BEEF whose children precede their parents is accepted at the internalize
    door where beta.1 refused it (bsv-rs #240); the two reader changes have no
    toolbox witness yet (#396).

- CI: a push to `main` or `beta` runs exactly the required set, the same as a
  pull request (the owner's word of 2026-10-27, through the stack keeper:
  "sick of seeing all these CI failures"). The door bench, coverage, the
  dependents check and the full OS and toolchain matrix left `ci.yml` for their
  own dispatched workflows (`bench.yml`, `coverage.yml`, `dependents.yml`,
  `matrix.yml`), run once before a tag and when a reading asks, each with its
  own README badge; `nightly-mutants.yml` is `mutants.yml` (`Mutants`). A
  cancelled push run (a newer landing's push supersedes it) is never a red.
  The required contexts and release.yml's tag run are unchanged.
- CI: the semver job compares a pull request against its base branch's tip
  (the PR's own breaks alone) and a push against the line's last tag; its
  api-break switch reads, on a push run, whether any
  pull request merged into the branch since the baseline tag carried the
  `api-break` label (a line collects its owner-worded breaks until its next
  tag, and every push run compares the whole tree against that tag); it
  read the pull request's own label alone, so beta's push run at the pin
  a3cc155 went red on breaks the landed PR #391 carried the label for.
- CI: the Test Suite job runs `tools/arch-doc-tests.sh` after the unit and
  integration tests, the `--lib` rows with the unit-test step's features and
  the `--test` rows with the integration step's (`ARCH_DOC_LIB_FEATURES`,
  `ARCH_DOC_TEST_FEATURES`), so cargo lists from the builds already made; a
  test named in an invariants table of `docs/ARCHITECTURE.md` that does not
  exist, or a D7 citation that goes missing, fails the required set. The
  script ran locally alone before.
- #317, public defaults: Arcade is the default broadcaster mode, the
  owner's word of 2026-10-27 ("arc is just deprecated, arcade is the
  standard"). The public lines that move:
  - `ServicesOptions::default()`, `mainnet()` and `testnet()`: `arcade_v2`
    is `true` (was `false`), `arc_gorillapool_url` is `None` (was
    GorillaPool's classic ARC URL on mainnet). `arc_url` (was the TAAL
    classic ARC URL of each chain) is the crate's public
    `ARCADE_V2_MAINNET` in `default()` and `mainnet()` (the keeper's
    reading on PR #342: the reference defaults its broadcaster to a hosted
    endpoint, ts-stack `DefaultBroadcaster.ts`) and empty in `testnet()`,
    since the crate has no Arcade testnet constant. Every field keeps its
    name and type.
  - `Services::with_options` refuses options in Arcade mode whose `arc_url`
    is empty, with `Invalid argument: ServicesOptions names no Arcade
    endpoint: Arcade is the default mode (#317); set one with
    ServicesOptions::with_arcade(url, config)`; the reference has no
    counterpart. `Services::new(Chain::Main)` and `Services::mainnet()`
    build Arcade at `ARCADE_V2_MAINNET`; `Services::new(Chain::Test)` and
    `Services::testnet()` refuse with that text, as does an `arc_url` a
    caller emptied.
  - New `ServicesOptions::with_classic_arc(url, config)`, deprecated since
    0.14.0: sets `arc_url` and `arc_config` and `arcade_v2 = false`, the
    classic ladder (TAAL ARC, GorillaPool ARC when `with_gorillapool` set
    it, Bitails, WhatsOnChain). It serves the CLI's hermetic stub until it
    speaks Arcade.
  - `ServicesOptions::with_arc(url, config)` is deprecated since 0.14.0 and
    is now the same as `with_classic_arc`: it also sets `arcade_v2 = false`
    (it left the mode alone before).

  What a dependent does: nothing on mainnet for the standard Arcade
  endpoint (`Services::mainnet()` and bare default options build it);
  another endpoint, or any on testnet, is
  `ServicesOptions::mainnet().with_arcade(url, config)` (or `testnet()`)
  before `Services::with_options`; a classic ARC stub moves onto `with_classic_arc`
  under `#[allow(deprecated)]`. A struct literal over `..mainnet()` that
  means classic ARC sets `arcade_v2: false` itself. For the CLI's captain
  (its beta at the time of writing): `services_env::services_options` calls
  `with_arcade` in Arcade mode, unchanged; its classic branch calls
  `with_arc(settings.arc_url or opts.arc_url)`, which now warns as
  deprecated and, with `ARC_URL` unset, gets where it had the TAAL URL
  `ARCADE_V2_MAINNET` on mainnet (a classic ARC provider at the Arcade
  endpoint) and an empty `arc_url` on testnet, so the CLI names its
  classic URL itself or moves to Arcade; `test_support::mock_services` and the tests building
  `Services::with_options(Chain::Main, ServicesOptions::mainnet())` now
  build Arcade mode at `ARCADE_V2_MAINNET` (the stub speaks classic ARC);
  `config/knobs.rs` shows `ARCADE_V2_MAINNET` as the `ARC_URL` default in
  classic mode on mainnet, empty on testnet;
  `services_env`'s GorillaPool default reads `None` on mainnet. CLI #300
  pins this version.
- #317: in Arcade mode (`ServicesOptions::arcade_v2`, set by `with_arcade`)
  no classic ARC provider is built, whatever `arc_gorillapool_url` says. The
  post ladder is `ArcadeV2`, then `Bitails`, then `WhatsOnChain`; TAAL's and
  GorillaPool's classic ARC are no longer failovers, and the
  broadcaster-status read (`get_broadcaster_statuses`) asks Arcade alone.
  The owner's word of 2026-10-27: "we use arcade now". Classic mode
  (`arcade_v2` false) is unchanged: TAAL from `arc_url`, GorillaPool when
  configured, then Bitails and WhatsOnChain; the entry above makes Arcade
  the default and classic mode the deprecated `with_classic_arc`.
- #317, public field: `Services::arc_taal` is `Option<StdArc<Arc>>` (was
  `StdArc<Arc>`), `None` in Arcade mode. A dependent that read
  `services.arc_taal` matches on the option (`if let Some(arc) =
  &services.arc_taal`) and treats `None` as Arcade mode, where
  `services.arcade` holds the broadcaster.
### Added

- #353 (bsv-wallet-cli #313): the offering call takes a must-spend list
  beside the offer, output ids the action spends ahead of the pick. The
  public lines that move: `Wallet::create_action_offering(args,
  originator, offer, must_spend: &[i64])` and
  `StorageSqlx::create_action_offering(auth, args, offer, must_spend:
  &[i64])` each gain the last parameter (a caller passes `&[]` for the
  former behaviour), and `storage::CreateActionDoor` gains the field
  `must_spend: &'a [i64]` (a struct literal adds `must_spend: &[]`). Each
  named coin is allocated as a picked coin (storage-provided change, signed
  with the wallet's derivation) and counts toward the target; the pick funds
  the remainder and the fee from the rest of the offer by the unchanged
  formula, and change returns as always. A named coin outside the offer,
  named twice, not this user's, not wallet-managed change, spent or
  reserved, a received coin whose transaction is not `completed` when the
  action does not carry `underReceivedRule` (R1 (d)), or a coin whose
  transaction is neither `completed` nor `unproven`, is refused with
  `Validation error: mustSpend[i]: output ...` naming it, nothing written.
  `createAction`'s `inputs`, the served `WalletInterface::create_action`
  and the refusal of a named change input are unchanged. A storage on the
  trait's default door refuses a non-empty list. The CLI's `--from` flag
  wraps this call (its captain told).
  The migration (the keeper's ruling of 2026-10-28 under 19.3: the three
  lines move as one): a `CreateActionDoor` literal written with
  `..Default::default()` builds as before, a full literal adds `must_spend: &[]`,
  and every caller of `create_action_offering` passes `&[]` where it names no
  coin.
- #187: `WalletStorageWriter::update_transaction_status_after_post`, a
  defaulted method taking the broadcasters' per-instance results
  (`WalletServices::post_beef`'s `Vec<PostBeefResult>`) or the post's own
  fault, under the owner's ruling 18.5 (a). `StorageSqlx` and
  `WalletStorageManager` write the post note and then move the statuses;
  the default (and `StorageClient`) moves the statuses only.

### Fixed

- #371: `internalize_action` refuses a `wallet payment` with no
  `paymentRemittance` and a `basket insertion` with no `insertionRemittance`
  at step 0, in the same check as #337 and with its texts, as the reference
  does (ts-stack@edf6e03 `validationHelpers.ts:972, 975`): ahead of the
  remittances' fields, the later outputs and any BEEF. A served text moves and
  comes earlier: `Wallet payment at index <i> missing paymentRemittance` and
  `Basket insertion at index <i> missing insertionRemittance`, answered after
  BEEF verification (an undecodable BEEF answered `Invalid BEEF ...`), are now
  `The output remittance parameter must be only paymentRemittance for wallet payment`
  and `The output remittance parameter must be only insertionRemittance for basket insertion`,
  served through `WalletInterface` as `Validation error: <text>`. The CLI's
  captain is told.

- #379: both doors trim a label with ECMAScript's whitespace (the
  WhiteSpace and LineTerminator productions), as the SDK's validateLabel
  trims it (ts-stack@edf6e03 `validationHelpers.ts:385-387, 411`), where
  they used Rust's `str::trim` (Unicode White_Space). The two sets differ
  at U+FEFF and U+0085 only. At internalizeAction a label of U+FEFF alone
  is refused with `The label parameter must be at least 1 length.` (it was
  stored), a label of U+0085 alone is stored (it was refused with that
  text), `\u{FEFF}p nosend ...` meets the BRC-177 refusal and
  `\u{0085}p nosend ...` is an ordinary label. At createAction
  `\u{FEFF}p nosend ...` meets the BRC-177 grammar and capability texts
  (it was an ordinary label) and `\u{0085}p nosend ...` is an ordinary
  label (it was refused). Every other label answers as before.

- #377: internalizeAction runs the reference's validateWalletPayment
  (ts-stack@edf6e03 `validationHelpers.ts:914-923`) on every wallet payment,
  per output in order, after the one-remittance check and ahead of the
  description and labels: a `derivationPrefix` or `derivationSuffix` that is
  not base64 (trimmed, not empty, the base64 alphabet, `=` only in the last
  two places, a length that is a multiple of 4) is refused with
  `The derivationPrefix parameter must be valid base64 string` or
  `The derivationSuffix parameter must be valid base64 string`, and a
  `senderIdentityKey` that is not a compressed secp256k1 key in hex with
  `The senderIdentityKey parameter must be ` and one of
  `even length, not <n>.`, `hexadecimal string.`, `at least 66 length.`,
  `no more than 66 length.`, `a valid compressed secp256k1 public key`
  (through `WalletInterface`, a `WalletError` reading `Validation error: `
  and that text). A non-base64 derivation or a sender such as `sender_key`
  was stored before; these are new refusals of input that succeeded before.
  The wallet's own refusal of an empty derivation, still ahead of the store
  (D13, #376), now reads the reference's text above, the prefix ahead of
  the suffix, in
  place of `Wallet payment outputs require non-empty derivation_prefix and
  derivation_suffix`. Both checks trim as ECMAScript's `trim()` does
  (`label.rs`): a value behind U+FEFF is read without it, and U+0085 is
  kept, so `cHJlZml4` followed by U+0085 is refused with the base64 text and
  a key followed by it with `even length, not 67.`. Two cases still answer
  in the wallet's place ahead of the door (D13, #376): on a P2PKH output
  the wallet parses the sender ahead of the derivation check, so `sender_key` there answers
  `Invalid sender_identity_key: invalid hex string: Invalid character 's' at
  position 0` (on any other output, the door's
  `Validation error: The senderIdentityKey parameter must be hexadecimal string.`);
  and the wallet reads an empty sender as self, so on a P2PKH output paying
  the wallet's self-derived key it passes the derivation check and the door
  refuses it with
  `Validation error: The senderIdentityKey parameter must be hexadecimal string.`,
  while on a P2PKH output paying another key it answers the wallet's
  `Derivation mismatch for output <n>: ...`.

- #380: internalizeAction bounds the description as the reference's
  validateStringLength does (ts-stack@edf6e03 `validationHelpers.ts:1053`,
  348-356): 5 to 2000 UTF-8 bytes as given, untrimmed, refused with
  `The description parameter must be at least 5 length.` or
  `The description parameter must be no more than 2000 length.` (through
  `WalletInterface`, a `WalletError` reading `Validation error: ` and that
  text), after the outputs' checks and ahead of the labels' and the BRC-177
  refusal. A description under 5 or over 2000 bytes was stored; beside an
  over-long label it answered the label text. This is a new refusal of input
  that succeeded before. The create door already bounds its description at
  the reference's place (755), in the port's own text (#383).

- #365: internalizeAction bounds every label as the reference's validateLabel
  does (ts-stack@edf6e03 `validationHelpers.ts:385-387, 409-417`, mapped at
  1054): 1 to 300 UTF-8 bytes once trimmed and lowercased, refused with
  `The label parameter must be no more than 300 length.` or
  `The label parameter must be at least 1 length.` (through
  `WalletInterface`, a `WalletError` reading `Validation error: ` and that
  text), ahead of the BRC-177 refusal of #357 as `Wallet.ts:1248-1250` orders
  them. A request carrying a `p nosend ` label beside an over-long label
  answered the BRC-177 text; a lone over-long or empty label was stored. This
  is a new refusal of input that succeeded before. Through `WalletInterface`
  a wallet payment whose derivation locks nothing still answers the wallet's
  `Derivation mismatch for output <i>: ...` ahead of every door refusal, the
  BRC-177 text included (the divergence row D13 in `docs/ARCHITECTURE.md`).

- #337: `internalize_action` refuses an output carrying the other protocol's
  remittance ahead of the remittance's fields, as the reference does
  (ts-stack@edf6e03 `validationHelpers.ts:971-976`, one remittance per
  output). A `wallet payment` with an `insertionRemittance` answers
  `The output remittance parameter must be only paymentRemittance for wallet payment`
  (was accepted when the insertion was well formed, else its
  customInstructions or tags text); a `basket insertion` with a
  `paymentRemittance` answers
  `The output remittance parameter must be only insertionRemittance for basket insertion`
  (was accepted, the payment remittance ignored).

- #311: a direct `acquire_certificate` refuses what
  `validateAcquireDirectCertificateArgs` refuses (`ts-stack@edf6e03`
  `validationHelpers.ts:1405-1439`, called at `Wallet.ts:697`), with its texts
  in its order, before any row is written. Input stored before is refused:
  a type or serial number that is not base64 of 32 bytes, a certifier that is
  not a compressed public key, a signature that is not canonical DER, a
  `fields` name outside 1 to 50 bytes or an unsafe key, an absent
  `keyringRevealer` or `keyringForSubject`, a keyring name outside 1 to 50
  bytes or a value that is not base64, a `privilegedReason` outside 5 to 50
  bytes or `privileged` without one. Served texts through `WalletInterface`
  are `WalletError("Validation error: The <parameter> parameter must be
  <rule>")`, `<parameter>` one of `privilegedReason`, `serialNumber`,
  `signature`, `revocationOutpoint`, `keyringRevealer`, `keyringForSubject`,
  `type`, `certifier`, `fields`, `field name`,
  `keyringForSubject field name` or `keyringForSubject field value`; an
  absent serial number or signature answered `serial_number required for
  direct acquisition` or `signature required for direct acquisition` and now
  answers `The serialNumber parameter must be valid when acquisitionProtocol
  is "direct"` (likewise `signature`, `revocationOutpoint`). The stored row
  and the returned certificate carry the trimmed type and serial number and
  the lowercased certifier and signature, and `verifier` is the normalized
  certifier (was the certifier as given) or the revealer's key; a missing
  revealer stored no verifier and is now refused. Of several bad names in one
  map, the one that sorts lowest decides the text (docs/ARCHITECTURE.md D12).
  The certificate's signature verification and field decryption after the
  validator (`Wallet.ts:705-728`) stay unported: #374.
- #351: `list_actions` with `includeInputs` reads the action's raw
  transaction where `ts-stack@edf6e03` does (`getRawTxOfKnownValidTransaction`,
  `StorageKnex.ts:507-508`, through `getProvenOrRawTx` at 189-211): the
  `proven_txs` row, else a `proven_tx_reqs` row `unsent`, `unmined`,
  `unconfirmed`, `sending`, `nosend` or `completed`; no longer
  `transactions.raw_tx` (`StorageSqlx::get_proven_or_raw_tx`, its one
  caller). Served values move: an action whose request is `invalid`,
  `doubleSpend` or in any other status outside the six (a listed `sending`
  row the review pass has not yet failed, an `unprocessed` request) answers
  `sequenceNumber` 0 and no `unlockingScript` where it answered the row's
  copy; a request in the six with no copy on the action's row answers its
  sequence where it answered 0. Raw bytes that do not parse stay the call's
  error (`SdkError(ReaderUnderflow { .. })` for short bytes), now witnessed.

- #345: classic mode refuses an empty ARC endpoint at construction, as
  Arcade mode does. Before, the deprecated `with_classic_arc` given an empty
  (or blank) url built a TAAL ARC rung at "" whose every post faulted: a
  host that passed an unset endpoint got a dead rung ahead of Bitails
  and WhatsOnChain and no error at startup. `Services::with_options` now
  answers `Invalid argument: ServicesOptions names no classic ARC endpoint:
  with_classic_arc(url) was given an empty url; name the TAAL ARC endpoint,
  or use ServicesOptions::with_arcade(url, config)` (the port's own text:
  the reference has no classic knob). A host on classic mode names its URL
  or moves to Arcade.
- #357: internalizeAction refuses a request carrying a BRC-177 `p nosend `
  label (matched trimmed and lowercased, as the SDK's validateLabel leaves
  it) with `The labels parameter must be BRC-177 noSend expiry labels only on
  outgoing createAction requests`, no full stop (through `WalletInterface`, a
  `WalletError` reading `Validation error: ` and that text), the reference's
  `WERR_INVALID_PARAMETER` at ts-stack@edf6e03 `Wallet.ts:1248-1250` (the
  prefix `startsWith` at `brc177NoSendExpiry.ts:119`). The door refuses it
  after the labels bound and writes nothing. Such a request was internalized,
  the label stored as metadata on the incoming action. This is a new refusal
  of input that succeeded before. `p nosendx` and `p nosend` alone stay
  ordinary labels.
- #312: createAction refuses a request carrying a BRC-177 `p nosend ` label
  (matched trimmed and lowercased, as the SDK's validateLabel leaves it)
  with `Invalid operation: Active storage does not support BRC-177 noSend expiry`
  (through `WalletInterface`, a `WalletError` with that text), the
  reference's refusal on a store without the capability (ts-stack@edf6e03
  `createNoSendExpiryAction.ts:201-203`), as BRC-177 `0177.md:53` asks; it
  ran after every other argument check and writes nothing. Such a request
  was accepted as an ordinary action, the label stored as metadata. This is
  a new refusal of input that succeeded before; BRC-177 itself is the 0.15
  epic #352. A label the BRC-177 grammar refuses gets the grammar's own
  text ahead of it (#359, below).
- #359: the create door answers the BRC-177 label grammar's refusals ahead of
  the capability refusal, with the reference's texts. Since #312 every label
  starting `p nosend ` (trimmed and lowercased) received `Invalid operation:
  Active storage does not support BRC-177 noSend expiry`; a request the
  grammar refuses now receives `Validation error: The labels parameter must
  be <mustBe>` (the reference's `WERR_INVALID_PARAMETER('labels', mustBe)`,
  ts-stack@edf6e03 `WERR_errors.ts:105`, no full stop; through
  `WalletInterface`, a `WalletError` with that text), as
  `parseBrc177NoSendExpiryLabels` runs at `Wallet.ts:1163` ahead of
  `createNoSendExpiryAction.ts:201-203`, in its order
  (`brc177NoSendExpiry.ts:81-116`): no expiry label, two of them or two
  module labels (`p nosend batch`, `p nosend expiry`) get `exactly one
  BRC-177 noSend expiry label` (:97); no single space between mode and value
  (`p nosend expiry never`, `p nosend expiry seconds 300 extra`) gets `a
  valid BRC-177 noSend expiry label` (:104); a mode outside `seconds`,
  `timestamp`, `blockheight` gets `a supported BRC-177 expiry mode` (:109);
  a leading zero, a sign or a non-digit gets `a canonical unsigned BRC-177
  expiry value` (:83); a value past 2^53 - 1 gets `a safely supported
  BRC-177 expiry value` (:87); `seconds 0` gets `a BRC-177 seconds duration
  greater than zero` (:113). A label the grammar accepts still gets the
  capability text. Nothing is written either way.
- #313: sync writes an incoming output's `purpose` as given, on insert and on
  the newer-row update, as `ts-stack@edf6e03` `EntityOutput.mergeNew` and
  `mergeExisting` do. A chunk row without `purpose` was stored as `change`;
  it is stored as `''` now (Go's zero value, `go-wallet-toolbox@e824899`
  `wdk/table_output.go:23`; the TS reference refuses such a row against its
  NOT NULL column, a cross-SDK disagreement held by #350; the column is NOT
  NULL here), so a synced coin without purpose is no longer offered as change by
  the create door, `StorageSqlx::allocate_change_input` or
  `count_change_inputs`. Rows already synced keep their stored `change`.
- #307: `list_actions` with `includeInputs` serves each input's
  `sequenceNumber` from the action's raw transaction, parsed and matched by
  source txid and vout, as `ts-stack@edf6e03` does (`listActionsKnex.ts`
  79-89), no longer from `outputs.sequence_number`. A served value moves: an
  input whose column was NULL answered 4294967295 and now answers its raw
  nSequence; an action with no stored raw transaction (an unsigned
  template), or an input its raw transaction lacks, answers 0 where it
  answered the column. With `includeInputUnlockingScripts` each input now
  carries its unlocking script from the same parsed input (it carried none).
  The inputs come by output id (the reference's unpaged finder), no longer
  by the sequence column. The cost is one raw-transaction read and parse per
  listed action that has inputs, where it was none. The column is still
  written, synced and read by the other readers as #306 left it.
- #304: `StorageSqlx::allocate_change_input` and `count_change_inputs` no
  longer take a received coin (`purpose = 'receive'`) whose parent is not
  `completed`: an unproven or sending receipt they picked before is now
  held, as the create door holds it for an action that does not name the
  rule (the wallet charter's R1 (d), #169). The two allocators share the
  door's `RECEIVED_COIN_CLAUSE`; the public signature names no rule, so it
  is off there. The same receipt under a `completed` parent and the
  wallet's own unproven change are taken as before. No signature moves.
  Nothing in the crate calls these two in production (the door and the
  door bench use the door's allocator); a dependent that calls them sees
  fewer coins while a receipt is unmined. bsv-wallet-cli's spend gate holds
  the same coin from outside.
- #187: `create_action` with `signAndProcess` now writes the
  `processActionPost` note on `proven_tx_reqs.history`, as the door's post
  path does and as every post in `ts-stack@edf6e03` writes its notes through
  `shareReqsWithWorld` (`processAction.ts:151`). Until now its history stayed
  `{}`. A host reading the history (the CLI's spend gate) sees a note on
  this path. On both paths the note now also carries `reqStatus` and
  `attempts` as the request stands before the status writes, and the
  reference's `successCount`, `doubleSpendCount`, `statusErrorCount` and
  `serviceErrorCount` (`attemptToPostReqsToNetwork.ts` 258-271). The note
  comes ahead of the status write's hint note.

- #309: the storage doors refuse an array past the reference's bound
  (ts-stack@edf6e03 `validateArray`) before its items, with
  `Validation error: The <name> parameter must be an array of at most 100000 items`
  for createAction's `inputs`, `outputs`, an output's `tags`, `labels`,
  `knownTxids` and `noSendChange`; internalizeAction's `outputs`, an
  insertion's `tags` and `labels`; listCertificates' `certifiers` and
  `types`; listOutputs' `tags`; listActions' `labels`; and
  `Validation error: The sendWith parameter must be an array of at most 1000 items`
  for createAction's `sendWith`. The create door's checks now run inputs,
  outputs, labels, where they ran labels, outputs, inputs, as the reference
  does (validateCreateActionArgs 758, 759, 762): a request with several bad
  arguments answers the inputs' texts ahead of the outputs' and the labels',
  and the outputs' ahead of the labels'. A bad input plus a bad output
  answered `outputs[0]: locking script cannot be empty` before and answers
  `inputs[0]: unlockingScript or unlockingScriptLength required` now.

- #300: the certificate issuance client nonce is the pinned reference's
  `createNonce` (`ts-stack@edf6e03` `createNonce.ts:21-44`, as `Wallet.ts:771`
  calls it): 16 random bytes and their `[2, 'server hmac']` HMAC with the
  certifier as counterparty, keyed by the bytes' TextDecoder text, base64 of
  the 48 bytes. The `clientNonce` a certifier receives moves from 44
  characters (32 random bytes, padded) to 64 characters without padding, so a
  certifier that decodes `clientNonce + serverNonce` as one string no longer
  meets `Invalid base64 padding`, and one that checks the client nonce with
  `verifyNonce(clientNonce, wallet, subject)` accepts it. The serial check now
  decodes the two nonces as one string (`Wallet.ts:809`): a bad character in
  the server nonce is named at its index in the joined string (64 past its
  own). With a server nonce long enough that `serverNonce + clientNonce`
  passes 800 bytes, the refusal is `The keyID parameter must be no more than
  800 length.` ahead of the serial's `The hmac parameter must be exactly 32
  bytes` (`validationHelpers.ts:227` via `:1772`, before `:1774`). The
  `client_nonce: String` field keeps its type. The CLI's captain and any
  certifier operator are told: the nonce's length and these two texts move.

- #295: both change allocators (the create door's and the public
  `StorageSqlx::allocate_change_input`) and both counts take only
  wallet-managed change, as `ts-stack@edf6e03`
  `StorageKnex.allocateChangeInput` 2241-2250 does: they no longer pick a
  caller-provided coin (`provided_by` other than `storage`), a non-P2PKH
  coin, a coin whose purpose is neither `change` nor `receive`, or a coin
  with a null or empty derivation prefix or suffix. The public allocator
  and its count also no longer pick a coin whose parent is not
  `completed`, `unproven`, or (unless `exclude_sending`) `sending`.
- #301: every `StorageSqlx` output reader returns the stored
  `sequence_number` as the full u32. `find_outputs`, `find_output_by_outpoint`,
  the create door's stored inputs and internalization's existing rows returned
  a decode error for a value at or above 2^31 (0xFFFFFFFF included), and
  `list_actions` with `includeInputs` panicked on it; they now return the
  value. The sync export panicked and `allocate_change_input` returned `None`
  on a negative stored value; a negative i32 now reads as the same bits
  (-1 is 0xFFFFFFFF) in every reader. A value outside both ranges is a decode
  error (the allocator still reads it as `None`). The reference's column is
  `integer('sequenceNumber').unsigned()` (`ts-stack@edf6e03`
  `KnexMigrations.ts:766`). No schema or public shape change.
- #280: `prove_certificate` refuses more than 100000 `fields_to_reveal`
  entries with `Validation error: The fieldsToReveal parameter must be an
  array of at most 100000 items` after the signature check and ahead of the
  name checks, as `ts-stack@edf6e03` `validationHelpers.ts:1506` does through
  `validateArray` (105-114, bound at 67). Such a request previously read
  `... must be at most 100 fields`, or a name or verifier text.
- #291: a direct `acquire_certificate` validates `revocationOutpoint` and
  stores it with its txid lowercased, as `ts-stack@edf6e03`
  `packages/sdk/src/wallet/validationHelpers.ts:1435` does, so a certificate
  acquired with an uppercase txid is found by `prove_certificate` (it answered
  `Certificate not found with the provided arguments` before) and the returned
  certificate carries the stored value. A malformed outpoint is now refused
  with `Validation error: The revocationOutpoint parameter must be a 32-byte txid and canonical numeric output index joined with '.'`
  (or the `revocationOutpoint vout` texts `a safe integer` and
  `no more than 4294967295 length.`) instead of being stored. A row stored
  raw before this change with an uppercase txid stays out of
  `prove_certificate`'s reach, as it was (the reference never stores such a
  row: 1435 runs on every direct acquire).
  `prove_certificate` now checks the argument's `certificate.fields` between
  `privilegedReason` and `certificate.type` (1478-1480): an unsafe key reads
  `The fields parameter must be an object with safe string keys`, more than
  100000 keys `... an object with at most 100000 properties`, and an empty or
  overlong name `The field name parameter must be at least 1 length.` or
  `... no more than 50 length.`, all with `Validation error: `, replacing the
  helper's `Certificate field name must be a safe UTF-8 string of 1–50 bytes`
  for a bad argument name and preceding a bad type.
- #269: certificate issuance reads the certifier's identity from the
  response header `x-bsv-auth-identity-key`, as the pinned reference does,
  and requires it: a response without it refuses with `Invalid certifier!
  Expected: <certifier>, Received: null` before the body is read, where it
  passed before. The old `x-bsv-identity-key` response header is no longer
  read, so a certifier answering only under it is now refused; a non-ASCII
  value is compared and refused as a mismatch where the check was skipped;
  a repeated header's values are joined with `, ` and refused where the base
  accepted its leading value. Certifier operators must send
  `x-bsv-auth-identity-key` with the certifier's public key; the request's
  headers are unchanged.

- #287: `StorageSqlx::allocate_change_input` returns the stored `outputs.type`
  in `TableOutput.output_type` and its serialized `outputType` field instead
  of silently returning an empty string for the missing `output_type` column.
- #276: synced stores retain output descriptions, spending descriptions,
  sequence numbers, and spender references. A newer row updates the reference
  merge's columns and leaves the stored sequence number, as the reference does.
  Spender references are translated to the receiving store's transaction IDs.
- #298: a newer synced output row no longer overwrites the stored `satoshis`,
  `derivation_prefix` or `derivation_suffix`; the update writes the reference
  merge's columns and no others, as `EntityOutput.mergeExisting` does.

- #283: certificate issuance now checks privilege, direct-only arguments,
  issuer URL and field records in the pinned reference's order before protocol
  work. Response checks require certificate before server nonce, retain both
  presence errors' `!`, and use the reference's field-count, missing-field,
  serial false-result, signature-error and master-decryption texts. The
  signature step decodes type and serial as the reference's `toBinary` does:
  a non-canonical type or serial answers `Certificate verification failed`
  (the serial's HMAC still decodes leniently). The revocation outpoint follows
  `toBinary`'s grammar: an unparsable text or a `.07` index answers
  `Certificate verification failed`, and a txid-only outpoint is verified as
  vout 0 over its txid; the answered outpoint, serial and signature text are
  stored as received. The nonces are decoded before the serial's hmac length
  is checked. The server nonce verifier now runs before the serial HMAC, using the
  reference's derivation inputs and ignoring its false result as Wallet.ts does.
  An empty master keyring answers `A MasterCertificate must have a valid
  masterKeyring!` before storage; otherwise returned fields must decrypt with
  the request's master keyring before storage.
  The identity header (#269), client nonce generation (#300) and signature `Ok(false)`
  policy (#188/#199) retain their existing behavior. CLI error snapshots need
  review when this lands.
- #187: a noSend-only `process_action` on stored actions (`is_new_tx`,
  `is_send_with` false, `is_no_send` true) answers an empty
  `send_with_results` before any handle is resolved, so one naming an
  unsigned template no longer answers `InvalidTransactionStatus`. At
  `ts-stack@edf6e03` `processAction.ts:113` shares only `args.sendWith`
  (empty for such a call, `:55`) and `shareReqsWithWorld` returns
  `{ swr: [] }` at `:313`. Nothing is written or posted, as before.
  Test: `test_no_send_only_call_naming_a_template_answers_unchanged`.
- #175: `MonitorStorage::review_status` now voids, each pass and in check 1's
  `write_tx()`, every `spendable = 1` output whose creator transaction is
  `failed` with no request that could still reconcile (no request, or one that
  is `invalid` or `doubleSpend`): `spendable = 0`, `spent_by` kept for check 4,
  `updated_at` advanced. A store from 0.13.0, which holds `failed` beside
  `spendable = 1`, heals on the pass after upgrade; #164 voided only the
  outputs of the pass's own failures. This follows `ts-stack@edf6e03`
  `packages/wallet/wallet-toolbox/src/storage/methods/reviewStatus.ts:93-119`
  (unbounded, every pass), except that the reference also clears `spentBy`.
  The pass's log gains `Voided N outputs created by failed transactions`.
  A false failure stays safe: `un_fail` re-validates `spendable = 0` outputs
  through `is_utxo` on recovery. Tests:
  `review_status_heals_failed_beside_spendable_left_by_an_earlier_release`
  (the sweep, the reconcile guard, a repeat pass writes nothing) and the moved
  pin in `review_status_voids_created_outputs_of_newly_failed_transactions`.
- #271: certificate issuance validates `type` before `certifier`, sharing the
  reference's structural Base64 and 32-byte length rules with `prove_certificate`.
  Invalid types now return `Validation error: The type parameter must be <rule>`,
  where the rule is `valid base64 string`, `at least 32 bytes`, or
  `no more than 32 bytes`, replacing the late `Invalid certificate_type ...`
  and `certificate_type must be 32 bytes when decoded ...` refusals.
  ECMAScript whitespace is trimmed and accepted type text is preserved in the request.
  The response's missing revocation outpoint now reads `Invalid revocationOutpoint!`;
  a differing field value reads `Invalid field! Expected: <sent>, Received: <received>`
  without the field name. Both retain the `Validation error: ` prefix and match
  `ts-stack@edf6e03 Wallet.ts:764,744`. The internal identity-key parse is unchanged;
  the reference derives that key and has no corresponding issuance error text.
- #263: unit-test and `bench-support` authenticated storage Peers now check
  their configured base URL against the #182 loopback policy before each SDK
  transport send, including sends through `peer()`. Non-loopback destinations
  return the existing rule 28 refusal; loopback handshakes reach the SDK.
  The SDK still owns redirects and proxy handling. Production keeps its
  transport and public types; wire fields are unchanged.
- #192: output finders now return the stored `purpose` and `providedBy`,
  including received outputs marked `receive` and `storage`. These reads
  also preserve output descriptions, spender IDs, sequence numbers,
  and spending descriptions. Internalization merge reads select the same
  columns, and row decoding reports missing columns instead of supplying defaults.
  The `list_outputs` wire fields are unchanged.
- #285: Deferred `create_action` results now hold the decoded storage reference
  bytes in `SignableTransaction.reference`, replacing the UTF-8 bytes of its
  base64 text. Base64-encode this field for `SignActionArgs.reference` so the
  create/sign round trip uses the original storage handle. Storage RPC
  reference strings stay base64; the SDK byte field's type is unchanged.

- #260: `prove_certificate` now validates type, serial, certifier, subject,
  signature, requested name lengths, then verifier before the certificate
  lookup, in the ported checks' order at `ts-stack@edf6e03`
  `packages/sdk/src/wallet/validationHelpers.ts:1483-1509`. Type and serial
  require structurally valid Base64 encoding 32 bytes; subject requires a
  compressed secp256k1 key, and signature requires hexadecimal canonical DER
  with nonzero in-range scalars. Hex arguments are trimmed and lowercased;
  Base64 arguments are trimmed before lookup.
  Argument refusals use `Validation error: The <name> parameter must be <rule>`.
  Subject uses the existing public-key rules; signature uses `even length, not N.`,
  `hexadecimal string.`, or `a canonical DER-encoded ECDSA signature`.
  Type and serial use `valid base64 string`, `at least 32 bytes`, or
  `no more than 32 bytes`. These now precede a missing-certificate refusal.
  Empty and overlong UTF-8 requested names now use
  `The fieldsToReveal <name> parameter must be at least 1 length.` or
  `The fieldsToReveal <name> parameter must be no more than 50 length.`,
  ahead of verifier validation and lookup, instead of the helper's
  `Certificate field name must be a safe UTF-8 string of 1–50 bytes`.
  The helper retains its own name, map, and canonical stored-serial gates.
  Permission checks retain the raw verifier, matching
  `packages/wallet/wallet-toolbox/src/WalletPermissionsManager.ts:5424-5434`.

- #270: `prove_certificate` now checks `certificate.revocationOutpoint`
  between subject and signature, `privilegedReason` ahead of every
  certificate argument, then the requested-name count and uniqueness, as
  `ts-stack@edf6e03` `packages/sdk/src/wallet/validationHelpers.ts:1477`
  (`validatePrivilege` at 207-222), `1498-1501` and `1513-1518` do (the check
  at 1511 repeats 1477 and never fires).
  The outpoint must be a 64-hex txid and a canonical vout up to 4294967295,
  else `The certificate.revocationOutpoint parameter must be a 32-byte txid and canonical numeric output index joined with '.'`
  or, for the vout, `a safe integer` or `no more than 4294967295 length.`
  under `certificate.revocationOutpoint vout`. Its txid is lowercased before
  lookup. A present `privilegedReason` outside 5 to 50 UTF-8 bytes reads
  `at least 5 length.` or `no more than 50 length.`, and `privileged: true`
  with no reason reads
  `The privilegedReason parameter must be a value when 'privileged' is true`
  (218-220). More than 100 requested
  names now read `The fieldsToReveal parameter must be at most 100 fields`
  instead of `fieldsToReveal must be an array of at most 100 strings`, and a
  repeated name `The fieldsToReveal parameter must be unique field names`
  instead of `fieldsToReveal contains duplicate field: <name>`. All read
  `Validation error: ` at the wallet boundary and precede the lookup.
- #261: certificate issuance now shares the `prove_certificate` public-key
  validator for `certifier`, before request preparation or HTTP. Invalid
  certifiers return `Validation error: The certifier parameter must be <rule>`
  with the reference's hex, length or compressed-key rule, replacing
  `Invalid certifier public key: <detail>`. ECMAScript trimming and lowercasing
  normalize the key for encryption and response comparisons, accepting
  uppercase, space-padded and U+FEFF-padded certifiers. A differing response
  identity header now reads `Invalid certifier! Expected: <expected>, Received: <received>`
  inside `Validation error:`, matching `ts-stack@edf6e03 Wallet.ts:789-791`.
- #248: unit-test and `bench-support` clients for overlay lookup, certificate
  issuance, and unauthenticated remote storage now use the #182 loopback-only
  transport. Remote requests and redirects are refused before transport with
  the host and rule 28 in the error. Loopback fixtures retain the existing
  headers and request bodies. Production clients and the public API are unchanged.
- #249: `prove_certificate` now validates `certificate.certifier` and then
  `verifier` before the certificate lookup, as `ts-stack@edf6e03`
  `packages/sdk/src/wallet/validationHelpers.ts:1490-1493,1509` does ahead of
  `packages/wallet/wallet-toolbox/src/signer/methods/proveCertificate.ts:28`.
  Both arguments are trimmed and lowercased as the reference's
  `validateHexString` does (`validationHelpers.ts:524,533`), with ECMAScript
  whitespace semantics: U+FEFF padding is accepted and U+0085 is refused.
  The normalized certifier is bound in the lookup, so uppercase and
  space-padded arguments resolve lowercase stored certifiers; verifier keys
  are derived from the normalized argument.
  A bad key refuses with `Validation error: The <name> parameter must be <rule>`
  (`validationHelpers.ts:185-191,522-534`), where the rule is
  `even length, not N.`, `hexadecimal string.`, `at least 66 length.`,
  `no more than 66 length.`, or `a valid compressed secp256k1 public key`.
  Previously, a bad key in a request matching no certificate read
  `Certificate not found with the provided arguments`, and one matching a
  stored certificate read `Invalid verifier public key: <detail>` or
  `Invalid certifier public key: <detail>` after the lookup, including for an
  empty `fields_to_reveal`. The stored certifier is no longer parsed: with
  SQLite, `certifier IN (?)` on an uncollated TEXT column matches it to the
  normalized argument byte for byte. The remote storage server decides its
  own collation.
- #245: `create_action` refuses a caller-named unspendable stored output
  under `noSend` too, preserving its previous spender if a later action is
  aborted. The stored-input guards check change, then locking script and
  integer satoshis, then `markKnownInputsSpent` in `ts-stack@edf6e03`
  `packages/wallet/wallet-toolbox/src/storage/methods/createAction.ts:399-436`,
  called at `:503`. The `:982` gate is disabled by default at `:61`.
  Without a competing txid, an unspendable row uses `Error::ValidationError`
  with the live `:420-423` text:
  `The inputs[N] parameter must be spendable output. output <txid>:<vout> appears to have been spent (spendable=false).`
  A stored spender with a txid requires double-spend review, even when its
  status is failed; the existing `Error::InvalidOperation` carries
  `Undelayed createAction or signAction results require review. doubleSpend: <txid>`.
  A spender without a txid falls through to the output's spendability flag.
  Missing script or satoshis now uses `Error::ValidationError` with
  `The <txid>.<vout> parameter must be output with valid lockingScript and satoshis`,
  without a trailing period (`:979-980` and
  `packages/wallet/wallet-toolbox/src/sdk/WERR_errors.ts:105`). Stored
  transaction bytes can still supply a pruned script. The change guard and
  its refusal text at `:972-976` are retained.

- #240: `prove_certificate` now refuses certificate serial numbers that do not
  canonically encode exactly 32 bytes, after map-value validation and before
  the subset loop or verifier key derivation. This follows `ts-stack@edf6e03`
  `packages/sdk/src/auth/certificates/MasterCertificate.ts:365,63-78`.
  Invalid Base64 uses `Certificate serial number must use canonical base64 encoding`;
  empty or oversized values use
  `Certificate serial number is empty, oversized, or non-canonical`;
  shorter nonempty values use `Certificate serial number must encode exactly 32 bytes`.
  These refusals use `bsv_rs::Error::WalletError`, as the existing gate does
  for the reference's plain `Error`.

- #240: an empty `fields_to_reveal` now returns an empty verifier keyring,
  retaining the input validation but performing no field recovery. Previously,
  the request disclosed every stored field. The reference passes the empty
  request through unchanged at `ts-stack@edf6e03`
  `packages/sdk/src/wallet/validationHelpers.ts:1506-1508` and
  `packages/wallet/wallet-toolbox/src/signer/methods/proveCertificate.ts:31-43`;
  `MasterCertificate.ts:348,370-415` then returns an empty keyring.

- #236: `prove_certificate` now validates requested names, both field maps,
  and every map value before the subset check and field recovery, following
  `ts-stack@edf6e03`
  `packages/sdk/src/auth/certificates/MasterCertificate.ts:348-364`.
  Empty, overlong, and reserved names use
  `Certificate field name must be a safe UTF-8 string of 1–50 bytes`
  (`:23-29,49,138`); duplicate requests use
  `fieldsToReveal contains duplicate field: <name>` (`:139-142`).
  The 100-entry limits use
  `fieldsToReveal must be an array of at most 100 strings` (`:125-129`),
  `Certificate fields cannot contain more than 100 entries`, or
  `Master certificate keyring cannot contain more than 100 entries` (`:43-45`).
  Invalid Base64 uses `<label> <name> must use canonical base64 encoding`;
  empty values use `<label> <name> is empty, oversized, or non-canonical`,
  where the label is `Certificate field` or `Master certificate keyring field`
  (`:63-73,351-364`). Missing fields use
  `Fields to reveal must be a subset of the certificate fields. Missing the "<name>" field.`
  (`:370-379`). These refusals use `bsv_rs::Error::WalletError`, mapping the
  reference's plain `Error` (and its array-limit `TypeError`). An empty
  request with empty maps now returns an empty keyring. Recovery failures
  still use `Failed to decrypt certificate field!`; value size caps beyond
  canonical Base64 remain with #223.
- #193: `create_action` now refuses caller-named change with the reference's
  exact text, `The inputs[N] parameter must be an unmanaged input. Change outputs are managed by your wallet.`,
  replacing `inputs[N]: cannot spend change output <txid>:<vout> directly. Change is managed by the wallet.`
  The change guard runs before the spendability guard, so unspendable change
  receives the same change refusal instead of
  `inputs[N]: output <txid>:<vout> is not spendable`. Both guards still refuse
  the same inputs. The class remains `Error::ValidationError`, mapping the
  reference's `WERR_INVALID_PARAMETER` as in #200 and #211. Reference:
  `ts-stack@edf6e03`, `packages/wallet/wallet-toolbox/src/storage/methods/createAction.ts:868-874,972-983`
  (zero-based input index and guard order) and
  `packages/wallet/wallet-toolbox/src/sdk/WERR_errors.ts:105` (message template).
  Hosts matching the old text should match the class instead; the CLI's
  fixture for #234 must re-pin to the new text.
- #182: unit-test and `bench-support` service clients now refuse non-loopback
  requests before transport, including redirects and default-provider posts
  in every post mode. The private HTTP client ignores proxy environment
  variables and pins `localhost` to loopback; refusals name the host and
  rule 28. Production clients and the public API are unchanged.

- #233: `prove_certificate` now refuses requested field names outside 1 to 50
  UTF-8 bytes or equal to `__proto__`, `constructor`, or `prototype`, using
  `bsv_rs::Error::WalletError("Failed to decrypt certificate field!")` after
  the subset check. Previously, present overlong or reserved names could
  produce a verifier keyring. The rule and recovery error follow
  `ts-stack@edf6e03`
  `packages/sdk/src/auth/certificates/MasterCertificate.ts:23-29,573-574,621-622`
  and `packages/sdk/src/primitives/SafeRecord.ts:2-3`.

- #228: `prove_certificate` now restores omitted leading zero bytes in legacy
  field keys before wrapping them for the verifier. A 31-byte master key
  previously produced a 31-byte verifier key; it now produces the same
  32-byte key used to decrypt the field, as returned by `ts-stack@edf6e03`
  `packages/sdk/src/auth/certificates/MasterCertificate.ts:393,606-619` and
  required by `VerifiableCertificate.ts:54-60,203`.

- #229: certificate field recovery now consistently refuses with
  `bsv_rs::Error::WalletError("Failed to decrypt certificate field!")`, including
  empty or missing master entries, invalid Base64, wallet or field decryption
  failures, and decrypted plaintext that is not UTF-8. Previously, empty,
  missing or non-Base64 master entries used the port's own messages, and
  non-UTF-8 plaintext was accepted. This follows `ts-stack@edf6e03`
  `packages/sdk/src/auth/certificates/MasterCertificate.ts:573-622`, including
  strict UTF-8 decoding at `:619`. The plaintext size bound at `:614-615`
  remains with #223; this change adds no size bound.
- #205: `StorageSqlx::mark_transaction_seen_on_network_by` in
  `src/storage/sqlx/storage_sqlx/monitor.rs` now puts both status updates and
  the broadcast hint in one `write_tx()`, using `record_broadcast_status_on`
  on that connection. The SEEN, IN_BLOCK and MINED callback arms previously
  waited once for the hint write and again for the status write under
  contention; two 5 s `busy_timeout` waits could exceed the Arcade webhook
  sender's 10 s timeout. They now take one status-transaction wait and retain
  the typed `StoreBusy` required by #44, with no writes on that busy. A status
  error or cancellation before commit rolls back both statuses and the hint.
  Hint-write errors remain logged without being returned. The existing
  `mark_transaction_seen_on_network` wrapper delegates to this method; MINED
  proof ingestion still runs separately after the tracker check, preserving
  #201's idempotent retry after a committed status update. CLI #249 keeps its
  host-side deadline.

- #222: `prove_certificate` now decrypts each requested field under its
  recovered master key before wrapping that key for the verifier. Previously,
  a corrupt or mismatched master entry produced a verifier keyring and failed
  only when the verifier decrypted the field. It now fails at prove time with
  `bsv_rs::Error::WalletError("Failed to decrypt certificate field!")`, mapping
  the reference's plain JavaScript `Error`. The check follows
  `ts-stack@edf6e03`, `packages/sdk/src/auth/certificates/MasterCertificate.ts:382-393`
  and its `decryptField` refusal at `:612-622`. Valid field and keyring bytes
  keep the #203 scheme.

- #216: `sign_action` previously accepted caller spends longer than the
  `unlockingScriptLength` declared at `create_action`, which could reduce the
  effective fee below the rate used to price the transaction. It now refuses
  those spends before signing or writing, with `Error::ValidationError` and
  `The args parameter must be spend unlockingScript length N exceeds expected length M`
  (no trailing period). As in `ts-stack@edf6e03`
  `signer/methods/completeSignedTransaction.ts:25-30` and `sdk/WERR_errors.ts:105`,
  the comparison uses bytes, while `N` reports the supplied script's
  hex-character count and `M` is the declared byte length. Equal and shorter
  spends remain accepted. The declared lengths ride the wallet's private
  pending-action cache from the create args, as the reference's
  `pendingSignActions` retains those args (`signer/methods/createAction.ts:189,267`);
  restarting a wallet between create and sign loses the pending action in both
  implementations.

- #203: certificate issuance now encrypts each field with a fresh 32-byte
  symmetric key, stores the ciphertext as Base64 text, and wallet-encrypts
  that key into the master keyring. Previously it wallet-encrypted the
  plaintext and copied the same ciphertext into both the field and keyring.
  Intake now preserves the signed Base64 text (#197); Base64 decoding is
  deferred to field decryption. The scheme follows `ts-stack@edf6e03`,
  `packages/sdk/src/auth/certificates/MasterCertificate.ts:277-293,380-420`,
  with the verifier's decrypt procedure in `VerifiableCertificate.ts:180-211`.
  The pinned bsv-rs #261 fixture witnesses reference-issued and Rust-issued
  certificates through intake, storage, proof and field decryption.

  **For hosts (#203):** every certificate issued through the toolbox's own
  issuance before this change does not decrypt under the reference, Go or
  bsv-rs's corrected scheme and must be re-issued. A certificate issued by the
  reference now proves and decrypts here. This does not migrate old keyrings.
  The pinned bsv-rs 0.9.0-beta.1 still has the separate signature-verifier and
  certificate decrypt-helper differences tracked by #199; the witness uses
  the reference's verification and decryption procedures directly.

- #211: `sign_action` previously accepted caller spends on storage-allocated inputs and replaced scripts already supplied at `create_action`. It now refuses a spend unless its vin names a caller input deferred with `unlockingScriptLength`, before signing or processing, with no storage writes and the unsigned action available for completion. The reference's `WERR_INVALID_PARAMETER` maps to `Error::ValidationError` with the text `The args parameter must be spend does not correspond to prior input with valid unlockingScriptLength.` Reference: `ts-stack@edf6e03`, under `packages/wallet/wallet-toolbox/src/`, `signer/methods/completeSignedTransaction.ts:15-32` (the refusal at lines 19-24), `signer/methods/buildSignableTransaction.ts:169-189` (caller inputs), and `sdk/WERR_errors.ts:105` (the complete message).
- #212: `create_action` rejects caller inputs with `unlocking_script: Some(vec![])` or `unlocking_script_length: Some(0)` using `Error::ValidationError`, before any BEEF parse or storage call. The messages are `The unlockingScript parameter must be hexadecimal string.` and `The unlockingScriptLength parameter must be a positive integer.` The empty-script text is the reference's byte for byte (`ts-stack@edf6e03` `packages/sdk/src/wallet/validationHelpers.ts:522-530,595,758` and `packages/sdk/src/wallet/WERR_INVALID_PARAMETER.ts:14`). The length text is the port's own in the reference's sentence shape; the positive-length bound follows `BRCs@5119a9b` `wallet/0100.md:649` (the TS helper at `:598` permits zero). Hosts that sent an empty script to reserve an input must omit the script and supply a positive `unlockingScriptLength`.
- #200: `sign_action` signs only inputs allocated by storage (`provided_by = "storage"`). Previously it could sign a caller-named input whenever the stored output carried derivation metadata, and deferred actions discarded scripts supplied at `create_action`. Caller scripts supplied at either step are now preserved. This port refuses a missing or empty named-input script before signing or processing, using the reference's `The inputs[N].unlockScript parameter must be valid.` text and mapping its `WERR_INVALID_PARAMETER` to `Error::ValidationError`; `ts-stack@edf6e03` completes and processes the action before `verifyUnlockScripts` reports a missing script's refusal, and refuses an empty script earlier, at its argument gate, as not a hexadecimal string (`validationHelpers.ts:522-530`), the same class. The WalletInterface boundary renders `Validation error: The inputs[N].unlockScript parameter must be valid.`, and the action stays unsigned for completion by the caller. Reference: `ts-stack@edf6e03`, under `packages/wallet/wallet-toolbox/src/signer/methods/`, `signAction.ts:65-90`, `createAction.ts:181-189`, `buildSignableTransaction.ts:176-189`, `completeSignedTransaction.ts:15-32,50-79`, and `verifyUnlockScripts.ts:51-64`; BRCs@5119a9b `wallet/0100.md:643-649,724-729`.
- `ArcadeEventsTask::apply_event` preserves `Error::StoreBusy` from inline proof ingestion on the MINED and IMMUTABLE arms (#201), following the #44 rule that a store busy keeps its type to the caller. Previously these arms swallowed the busy, raised the monitor's proof-fetch flag and returned `Ok`, so a host could acknowledge and discard a delivery whose inline proof was not stored. The typed busy now lets the relay poller hold its cursor and the webhook door request a retry, without triggering a fetch fallback. On MINED and IMMUTABLE, the status write may have committed when `StoreBusy` returns; retrying the delivery is idempotent. The SEEN, IN_BLOCK, REJECTED and CONFLICT arms already forwarded errors returned by their storage calls. `record_broadcast_status_quiet` logs a busy and never returns it, by design, because that write records a hint for reduced sends, not a delivery. Other proof-ingest faults retain the fetch fallback.
- #202: `StorageClient` sends `ValidCreateActionArgs.input_beef` as `inputBEEF`,
  previously `inputBeef`, with byte-array encoding (`ts-stack@edf6e03`,
  SDK `wallet/validationHelpers.ts:716,756-757`; BRC-100 at `5119a9b`,
  `wallet/0100.md:676`). Local storage fixtures keep their `inputBeef` shape.
- #202: the remote wire wraps certificate `userId` and basket `userId`/`name`
  filters in `partial` (WT `sdk/WalletStorage.interfaces.ts:265,272`). Entity
  timestamps send `created_at`/`updated_at` instead of `createdAt`/`updatedAt`
  (WT `storage/schema/tables/TableUser.ts:4-5`); certificate `certType` sends
  `type` (`TableCertificate.ts:10`); transaction and proof-request `inputBeef`
  send `inputBEEF` (`TableTransaction.ts:35`, `TableProvenTxReq.ts:33`);
  output `outputType` sends `type` (`TableOutput.ts:20`). These names apply to
  the storage wire only; the shared Rust entities keep their local serialization.
- #202: sync entities send basket `numberOfDesiredUTXOs`/`minimumDesiredUTXOValue`
  instead of `numberOfDesiredUtxos`/`minimumDesiredUtxoValue`
  (`TableOutputBasket.ts:9-10`), label `txLabelId` instead of `labelId`
  (`TableTxLabel.ts:6`, `TableTxLabelMap.ts:6`), tag `outputTagId` instead of
  `tagId` (`TableOutputTag.ts:6`, `TableOutputTagMap.ts:6`), and commission
  `lockingScript` instead of `payerLockingScript` (`TableCommission.ts:12`).
  Transaction status values send `nosend`/`nonfinal` instead of
  `noSend`/`nonFinal` (WT `sdk/types.ts:84`). All TS citations here are at `edf6e03`;
  WT paths start at `packages/wallet/wallet-toolbox/src/`, SDK paths at
  `packages/sdk/src/`.
- #202: remote results accept input `type` as `input_type` (previous wire name
  `inputType`, WT `sdk/WalletStorage.interfaces.ts:295`), output `type` as
  `output_type` (`TableOutput.ts:20`), and transaction/proof-request `inputBEEF`
  as `input_beef` (`TableTransaction.ts:35`, `TableProvenTxReq.ts:33`). Sync
  state accepts `when` as `when_last_sync_started` (previous wire name
  `whenLastSyncStarted`, `TableSyncState.ts:14`); sync results accept
  `maxUpdated_at` as `max_updated_at` (previous wire name `maxUpdatedAt`,
  WT `sdk/WalletStorage.interfaces.ts:715`). Existing response spellings remain
  accepted. [The field audit](docs/STORAGE-WIRE.md) records all 40 methods,
  remaining owner decisions and bsv-rs #105 changes; 41 request witnesses under
  `tests/fixtures/data/storage-wire/` are replayed by `tests/storage_wire.rs`.

For hosts: a toolbox wallet on a reference storage server now sends `inputBEEF`.
A reference client against a toolbox server uses the same storage-wire field
contract. This crate supplies a client, so that reverse-host direction is a
contract statement, not a server run. The CLI's toolbox wallet with toolbox
storage does not use this remote wire unless configured for a reference server.

## [0.14.0-beta.1] - 2026-10-26

The programme line's (epic #80) opening prerelease, cut from beta on the owner's word of 2026-10-26 after the bsv-rs `=0.9.0-beta.1` pin. What moves for a host coming from 0.13.1: the pin itself (bsv-rs's types are shared, so a host re-pins both); the internalize door runs bsv-rs's spend verifier and needs the tracker's `mining_tip`; a received unproven coin is offered only under `underReceivedRule`; `WalletOptions::rng_seed` and `StorageSqlxOptions` for deterministic references; `PostBeefMode::PromiseAll` is the default; `Wallet::process_action` posts stored signed actions; abort and retire refuse once the store holds an accepting word. A `-beta.N` is a semver prerelease: a plain `bsv-wallet-toolbox-rs = "0.14"` never resolves to it. Phase exit merges beta into main as 0.14.0 after the CLI's gate passes on this prerelease and a day of canary (RELEASING.md).

### Added

- #106: `WalletOptions::rng_seed: Option<[u8; 32]>` and `storage::sqlx::StorageSqlxOptions { pub rng_seed: Option<[u8; 32]> }`, accepted by `StorageSqlx::new_with_options` and `open_with_options`. The same seed and call order repeat SQLx action references, change derivations and process-action batch references; cloned stores share one stream. `None` preserves OS entropy and the default behavior, and the toolbox never reads an environment variable to select a seed. The wallet holds a separate seeded stream; routing it to remote authentication, the managers and the monitor still needs an owner-approved interface. BRC-2 IVs remain bsv-rs's `SymmetricKey::encrypt` responsibility and are not controlled by these seeds.
- `Wallet::process_action(StorageProcessActionArgs, &str) -> Result<StorageProcessActionResults>` posts stored signed actions by reference or txid through `send_with` (#66). Build with `create_action_offering` and `no_send`, then post without the wallet's spending lock. The door rebuilds BEEF from storage, records each broadcaster's result, refuses unsigned templates with `InvalidTransactionStatus`, and returns `unproven` without posting again for an already posted action. The reference is `ts-stack@edf6e03` `packages/wallet/wallet-toolbox/src/storage/methods/processAction.ts` (the sendWith path, `shareReqsWithWorld`); the transitions are the wallet charter's section 5.
- `Error::SpendRefused { offset: u64, txid: String, input: Option<u32>, why: SpendRefusal }` (#103), with the verifier's byte offset, hexadecimal transaction ID, input and reason preserved; the one public addition of this change (`Error` is `#[non_exhaustive]`). `why` is bsv-rs's `beef_stream::SpendRefusal`, named through bsv-rs; the error's text is `spend refused at offset N, input I of <txid>: <why>`, where `<why>` is bsv-rs's own `Display` for the reason (bsv-rs #213, 0.9.0-beta.1): a script refusal prints the interpreter's own sentence and every other arm bsv-rs's words for its rule.

- `fuzz/`: twelve cargo-fuzz targets on the bytes from outside (the doors' BEEF, the TSC proof and BUMP intake, the broadcast-memory decoder, the Arcade SSE stream, and the WhatsOnChain, Bitails, BananaBlocks, ARC, Arcade, BHS and Chaintracks response parsers), seeded from the repository's fixtures, with a dispatch-only workflow `.github/workflows/fuzz.yml` and `docs/FUZZ.md` (#58). No production code changes.

### Changed

- `create_action` offers an unproven received wallet payment only when that call's labels contain the exact string `underReceivedRule` (#169; `wallet.md` section 2 and section 5, R1 (d)). The rule is off by default and recorded in the action's labels. Hosts that relied on the old offer receive `Insufficient funds` until they name the rule; mined (`completed`) receipts and the wallet's own change remain eligible as before. Received origin uses the existing `outputs.purpose = "receive"`, including custom outputs converted to wallet payments, with `provided_by = "storage"` for managed signing. This implements the keeper's ruling and deliberately diverges from `ts-stack@edf6e03`: `internalizeAction` writes received payments with `providedBy: "storage"`, `purpose: "change"`, `change: true` in the default basket, and `StorageKnex.allocateChangeInput` selects those fields without an action rule.
- `PostBeefMode::PromiseAll` is the default: submit to every named broadcaster in parallel and retain each result, including a named transport fault (#167, broadcaster charter B3). `UntilSuccess` remains an explicit opt-in on the 0.14 line; removal requires the owner's word.
- The wallet's spending lock is released after building and signing in `create_action_offering`, before the broadcast await (#66). Existing one-call transaction rows are preserved.
- Signed noSend actions keep every output's `spendable` flag set, matching the reference at `edf6e03`; transaction status still gates allocation and the action's inputs remain locked. The later sendWith call does not rewrite output flags (#66).
- **The pin (#144):** `bsv-rs` moves from `0.8.0` to `=0.9.0-beta.1`, the owner's word, with the same `full` and `http` features. What a host sees: a transport fault of `StorageClient`'s authenticated peer is bsv-rs's typed `Error::TransportFault` (below); `Error::SpendRefused` prints bsv-rs's `Display` for its reason (below); the internalize door's `customInstructions` refusal is bsv-rs's validator's, in its words (#143, under Fixed). bsv-rs's `Kind` and `Reason` gain `DuplicateOffset` and are `#[non_exhaustive]`, so `Error::InvalidBeef`'s `kind` may carry a kind this crate's docs no longer count; the seven key-related args types gain `privileged`, `privileged_reason` and `seek_permission`, which the toolbox's own calls leave `None`. bsv-rs 0.9.0 judges a BEEF in the order held, parents ahead of their children (BRC-62; bsv-rs #86), where 0.8 sorted before judging: `createAction`'s assembly and its change-coin check now sort before judging, so no wallet answer moves, but a host passing an in-memory `Beef` held child-first to `verify_beef_merkle_proofs` reads `BEEF structure is invalid` until it calls `sort_txs()`. A TSC proof whose `*` marks a duplicate at an even offset (a left node, which BRC-74 at 5119a9b never duplicates: the flag is a right-hand optimization, `transactions/0074.md:41`) is refused by `tsc_proof_to_merkle_path`, bsv-rs 0.9.0's stated divergence (its #167 parity note: a BUMP read alone is the BEEF reader's BUMP decoder); the reference's `MerklePath.ts:60` tolerates it only as the working hash's own sibling, and Go's converter accepts it unchecked (the `success_duplicate_marker` vector, which the toolbox's test now pins as refused and runs at index 0). A host that feeds a BIP-39 phrase to bsv-rs: 0.9.0 reads a phrase and passphrase in NFKD (bsv-rs #163), so a French, Japanese, Korean or Spanish phrase with a character NFKD changes, or a passphrase NFKD changes, seeds different bytes from 0.9.0 on (BIP-39's); read bsv-rs's migration note before re-pinning. The toolbox's managers derive from a root key and feed no phrase. A host re-pins `bsv-rs` to `=0.9.0-beta.1` with this crate, as the two share bsv-rs's types.

- `Error::SpendRefused`'s text prints bsv-rs's `Display` for `SpendRefusal` (bsv-rs #213), where the toolbox rendered each arm in its own words: the script arm is unchanged (the interpreter's sentence; the fixture `internalize_refuses_a_failing_script` re-blessed byte for byte), and the other arms now read as bsv-rs writes them (for example `the transaction is not final for mining in the next block under the supplied headers: lock_time N`, where the toolbox said `not final for the next block (lock time N)`). A host matching an arm's old text matches the `why` field instead. Test: `a_spend_refusal_prints_bsv_rs_words_for_its_reason`.

- `StorageClient`'s authenticated send keeps bsv-rs's typed transport fault (#63, #140; bsv-rs #87): a connection never made, a timeout or cut on the way, or a handshake answered 429, 502, 503 or 504 is `Error::SdkError(bsv_rs::Error::TransportFault { fault, status, detail })`, and `From<Error> for bsv_rs::Error` hands it through, so a host reads `transport_fault()` (`NEVER_SENT`, `MAY_HAVE_BEEN_RECEIVED`, `RATE_LIMITED` with the `Retry-After` wait) instead of the text. Before, every send failure was `NetworkError("Failed to send request: ...")`, a `WalletError` text to the host. Any other send failure, and the response wait's own timeout, stay `NetworkError`. Tests: `a_refused_connection_is_bsv_rs_never_sent`, `a_rate_limited_handshake_carries_its_wait`.

### Fixed

- `internalize_action` refuses a basket insertion whose `customInstructions` is over 1000 UTF-8 bytes (#138): `Error::ValidationError("customInstructions length must be at most 1000 bytes, got N")`, raised before the BEEF is read or anything is written, so nothing is stored. Bytes (`str::len`), not characters. BRC-100 wallet/0100.md:867 caps the field at 1000 bytes; divergence 1 of the BRCs recon was ruled conform on 2026-10-24, and the reference's `validateBasketInsertion` bounds it the same way. `createAction` outputs are unchanged: the reference's `validateCreateActionOutput` sets no bound there. Tests: `custom_instructions_over_1000_bytes_is_refused_and_1000_is_stored_whole`, fixtures `internalize_custom_instructions_1001_refused`, `internalize_custom_instructions_1000_stored`.
- The change pick ranks no coin by `LENGTH(input_beef)` and ends every pick on `output_id` (#105, found by bsv-wallet-cli's differential harness, CLI #70): after covering the target, `completed` before `unproven`, and the nearest value, the lower `output_id` wins, as the reference's `allocateChangeInput` ends on `outputId`. A BEEF's length moves by a byte between runs with DER signature lengths, so two coins of equal value and status were spent in a different order on the same keys and chain. Test: `equal_coins_pick_the_lower_output_id_whichever_beef_is_longer`.
- Stored action posting sorts rebuilt BEEF in BRC-62 order before validation and broadcast (#144), including ancestors restored from storage when the saved input BEEF was trimmed, so bsv-rs 0.9.0-beta.1 accepts the assembly.
- **For hosts (#103):** `internalize_action` now needs the chain tracker's `mining_tip` (bsv-rs `ChainTracker::mining_tip`, default `None`): a tracker that answers no tip makes the door hold every unproven payment with `ServiceError` ("the header service supplied no mining tip"), nothing stored, never a verdict. The toolbox's Chaintracks client and `services::mock::MockChainTracker` supply one; a host's own tracker or test mock must, or wrap one that does (as `benches/common` does).
- `internalize_action` now requires bsv-rs's streaming spend verifier to accept the submitted AtomicBEEF before storing anything (#103, wallet.md:61 and :87). It checks structure, roots, scripts and mining context; a failing spend returns `SpendRefused`, invalid bytes return `InvalidBeef`, and unavailable headers or mining context return `ServiceError` with nothing stored or broadcast. The header adapter supplies the active tip and the median time past from one eleven-header range; network activation heights come from the service's network. Proof-root transport faults also return `ServiceError`, preserving the hold instead of declaring the payment invalid. This deliberately goes beyond the reference: `internalizeAction` calls `Beef.verify`, which checks structure and roots only. Filed upstream under the owner's name as bsv-blockchain/ts-stack#1380 (2026-10-26), with the vector under `conformance/upstream/103-internalize-script/`. The two-pass verifier retains referenced output scripts until spent; the storage door retains only the subject, its proof and element metadata beside the submitted bytes.

## [0.13.1] - 2026-10-25

A quick fix release on the stable line: the mock's spender setter for bsv-wallet-cli, the two #96 corrections to the 0.13.0 ladder, the #161 review-pass fix that voids a failed transaction's created outputs, and the mutation nightly's workflow file, all below. No other public shape moves.

### CI

- `.github/workflows/nightly-mutants.yml` lives on `main` so GitHub registers it (#153, #154); its schedule is off and it runs on manual dispatch only, after two dispatches showed sixteen shards running past the 330 minute job limit with no scores (#157); #159 reshapes the run. `docs/MUTANTS.md` records the measurement.

### Added

- `MockWalletServicesBuilder::is_utxo_with_spender_response(MockResponse<(UtxoVerdict, Option<BoundSpender>)>)` and `MockWalletServices::set_is_utxo_with_spender_response` (the same, on a built mock), for bsv-wallet-cli: the mock's `is_utxo_with_spender` answers the set response, so a host can test a bound spender against the mock (an error answers `Unknown` and no spender; a `Sequence` is consumed by this call's own count). Every call is recorded in `call_history` as `is_utxo_with_spender`. Unset, it answers as before, `is_utxo`'s verdict and `None` (the trait's default), with that `is_utxo` call recorded beside it; no existing caller's answer moves. Tests: `the_mock_answers_a_set_spender_and_records_the_call`, `the_mock_unset_answers_is_utxos_verdict_and_no_spender`.

### Fixed

- #161: `MonitorStorage::review_status` check 1 now voids the created outputs of the transactions it marks `failed` from an `invalid` request (`spendable = 0`, `spent_by` kept, `updated_at` advanced) in the same `write_tx()`; consumed inputs remain unchanged. Keeping `spent_by` lets check 4 still find and retire live descendants of newly failed roots in the same review pass. This implements `wallet.md:22,117`: a rejected creator's coins are `void`. At `ts-stack@4d0ea63c8`, `packages/wallet/wallet-toolbox/src/storage/methods/reviewStatus.ts:54-61` writes `failed` directly, bypassing `StorageProvider.ts:1090-1099,1164-1170`, but `reviewStatus.ts:93-116` separately voids terminal failed creators' outputs (tested in `test/storage/reviewStatus.test.ts:100-113`). The toolbox was behind that output cleanup; this transition also makes the two writes atomic. Tests: `review_status_voids_created_outputs_of_newly_failed_transactions` (created outputs, preserved spender links, consumed inputs, repeat pass and output-write rollback) and `review_status_newly_failed_root_retires_live_descendant` (descendant retirement and its log in one pass).
- `abort_action` now refuses every `unproven` transaction and any transaction with a stored accepting word (#165), using `Error::InvalidTransactionStatus`. The toolbox was behind the charter: `wallet.md:98`, `tracker.md:25`, and broadcaster B4 (d) and section 6 item 2 refuse abandonment after announcement. The abort reads the request, post history, broadcast memory and tracker word under its write lock, without a header-service or explorer call. The reference at `ts-stack@edf6e03`, `StorageProvider.ts:712-718`, lists `sending` and `unproven` as unabortable; the toolbox retains the finer charter case: a `sending` transaction no instance accepted is still `built` and remains abortable, including when every named instance refused. Descendants of an honoured `sending` abort still retire; their outside inputs wait for monitor rechecks.
- `retire_undeliverable_txid` refuses a transaction with a stored accepting word (#165; the charter's rule, wallet.md:98, tracker.md:25, broadcaster.md B4 (d), the keeper's word of 2026-10-25; the reference's `StorageProvider.ts:712-718` at edf6e03 refuses `sending` and `unproven` outright, the toolbox finer on a `sending` row no instance accepted), the guard `abort_action` uses, with `Error::InvalidTransactionStatus`: it retires only a transaction no instance accepted (a `sending` row whose every named instance refused). The guard is read before the status service is asked (an accepted transaction asks no service) and again inside the one write transaction (`BEGIN IMMEDIATE`) that now writes the retire's request, own outputs and `failed` status, so an acceptance committed during the alive check refuses too. The alive check stays before the lock; the inputs' re-check schedule, broadcast memory and descendants follow the commit. A competitor's checked proof still retires without the guard. Tests: `an_accepted_tx_is_refused_with_nothing_written_and_no_service_asked`, `an_acceptance_committed_during_the_alive_check_refuses_the_retire`.
- `abort_action` and `retire_undeliverable_txid` read the request word `unfail` as an accepting word (#165): the unfail task writes it when a merkle path exists for a failed request, so the transaction is on chain and is refused with `Error::InvalidTransactionStatus`, nothing written, no service asked. The keeper's word (`tracker.md:140`, `wallet.md:32`): the evidence is the proof's presence, not the label, so a stored proof refuses too when the request says `failed` (already read from `proven_txs`, now pinned). Tests: `an_unfail_request_refuses_the_abort_and_the_retire`, `a_failed_request_with_a_stored_proof_refuses_the_abort_and_the_retire`.
- `internalize_action` refuses a basket insertion whose `customInstructions` is over 1000 UTF-8 bytes (#138): `Error::ValidationError("customInstructions length must be at most 1000 bytes, got N")`, raised before the BEEF is read or anything is written, so nothing is stored. Bytes (`str::len`), not characters. BRC-100 wallet/0100.md:867 caps the field at 1000 bytes; divergence 1 of the BRCs recon was ruled conform on 2026-10-24, and the reference's `validateBasketInsertion` bounds it the same way. `createAction` outputs are unchanged: the reference's `validateCreateActionOutput` sets no bound there. Tests: `custom_instructions_over_1000_bytes_is_refused_and_1000_is_stored_whole`, fixtures `internalize_custom_instructions_1001_refused`, `internalize_custom_instructions_1000_stored`.
- `internalize_action` refuses a basket insertion whose `customInstructions` is over 1000 UTF-8 bytes (#138): `Error::ValidationError("Invalid outputs[i].insertionRemittance.customInstructions: must be no more than 1000 bytes, got N")`, raised before the BEEF is read or anything is written, so nothing is stored. Since #143 the door asks bsv-rs's `wallet::validation::validate_internalize_action_args` (bsv-rs #117, 0.9.0-beta.1), which holds the same bound in the same bytes, and wraps its refusal as `ValidationError` in bsv-rs's words, so the two texts cannot drift; the toolbox's own check is gone. Bytes (`str::len`), not characters. BRC-100 wallet/0100.md:867 caps the field at 1000 bytes; divergence 1 of the BRCs recon was ruled conform on 2026-10-24, and the reference's `validateBasketInsertion` bounds it the same way. `createAction` outputs are unchanged: the reference's `validateCreateActionOutput` sets no bound there. Tests: `custom_instructions_over_1000_bytes_is_refused_and_1000_is_stored_whole`, fixtures `internalize_custom_instructions_1001_refused`, `internalize_custom_instructions_1000_stored`.
- The change pick ranks no coin by `LENGTH(input_beef)` and ends every pick on `output_id` (#105, found by bsv-wallet-cli's differential harness, CLI #70): after covering the target, `completed` before `unproven`, and the nearest value, the lower `output_id` wins, as the reference's `allocateChangeInput` ends on `outputId`. A BEEF's length moves by a byte between runs with DER signature lengths, so two coins of equal value and status were spent in a different order on the same keys and chain. Test: `equal_coins_pick_the_lower_output_id_whichever_beef_is_longer`.
- `is_utxo` and `is_utxo_with_spender`: two corrections to the 0.13.0 ladder (#96, found by the cold review of bsv-wallet-cli's re-pin to 0.13.0; the stack keeper's word of 2026-10-23: corrections to the words already written, not a change to the verdict rule). (1) A spender named whose bytes cannot be read (every raw-transaction rung faults or lacks them) or whose bytes do not spend the outpoint is "could not look" for the whole question: the answer is `(Unknown, None)`, logged at `warn` as a spender named that could not be read, where 0.13.0 dropped the name and answered the unspent sets' verdict, so two "unspent" answers came back `(UnspentHint, None)` and a host released the coin. BananaBlocks is still asked after an unreadable WhatsOnChain name, as before, and a spender it binds by its bytes stands. (2) With no header service configured `spend_evidence` returned before any route, so no spender was named; the bytes binding needs no header, so it now runs, and only `confirmed` waits on the header service: a bound spender answers `(SpentHint, Some(BoundSpender { confirmed: false }))`, with no header service as with one that does not answer (that case already answered so, and is now pinned). With no header service a probe now asks `/spent` and the spender's bytes, the routes it asks with one; no proof is fetched without a header service, and no new route or explorer call is added. `is_utxo` is still `is_utxo_with_spender` with the spender dropped. Request counts, both explorers listing the outpoint: bytes faulting, five (two unspent sets, `/spent`, the bytes at WhatsOnChain and at Bitails); bytes of another spend, four; no header service, four and no proof asked; a header service answering 500, four, one proof and one header ask. Fixtures that pinned the old answers now pin the new ones: `a_name_its_bytes_refuse_answers_no_spender` and `a_proven_transaction_that_does_not_name_the_outpoint_is_not_a_spend` answer `Unknown`, `without_a_header_service_no_spend_is_a_fact` (was `..._and_no_spender_is_asked`) asks the spender, and `is_utxo_gives_the_three_answers`' rows where BananaBlocks names a spender no rung holds answer `Unknown`. Tests: `a_named_spender_whose_bytes_fault_answers_unknown`, `a_named_spender_whose_bytes_spend_another_outpoint_answers_unknown`, `without_a_header_service_the_bound_spender_is_named_unconfirmed`, `a_header_service_fault_names_the_bound_spender_unconfirmed`, `is_utxo_is_the_spender_verdict_in_the_scenes_of_96` (every route counted).

## [0.13.0] - 2026-10-23

### Added

- `WalletServices::is_utxo_with_spender(txid, vout, locking_script) -> (UtxoVerdict, Option<BoundSpender>)` and `services::BoundSpender { txid, confirmed }` (#44): `is_utxo`'s verdict with the spender its ladder bound beside it. The spender is the one `spend_evidence` already binds by its own bytes; `confirmed` is `true` when its merkle path met the header service's header (the verdict is `Spent`) and `false` when no proof is held (`SpentHint`); every other verdict, and a name its bytes do not bear out, answers `None`. Same pass, same requests, same verdict: `is_utxo` is this call with the spender dropped. A caller that wanted the spender's name (bsv-wallet-cli's `probe_input_spend`) asked WhatsOnChain's `/tx/{txid}/{vout}/spent` and BananaBlocks' `/txo/{txid}/{vout}`, then read the named spender's bytes, before `is_utxo` asked the same routes again: four to six explorer requests an outpoint. On this call it drops its own step and the probe costs one ladder pass (for a bound spender over the three explorers, four requests: the unspent set, `/txo`, `/spent`, the spender's bytes). Option (b) of the issue, a new method beside `is_utxo`, not a field on `UtxoVerdict::Spent`: the enum is `Copy` and bsv-wallet-cli 0.18.0 matches `Spent` and `SpentHint` as unit patterns (`cleanup_abandoned.rs`), so a field would break it. The trait method has a default body (`None` beside `is_utxo`'s verdict), so other implementors need not write one. No new route or request; the ladder's order, the two-explorer rule, a fault's move to the next rung and every verdict's meaning are unchanged. Tests: `a_proven_spend_answers_its_spender_confirmed`, `an_unproven_bound_spender_answers_its_spender_unconfirmed`, `an_unspent_answers_no_spender`, `a_name_its_bytes_refuse_answers_no_spender`, `the_spender_costs_no_request_beyond_is_utxo` (each route counted, the same four for both calls).

## [0.12.1] - 2026-10-23

### Changed

- The header ladders `get_chain_tip_header` and `get_header_for_height` (chaintracks, then BHS) report the rungs' errors (#47): when no rung gives a header the answer is `<call>: no header service answered (chaintracks: <error>; bhs: <error>)`, naming each configured rung that failed, as `get_height` beside them already kept its last rung's error. `<call>: no header service configured` is answered only when chaintracks and BHS are both unconfigured. Before, both ladders answered "no header service configured" whenever every rung failed, so a caller (the monitor's new-header task, a spend gate's "headers unavailable") read "not configured" for "could not look" and could not tell a timeout from a reset or a 5xx. Each rung's failure is logged at `warn` (it was `debug`); a rung that answers after a failed one logs nothing new. The ladders' order and how often a rung is asked are unchanged. Tests: `header_ladder_tests` (a failing chaintracks with no BHS is its error, nothing configured is "not configured", a failing chaintracks then an answering BHS is the header, both failing names both, an answering chaintracks asks no BHS; for both ladders).

## [0.12.0] - 2026-10-22

BananaBlocks as the third explorer in the services' ladders, and a fault moves to the next rung (bsv-wallet-cli#50). The owner's word of 2026-10-22: each chain scanner has its own rate limits, so when one fails we go to the next; every chain question that leaves our services asks WhatsOnChain, BananaBlocks and Bitails as each other's fallbacks; a negative ("unspent", "absent") needs a second provider's clean answer; "could not look" is never read as "nothing there". Bitails is pruned and retired its per-output spend data, so before this an unspent verdict rested on WhatsOnChain alone. A minor, 0.12.0: a new provider, new `ServicesOptions` methods and field, new `Error` variants (`RateLimited`, `NoExplorerAnswered`; `Error` is `#[non_exhaustive]`), and a new `WalletServices` method with a default body (`get_utxo_leads`).

### Added

- `services::providers::BananaBlocks` (GorillaPool's explorer; mainnet `https://bananablocks.com/api/v1`, testnet `https://test.bananablocks.com/api/v1`, no key), live-probed 2026-10-22 (the endpoints and shapes are in the PR): `get_raw_tx` (`/tx/{txid}/hex`, the bytes must hash to the txid), `get_txo` / `get_spend` (`/txo/{txid}/{vout}`: 200 with `script_pubkey`, whose big-endian sha256 is the output's script hash (a `script_hash` beside it must agree), `spent: false` a clean unspent, `spent: true` with a 64-hex `spent_txid` the spender; a 404, 429, 5xx, malformed body, no script, or a spender that is not 64 hex is a fault), `get_utxo_status` (an outpoint only, and only for the asked script: another script, the empty one included, is a fault), `get_merkle_path` (`/tx/{txid}` then `/tx/{txid}/proof`, a TSC proof whose target is the merkle root; its nodes must fold to that root, and it is handed on with the block hash so the ladder checks it against the header service like every courier's; the probe's proof for block 972169 folds to the root two Chaintracks deployments hold; a 404 at `/tx` is "not indexed", inconclusive, never "not mined"), `get_status_for_txids` (`/tx/{txid}` each).
- `services::providers::RateGate` (`pace.rs`): a 429 is `Error::RateLimited { provider, retry_after_secs }` returned at once, never slept on, and the rung is skipped without a request until its Retry-After passes (seconds or an HTTP date, held to 600 s). A gate is the process's, one per host and key (`RateGate::shared`): a process that builds several `Services` (the daemon builds two per tenant) spends one budget per host, and a 429 one met closes the rung for all. BananaBlocks also paces itself per process with a token bucket: a burst of 30 refilled at 0.5/s, so 60 in the first minute and 30 a minute sustained, under BananaBlocks' 80. WhatsOnChain, BananaBlocks and Bitails each pass one; WhatsOnChain and BananaBlocks expose theirs (`rate_gate()`), Bitails holds its gate privately.
- `Error::RateLimited { provider, retry_after_secs }`; the ladders record it in the call history with code `RATE_LIMITED`, apart from `ERROR`.
- `ServicesOptions::bananablocks_base` (`None` = the chain's default, empty = off), `with_bananablocks_base(url)`, `without_bananablocks()`, `bananablocks_base_for(chain)`. On by default for main and test. `Services::bananablocks` holds the provider.
- `WalletServices::get_utxo_leads(script_hash, output_format) -> Result<UtxoLeads>` (`UtxoLeads { leads: Vec<UtxoLead { txid, vout, satoshis: Option<u64>, listed_by }>, answered, faults, partial }`) and `Error::NoExplorerAnswered { faults }`: discovery, not a verdict. bsv-wallet-cli's `sync` lists a deposit address's outputs to receive the payments it finds, and verifies each by its bytes and its proof against the header service before anything is stored; on this release's rule (an unspent needs two explorers) its `get_utxo_status` call stopped with "could not look" whenever WhatsOnChain or Bitails faulted, and took the first explorer's list only (the CLI review's M1, an availability regression). A listed output is a LEAD: one explorer's list is enough to look, and the union of every explorer's list, keyed by outpoint with `listed_by` naming each explorer that listed it, is the fullest look. WhatsOnChain and Bitails are asked by script hash, BananaBlocks by address (`/address/{address}/utxos`, probed 2026-10-22; no script-hash route) when the script is P2PKH and handed as itself; each through its shared rate gate, a fault recorded and moved past at once (a transient one asked once more in the retry round, a 429 or a deterministic one not); `Err(NoExplorerAnswered)` ("Could not look: no explorer answered: …") only when none answered. A lead is not `UtxoVerdict::UnspentHint`; `get_utxo_status` and `is_utxo` are unchanged. The trait method has a default body (that error), so other implementors need not write one. Each explorer's list is read page by page, each page through its gate (probed 2026-10-22, read-only and paced: WhatsOnChain's `/unspent/all` answers 1000 a page and a `nextPageToken`, `?token=` the next; Bitails' `/unspent` 1000 a page by `limit`/`from`, saying nothing of more; BananaBlocks 1000 by `limit` and `cursor` while `hasMore`), at most `LEAD_PAGES_MAX` = 10 pages (10,000 outputs) an explorer; `UtxoLeads::partial` names each explorer whose list was cut short (the cap reached, or a later page that could not be read), its pages' leads kept: `answered` means "answered", `partial` "cut short". A list holding a txid that is not 64 hex characters is that explorer's malformed answer (a deterministic fault), so `zz` or `../x` never becomes a lead. BananaBlocks is asked only for the exact P2PKH template `76a914<20>88ac` (a non-minimal push of the hash is another script), and its items whose `script_type` is given and is not `p2pkh` are left out. `UnspentPages`, `WhatsOnChain::list_script_unspent`, `Bitails::list_script_unspent` (`BITAILS_LEAD_PAGE`) and `BananaBlocks::list_address_unspent` read the pages. `MockWalletServices` answers `get_utxo_leads` (`get_utxo_leads_response`, or the leads of `get_utxo_status_response`'s details). Tests: `the_leads_are_the_union_of_every_explorers_list`, `a_429_leaves_the_other_explorers_list_without_a_sleep`, `every_explorer_faulting_is_could_not_look`, `a_malformed_list_leaves_the_good_one`, `bananablocks_lists_a_p2pkh_script_by_its_address`, `a_script_with_no_address_is_not_asked_of_bananablocks`, `the_address_list_answers_only_in_its_documented_shape`, `a_bad_txid_is_that_explorers_malformed_answer`, `bananablocks_pages_by_its_cursor`, `bananablocks_at_the_page_cap_is_partial`, `a_full_bitails_page_is_read_on_or_partial`, `whatsonchain_pages_by_its_token`, `a_closed_gate_is_skipped_without_a_request`, `the_mock_answers_leads_as_configured_or_from_the_status_response`.
- `NOTE_INCONCLUSIVE` gains `getMerklePathNotIndexed` (BananaBlocks' 404 at `/tx/{txid}`): a reorg re-prove never demotes a proof on it.

### Changed

- The ladders: `get_raw_tx` WhatsOnChain -> BananaBlocks -> Bitails; `get_merkle_path` (Arcade ->) WhatsOnChain -> BananaBlocks -> Bitails; `get_utxo_status` WhatsOnChain, BananaBlocks, Bitails with the rotating start over the three, asked until two agree or none is left (the verdict does not depend on the start): an unspent only from two (`UTXO_POSITIVE_PROVIDERS = 2`, new), a not-listed only from two (`UTXO_NEGATIVE_PROVIDERS` stays 2), anything else `status: "error"` (could not look). The owner's rule as decided: a negative, "unspent" or "absent", needs a second provider's clean answer. Before, one explorer's unspent stood; the callers that act on it restore or release coins (`un_fail`, `poisoned_chain`, `locked_inputs`, bsv-wallet-cli's `probe_input_spend`), where a wrong hint is a phantom coin, and BananaBlocks' index missed a mempool spend WhatsOnChain showed (`docs/providers/bananablocks.md`). `is_utxo` asks for a spender even after two unspent answers: a spender bound by its own bytes stands on one; `get_status_for_txids` (Arcade ->) WhatsOnChain -> Bitails -> BananaBlocks, BananaBlocks not a chain index (its index lags the mempool: its `unknown` is a gap, its `known` a word). A transaction's absence stands only when both chain indexes (WhatsOnChain and Bitails) answer `unknown`: those two answers make it `unknown` even against a broadcaster's `known` (Arcade's `SEEN_MULTIPLE_NODES` for a phantom the chain never saw, the 2026-09-02 case, which the reconciler's alive check acts on; the respend gate's own broadcaster check refuses while Arcade still answers SEEN). One chain index's `unknown` never stands alone: not as a clean answer, and not against any provider's `known` (Arcade's or BananaBlocks', in either order); BananaBlocks' 404 is a gap, never an absence vote. Every other `unknown` is left out of the answer, whose `error` says `could not look: no chain index answered <txids>` (`status: "error"` when nothing is left); a txid an earlier provider left out (it stopped early) is asked of the next. The verdict does not depend on the provider order (review 2 H1/M1 of #43; `test_the_status_verdict_is_the_same_from_either_start` covers Arcade, WhatsOnChain and Bitails each `known`, `unknown` or a fault, from both starts; review 3 of #43, `test_the_status_verdict_is_the_rule_over_all_81_rows_from_both_starts` adds BananaBlocks' `known`, 404 or fault to every row, from each of the four starting rungs, with and without `use_next`).
- BananaBlocks answers an outpoint only: for a `get_utxo_status` question without an outpoint it is not applicable, neither asked nor counted as a negative or a fault.
- A fault on one rung moves the question to the next within the same call, before any retry round. `get_utxo_status`'s retry round (one, after a 2 s pause, the only ladder with one) re-asks a transient fault only: a 5xx, a 408, a timeout or a transport failure; never a deterministic one (a 404 that is an answer, a malformed body, a BananaBlocks script mismatch, the same bytes again) and never a rate-limited rung (review 2 L2 of #43; `FaultKind`, tests `a_fault_is_transient_only_for_an_outage`, `a_deterministic_fault_is_not_asked_again`).
- `spend_evidence` (behind `is_utxo`) asks WhatsOnChain's `/tx/{txid}/{vout}/spent` and then BananaBlocks' `/txo/{txid}/{vout}` `spent_txid`, each the other's fallback, each name bound by the spender's own bytes as before. When the utxo ladder already read BananaBlocks' `/txo` for the outpoint, its `spent_txid` is carried over and `/txo` is not asked again: an unspent verdict costs one BananaBlocks request when it answers; after a fault `spend_evidence` asks it once more (up to three with a 5xx retried in the ladder) (review 2 L3, review 3 L2 of #43; `an_unspent_verdict_costs_one_bananablocks_request`).
- WhatsOnChain no longer sleeps on a 429: a GET slept 2 s and retried twice, a broadcast slept 2 s up to five times; now the 429 is `Error::RateLimited` at once (a broadcast's `PostTxResultForTxid` with `service_error: true`, note `postRawTxRateLimited`), and the rung is skipped until Retry-After passes (1 s when none is sent). No test pinned the sleeping retry. `get_status_for_txids` keeps the chunks of 20 answered before a later chunk's fault, with `error` set (`stopped early: …`), and leaves the rest out.
- WhatsOnChain's and Bitails' unspent-set reads tell a body cut short (a timeout, a dropped connection: `Error::NetworkError`, `… read failed`, transient, asked again in the retry round) from a body that arrived malformed (`Failed to parse …`, deterministic); both were a parse error before, and lost their one retry (review 3 L1 of #43; `a_whatsonchain_unspent_body_cut_short_is_transient_a_malformed_one_deterministic`, `a_bitails_unspent_body_cut_short_is_transient_a_malformed_one_deterministic`).
- Bitails passes a rate gate too: its 429 is `Error::RateLimited` at once and the utxo ladder's retry round does not re-ask it, so no explorer's 429 is slept on.
- WhatsOnChain sends a configured API key as `woc-api-key` as well as `Authorization: Bearer` (the header the WhatsOnChain docs and the overlay's production reads use).
- `cargo clippy --all-features --all-targets -D warnings` is clean on clippy 1.98 (`storage/client`: two needless borrows, `sign_request`'s eight arguments allowed, an unused test helper removed, a `vec!` that is an array).

## [0.11.2] - 2026-10-22

- The publish of 0.11.1's content: the `v0.11.1` tag fired no Release run (the push event after a rate-limited hour created no workflow run, and the tag cannot be moved or deleted under the repository's rules), so this patch carries the same tree and publishes it; no code change (RELEASING.md step 8: tag the next patch).

## [0.11.1] - 2026-10-22

A double-spend loser's descendants stayed spendable (#42, from bsv-low #818 at 0.8.0 and #831 at 0.11.0; the reading is bsv-stack-lean `docs/p0/toolbox-void-lineage.md`, sections 3 to 6 and 9). Four of #42's five points are fixed here; the reading's five witnesses land as `src/storage/sqlx/void_lineage_tests.rs`, each fix's test red at 0.11.0 and green here.

### Fixed

- A competitor's checked proof outranks a status source's `known` (#42 point 1). `ask_queued_competitor_proofs`, on a named competitor whose bytes spend our input and whose proof meets our header, called `retire_undeliverable_tx`, which began with `promote_if_alive`: one source answering `known` promoted the loser to `unproven` and retired nothing, its change and its descendants' change kept funding actions. The competitor pass now retires on the proof and asks no status source; the host's retire by txid (`retire_undeliverable_txid`) keeps the alive check. Test: `a_checked_competitor_proof_outranks_a_known_word`.
- Arcade's `known` for the network words only (#42 point 2). `status_detail` answered `known` for `RECEIVED`, `SENT_TO_NETWORK` and `ACCEPTED_BY_NETWORK` (Arcade holds it, the network may not), and the send-waiting and immediate-post reconciliations promote on `known`: a transaction the network never held became `unproven` and its change funded the next action (#831). `known` is now `SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES` and `STUMP_PROCESSING`; the three holding words answer `unknown`, a hint that promotes nothing. `tests/vectors/arcade_status_verdicts.json` says `toolbox_status: unknown` on its three `status_*` rows. Tests: `arcade_received_alone_promotes_nothing`, `arcade_seen_on_network_promotes`.
- A failed ancestor is refused before broadcast (#42 point 4). The BEEF walk's lookup (`get_tx_with_proof`) read a transaction by txid with no status condition, so a child of a `failed` parent was assembled and posted (#831's refund); the explicit-input check read the output's `spendable` flag and not its creator. Both now refuse with `Error::FailedAncestor { txid }` (Display `Failed ancestor: <txid> ...`; `Error` is `#[non_exhaustive]`), before anything is written or posted; a stored proof still outranks a `failed` word. Send-waiting's rebuild does not fall back to the stored inputBEEF on that refusal (it carries the same lineage): nothing is posted (marker `failed_ancestor_not_posted`). Tests: the witness `a_checked_competitor_proof_retires_the_lineage` (its BEEF half), `create_action_refuses_an_input_whose_creator_is_failed`, `create_action_refuses_a_child_of_a_failed_parent`, `send_waiting_posts_nothing_over_a_failed_parent`.
- Nothing released on an explorer's word (#42 point 5). `retire_undeliverable_tx` released an input when `services.is_utxo` found it in an unspent set, so a coin a mined competitor spent, read unspent at a lagging explorer, went back to coin selection. The retire now asks no explorer: every input stays locked; an input the proven competitor's bytes spend is scheduled `spent-proven` (lost, terminal), every other input for the locked-input re-check; the descendants' outside inputs are kept and scheduled. `RetireOutcome::Retired`'s `restored` is 0 on this path. Tests: `the_hosts_retire_releases_nothing_on_an_explorers_unspent_word`, `a_coin_a_proven_competitor_spent_is_lost_whatever_an_explorer_says`; three integration tests stated the old rule and now state this one: `tests/monitor_tests.rs`'s `invalid_after_retries_keeps_an_explorers_unspent_input_locked` (was `invalid_after_retries_restores_only_verified_unspent_inputs`), `tests/retire_rejected_tests.rs`'s `absent_tx_is_failed_and_an_explorers_unspent_input_stays_locked` (was `absent_tx_with_a_verified_unspent_input_is_failed_and_the_input_released`) and `tests/poisoned_chain_tests.rs`'s `retire_undeliverable_walks_the_descendants_too` (now `restored: 0, kept: 2`).

### Open

- #42 point 3, the orphan re-ask's climb: an orphan word, a bare `REJECTED` or a 404 names no competitor, so the proof pass never asks for the mined spend of the parent's input and the lineage stays `sending` / `unproven` until the host's retire (#818). It waits on the tracker's climb statement, so the toolbox and bsv-tracker climb the same way; the witness `a_mined_competitor_no_courier_names_is_never_asked_for` states today's behaviour.
- The locked-input re-check (`recheck_locked_inputs`) still restores a kept input on an explorer's unspent hint, on its backoff; this release changes only what the retire itself releases.

## [0.11.0] - 2026-10-21

The store's write lock (bsv-wallet-cli #44). A served `createAction` answered `database is locked (code 517)`, `SQLITE_BUSY_SNAPSHOT`: its write transaction opened with a deferred `BEGIN`, read under a WAL snapshot, and wrote after another pooled connection (the monitor's proof task) had committed; `busy_timeout` never applies to 517. The fault is now impossible by construction, not retried. A minor, 0.11.0: `pool()` no longer writes, `Error` is `#[non_exhaustive]` and gains two variants (`StoreBusy`, `UnspendableCoin`) and a type (`StoreBusyReason`), and bsv-rs moves to a new line (0.8.0).

### Changed

- The minor's storage-trait break, for an implementor outside this crate: `WalletStorageWriter::insert_certificate_with_fields(auth, certificate, fields) -> Result<i64>` is a REQUIRED method (no default; see Fixed), and `get_settings`, `storage_identity_key` and `storage_name` change their return types (below). An implementor outside this crate must add the one and change the three; a caller of the three compiles unchanged unless it held the reference across statements (review 5 H1 of bsv-wallet-cli #44).
- Trait change: `WalletStorageReader::get_settings(&self) -> TableSettings` (was `&TableSettings`), `WalletStorageProvider::storage_identity_key(&self) -> String` and `storage_name(&self) -> String` (were `&str`): owned copies taken under the lock, in every implementor (`StorageSqlx`, `WalletStorageManager`, `StorageClient`); the crate has no `unsafe` left (review 4 G7 of bsv-wallet-cli #44). The three lent references out of their `RwLock`s through `unsafe` pointer casts ("effectively immutable after `make_available()`"), and with one store shared by several handles (`impl Clone for StorageSqlx`) a second handle's `make_available` or `migrate` replaced the value another handle was reading: a use-after-free. The fix5 lane's mitigation (replace only when the value differs) is gone with the unsafe: a write replaces, a reader holds its own copy. A caller that used the value in an expression (`storage.get_settings().chain`) compiles unchanged; one that held the reference across statements now holds the copy. Test: `reads_through_one_handle_race_make_available_through_another` (four threads read the settings, identity and name through one handle while the other replaces them 300 times with changing values; every value read is one written, whole).
- bsv-rs 0.8.0 by caret (`bsv-rs = "0.8.0"`, the same features; the lock carries one bsv-rs, 0.8.0). A new line, so this release is a minor. bsv-rs's `Error` is `#[non_exhaustive]` and gains `Error::WalletBusy { detail, retry_after_ms }`; this crate has no exhaustive match on it.
- What a host sees at the `WalletInterface` boundary (the typed boundary): a `StoreBusy` is `bsv_rs::Error::WalletBusy { detail: "Store busy: <the reason's words>", retry_after_ms: None }` (the store does not know the host's retry policy; the host sets its own `Retry-After`), at every door, `createAction`, `signAction`, `abortAction` and `internalizeAction` included, only when the action writes nothing, builds nothing, locks nothing and posts nothing (the pre-check's evidence records, a stored proof's root-check record, a proof the tracker refutes demoted, the default basket, may be written before some busy answers: they are idempotent and repeating is safe): the store refused before acting, and the same call may be repeated unchanged (see the one-transaction door below). Never after a committed write, never after a broadcast: a store fault there is logged with a marker and the call answers its success (the post-broadcast rule below; `abortAction` on a broadcast transaction, whose descendants are retired after the root's abort commits, below). The text of every other error is unchanged (`WalletError` with the toolbox Display, as before), except an `SdkError`, which is now its bsv-rs error itself rather than `WalletError("SDK error: ...")`. `signAction` on a busy store no longer aborts the action: the pending transaction stays cached for the repeat (its own one transaction wrote nothing; the template from the earlier `createAction` stays, as it should). Any other `processAction` error aborts as before (review 2 F5).
- `createAction` with `signAndProcess` is ONE write transaction (the captain's finding on 516195e, whose F5 answered `WalletBusy` for a busy in `createAction`'s `processAction` while the create step's rows were committed and its coins locked: a false promise, a consumer that repeats would create a second action over locked coins). The create step's rows and allocation, the transaction build, the signing (local CPU, nothing awaited) and `processAction`'s writes (the signed raw tx, the status, the `proven_tx_reqs` row) are made inside the one `BEGIN IMMEDIATE` and committed once; then the broadcast. A busy can arise only at the writer's acquire, at `BEGIN` or at the commit, and any refusal rolls the whole step back: nothing is ever left half done, so `WalletBusy` is honest there too. The plumbing: `WalletStorageWriter::create_action_at_door(auth, args, CreateActionDoor { offer, sign_and_process })` returns `CreateActionDone { created, processed }` or a `DoorRefusal` naming the step (`Create`, `Sign`, `Process`) so the wallet keeps its texts (`Signing failed: ... Transaction aborted and UTXOs released.`, `Process action failed: ...`); `SignAndProcess` carries the wallet's signer (`DoorSigner`, a sync closure over the template) and `processAction`'s flags; `StorageSqlx` runs the signer and `process_action_writes` (processAction's writes, split from `process_action_internal`, on the open transaction's connection) inside the create step. `process_action` keeps its shape (its own one transaction) for `signAction` and every other caller. `WalletStorageManager` delegates to the active storage's door; the trait's default (`StorageClient`) keeps separate steps, aborts on a sign or process refusal and answers a busy there as `Storage error: Store busy: ...` (the abort wrote: never `WalletBusy`). Deferred signing (`signAndProcess: false`, or a caller input with no unlocking script) keeps the create step as its own one transaction (the template is the step). The success response is built exactly as at c443eff: the same fields from the same values; only the transaction boundary moved.
- After the broadcast a store fault is never the call's error: the transaction is on the network and the answer says so. A status write after a post that fails, busy or not, is logged at warn with the marker `post_broadcast_status_write_failed` (the txid, the error) and the call returns its success result with the txid: `update_transaction_status_after_broadcast` in `createAction` and `signAction`, and `internalizeAction`'s writes after its post (the hint record, the request's advance to `unmined`; they failed the call before). The row stays `sending` with its request `unsent`, and the monitor's send-waiting task (`SendWaitingTask`, `send_waiting_transactions`) re-posts it and writes `unproven` / `unmined`. `internalizeAction`'s site is tested by `a_busy_status_write_after_an_internalize_broadcast_is_its_success_and_send_waiting_heals_it` (a busy at the req's advance after the post: the success, the marker, the req `unsent`, then `unmined` after the send-waiting pass; review 3 F3).
- `abortAction` on a broadcast transaction (`unproven` / `sending`) commits the root's abort (failed, inputs released, outputs dead, req `invalid`), then retires its descendants (`retire_poisoned_descendants`, one writer acquire per statement). A busy there was the call's error, `WalletBusy` after a committed write: the host repeated, the repeat refused (`failed` cannot be aborted) and the descendants stayed half retired (review 3 F1). Now an error of that retirement is logged at warn with the marker `post_commit_retire_failed` (the txid, the error) and the call answers `{ aborted: true }`: the abort is committed. The monitor's review-status pass (`ReviewStatusTask`, `MonitorStorage::review_status`) gains a check that finishes it: a failed transaction whose req is `invalid` or `doubleSpend`, with no chain evidence, spent by a transaction still `unproven`, `sending` or `nosend`, has its descendants retired (the abort's own retirement: outside inputs released only on verification, a proven descendant refuses). `retire_one_poisoned` writes the req before the status, so a retirement interrupted between the two is finished by the pass's first check then the new one. Tests: `a_busy_retirement_after_a_broadcast_abort_is_its_success_and_review_status_finishes_it` (a busy injected at the retirement after the root's commit: the success, the marker, the root `failed` / `invalid`, the descendant `unproven` until the pass, `failed` / `invalid` and its change dead after it, a second pass a no-op), `review_status_leaves_the_descendants_of_a_failed_root_with_chain_evidence`. The other doors audited for a write after a commit inside one call: `internalizeAction` (every write after its post is logged, `post_broadcast_status_write_failed`), `relinquishOutput` and `relinquishCertificate` (one statement each: a busy is before their only write), `processAction` via `signAction` (its own one transaction; after it only the post, the status write under the post-broadcast rule, and the abort of a broadcast it could not build, answered `WalletError`, never busy) and via `createAction`'s door (one transaction; after the commit the same), `StorageClient`'s default door (a busy after its create step is `Storage error:`, never busy).
- A pre-check short only because other `createAction` calls of this process hold the coins (their in-flight reservations) answered `Insufficient funds: need 1005, have 0` on a funded wallet (review 3 F2; the reviewer's probe: eight coins held, refused; released, funded). An uncovered dry run now plans once more with a private reservation, ignoring the other calls'; covered so, the refusal is `StoreBusy { reason: InFlight }` (`Store busy: coins held by calls in flight`, `WalletBusy` at the boundary), at once, with no wait: a call in flight that commits spends its coin, so after it the true answer may be a shortfall, which the host's repeat reads. Uncovered so, it stays `InsufficientFunds` (or `UnspendableCoin`). Tests: `nine_concurrent_creates_on_eight_coins_are_done_or_busy_never_insufficient_funds`, `coins_held_by_calls_in_flight_are_busy_then_done_once_released`, `a_true_shortfall_with_a_call_in_flight_stays_insufficient_funds`; `a_coin_outside_the_offer_is_never_allocated_under_concurrency` accepts the fifth call's `InFlight` as well as `InsufficientFunds`.
- A non-busy failure of the commit of `createAction`'s one-transaction door (with `signAndProcess`) reads `Database error: ...` where 0.10.0 read `Process action failed: ...` (processAction committed its own transaction then; now the door's one commit carries processAction's writes, and its failure is the commit's error). A host maps both to 500 (review 3 F5).
- The BEEF a `createAction` assembles ends at a stored proof; its raw segment (transactions without a proof, carried whole) is the wallet's own unmined chain. The host's spend guard offers unmined own change by default (bsv-wallet-cli `SPEND_OWN_CHANGE=1`), so an offered coin's raw ancestry runs back through the wallet's own unmined transactions to the first stored proof, and its length is bounded only by that chain's length: there is no size gate, on purpose (the stack's rule: fix the cause, do not gate on size). The bound on the chain is the monitor's to give (proofs fetched as blocks come); a measured guarantee is the follow-up (review 3 F6).
- Only a timeout acquiring the WRITER is `StoreBusy { reason: WriterNotAcquired }`: the conversion from `sqlx::Error` cannot tell the pools apart, so it no longer maps `PoolTimedOut`, and the writer's acquire sites do: `begin_immediate` (every `write_tx`) and `writer_conn()` (every single-statement write). A reader-pool `PoolTimedOut` is the plain `SqlxError`; `sqlite_busy_code` has no generic arm. A writer timeout in a schema migration is `StoreBusy` (it was a `MigrationError`). Tests: `a_reader_pool_timeout_is_not_store_busy`, `the_writer_not_acquired_for_one_statement_is_store_busy`; `the_writer_not_acquired_within_the_ceiling_is_store_busy` kept.
- `StorageSqlx::new` opens two pools over the one file, both with the store's pragmas (`foreign_keys`, WAL, `busy_timeout` 5000, `synchronous` NORMAL): a writer of exactly one connection and a reader of ten, every reader connection `PRAGMA query_only = ON`. `pool()` returns the reader: a write sent there fails loudly (`attempt to write a readonly database`), and the existing suite is how every misrouted write was found.
- Every transaction opens `BEGIN IMMEDIATE` on the writer through `write_tx()`: `create_action`, `process_action` (2), `internalize_action`, `abort_action` (2), `ingest_merkle_proof`, `demote_stale_proof`, the broadcast memory (3), the read check's demotion, the proof fetched during BEEF assembly, and the proof-attempt counter (a read then a write on a bare pooled connection before). A contender in this process waits for the writer connection; one in another process waits `busy_timeout` on the lock. `clippy.toml` disallows `Pool::begin`, `Pool::begin_with`, `Connection::begin` and `Acquire::begin`; the one `#[allow]` is on the function `write_tx` calls.
- Every single-statement write runs on the writer, through `writer_conn()` (its acquire is where a writer timeout becomes `StoreBusy`). The schema migrations too, one writer acquire per migration.
- A begin or a commit that fails for anything but busy keeps the `Database error: ...` prefix these sites answered before #44 (a host's 500): `begin_immediate` and `commit()` (every `write_tx` commit) map a writer timeout to `StoreBusy { WriterNotAcquired }`, SQLite busy to `StoreBusy { SqliteBusy { code } }`, anything else to `DatabaseError(e.to_string())`. Between the two, `abort_action`, `create_action`, `internalize_action` and `process_action` answered `SQLx error: ...` (a host's 400) for it (review 2 F4). Tests: `a_non_busy_begin_failure_is_a_database_error`, `a_begin_or_commit_error_is_store_busy_only_when_busy`.
- The rejected-BEEF dump (`describe_invalid_beef`) and the wallet's pre-broadcast BEEF dump are written under `std::env::temp_dir()` (`$TMPDIR`), not a literal `/tmp`.
- No write lock is held across a service or a chain-tracker await, and `createAction`'s door is one step: ok, or refused, and the action writes nothing (the captain's ruling of 2026-10-21, option B, under the stack's wallet machine `Wallet.lean` 105-109, 339-350, 568-577). Before the lock, on a reader: the roots of the caller's `inputBEEF` are read against the tracker (the reference's `validateRequiredInputs` does so before any allocation); the change coins are picked exactly as the allocation would pick them, marking nothing; and the BEEF those coins and the caller's inputs need is assembled once, every stored proof's root read against the tracker (the charter's stillConfirmed rule) and every answer kept. Then one `BEGIN IMMEDIATE` transaction writes the action, allocates only from the pre-checked coins, assembles the BEEF from stored bytes with the tracker's answers in hand (no service, no tracker awaited under the lock), stores it and commits. A coin taken meanwhile, so the pre-checked set no longer covers the action, or a root the pre-check did not read (a proof stored meanwhile), rolls the transaction back, the action unwritten, and the pre-check runs once more; the second time a coin taken refuses `StoreBusy { reason: MovedUnderUs }` (`Store busy: the store moved between the pre-check and the write lock twice`): the latest pre-check covered the action, so the refusal is transient, never `InsufficientFunds`. A root left unread the second time is not the store moving (610225e answered `MovedUnderUs` for it): the header service was not read for it, and the call refuses with the service-class error naming the height (`Service error: the header service was not read for the merkle root <root> at height <h> before the write lock, twice; nothing written`, a host's 502), never `StoreBusy`. `Insufficient funds:` is answered only when a pre-check itself does not cover the action. The coins a pre-check picks are reserved in this process until its call returns (`InFlightPicks` on `StorageSqlx`, a `Mutex<HashSet<output_id>>`; a `PickReservation` guard releases them on every return, every `?` and a panic), and `ChangePick::DryRun` picks no reserved coin, so concurrent calls in one process never pick the same coin; the bounded re-check stays for another process's writes (review 2 F1). What the action writes is nothing; the pre-check's evidence steps may write before a refusal, as they did before #44: a stored proof's root-check record, a demotion of a proof the tracker refutes, and the default basket. 02727dd's commit-then-undo (`undo_unassembled_action`, which left the label, tag and basket rows it had inserted) is gone: there is no undo and no half state. The success response is byte-identical to 0.10.0's for the same store state.
- `createAction` asks no provider: `services.get_merkle_path` and `services.get_raw_tx` are never called from it (`build_input_beef` takes no storage; the host's spend guard offers only coins whose ancestry is stored, and the monitor's backstop fetches proofs). An ancestor of the caller's inputs whose bytes are not stored, or a chain that reaches no stored proof, is a refusal naming the txid (`ancestor(s) not stored, and createAction fetches none: [...]`). A change coin whose BEEF cannot be built from what the wallet stores (a reorg-demoted received coin, `unproven`, its parents never stored) is skipped by the pre-check and the coins picked again, bounded by the number of coins (review 2 F2: before, it refused the whole action as `Validation error: inputBEEF: ...`, a host's 400, though the caller passed no `inputBEEF` and other proven coins covered it); when the coins left do not cover the action, the refusal is `Error::UnspendableCoin` (see Added). Two paths still fetch: the broadcast rebuild (`rebuild_beef_for_broadcast`) and `listOutputs` with whole transactions (`include: entire transactions`, its BEEF walk passes the storage); its fetch tests are retargeted to it (`test_the_broadcast_rebuild_has_a_network_fallback`, `test_the_broadcast_rebuild_skips_a_proof_lookup_for_a_young_transaction`, `test_the_broadcast_rebuild_fetches_a_missing_proof_and_stores_it_through_the_writer`).
- The BEEF walk (`beef_bfs_walk`, `compact_stored_beef`, `rebuild_beef_for_broadcast`, `check_unchecked_stored_proof_on`) reads on the caller's connection and writes through a `writer` it is passed, each write after the network answered; with no writer (`None`: `createAction` under its lock) a read check decides and writes nothing.

### Fixed

- The words for `StoreBusy` (review 4 G5): "wrote nothing" was absolute, while the pre-check's idempotent evidence records (a stored proof's root-check record, a demotion of a proof the tracker refutes, the proof cache) may be written before some busy answers (`createAction` refused after its pre-check; `listOutputs`). It reads now: the action writes nothing; the pre-check's evidence records are idempotent and repeating is safe. `Error::StoreBusy`'s and the `From` impl's docs say the same (the latter no longer describes the pre-door `createAction` that kept its action on a busy `processAction`).
- The req-before-status order of `retire_one_poisoned` (review 3's reorder) had no test; the reviewer's mutation m4 (the order reverted) survived (review 4 G4). Test: `a_retirement_cut_between_its_two_writes_is_finished_by_the_next_pass` (a test hook cuts the retirement after the req's write: the descendant `unproven` with its req `invalid`; the next pass fails it and its change is dead; the pass after says nothing). Reverted, the cut leaves a `failed` row with its req `unmined`, which no check revisits, and the test fails.
- A completed descendant made the descendants check count its root and log `Retired the unproven descendants of 1 failed transactions` (and the retirement's refusal at error, `a descendant is PROVEN`) on every pass, forever (review 4 G3, probe P2). The root is skipped, uncounted, when a descendant is completed: the pass is quiet. Test: `review_status_with_a_completed_descendant_is_quiet_every_pass`.
- The review-status pass's descendants check (`MonitorStorage::review_status`, check 4) read chain evidence for the root only, so a spender a broadcaster reports mined was retired, `failed` / `invalid` with its change dead, until the unfail canary recovered it (review 4 G2 of bsv-wallet-cli #44, the reviewer's probe P1). Before retiring, the pass reads `poisoned_descendants(root)` and leaves the root when any descendant has chain evidence (`chain_evidence_for`: a proof, a `mined` row, the chain's `seen`). Test: `review_status_leaves_the_descendants_when_a_descendant_has_chain_evidence`.
- `acquireCertificate` answered `WalletBusy` after a committed write (review 4 G1 of bsv-wallet-cli #44): the direct path and the issuance path inserted the certificate row, then each field as its own write, so a busy at a field left the certificate committed with no fields, and the repeat the host made on `WalletBusy` refused `UNIQUE constraint failed: certificates` (relinquishing only soft-deletes: the certificate could never be acquired again). 0.10.0 answered `WalletError` there; the `From` mapping made it `WalletBusy`. The certificate and its fields are now one write transaction, `WalletStorageWriter::insert_certificate_with_fields(auth, certificate, fields) -> Result<i64>` (the reference's `insertCertificateAuth` with a `TableCertificateX`: `StorageSqlx` one `write_tx`, `WalletStorageManager` delegates to the active store, `StorageClient` sends the certificate with its `fields` in one `insertCertificateAuth` call); `insert_certificate` and `insert_certificate_field` stay for other callers (the wallet calls neither now). The direct path: a busy writes nothing and `WalletBusy` is honest; the same call repeated stores it. The issuance path writes after the exchange with the certifier, a post to a third party that has issued: a store fault there is never `WalletBusy` (a host would repeat the exchange); it is `Storage error: Store busy: ...` (`WalletError` at the boundary, a host's 500) with nothing stored, a failure the caller must see (`not_busy`, now `pub(crate)`). Tests: `a_busy_certificate_field_insert_writes_nothing_and_is_wallet_busy`, `a_busy_store_after_the_certifier_exchange_is_never_wallet_busy`.
- The issuance path lost the certificate the certifier had issued when the store was busy, and its error did not say one was issued (review 5 H2 of bsv-wallet-cli #44). On `StoreBusy` only (the one transaction rolled back: nothing written, a repeat is safe) the insert is tried again, 3 tries in all (`CERTIFICATE_STORE_TRIES`), sleeping 250 ms then 500 ms between tries (`CERTIFICATE_STORE_BACKOFF_MS`, doubled: 750 ms of sleeps). Each try can also wait up to the writer's 5 s acquire ceiling (`STORE_BUSY_CEILING_MS`), so a call that stays busy waits about 16 s in one process (3 x 5 s + 0.75 s) and about 31 s across processes (another process's lock adds up to 5 s per try). Still busy, the error is `Storage error: Store busy: ... (the certifier issued a certificate that was not stored: serial number <base64>, certifier <hex>, type <base64>; tried 3 times)`, so an operator can recover it from the certifier; any other store error is unchanged and not retried. Tests: `a_busy_store_that_clears_on_the_second_try_stores_the_issued_certificate` (stored, success), `a_busy_store_after_the_certifier_exchange_is_never_wallet_busy` (a busy that never clears: exactly 3 tries, the storage error names the serial and the certifier, never `WalletBusy`, nothing stored).
- `create_action` with two outputs naming one new basket (review F6, predating #44): `find_or_insert_output_basket` read on the reader inside the write transaction, so it could not see its own insert; a file store failed `UNIQUE constraint failed`, an in-memory store hung. It reads on the transaction's connection now (and no longer takes the storage). Tests: `two_outputs_to_one_new_basket_on_a_file_store`, `two_outputs_to_one_new_basket_in_memory`.
- `StorageSqlx::allocate_change_input` (public; review F7, predating #44) read a candidate on the reader and marked it on the writer with no transaction and no guard, so two callers could allocate the same coin. The claim is now the `UPDATE` itself (`AND spendable = 1 AND spent_by IS NULL`, `rows_affected` checked); a coin taken meanwhile is skipped and the next candidate read. Test: `racing_callers_never_allocate_the_same_change_coin` (sixteen callers, three coins; red without the guard).

### Added

- One store per file per process: `impl Clone for StorageSqlx` (the CLI review of 2026-10-21). The host daemon (bsv-wallet-cli `daemon_tenant.rs`) opened two `StorageSqlx` over the one wallet file in one process, one for the monitor and one for the served `Wallet` (`Wallet::new` takes its storage owned): two writer pools, so the monitor's and the handlers' writers met in SQLite and waited on its busy handler (`BEGIN IMMEDIATE` up to `busy_timeout`, then `StoreBusy { SqliteBusy }`) instead of queueing on one fair in-process writer, and two in-flight reservation sets. A clone is a second handle over the SAME store: the writer pool (one connection) and the reader pool (`sqlx::Pool` is an `Arc` inside), and every other field behind one `Arc`, so a change through one handle is seen through every other: the cached settings, the storage identity key and name, the chain tracker, the services, the broadcast memory, the in-flight pick reservations (`InFlightPicks`), the trx tokens (`active_transactions`), the task locks and the tests' busy hooks. No field is copied. A host gives its `Wallet` and its `Monitor` (or two `Wallet`s) clones of one store; a host that opens the file twice gets two writers queueing on SQLite's busy handler. The cached settings, identity key and name are shared by every handle and read as owned copies (see `get_settings` under Changed); `TableSettings` derives `PartialEq`. Tests: `two_handles_of_one_store_queue_on_one_writer_and_both_commit`, `a_store_opened_twice_has_two_writers_and_a_shared_store_one` (the daemon's shape counted: opened twice, two writer connections and the second writer's `BEGIN IMMEDIATE` answers `SqliteBusy` after `busy_timeout`; shared, one writer and the second handle queues for it), `a_reservation_through_one_handle_is_seen_by_the_others_dry_run`, `the_tracker_services_and_settings_set_on_one_handle_are_seen_by_the_other`, `a_wallet_and_a_monitor_over_one_store_share_the_writer` (a `Wallet` and a `Monitor` over two handles: one writer; 300 rounds of `Monitor::run_once` beside `createAction`, every one done, never busy).
- `StorageSqlx::writer(&self) -> &SqlitePool` and `StorageSqlx::write_tx(&self) -> Result<sqlx::Transaction<'static, Sqlite>>`; `storage::sqlx::STORE_BUSY_CEILING_MS` (5000).
- `Error::StoreBusy { reason: StoreBusyReason }`, Display `Store busy: ` then the reason's words: a transient refusal; the action writes nothing; the pre-check's evidence records are idempotent and repeating is safe (review 4 G5). `StoreBusyReason` (`#[non_exhaustive]`, re-exported): `SqliteBusy { code }` (`sqlite code N`): every `sqlx::Error` whose SQLite primary code is 5 (`SQLITE_BUSY`, 517 included via the primary code), with the extended code; `WriterNotAcquired` (`the writer was not acquired within the store's ceiling`): the writer not acquired in time, at the acquire site only (see Changed); `MovedUnderUs` (`the store moved between the pre-check and the write lock twice`): `createAction`'s bounded re-check ended uncovered, a coin taken twice (see Changed; a root unread twice is a `ServiceError`); `InFlight` (`coins held by calls in flight`): `createAction`'s pre-check covered only without other in-flight calls' reservations (see Changed). For the ceiling, the writer pool's `acquire_timeout` is set on purpose to the `busy_timeout`, 5 s (`STORE_BUSY_CEILING_MS`), the store's ceiling on waiting for the write lock in this process or another. Code 6 (`SQLITE_LOCKED`, a shared-cache or same-connection conflict) is a deterministic bug, never transient, and stays `SqlxError`, as does every other code. Consumers classify on the prefix at the SDK boundary.
- `Error::UnspendableCoin { outpoint, missing }`, Display `Unspendable coin: <txid.vout>: its stored ancestry is incomplete, ancestor(s) not stored: [...]; createAction fetches none, and the wallet's other coins do not cover the action`: the coins offered covered the action only with a coin whose BEEF cannot be built. Its own prefix because it is neither a shortfall (`Insufficient funds:`, a host's 402) nor the caller's bad input (`Validation error:`, 400): the wallet's own state refuses a well-formed request, which a host maps apart (409). Tests: `a_precheck_skips_a_coin_whose_beef_cannot_be_built`, `an_unspendable_coin_refusal_names_the_coin_and_its_missing_ancestor`; `a_beef_failure_refuses_with_nothing_written` reads it.
- `Error` is `#[non_exhaustive]` (bsv-stack RULINGS-2026-10-09 section 2): a `match` on it outside this crate needs a wildcard arm. This change and `StoreBusy` are the minor's error-type break.
- `impl From<bsv_wallet_toolbox_rs::Error> for bsv_rs::Error`: `StoreBusy` is `WalletBusy { detail: its Display, retry_after_ms: None }`, `SdkError(e)` is `e`, every other variant `WalletError(e.to_string())`. The wallet's 54 erasure sites (`.map_err(|e| bsv_rs::Error::WalletError(e.to_string()))`) are now `?` through it. Tests (`src/wallet/busy_door_tests.rs`): `create_action_on_a_busy_store_is_wallet_busy`, `sign_action_on_a_busy_store_is_wallet_busy_and_keeps_the_action`, `create_action_busy_in_the_process_step_writes_nothing` (it replaces F5's `create_action_busy_in_process_action_keeps_the_action_for_a_sign_action`), `sign_action_busy_in_process_action_keeps_the_action_and_its_coins`, `sign_action_on_another_process_action_error_still_aborts`, `abort_action_on_a_busy_store_is_wallet_busy`, `internalize_action_on_a_busy_store_is_wallet_busy`, `a_store_error_that_is_not_busy_keeps_its_wallet_error_text`.
- The host's offer: `Wallet::create_action_offering(args, originator, offer: &[i64]) -> bsv_rs::Result<CreateActionResult>` (an inherent method; the `WalletInterface` `create_action` is unchanged and offers every coin) and its storage-level equivalent `StorageSqlx::create_action_offering(auth, args, offer) -> Result<StorageCreateActionResult>` (and `CreateActionDoor::offer`). The offer is the output ids of the change coins the host offers: the pre-check's dry-run pick (`ChangePick::DryRun { offer }`) and the locked set (`ChangePick::Within`, the pre-checked coins intersected with the offer) take from it only, so a coin outside it is never allocated, under concurrency too; an offer that does not cover the action, or an empty one, refuses `Insufficient funds:`. Why: bsv-wallet-cli held the coins it does not offer by flipping `outputs.change` to 0 before the call and restoring it after, a write after the broadcast (LOW's finding, bsv-low #828); with the offer the host never writes around the door. The trait's default refuses an offer it cannot enforce (`Invalid operation:`). Tests: `a_coin_outside_the_offer_is_never_allocated_under_concurrency` (eight coins, an offer of four, five concurrent calls: four funded, the fifth `InsufficientFunds`, the allocated set exactly the offer), `the_precheck_picks_within_the_offer`, `an_offer_that_does_not_cover_refuses_insufficient_funds`, `an_empty_offer_refuses_insufficient_funds`, `create_action_offering_spends_only_the_offer`.
- The door's types (`storage::traits`, re-exported): `CreateActionDoor`, `SignAndProcess`, `DoorSigner`, `CreateActionDone`, `DoorProcessed`, `DoorRefusal` (with `DoorRefusal::into_error(self) -> Error`, the refusal's error whichever step refused) and `WalletStorageWriter::create_action_at_door` (a default method: no implementor must change). Tests: `create_action_busy_in_the_process_step_writes_nothing` (a busy injected at the process step's write within `createAction`: `WalletBusy`, every row count, coin lock and `sqlite_sequence` counter unchanged, nothing cached, the same call repeated succeeds), `a_busy_status_write_after_the_broadcast_is_logged_and_healed_by_send_waiting` (the success with the txid, the marker, then the send-waiting pass sets `unproven` / `unmined`), `a_busy_status_write_after_a_sign_action_broadcast_is_its_success`, `a_root_unread_twice_refuses_as_a_service_error_naming_its_height`.
- `SqlxBroadcastMemory::with_pools(reader, writer)`; `SqlxBroadcastMemory::new(pool)` keeps one pool for both.
- Tests: `tests/store_write_lock_tests.rs` reproduces 517 on two raw connections over one file (`a_deferred_begin_that_read_first_answers_517_after_another_commit`), shows the same shape through `write_tx` waits and both commit across two stores and within one (`the_same_shape_through_write_tx_waits_and_both_commit_across_stores`, `..._in_one_store`), pins `query_only` and the pragmas (`a_write_on_pool_fails_under_query_only`, `both_pools_keep_the_pragmas`, `the_store_busy_display_prefix`), and pins `StoreBusy`'s scope (`a_unique_violation_is_not_store_busy`: 2067 converts to `SqlxError`; `the_writer_not_acquired_within_the_ceiling_is_store_busy`). In `create_action`: `the_tracker_is_never_asked_while_the_writer_is_held` (with `control_the_probe_sees_a_held_writer`), `a_caller_root_the_tracker_refuses_is_refused_before_any_allocation`, `a_beef_failure_refuses_with_nothing_written` (a missing ancestor: no provider asked, the coins, rows, labels, tags, baskets, links and the sequence counter unchanged), `a_coin_taken_after_the_precheck_is_skipped_and_the_precheck_runs_again`, `a_coin_taken_after_the_precheck_with_none_left_refuses_with_nothing_written`, `a_proof_stored_after_the_precheck_is_read_before_the_lock_on_the_second_try`; concurrency (review 2 F1, the reviewer's scratch race): `four_concurrent_creates_on_eight_coins_all_succeed` (at 87d3d43 two of four refused `Insufficient funds: need 1005, have 0`), `without_the_reservation_a_twice_lost_race_is_store_busy_never_insufficient_funds`, `a_refused_call_releases_its_reservation`. In `error.rs`: `every_store_busy_reason_is_wallet_busy_with_its_words`.

## [0.10.0] - 2026-10-20

The reorg below the user (bsv-stack-lean rulings 5 and 7 of 2026-10-20; the draft `corpus/scenarios/2026-10-20-reorg-below-the-user.md`). A minor: public monitor types change (`ReproveOutcome`, `ReproveTally`, `DEMOTION_WITNESSES` removed), `MonitorStorage` gains two methods, and an observable status changes (an action whose proof's root moved reads `unproven` on the local header read alone). This crate does not host bsv-tracker; the rule is written in its own reorg path.

### Changed

- Ruling 5: a stored proof is stale when, and only when, its root is not the active header's root at its height in the wallet's own headers. `reprove_block_hash` and `reprove_anchor` read the header at the proof's height before anything else; a kept root moves nothing and is not retried, and only proofs whose root moved are re-proved, `REPROVES_PER_PASS` (4) a pass, the rest on the next try. The review task does the same. Before, every proof citing a deactivated hash was fetched from the providers, and demotion needed the tracker's refutation and two "not mined" witnesses. Tests: `ruling_5_step_3_a_reorg_that_keeps_the_root_moves_nothing`, `ruling_5_the_root_read_decides_stale_and_an_unknown_never_does`.
- Rulings 7a and 7b: a proof whose root moved and has no replacement now is demoted: the action is `unproven` ("confirming"), its inputs stay allocated to it, its change stays spendable as an unproven creator's is, nothing is released and nothing is failed; only a competitor's checked proof fails it. Tests: `rulings_5_7a_7b_7d_steps_3_to_5_a_lost_root_keeps_the_coin_and_the_action_confirming`, `rulings_7a_7b_step_8_only_a_competitors_checked_proof_fails_the_action`.
- Ruling 7c: the BEEF walk reads every stored path, and every root of a stored BEEF, against the wallet's headers through the caller's tracker or else the storage's own, for every caller, the broadcast rebuild included. A refused path is not attached; the ancestor rides as raw bytes and the walk continues. Before, a checked stored path was merged unchecked when the caller passed no tracker. Tests: `ruling_7c_step_6_the_beef_carries_the_stale_ancestor_as_bytes` (red before this change), `ruling_7c_control_a_kept_root_merges_the_stored_path`.

### Added

- Ruling 7d: after a demotion the courier is asked once (`reask_lost_proof`, the broadcasters' status read). A broadcaster that knows the transaction ends the matter; only when none knows it is it announced, once (`MonitorStorage::announce_lost_proof`); a refusal is written on the request's history (`MonitorStorage::note_lost_proof_refusal`) and a competitor a double-spend word names is queued for the proof ask. No status moves on a courier's word. Tests: `ruling_7d_step_4_the_courier_word_announces_only_on_unknown`, `ruling_7d_step_4i_a_broadcaster_that_knows_it_is_not_told_again`, `ruling_7d_step_4_a_refusal_is_written_and_never_announced`.
- `RootRead`, `read_root`, `active_root_at`, `CourierWord`, `courier_word`, `LostProofReask`, `reprove_moved_anchor`, `reprove_moved_anchors`, `REPROVES_PER_PASS` in `monitor::reorg_ops`.

### Removed

- `ReproveOutcome::Deferred`, `ReproveTally::deferred` and `DEMOTION_WITNESSES`: a replacement above the proof gate no longer holds a stale proof in place; the proof is demoted and the proof pass stores the replacement when its header has aged.

## [0.9.0] - 2026-10-18

bsv-rs 0.7.0, whose default signing digest is the node's (the dispatch on FORKID and CHRONICLE, the SINGLE sentinel, an explicit scriptCode). caret `0.7.0` starts a new cargo compatibility line and bsv-rs types sit in this crate's public API, so this is a minor. No source change was needed: the crate builds no `SigningContext` and calls no bsv-rs signing template.

### Changed

- Declare `bsv-rs = "0.7.0"` by caret.

### Added

- `wallet::signer` computes its own BIP143 digest (ALL|FORKID over the locking script), so 0.7.0's change does not reach it. The test `the_digest_the_toolbox_signs_with_is_the_nodes_for_a_p2pkh_all_forkid_spend` pins that digest against bsv-script-lean v1.1.1 `corpus/signer/p2pkh.jsonl` row `signer-p2pkh-00001`: the digest `737019101a2340f7f56047d03ff616c4b858c6829c3fa25adac60bac72a906b3` and, signed and assembled as `sign_transaction` does, the txid `e418f32d280c6e1cc6f62d881a8db88469c3599e0eeb5d2711d6601c55c97a96`. Both are the control column of the row's record (`docs/records/20261017-020326-s3-147-signer-column-bsv-rs-s3.json`), which bitcoin-sv v1.2.3 `verifyscript` judged valid under the block and relay words. The record was cited, not re-run here.

## [0.8.0] - 2026-10-15

bsv-rs 0.6.0 is the home of the six hand-rolled primitives, and the stack's `deny.toml` is adopted. The crate keeps no hash and no varint of its own. bsv-rs types sit in this crate's public API (the `bsv_rs::wallet` re-exports, `WalletSigner::new`, `BlockHeader::from_transaction`), and caret `0.6.0` starts a new cargo compatibility line, so this is a minor.

### Changed

- Declare `bsv-rs = "0.6.0"` by caret. The six sites now call bsv-rs: `wallet::signer` `read_varint` (now `bsv_rs::primitives::from_var_int`), `double_sha256` (`sha256d`), `hash160` (`hash160`) and `write_varint` (`to_var_int`); `wallet::wallet` `write_varint` (`to_var_int`); `services::traits` `double_sha256` (`sha256d`). The copies are deleted.
- Remove the public `services::traits::sha256` and `services::traits::double_sha256`; callers use `bsv_rs::primitives::{sha256, sha256d}`. `txid_from_raw_tx`, `validate_txid` and `convert_script_hash` are unchanged.
- Move the remaining direct hashes onto bsv-rs: the txid helpers of `wallet` and `process_action`, the BRC-31 body hash of the storage client, the header hash checks, the address checksum and the tests. `sha2` and `ripemd` are no longer direct dependencies.
- The signer's transaction parser reads the node's canonical varint (bsv-rs 0.6.0 `Reader::read_var_int`, bitcoin-sv `serialize.h:315-340` at 6504a3af). A count or script length in a wider encoding than its value needs is refused, and a short varint is now a `SdkError` where it was a `TransactionError`.

### Added

- `deny.toml`, the stack's template byte for byte (bsv-stack-lean `template/deny.toml`). `cargo deny check` refuses one crate the toolbox still needs: `sha2` under `sqlx-core` and `sqlx-macros-core`, because sqlx 0.8.6 always enables `sqlx-core`'s `migrate` feature. The file is not edited locally; the wrapper is the template's ruling.

## [0.7.7] - 2026-10-12

- Declare `bsv-rs = "0.5.0"` by caret in the published libraries. The update loop can deliver compatible SDK patches to consumers without another library release; deployables keep their exact pins and committed locks.

## [0.7.6] - 2026-10-12

- Check incoming AtomicBEEF structure with or without a configured chain tracker; an incomplete stub is refused before storage in either case.
- Run the structural BEEF rule before the no-BUMP return in proof verification. Strict internalization refuses an unmined one-transaction stub before storing `input_beef`, matching [the reference's AtomicBEEF check](https://github.com/bsv-blockchain/ts-stack/blob/edf6e03/packages/wallet/wallet-toolbox/src/signer/methods/internalizeAction.ts#L113-L115).
- Validate stored BEEF structure with or without BUMPs. A body with unsourced inputs falls back to the recursive walk, carrying locally held proven parents; only a valid stored graph can end that walk. Match [the reference's missing-ancestor refusal](https://github.com/bsv-blockchain/ts-stack/blob/edf6e03/packages/wallet/wallet-toolbox/src/storage/methods/getBeefForTransaction.ts#L456-L474).
- Validate the final `build_input_beef` graph for every assembly, including bodies without BUMPs or a tracker. An invalid assembly returns an error instead of transaction data.
- Add synthetic regression pins for refusal without storage, recovery of the proven parent, and strict internalization of the wallet's own assembly, with controls for stored structure, final invalid assemblies and the tracker-less local walk. See [toolbox #23](https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/23).
- Pin `bsv-rs` to `=0.5.0`, retaining its streaming mining, lone-leaf and work-budget checks. No API or database migration; previously accepted incomplete bodies now fail validation and require complete ancestry.

## [0.7.5] - 2026-10-12

bsv-rs 0.4.5 (the lone leaf at offset 0 only; the LOCAL work bound inside one input's script); no behavior change under the defaults. The L2 re-pin of bsv-stack-lean item 2. The known-ancestor witnesses of #69 (a declared known ancestor is txid-only in the result BEEF, ts-stack #660 parity) landed before this release.

### Changed

- bsv-rs 0.4.5: the lone leaf at offset 0 only; the LOCAL work bound; no behavior change under the defaults.

## [0.7.4] - 2026-10-10

No count and no broadcaster word writes `failed` or `invalid`, in the monitor's paths and on the immediate post of `createAction`, `signAction` and `internalizeAction` (bsv-stack-lean #66, the two items #65 left open and the immediate paths; the tracker's rule, bsv-stack-lean `docs/charters/tracker.md` sections 2 and 10: every broadcaster word is a hint that schedules a re-ask, never an age-out word, and `conflicted` is a mined competitor's proof checked against our headers). A `sending` transaction ends with a proof, with a competitor's checked proof, or by the host's explicit retire, `StorageSqlx::retire_undeliverable_txid`, the only way it ends without a proof.

### Fixed

1. **A count of proof attempts writes no word.** `synchronize_transaction_statuses` wrote a request `invalid` and its transaction `failed` through the release rule once its attempts passed the limit (144 on mainnet, 10 off it; Calgooon/zanaadu-v2#368) and no source held it. Now past the limit nothing is written: the request stays in the proof set and every header's pass asks for it again, without end. The hold checks stay and only say in the log which case it is (`proof_backstop_held`, `proof_backstop_past_count`). The reference, past its limit, resets the request to `unsent` for up to `maxRebroadcastAttempts` rebroadcast cycles and then writes it `invalid` (ts-stack@edf6e03 wallet-toolbox `src/monitor/tasks/TaskCheckForProofs.ts:130-158`; its limits are 144 on main and 100 otherwise, `src/monitor/Monitor.ts:125-126`, so the toolbox's 10 off mainnet is not the reference's at this pin). Witnesses: `the_145th_proof_attempt_with_no_source_holding_it_writes_no_word_and_is_asked_again` and `the_11th_proof_attempt_off_mainnet_with_no_source_holding_it_writes_no_word`, red at 0.7.3.
2. **A broadcaster's definitive word is a hint.** In `send_waiting_transactions` a 465, every other `status_codes::is_rejection` code, Arcade's `REJECTED` and a double-spend word (a competitor named or not) took the release rule to `failed` (`invalid` or `doubleSpend`). Now the status sources are read once, as before (a transaction they hold is promoted); otherwise the word is recorded on the request's history, the re-ask waits for the cadence of 0.7.3, the transaction keeps its word, its inputs stay locked and its change is not written off. A competitor the word names (`competingTxs`) is queued on the request's history (`competitorsQueued`). Arcade's pushed `REJECTED` and `DOUBLE_SPEND_ATTEMPTED` (`MonitorStorage::mark_transaction_rejected_by`) are hints the same way: the refusing broadcaster is remembered, the word noted, the competitors queued; 476 stays unrecorded.
3. **A competitor's checked proof is the one evidence for the double-spend word.** `synchronize_transaction_statuses` first asks for every queued competitor through the proof path's sources (`get_raw_tx`, then `get_merkle_path`; no spender is looked up): the competitor's bytes must hash to its txid and spend one of our inputs, and its proof's root must meet our `ChainTracker` at its height under the proof gate (the check of `ingest_merkle_proof`; Rule 28's bind-then-prove of 0.6.0 with the name taken from the broadcaster's word). Then the existing word is written by the existing path, the release rule with `doubleSpend` (`retire_undeliverable_tx`); anything less writes nothing and the competitor stays queued. Witnesses (`src/storage/sqlx/broadcaster_hint_tests.rs`), red at 0.7.3: `a_465_naming_a_competitor_without_its_proof_writes_no_word_and_queues_it`, `a_465_naming_a_competitor_whose_proof_meets_our_header_writes_the_double_spend_word`, `every_rejection_code_is_a_hint_that_keeps_the_word`, `a_double_spend_word_naming_no_competitor_writes_no_word_and_schedules_the_reask`, `an_arcade_double_spend_or_rejected_event_is_a_hint`, `a_competitor_proof_off_our_headers_or_bytes_that_spend_nothing_of_ours_write_no_word`.
4. The vector `tests/vectors/arcade_status_verdicts.json`: `toolbox_push` for the four refusal bodies (`push_rejected_467`, `push_rejected_466_conflict`, `push_double_spend_attempted`, `push_rejected_lower_case`) is `none`, and its outcome list no longer offers `invalid` or `double_spend` for the push. Six tests that asserted the old words now assert the rule (four in `un_fail_recovery`, two in `tests/monitor_tests.rs`, which reach the release rule through the host's explicit retire), as do four of the attempt-backstop tests; one backstop test whose case `tests/retire_rejected_tests.rs` covers is removed.

5. **A refusal on the immediate post is a hint.** `createAction` and `signAction` (`update_transaction_status_after_broadcast`) and `internalizeAction` (its post of a new transaction) wrote the transaction `failed` and its request `invalid` or `doubleSpend` on a 465, any other refusal or a double-spend word, released its inputs, wrote its change (an internalized transaction's outputs) off, and returned the word as an error ("Transaction broadcast failed for txid ...", "internalized tx ... rejected by network"). Now they take the hint of item 2: the status sources are read once (a transaction they hold is `unproven`), otherwise the word is recorded on the request's history (`immediateBroadcastHint`), the request is left `unsent` for the send-waiting re-ask on the cadence, a competitor the word names is queued (`competitorsQueued`; a refusal that names a competitor now classifies as `DoubleSpend` so the competitor is carried), and the transaction keeps its word with its inputs locked. The caller receives no error for a broadcaster's word: `createAction` and `signAction` return the txid (and the signed transaction) with `sendWithResults` `sending`, BRC-100's word for a transaction not yet proven (BRCs@ed1b015 `wallet/0100.md:705-708`); `internalizeAction` returns `accepted`. A transport fault or an orphan hold on the immediate post takes the same hint (it left the request `sending` with no note). What stays an error is the caller's or the wallet's own fault: malformed arguments, a signing failure, insufficient funds, a storage failure, and a broadcast the wallet could not build (no BEEF), which nothing posted and which is now aborted through `abort_action`. The reference throws `WERR_REVIEW_ACTIONS` with `sendWithResults` `failed` and writes `failed` on these words (ts-stack@edf6e03 wallet-toolbox `src/Wallet.ts:1525-1551`, `src/utility/aggregateResults.ts:29-44`, `src/storage/methods/attemptToPostReqsToNetwork.ts:209-212`); this crate departs from it here by the tracker's rule. Witnesses (`tests/immediate_broadcast_hint_tests.rs`), red at the first half of this release: `create_action_answered_465_naming_a_competitor_is_a_hint_not_an_error`, `create_action_answered_a_double_spend_word_naming_no_competitor_schedules_the_reask`, `a_status_source_holding_it_on_the_next_pass_promotes_it`, `create_action_whose_post_reaches_no_broadcaster_is_the_same_hint`, `sign_action_answered_465_is_a_hint_not_an_error`, `internalize_action_answered_465_is_a_hint_not_an_error`. Eleven older tests that asserted the release now assert the rule (eight in `process_action`, two in `internalize_action`, one in `tests/caller_complete_funding_tests.rs`).

### Upgrading

- A host that read the immediate post's error as "the transaction failed and its inputs are released" (bsv-wallet-cli maps "Transaction broadcast failed" to `BROADCAST_REJECTED`) now receives a result: the txid with `sendWithResults` `sending`. The inputs are not released; they stay locked until a proof, a competitor's checked proof or the host's retire (`retire_undeliverable_txid`). `MonitorStorage::mark_transaction_rejected` stays a host's explicit act; no path of the crate calls it on a broadcaster's word. The remote `StorageClient` still sends `updateTransactionStatusAfterBroadcast` its legacy bool; what a remote server writes on `false` is that server's.
- Nothing stored is migrated. A request's `history` may carry `competitorsQueued`. A request an earlier release wrote off stays `invalid` and the unfail canary keeps watching it. A host that relied on the toolbox failing a refused or long-unproven transaction by itself retires it by its explicit act, `StorageSqlx::retire_undeliverable_txid`, or leaves it re-asked. One public item added: `MonitorStorage::queue_competitor_proof_asks`, with a default that queues nothing. Rollback: pin 0.7.3.

## [0.7.3] - 2026-10-10

A broadcaster's word is a hint that schedules a re-ask and writes no word (bsv-stack-lean #65; the tracker's rule, bsv-stack-lean `docs/charters/tracker.md` section 2). No number of transient words, 400s or faults retires a transaction the wallet handed to the broadcasters; the one retire is the host's explicit act, `StorageSqlx::retire_undeliverable_txid`.

### Fixed

1. `send_waiting_transactions` retired a transaction once its attempts passed six, whatever the words were (ARC's 400 for a request it could not read, a 5xx, a 429, a transport fault): the alive check, its inputs released on an `is_utxo` hint, its change written off, the transaction `failed`. Now a transient word, an orphan-mempool hold or a fault leaves the request `unsent` with every provider's word recorded on its history (`{"notes": [{"what": "sendWaitingHint", ...}]}`, the newest 32 kept), the transaction `sending`, its inputs locked, and the next re-ask waits for `services::cadence::send_waiting_reask_minutes`: 1, 2, 4, 8, 16, 32 minutes, then every 64, never ending. A pass posts only the requests that are due. The reference counts a service error and keeps the request sending (ts-stack@edf6e03 wallet-toolbox `src/storage/methods/attemptToPostReqsToNetwork.ts:213-215`). A definitive rejection (465 and the other `is_rejection` codes, Arcade's fatal words) and a double spend keep their release-rule path; the status sniff of that path no longer reads a service error as "invalid".
2. `abort_abandoned` (the `fail_abandoned` task, every five minutes with a five-minute timeout by default) failed every `sending` transaction past the timeout, with the same release of its inputs and write-off of its change: a retire by age of the transactions item 1 keeps announced. It now ages out only `unsigned` and `unprocessed`, as its trait doc said and as the reference does (`src/monitor/tasks/TaskFailAbandoned.ts:41`; `src/storage/StorageProvider.ts:712` lists `sending` as unabortable). The change of a `sending` transaction stays out of coin selection, as before.
3. `Arc::post_beef`'s doc said a V2 BEEF "will be converted automatically" to V1. The code never converted a version and need not: the pinned ARC (e7efc5b) parses V2 (bsv-stack-lean #64). The doc now says the BEEF is posted as written.
4. The witnesses: `seven_transient_words_keep_the_transaction_announced_with_a_scheduled_reask` and `fail_abandoned_never_retires_an_announced_transaction` (`tests/monitor_tests.rs`), red at 0.7.2 (`failed`) and green here; `a_beef_v2_is_posted_as_written` (`tests/arc_broadcast_body_tests.rs`) pins what `post_beef` does. Two tests that pinned the old retires now say the opposite: `transport_dead_after_retries_retires_nothing`, `test_abort_abandoned_leaves_sending_status`.

### Upgrading

- Nothing stored is migrated. A host that relied on the toolbox retiring an undeliverable transaction by itself (after seven attempts, or five minutes in `sending`) now retires it by its own explicit act, `StorageSqlx::retire_undeliverable_txid` (the alive check first, each input released only on its own chain verification), or leaves it to be re-asked. A request's `history` column now carries the notes above. One public item added: `services::cadence::send_waiting_reask_minutes` and its two constants. Rollback: pin 0.7.2.

## [0.7.2] - 2026-10-10

The ARC provider posts a BEEF in a form ARC reads, and reads ARC's 400 as a request ARC could not read, never as the transaction's rejection (bsv-stack-lean #63, finding F2 of its reading of the broadcast body at the pins).

### Fixed

1. `Arc::post_beef` on the full-BEEF path (an unproven ancestor this ARC has not seen, or an EF that could not be built) posted the bytes as handed in, hex, as `{"rawTx": hex}`. `internalizeAction` hands it the caller's AtomicBEEF, and ARC tells a BEEF by its bytes 2 and 3 and has no case for the BRC-95 prefix: at arc@e7efc5b the AtomicBEEF goes to the raw-transaction parse and is refused 400 before any validation (`internal/validator/helpers.go:36-58`, `internal/api/handler/default.go:405-414, 601-640`). The provider now strips the prefix and posts the plain BEEF as `application/octet-stream` to `/v1/tx` (`parsers.go:20-61`); the reference posts the BEEF without the prefix (ts-stack@edf6e03 wallet-toolbox `src/services/providers/ARC.ts:320-340`). Every other path is unchanged: EF when every ancestor is proven, the reduced send of 0.3.56, `/v1/txs` for a batch.
2. Classic ARC's 400 was read as a definitive rejection, so `send_waiting` retired the transaction on it. At arc@e7efc5b a 400 is a request ARC could not read (a content type, a hex error, a callback option, a body its intake does not parse: `default.go:304-309, 393-401, 631-634`), never a verdict on a parsed transaction. It is now a request fault: transient (`service_error`, note `postRawTxRequestFault`), a hint that schedules the re-ask, never the transaction's rejection. The post is retried in the other form ARC reads: the plain BEEF as bytes is re-posted once as the reference's `{"rawTx": hex}` (note `postBeefRetryJson`), and an EF or a reduced send ARC cannot read falls back to the full BEEF. `status_codes::is_request_fault` is new. `status_codes::is_rejection` still lists 400 for Arcade, whose 400 can carry a validator verdict (arcade@1ae1208 `services/api_server/handlers.go:891-896`); the Arcade provider is unchanged.
3. The witnesses: `tests/arc_broadcast_body_tests.rs`, against the six rows of the broadcast-body replay (`tests/vectors/broadcast_body/`, three AtomicBEEFs and the same three BEEFs) and a mock ARC that reads a body as its intake at e7efc5b does. At 0.7.1 two AtomicBEEF rows were refused 400 and ARC's 400 retired the transaction through `send_waiting`; at 0.7.2 every row is read and the transaction's word is unchanged. Ten tests that matched the old hex body now match the bytes and the type.

### Upgrading

- Nothing stored changes and nothing is migrated. No public item changed; one was added (`status_codes::is_request_fault`). An ARC behind a proxy that refuses `application/octet-stream` with a 400 gets the JSON form on the re-post. Rollback: pin 0.7.1.

## [0.7.1] - 2026-10-10

The crate depends on bsv-rs 0.4.3 (was 0.4.1). bsv-rs 0.4.3 reads a raw transaction with no output as invalid bytes, as one with no input has been since 0.4.1 (bsv-stack-lean #59; the node's rule, bsv-script-lean@87f0461 `lean/BsvScript/TxRules.lean:85-86`). Through it the crate also takes bsv-rs 0.4.2's whole-path rules. `createAction` honours `trustSelf: 'known'` before it verifies the caller's `inputBEEF`, as the reference does.

### Changed

1. bsv-rs 0.4.3. A caller resolving 0.7.0 fresh already got 0.4.3 by the caret; this release makes it the floor.
2. A BEEF carrying a transaction with no output is refused wherever the crate reads or verifies one: `Error::InvalidBeef { kind: NoOutputs, .. }` at `internalizeAction` (`tx`) and `createAction` (`inputBEEF`), and "BEEF structure is invalid" from `verify_valid`. `transaction::Kind` gains `NoOutputs`, so an exhaustive `match` on the `kind` of `Error::InvalidBeef` needs the arm.
3. A txid-only entry is read on its own (bsv-rs 0.4.2): valid when a BUMP of the same BEEF proves its txid, refused as `StubNotProven` otherwise, whatever `allow_txid_only` says. The crate calls `verify_valid(true)` in three places: `verify_beef_merkle_proofs` at `internalizeAction`, `validate_stored_beef` on a stored `input_beef`, and the built `inputBEEF` under a chain tracker. When the BEEF also carries a BUMP, an unproven txid-only entry was accepted at all three. Now internalize refuses it and a stored `input_beef` is discarded and the transactions are looked up again; `createAction` refuses it unless the caller sets `trustSelf: 'known'` (item 4). The reference refuses every txid-only entry at internalize, so this moves the crate toward it there.
4. `createAction` with `trustSelf: 'known'`: every txid-only entry of the caller's `inputBEEF` that an input reaches and no BUMP of that BEEF proves is resolved from the wallet's own storage before `verify_valid`: its bytes and, where storage holds it, its proof, or else its stored ancestry. The BEEF verified carries no unproven txid-only entry. The wallet honours an entry its storage holds; it refuses one storage does not know, as `ValidationError` naming the txid ("not known to this wallet"); it drops one no input reaches. Without `trustSelf` such an entry is read as in item 3. The reference: ts-stack@edf6e03 wallet-toolbox `src/storage/methods/createAction.ts:876`, `893-896`, `906-911`, `923-963`, `996-1002`. 0.7.0 on bsv-rs 0.4.1 accepted the entry unread.
5. `Wallet::create_action` passes the wallet's own `trustSelf` (`WalletOptions.trust_self`, `"known"` by default) when the caller names none, as the reference's `Wallet.ts:1150-1151` does; before this release the option was never passed to storage.
6. Two `beef_verification` tests re-read for item 3; no fixture carries a transaction with no output into a BEEF. Four `createAction` tests witness item 4 (`trust_self_known`), one `Wallet` test item 5.

### Upgrading

- Nothing stored changes and nothing is migrated. No public item changed. A caller of `Wallet::create_action` that wants a txid-only entry refused rather than resolved from storage builds the wallet with `WalletOptions { trust_self: None, .. }`. Rollback: pin 0.7.0 and bsv-rs `=0.4.1`.

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
