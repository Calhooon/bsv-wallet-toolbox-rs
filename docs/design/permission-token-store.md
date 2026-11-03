# The permissions manager's stored grants: the reference's tokens beside the port's cache (#751)

A decision brief for the owner, no code. The keeper's 2026-11-03 21:0xZ reading asks for it
before any lane on #751. Labels: `[X]` read at the pin named; `[D]` told (the issue, the brief).
Nothing here was run: every claim is a reading, with its lines, and every figure in section 4
is arithmetic, not a measurement.

Pins: ts-stack `edf6e03` (`~/bsv/ts-stack`, read with `git show edf6e03:<path>`; the working
tree sits at `4d0ea63c8` and was not read); the file is
`packages/wallet/wallet-toolbox/src/WalletPermissionsManager.ts`, cited below by line alone.
This crate at beta `0a036015`, `src/managers/permissions_manager.rs` cited as `pm.rs`.
bsv-wallet-cli at `origin/beta` `c2b20f9` (`~/bsv/bsv-wallet-cli`, present on this host).

## 1. The reference's token lifecycle

### 1.1 The token and its baskets

- `:347-355` `[X]`: a permission is "typically stored as a single unspent PushDrop output in a
  special internal admin basket belonging to the user, held in their underlying wallet".
- `:356-411` `[X]` `PermissionToken`: `txid`, `tx` (the BEEF), `outputIndex`, `outputScript`,
  `satoshis` ("often 1"), `originator`, `rawOriginator`, `expiry` (unix seconds; 0 for a
  spending authorization, indefinite), then per kind `privileged`, `protocol`,
  `securityLevel`, `counterparty` (DPACP), `basketName` (DBAP), `certType`, `certFields`,
  `verifier` (DCAP), `authorizedAmount` (DSAP).
- `:418-423` `[X]` `BASKET_MAP`, the four admin baskets: `admin protocol-permission`,
  `admin basket-access`, `admin certificate-access`, `admin spending-authorization`.
- `:2711-2714` `[X]` the field protocol `[2, 'admin permission token encryption']`, keyID `"1"`;
  `:2726-2737` each field is the wallet's own `encrypt` under it, as the admin originator.
  `:2770-2796` the locking key is that protocol's `getPublicKey(counterparty 'self', forSelf)`.
- `:3708-3749` `[X]` `buildPushdropFields`, the fields in order. Protocol: domain, expiry,
  privileged (`'true'`/`'false'`), securityLevel, protocolName, counterparty (`''` below level 2,
  `'self'` by default at level 2, `:3712`). Basket: domain, expiry, basket. Certificate:
  domain, expiry, privileged, certType, the fields as JSON, verifier. Spending: domain,
  authorized amount (no expiry field).
- `:3755-3787` `[X]` `buildTagsForRequest`, the output's tags: `originator <o>`, and for a
  protocol `privileged <b>`, `protocolName <n>`, `protocolSecurityLevel <l>`, with
  `counterparty <c>` at level 2 only; `basket <b>` for a basket; `privileged`, `type`,
  `verifier` for a certificate; the originator alone for spending. The lookups query by these
  tags (`:3095`, `protocolTokenTags`).

### 1.2 Creation on grant

- `:2550-2560`, `:2574-2610`, `:2652-2668` `[X]` `requestPermissionFlow`: the request is queued
  under a request key in `activeRequests` (`:2654`) and an event fired by kind
  (`:2671-2690`: `onProtocolPermissionRequested`, `onBasketAccessRequested`,
  `onCertificateAccessRequested`, `onSpendingAuthorizationRequested`, `:430-433`), carrying the
  `requestID`. The caller's promise waits for the host's answer.
- `:1160-1180` `[X]` `grantPermission({requestID, expiry?, ephemeral?, amount?})`, called by the
  host: every pending caller is resolved `true` (`:1171-1176`); `:1179` an ephemeral grant
  returns there, no token ("a one-time authorization without an associated persistent on-chain
  token", `:304-305`); else `grantPersistentPermission` (`:1180`).
- `:1104-1149` `[X]` `grantPersistentPermission`: a duplicate mint for the same key is skipped
  while one is cached, in its 15-second cover (`RECENT_GRANT_COVER_MS`, `:748`) or in flight
  (`:1112-1124`, never for spending); a renewal renews (`:1127-1128`), else
  `createPermissionOnChain` (`:1129`); the mint is published in `mintsInFlight` (`:1133-1144`);
  then the five-minute cache entry (`:1147`, not for spending) and the recent-grant mark
  (`:1148`). `denyPermission` (`:1189-1205`) rejects the callers and mints nothing.
- `:3349-3392` `[X]` `createPermissionOnChain`: the fields (`:3356`), `PushDrop.lock(fields,
  protocol, '1', 'self', true, true)` (`:3360-3367`), the tags (`:3370`), and one action with
  one output, `satoshis: 1`, the kind's admin basket, no explicit input ("the wallet can
  internally fund it from its balance", `:3372-3384`), `acceptDelayedBroadcast: true`
  (`:3389`). `completePermissionTokenAction` (`:3566-3576`) runs it through `createAction` as
  the admin originator.
- `:1213-1340` `[X]` `grantGroupedPermission` (BRC-73) mints per granted item, checking for an
  existing token before it mints (`:1256`).

### 1.3 The lookup, and the seek-false call it admits

- `:1481-1567` `[X]` `ensureProtocolPermission`, in order: admin (`:1501`); level 0 open
  (`:1505`); level 1 drops the counterparty (`:1506`); admin-only protocol (`:1508-1510`);
  usage exemptions, privileged normalizing, whitelisted counterparty (`:1513-1516`); the cache
  or a pending grant (`:1518-1519`); then **the stored token** (`:1522-1529`,
  `findProtocolToken(..., includeExpired = true, ...)`).
- `:1531-1544` `[X]` no token: refuse `No protocol permission token found (seekPermission=false).`
  (`:1533`) or prompt with `renewal: false`.
- `:1546-1562` `[X]` a token past its expiry (`isTokenExpired`, `:2900-2903`: `expiry > 0 &&
  expiry < now`): refuse `Protocol permission expired and no further user consent allowed
  (seekPermission=false).` (`:1549`) or prompt with `renewal: true, previousToken: token`.
- `:1564-1566` `[X]` a live token: cached (`:1565`) and admitted. **A seek-false call with a
  live token is admitted here at any age of the grant and across a restart**: the token is an
  output in the wallet's own store.
- `:3080-3117` `[X]` `findProtocolToken`: per originator lookup value, `listOutputs` on
  `admin protocol-permission` with the tags, `tagQueryMode 'all'`, `include 'entire
  transactions'`, limit 10000, as the admin originator (`:3092-3101`); each output decoded and
  its fields decrypted and matched (`:3103-3111`, `parseProtocolTokenOutput` at `:2951`,
  `decryptProtocolTokenFields` at `:2906`); the PushDrop source checked against the locking key
  (`authenticatedPermissionTokenSource`, `:2798-2828`).
- The same shape for the siblings `[X]`: `ensureBasketAccess` `:1595-1615`
  (`findBasketToken` `:3163`), `ensureCertificateAccess` `:1669-1709` (`findCertificateToken`
  `:3196`), `ensureSpendingAuthorization` `:1746-1778` (`findSpendingToken` `:3241`; the month's
  spend from `listActions` on the labels `admin originator <o>` and `admin month <m>`,
  `querySpentSince` `:3306-3330`; an exceeded ceiling prompts a renewal, `:1754-1766`).

### 1.4 Renewal

- `:3634-3700` `[X]` `renewPermissionOnChain`: new fields and script with the new expiry
  (`:3642-3652`); every token for the same parameters found (`:3654-3660`); more than one:
  coalesced into one (`coalescePermissionTokens`, `:3578-3623`); one: an action spending the
  old token (`inputBEEF: oldToken.tx`, the outpoint, `unlockingScriptLength: 73`) with one new
  1-satoshi output in the same basket and tags (`:3674-3700`), signed by the manager's input
  signer for the token (`:3566-3576`, `permissionTokenInputSigners`).
- The renewal prompt is the ordinary prompt with `renewal: true` and `previousToken`
  (`:336-337`, `:1559-1560`); the host's display originator falls back to the old token's
  `rawOriginator` (`:2583`).

### 1.5 Listing and revocation

- `:3802-3836` `[X]` `listProtocolPermissions({originator?, privileged?, protocolName?,
  protocolSecurityLevel?, counterparty?})`: `listOutputs` on the basket with the filter tags,
  decoded and filtered. Siblings: `listBasketAccess` `:3920`, `listCertificateAccess` `:4079`,
  `listSpendingAuthorizations` `:4009`; the probes `hasProtocolPermission` `:3895-3912` (an
  `ensureProtocolPermission` with `seekPermission: false`, so it reads the store),
  `hasBasketAccess` `:3992`, `hasCertificateAccess` `:4162`, `hasSpendingAuthorization` `:4058`.
- `:4269-4287` `[X]` `revokePermission(token)`: an action spending the token with no output
  (`description 'Revoke permission'`, `acceptDelayedBroadcast`). `revokePermissions(tokens)`
  `:4185-4187` and `revokeAllForOriginator(originator, opts?)` `:4189-4228` (the four lists,
  deduplicated, revoked in chunks of 15 in one action each, `:4230-4263`).

## 2. The port today

- `pm.rs:309-317` `[X]` `CachedPermission { expiry, cached_at }` and `CACHE_TTL_SECS` 300: the
  only record of a grant, a `RwLock<HashMap<String, CachedPermission>>` in the manager
  (`pm.rs:497`). Nothing persists it.
- `pm.rs:28-37` `[X]` the handler is `Fn(PermissionRequest) -> Future<Result<PermissionToken>>`:
  the host answers inline with a token, not by a later `grantPermission(requestID, ...)`.
  `pm.rs:700-725` `request_permission_flow` calls it and caches the returned token's `expiry`
  under the request key; the token's other fields are dropped and nothing is minted. A host
  cannot say "ephemeral": every grant is cached for five minutes.
- `pm.rs:772-921` `[X]` `ensure_protocol_permission_seeking`, the reference's order to the
  whitelisted counterparty, then the cache (`pm.rs:855-868`) and, with `seek_permission` false,
  the reference's `:1533` text (`pm.rs:869-873`, #720 by PR #750), else the prompt. The
  doc comment at `pm.rs:762-771` names the gap. Basket, certificate and spending (`pm.rs:928`,
  `:1008`, `:1083`) read the cache alone in the same way `[X]` (read for the cache step only).
- `pm.rs:1274-1300` `[X]` `cache_token` stores the expiry alone; `get_cached_token` always
  returns `None`; `purge_expired_tokens` prunes the cache. `PermissionToken` (`pm.rs:257-300`)
  is the reference's shape, but no function builds one from a stored output.
- No listing, `has*` probe, revocation or renewal function exists in `pm.rs` (`grep -n
  "list_\|revoke\|renewal" pm.rs` finds only the `allow_list_*` flags) `[X]`.

What a restart, or five minutes, loses: every grant. What a host written to the reference
expects and does not get:

1. A seek-false call admitted by a grant older than five minutes or from before a restart
   (`:1522-1529`, `:1564-1566`); the port refuses it with `:1533`'s text (#751's finding).
2. With seek true, the same call prompts again here where the reference admits it silently.
3. `listProtocolPermissions` and its siblings, the `has*` probes, `revokePermission`,
   `revokePermissions`, `revokeAllForOriginator`: absent. A host's "connected apps" screen
   has nothing to read or revoke.
4. The renewal prompt: a host never sees `renewal: true` or a `previousToken`, since nothing
   expires except the cache entry, which simply prompts afresh. `PermissionRequest` here has
   no `renewal` or `previous_token` field (`pm.rs:184-217`).
5. Grants visible to a second process or device over the same store (the reference's tokens
   are outputs, so they sync with the wallet's other outputs).

## 3. The precedent at the host's layer

bsv-wallet-cli `origin/beta` `c2b20f9` `[X]`: `src/store/origin_grants.rs` (module doc
`:1-22`, the tables `:58-110`) persists an origin's standing in four tables the CLI owns
beside the toolbox's, `origin_grants`, `origin_pending`, `origin_spends`, `origin_probes`.
`origin_grants` (`:61-72`): `originator` (normalized as `:5702-5729`), `raw_originator`,
`kind` (the four), `key` (the kind's instance key in canonical text), `amount` (spending
only), `granted_at`, `expiry` (0 never), `revoked_at`; unique on `(originator, kind, key)`.
The design is that repository's `docs/design/origin-grants.md` (revised under the owner's
ruling W1 (a) of 2026-11-03, `:2-3`) `[D]` (the file named in the module doc; not opened). The
CLI answers its served doors from these rows; it spends no coin per grant and writes no token.

## 4. The two shapes

### (P) Parity: the reference's token store

What it is: on a grant the manager mints one PushDrop output per grant through the wallet's
own `create_action`, as the admin originator, in the reference's four admin baskets with its
tags and its six encrypted fields; the lookups read them through `list_outputs` ahead of the
seek-false refusal and ahead of the prompt; renewal and revocation spend them; the listing and
revocation calls exist.

What it needs:

- **Public shape (the owner's word).** `grant_permission(request_id, expiry, ephemeral,
  amount)` and `deny_permission` beside, or instead of, the inline handler: the reference's
  host answers after the event, and the port's handler returns a `PermissionToken` the host
  cannot honestly build (it has no txid until the manager mints). The least-breaking form keeps
  the handler and reads its answer as the grant (expiry from the returned token, an ephemeral
  flag needing a new field or a new answer type). New public methods: the four `list_*`, the
  four `has_*`, `revoke_permission`, `revoke_permissions`, `revoke_all_for_originator`;
  `PermissionRequest` gains `renewal` and `previous_token` (a constructible public struct: a
  MAJOR change to `cargo semver-checks`, as `src/managers/CLAUDE.md` records for the config).
- **Schema: none.** The tokens are outputs, baskets, tags and actions the store already holds.
  The basket names are the reference's (`:418-423`), so no naming decision; but the four names
  begin with `admin`, and `is_admin_basket` (`pm.rs:592-594`) refuses them to every non-admin
  originator, which is the intent. Whether the port's create door admits an `admin`-prefixed
  basket from the admin originator is not read here: the lane's opening probe.
- **Doors touched:** create (mint, renew, coalesce, revoke), sign (the manager signs the
  token input itself, `:3566-3576`; the port has no `completeBoundAction` or token input
  signer, so a signing path for a PushDrop input under the admin protocol key is new code),
  list outputs (every lookup), list actions (the spending month, `:3306-3330`), encrypt and
  decrypt (every field, every lookup), and broadcast (each grant is a transaction; with
  `acceptDelayedBroadcast` it rides the monitor's send path).
- **Cost per grant (arithmetic, not a run).** The output is 1 satoshi (`:3379`). The script
  carries six ciphertexts (each the plaintext plus the SDK's 32-byte IV and 16-byte tag, so
  about 50 to 90 bytes), the 33-byte key, `OP_CHECKSIG` and the signature field that the
  trailing `true` asks for (about 71 bytes): about 500 to 650 bytes. With one P2PKH funding
  input (about 148 bytes), one change output (34) and the frame (10), a grant is about 700 to
  850 bytes; at the port's default 101 sat/kB (`src/storage/sqlx/create_action/fees.rs:4`)
  about 70 to 90 satoshis of fee and 1 locked. A renewal spends the token (about 115 bytes
  in) and mints its successor: the same order. A revocation spends the token with no output:
  the 1 satoshi does not cover its fee, so the wallet funds it, about 30 satoshis. A wallet
  with no spendable coin cannot record a grant at all (the reference's own limit).
- **Tests:** the mint, lookup, renewal and revocation against the in-memory SQLite store with
  `MockWalletServices`; a restart witness (a second manager over the same store admits the
  seek-false call); a cross-SDK vector of one token's script and tags if the owner wants a
  TypeScript wallet's tokens read here (a migrated user's existing grants).

What a host gets: everything in section 2's list, and a TypeScript wallet's tokens readable in
the port over a synced store. What it does not: nothing the reference gives, once the event
and grant shape is decided.

### (C) The cheaper shape: a persisted grant table, a stated divergence

What it is: the manager records each grant as a row with the token's fields (originator, raw
originator, kind, the kind's key: privileged, security level, protocol name, counterparty;
basket; certificate type, fields, verifier; amount; expiry; granted at; revoked at) and no
output, as the CLI's `origin_grants` does at its layer; the lookup reads the row ahead of the
seek-false refusal and the prompt; listing and revocation read and mark rows.

What it needs:

- **Where the rows live, two forms.** (C1) a table under the storage layer: a schema change
  (the owner's word), a migration, and a method on the storage traits, which CLAUDE.md requires
  in all three implementors (`StorageSqlx`, `WalletStorageManager`, `StorageClient`, the last
  an RPC the reference's storage server does not serve). (C2) a store under the manager:
  `WalletPermissionsManager` takes a grant store it is given (a trait with a SQLite or file
  implementation the host supplies, as the CLI's tables are the CLI's): no toolbox schema, one
  new public trait and a builder method (`with_grant_store(..) -> Self`, the
  `with_whitelisted_counterparties` shape, so not a MAJOR change to the config).
- **Public shape:** the same listing, probe and revocation methods as (P) if hosts are to
  have them, and the same answer-shape question (ephemeral); renewal as an update of the row's
  expiry with the same `renewal`/`previous_token` prompt fields (a `previous_token` with no
  txid: the token type's outpoint fields empty, or a new row type).
- **Doors touched:** none of the transaction doors. No coin, no fee, no broadcast; a grant
  works on an unfunded wallet.
- **Divergence row:** a grant made here is not a token, so a TypeScript wallet over the same
  store sees none of it, and a TypeScript wallet's tokens are not admitted here unless (C)
  also reads the admin baskets (a read-only half of (P): `list_outputs`, decrypt, decode,
  with no mint); grants do not sync between devices except where (C1)'s table syncs, which
  the sync protocol's entity list does not carry today.

What a host written to the reference still does not get under (C): tokens visible to another
SDK's wallet, the `PermissionToken` outpoint fields filled, and cross-device grants.

### Risks

- (P): a coin and a transaction per grant, revocation and renewal, so a busy host's
  new-origin prompts become a fan of small transactions (the reference coalesces,
  `:3654-3669`, and chunks revocations by 15, `:4230-4236`); a failed mint after the host said yes
  leaves the caller admitted and nothing stored (the reference resolves callers before minting,
  `:1171-1180`); a new signing path for the token input; the largest public delta of the
  three, and the encryption, decode and source check (`:2798-2828`) must match the reference
  byte for byte or a migrated user's tokens read as absent (a silent re-prompt, not a wrong
  admit).
- (C1): a schema change and a trait method in three implementors, one of them remote with no
  reference counterpart; a divergence row that never closes unless (P) follows.
- (C2): every host builds its own store, so a host that supplies none keeps today's behaviour;
  two stores for one fact if the host also keeps its own (the CLI's tables already answer at
  its doors).
- Both: a stored grant widens what a seek-false call admits, which is the reference's
  behaviour and the point of #751, but it is a prompt that no longer appears (the host note on
  bsv-wallet-cli#386).

## 5. The recommendation

**(P) as the parity target, reached in two steps; (C) not taken.**

1. A read-only lookup, ahead of the rest: `find_protocol_token` and its three siblings over the reference's
   admin baskets (list, decrypt, decode, source check), ahead of the seek-false refusal and the
   prompt, as `:1522-1566` does. It closes #751's finding for every grant a token exists for
   (a migrated TypeScript user's, and any host that mints), touches no transaction door, needs
   no schema, and its witnesses are a seeded token output in the in-memory store. Its only
   public question is none: the lookup is private.
2. The mint, renewal, revocation and listing after it, on the owner's word for the answer
   shape (`grant_permission` beside the handler, ephemeral, `renewal`/`previous_token`), with
   the signing path for the token input as its own lane.

Reasons: the reference keeps grants as tokens so that they live with the wallet's outputs; a
table is a second store the reference does not have, and its divergence row would stay open
forever or be replaced by (P) later, paying twice. (C2)'s precedent is real, but it is the
CLI's own layer answering its own doors under the owner's W1 (a); the toolbox's manager is the
reference's port, and the host already has (C) where it wants one. Step 1 is cheap and
parity-safe on its own, so the decision on the public shape (step 2) can wait without leaving
#751's refusal open for migrated grants. If the owner prefers no coin per grant, (C2) is the
cheaper of the two (C) forms, as a builder method, with step 1's read of the admin baskets
beside it so that tokens are still honoured.

Questions for the owner: (a) the parity target (P) or the divergence (C1/C2); (b) under (P),
the answer shape: `grant_permission(request_id, ..)` beside the handler, or the handler's
answer read as the grant with a new ephemeral flag; (c) whether step 1 may land alone ahead of
the decision on (b).

## 6. The neighbour question (#726)

`[X]` #726: the reference delegates a `p <scheme>` protocol or basket to a registered
permission module (`delegateToPModuleIfNeeded`, `:843-880`, called from nine wrappers, `:5086`, `:5120`,
`:5149` and the key calls `:5226-5364`) before any protocol check, and the port declares
`permission_modules` (`pm.rs:327`) and reads it nowhere. Where it touches this note: a module
answers for its own scheme's calls, so a `p `-prefixed grant never reaches the token lookup or
the token store in the reference (`:851-853` returns to the ordinary path only for a name not
beginning `p `, and an unregistered scheme throws at `:858-860`). Neither shape here stores a
grant for a `p ` name; #726's lane and #751's lane share only the order of the checks in
`ensure_protocol_permission_seeking`, where the delegation runs ahead of the lookup.
