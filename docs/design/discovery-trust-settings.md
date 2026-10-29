# Discovery under the user's trust settings: the reference's path and the port's shape options (#388)

A design brief for the owner, no public shape change. Labels: `[X]` read at
the pin named here or run; `[D]` told (the issue, its comments, the CLI's
issues). Pins: the reference ts-stack `edf6e03` (paths under
`packages/wallet/wallet-toolbox/src/` are written `WT/`, under
`packages/sdk/src/` `SDK/`); the Go reference go-wallet-toolbox `e824899`
(`GO/`); bsv-rs `0.9.0-beta.3` from the registry (`RS/`); BRC-100 at BRCs
`5119a9b` (`wallet/0100.md`); this crate at beta `99db80ec`; the CLI at
bsv-wallet-cli `origin/beta` with its #331 and #347 `[D]`. The CLI's intake
(#388's comment, 2026-10-28 08:55Z) names the gap: `certifier_info` answers
`None` for every certifier, neither `WalletOptions` nor the two methods take
trust settings, and `WalletSettingsManager` is not read by `Wallet`.

## 1. The reference's path

### 1.1 The wallet holds a settings manager over itself

`WT/Wallet.ts:274` (`WalletArgs`) `settingsManager?: WalletSettingsManager`;
`:324` the field; `:401` in the constructor:

```ts
    this.settingsManager = args.settingsManager || new WalletSettingsManager(this)
```

No chain is passed: a wallet nobody configured gets `DEFAULT_SETTINGS` on
every network (section 1.4).

### 1.2 Discovery reads the settings fresh on every call

`WT/Wallet.ts:988-991` in `discoverByIdentityKey`:

```ts
    // Authorization policy is read fresh on every call. A removed certifier or
    // raised threshold must take effect immediately rather than after a cache TTL.
    const trustSettings = (await this.settingsManager.get()).trustSettings

    const certifiers = trustSettings.trustedCertifiers.map(c => c.identityKey).sort((a, b) => a.localeCompare(b))
```

The sorted trusted keys ride in the overlay query (`:1003-1008`,
`{ identityKey: args.identityKey, certifiers }`), so the overlay filters by
certifier before the wallet does. The answer is filtered by subject and
transformed (`:1009-1017`):

```ts
    return discoveryPage(
      transformVerifiableCertificatesWithTrust(
        trustSettings,
        filterCertificatesByIdentityKey(certificates, vargs.identityKey)
      ),
      vargs.offset,
      vargs.limit
    )
```

`discoverByAttributes` is the same shape: the fresh read at `:1046-1049`,
`{ attributes: args.attributes, certifiers }` at `:1069-1074`, the transform
at `:1075-1083`. The field `_trustSettingsCache` (`:862-866`, "2-minute cache
of trust settings for identity resolution paths") is declared and never read
or written at this pin (one grep hit, its declaration `[X]`); the comment at
`:988` is the live rule. Before the overlay, both methods consult an optional
caller-installed contact source (`:977-987`, `:1035-1045`); the reference has
no local-storage leg.

### 1.3 The transform

`WT/utility/identityUtils.ts:313-384`. The filter and the fill (`:325-338`):

```ts
  certificates.slice(0, MAX_IDENTITY_RESULTS).forEach(cert => {
    const { subject, certifier } = cert
    if (subject === '' || certifier === '') return

    const trustedCertifier = certifierCache.get(certifier)
    if (trustedCertifier == null) return

    // Create the IdentityCertifier object that we want to attach.
    const certifierInfo: IdentityCertifier = {
      name: trustedCertifier.name,
      iconUrl: trustedCertifier.iconUrl ?? 'https://bsvblockchain.org/favicon.ico',
      description: certifierDescription(trustedCertifier.description),
      trust: trustedCertifier.trust
    }
```

Then the extended certificate (`:341-352`: the certificate's fields,
`decryptedFields`, `publiclyRevealedKeyring: { ...cert.keyring }`,
`certifierInfo`), grouping by subject with one trust contribution per
distinct certifier and one entry per `type\0serial\0certifier` (`:355-367`),
the threshold `group.totalTrust >= validatedTrust.trustLevel` (`:371-376`),
the sort by `certifierInfo.trust` descending (`:379`), and
`{ totalCertificates, certificates }` (`:381-384`). `MAX_IDENTITY_RESULTS`
is 256 (`:22`). The description bound (`:27-32`, `:35-46`): BRC-100's
`DescriptionString5to50Characters` (`0100.md:1404`) is applied where the
result is built, cutting at 50 UTF-8 bytes on a code point boundary,
trimming the end, and answering `Trusted certifier` when fewer than 5 bytes
remain; the settings themselves accept up to 500. The settings are validated
on entry (`:317`, `validateTrustSettings`, section 1.4).

### 1.4 The settings manager and its store

`WT/WalletSettingsManager.ts`. The types (`:4-24`): `Certifier { name,
description, identityKey, trust, iconUrl?, baseURL? }`, `TrustSettings {
trustLevel, trustedCertifiers }`, `WalletSettings { trustSettings, theme?,
currency?, permissionMode? }`. The basket (`:157`) `SETTINGS_BASKET = 'wallet
settings'`. The defaults (`:160-182`):

```ts
export const DEFAULT_SETTINGS = {
  trustSettings: {
    trustLevel: 2,
    trustedCertifiers: [
      {
        name: 'Metanet Trust Services',
        description: 'Registry of protocols, baskets, certificate types',
        iconUrl: 'https://bsvblockchain.org/favicon.ico',
        identityKey: '03daf815fe38f83da0ad83b5bedc520aa488aef5cbc93a93c67a7fe60406cbffe8',
        trust: 4
      },
      {
        name: 'SocialCert',
        description: 'Certifies social media handles, phones and emails',
        iconUrl: 'https://socialcert.net/favicon.ico',
        trust: 3,
        identityKey: '02cf6cdf466951d8dfc9e7c9367511d0007ed6fba35ed42d425cc412fd6cfd4a17'
      }
    ]
  },
  theme: { mode: 'dark' },
  permissionMode: 'simple'
} as WalletSettings
```

`TESTNET_DEFAULT_SETTINGS` (`:192-202`) maps each default certifier's key
through `TESTNET_IDENTITY_KEYS` (`:185-189`), which names `Babbage Trust
Services`, `IdentiCert` and `SocialCert`; only `SocialCert` matches a default
name and its testnet key equals its mainnet key, so at this pin the testnet
defaults carry the mainnet keys `[X]`. The constructor and `get()`
(`:211-232`):

```ts
  constructor(
    wallet: WalletInterface,
    config: WalletSettingsManagerConfig = {
      defaultSettings: DEFAULT_SETTINGS
    }
  ) {
    this.config = { defaultSettings: validateWalletSettings(config.defaultSettings) }
    this.kv = new LocalKVStore(wallet, SETTINGS_BASKET, true)
  }

  async get(): Promise<WalletSettings> {
    const encoded = await this.kv.get('settings', JSON.stringify(this.config.defaultSettings))
    if (typeof encoded !== 'string' || toArray(encoded, 'utf8').length > MAX_SETTINGS_BYTES) {
      throw new Error('Invalid wallet settings: encoded value is missing or oversized')
    }
    return validateWalletSettings(JSON.parse(encoded))
  }
```

`set` (`:239-245`) validates, bounds to 256 KiB and `kv.set('settings',
encoded)`; `delete` (`:250-252`) `kv.remove('settings')`. The KV store
(`SDK/kvstore/LocalKVStore.ts:112-125`) takes the context as the basket name
and `encrypt = true`; a value is a PushDrop token in that basket (`:180`),
encrypted under protocol `[2, context]` with the key as key ID (`:172`), read
through `listOutputs` and written through `createAction`. So the reference's
settings live in the wallet's own store as an output in the `wallet settings`
basket, and a `set` is a transaction. With no token stored, `kv.get` answers
its default, the JSON of `config.defaultSettings`: a wallet with no stored
settings answers the two default certifiers' certificates.

`validateTrustSettings` (`:98-136`): `trustLevel` an integer 1 to 2560; at
most 256 certifiers; each `identityKey` a canonical compressed key, unique;
`trust` an integer 1 to 10; `name` 1 to 100 UTF-8 bytes; `description` 1 to
500; `iconUrl` and `baseURL` credential-free absolute HTTP(S) URLs.

### 1.5 The Go reference

`GO/pkg/wallet/internal/wallet_settings_manager/wallet_settings_manager.go`:
an in-memory `WalletSettingsManager { settings, mu }` (`:73-76`),
`NewWalletSettingsManager(settings)` (`:78`), `Get` (`:85`), `Set` (`:92`),
`DefaultManager(chain)` (`:98-124`, testnet keys by chain). It is supplied
through the wallet options (`wallet_opts.go:24`
`WalletSettingsManager *wallet_settings_manager.WalletSettingsManager`;
`wallet.go:184` `WithWalletSettingsManager`; the default
`wallet_settings_manager.DefaultManager(chain)` at `wallet.go:226`). Discovery
reads it through a two-minute cache (`wallet.go:1368-1381`,
`discoverCertificatesTTL = 2 * time.Minute` at `:46`), sends the sorted
trusted keys in the overlay query (`:1129-1141`) and runs
`MapVerifiableCertificatesWithTrust`
(`internal/mapping/mapping_verifiable_certificates_with_trust.go:25-100`),
the TS transform with the description passed through unbounded (`:54`) and
`trust` cast to `uint8` (`:56`). The Go defaults' descriptions are
`Registry for protocols, baskets, and certificate types` and `Certifies
social media handles, phone numbers and emails` (`:16`, `:23`), 54 and 56
bytes. The port's settings manager is Go's, text for text (section 2).

## 2. The port today

- `src/wallet/wallet.rs:2814-2887` `discover_by_identity_key`: a local leg
  (`find_certificates`, filtered by subject, `:2825-2841`; the reference has
  none), then the overlay leg through `HttpLookupResolver::mainnet()` or
  `::testnet()` by `self.chain` (`:2862-2865`), both through
  `certifier_info` (`:2842`, `:2870`), merged and deduplicated (`:2879-2886`).
  `discover_by_attributes` (`:2889-2914`) is the overlay leg alone (`:2905`).
  The lookup sends `{ identityKey }` or `{ attributes }` with no certifiers
  (`src/wallet/lookup.rs:367-380`).
- `certifier_info` (`wallet.rs:2998-3000`) answers `None` for every
  certifier; its doc comment (`:2991-2997`) names the gap and #388.
- `src/managers/settings_manager.rs`: the Go shape, in memory.
  `WalletSettingsManager::new(Option<WalletSettingsManagerConfig>)` (`:183`),
  `get` (`:192`), `set` (`:197`), `reset` (`:202`), `save`/`load` (`:207`,
  `:213`), `save_to_string`/`load_from_string` (`:220`, `:226`),
  `default_settings` (`:233`); `DEFAULT_SETTINGS` (`:68-102`) with Go's
  description texts (`:75`, `:86`), `TESTNET_DEFAULT_SETTINGS` (`:105-130`).
  `Certifier.trust: u32` and `TrustSettings.trust_level: u32` (`:23`, `:37`).
  Every item is exported at the crate root (`src/lib.rs:164,178,187-189,
  193-194`). `Wallet` holds no handle to it and nothing reads it `[X]`. No
  dependent uses it: the CLI's `origin/beta` has no `WalletSettingsManager`,
  `TrustSettings` or `trusted_certifiers` `[X]`.
- bsv-rs `0.9.0-beta.3` carries a `LocalKVStore<W: WalletInterface + Debug>`
  (`RS/src/kvstore/local.rs:46-50`): `new(wallet: W, KVStoreConfig)` (`:85`),
  `get(key, default)` (`:113`), `set` (`:219`), `remove` (`:389`); the basket
  is `config.protocol_id` (`:302`, `:517`) and the protocol
  `SecurityLevel::Counterparty` over it (`:746-747`), the SDK's
  `[2, context]`. It takes the wallet by value. bsv-rs has no
  `impl WalletInterface for Arc<W>` or `&W` `[X]`, and `Arc` is not a
  fundamental type, so this crate cannot add one either: a shareable handle
  is a local newtype delegating the 28 methods.
- bsv-rs's `IdentityCertifier { name, icon_url, description, trust: u8 }`
  (`RS/src/wallet/types.rs:1491-1500`) requires every field the transform
  fills, and `IdentityCertificate.certifier_info` is required (`:1505-1517`),
  which is why `None` drops the entry rather than emit an empty one.
- The default descriptions: the port's (Go's) are 54 and 56 bytes, over
  BRC-100's 5 to 50 (`0100.md:1404`); the TS reference's are 49 and 49 bytes
  (`WalletSettingsManager.ts:166,173`) and TS cuts a long one at the result
  (section 1.3). A port that fills `certifierInfo` from today's
  `DEFAULT_SETTINGS` without the cut emits descriptions the reference's
  identity client refuses; with the cut it emits
  `Registry for protocols, baskets, and certificate t`.
- The CLI (`[X]` at `origin/beta`): `Wallet::with_options(.., config.wallet.options())`
  (`src/context.rs:50-56`, `src/commands/daemon_tenant.rs:129`) with
  `WalletOptions { rng_seed, ..Default::default() }` (`src/config.rs:256-261`);
  no `trust` section yet (#331 designs one beforehand). The two doors are
  `src/server/handlers.rs:1186-1209`. Fixtures 053, 054 and 137 to 140 and
  the hermetic scenario `tests/e2e/scenarios/06-certificate-lifecycle.js:37-41`
  pin `[]` with the reason "toolbox #388"; that scenario's certifier is the
  harness's own key (`tests/e2e/lib/certifier`), not a default certifier.

## 3. The options and their public-shape cost

Common to every option, the feature lane's work whichever is chosen: the
transform ported from `identityUtils.ts:313-384` (section 1.3) at
`certifier_info`'s place, which becomes a method or takes the settings;
`validateTrustSettings`' bounds where settings enter; the description cut
and the icon fallback; `discover_by_attributes` through the same path. Two
rows stand apart from the settings' home and are asked as their own words
(section 6): the overlay query carrying the sorted trusted keys needs a
parameter on the `pub` trait `OverlayLookupResolver` (`lookup.rs:99-114`),
and the port's local leg has no reference counterpart.

| | (a) the reference's own: a store-backed manager | (b) additive: the host supplies at construction | (c) nothing supplied: defaults by chain |
|---|---|---|---|
| `pub` items that change | `WalletSettingsManager` becomes `WalletSettingsManager<W: WalletInterface + Debug>`; `new(wallet: W, config) -> Result<Self>`, `get() -> Result<WalletSettings>`, `set(&WalletSettings) -> Result<()>`, `delete()`; `new(Option<Config>)`, `reset`, `save`, `load`, `save_to_string`, `load_from_string`, `default_settings` go (breaking on a type no dependent uses) | none | none |
| `pub` items that appear | a shareable `WalletInterface` handle over `Arc<Wallet<S, V>>` (a newtype with 28 delegations) for a host to build its manager; inside, a crate-private newtype over `&self` so the wallet reads its basket per call without a cycle or `Arc<Self>` | (b1) `WalletOptions.trust_settings: Option<TrustSettings>`, or (b2) `WalletOptions.settings_manager: Option<Arc<WalletSettingsManager>>` with `impl Debug for WalletSettingsManager` (`WalletOptions: Debug + Clone`); one additive field, source-compatible for a host using `..Default::default()` (the CLI does; the by-name sites `src/wallet/rng_tests.rs:22` and `src/storage/sqlx/create_action/tests/seed.rs:142` are ours) | none |
| where the settings live | a PushDrop token in the `wallet settings` basket of the wallet's own store, encrypted under `[2, 'wallet settings']`, read through `list_outputs` per discovery call, written through `create_action` | in the host's process; the manager's `set` takes effect on the next call (b2) or never (b1) | nowhere; `DEFAULT_SETTINGS` or `TESTNET_DEFAULT_SETTINGS` by `self.chain` |
| the CLI's call (#347) | at start `get()`, compare with its typed `trust` section, `set` when they differ: a spend of a 1 sat token and its fee, so a daemon over an unfunded store cannot record its settings; the hermetic harness funds before discovery | `WalletOptions { rng_seed, settings_manager: Some(Arc::new(WalletSettingsManager::new(Some(WalletSettingsManagerConfig { default_settings })))), ..Default::default() }` with `default_settings` built from its `trust` section; no spend | nothing; the doors answer the default certifiers' certificates alone |
| divergence rows | none against TS; against Go (in-memory) one | D1 settings not persisted in the wallet's store (TS stores a token; Go does not); D2 a TS wallet over the same store neither sees the host's settings nor is seen; the empty-store default is matched | D1, D2, and D3: no host input at all, where both references take one |
| test plan | the KV round trip over in-memory storage with `MockWalletServices` (a `set` needs a funded fixture); defaults when the basket is empty; the two witnesses; the threshold, the dedupe, the sort, the cut | the two witnesses; a host set that names a stranger keeps it; a raised threshold drops; two certifiers of one subject sum; the cut and the icon fallback; the defaults by chain when absent | the two witnesses; the defaults by chain |
| size | large: the handle newtype, the manager rewrite, the basket read in the wallet, funded fixtures; 600 to 900 lines | small: 150 to 250 lines with tests, plus the transform | smallest: the transform and one read of the defaults |

Does (c) answer the CLI without a host API? No. It closes the symptom the
intake describes (discovery answering nothing), and it is the reference's
own behaviour with no stored settings, so (a) without a host write answers
exactly what (c) does. But CLI #331's "done when" is the CLI supplying its
trust settings through its typed config, and the hermetic scenario's
certifier is the harness's own key, so under (c) fixtures 053, 054, 137 to
140 and scenario 06 keep recording `[]`. (c) is (b)'s fallback branch, not
a shape.

## 4. The recommendation

(b2): `WalletOptions.settings_manager: Option<Arc<WalletSettingsManager>>`,
read fresh on every discovery call, the chain's defaults when absent, the
transform at `certifier_info`'s place. Why:

- It is the Go reference's shape line for line (`wallet_opts.go:24`,
  `wallet.go:184`, `:226`, `:1374`), and the port's manager is already Go's.
- It is additive on the public surface and the one the CLI's typed `trust`
  section maps onto without a spend; a daemon over an empty store discovers
  on its own trust from its own start.
- It keeps the TS rule that a removed certifier or a raised threshold takes
  effect on the next call (`Wallet.ts:988-989`): the manager's `get` is an
  in-memory read, so no cache is needed; Go's two-minute cache is not ported.
- (a)'s token matters when a TS wallet and this port share one store, which
  no host does today. It adds later without moving (b2)'s surface: a second
  constructor of the manager over a store, the same handle in the options.
- The default descriptions move to the TS reference's texts in the same
  lane (a value of a `pub static`, not a shape), so the defaults pass
  BRC-100's bound without the cut; the cut still guards a host's text.

## 5. The CLI's view

Under (b2) the CLI writes its `trust` section into
`WalletSettingsManagerConfig { default_settings: WalletSettings {
trust_settings: TrustSettings { trust_level, trusted_certifiers }, theme:
None, currency: None, permission_mode: None } }`, wraps the manager in an
`Arc` and names it in `WalletOptions`; a `bsv-wallet init` with no section
leaves the field `None` and gets the chain's defaults. Fixtures 053, 054 and
137 to 140 re-record with the trusted certifiers' certificates and their
`certifierInfo`; scenario 06 lists the harness's certifier in its `trust`
section with a trust at or above the threshold, and its discovery assertions
turn from "empty" to the issued certificate. The MCP's two discover tools
follow the doors. Under (a) the CLI would also `get`, compare and `set` a
token per change, and the harness would fund the wallet before its
discovery step. Under (c) alone the CLI keeps `[]` in its scenario and its
`trust` section has nowhere to go.

## 6. Open questions for the owner

1. The settings' home: (b2) now, (a)'s store-backed manager as a later
   constructor; or (a) now.
2. `OverlayLookupResolver::lookup_by_identity_key` and `lookup_by_attributes`
   gaining the sorted trusted keys (a `pub` trait change, `lookup.rs:99-114`;
   the reference's query at `Wallet.ts:1003-1008`), or the client-side filter
   alone with a divergence row.
3. The default description texts: the TS reference's 49-byte texts replacing
   Go's in `DEFAULT_SETTINGS` (`settings_manager.rs:75,86`).
4. The port's local leg of `discover_by_identity_key` (`wallet.rs:2825-2841`),
   absent from both references, kept or dropped.
5. Testnet defaults by chain (Go, `DefaultManager(chain)`) or the mainnet
   defaults on every network unless a host supplies others (TS, `Wallet.ts:401`).
6. `Certifier.trust: u32` and `TrustSettings.trust_level: u32` kept with the
   reference's bounds checked on entry (1 to 10, 1 to 2560), or narrowed (a
   `pub` type change).
7. `WalletSettingsManager::get` stays infallible in memory, or becomes
   `Result` now so (a)'s constructor needs no second signature later.

## 7. The witness in this lane

`src/wallet/discovery_trust_tests.rs`, included from `wallet.rs` under
`#[cfg(test)]` after `certifier_info`; both tests `#[ignore]` with #388 in
their reason, red on base and on this tip:

- `certifier_info_fills_a_default_certifier_and_drops_a_stranger`: each
  default certifier's entry carries its name, icon, description through the
  reference's cut, and trust; a stranger's key has none.
- `discovery_keeps_a_default_certifiers_certificate_and_drops_a_strangers`:
  a wallet over in-memory storage with two stored certificates for its own
  identity, one from `Metanet Trust Services` and one from a stranger;
  `discover_by_identity_key` answers the one with its `certifierInfo`. The
  overlay leg is refused by the test transport before any request.

Run with `cargo test --lib discovery_trust -- --ignored` under rule 10's
proxy prefix; the tails are in the lane report. The feature lane moves the
seam witness to `certifier_info`'s new signature and drops the `#[ignore]`.
