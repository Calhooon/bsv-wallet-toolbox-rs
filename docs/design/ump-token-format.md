# The CWI manager's key file as the reference's UMP token (#560)

Status: stage one, the owner's word of 2026-11-02 13:2xZ ("the
recommendation as given, no shortcuts; a key-file format, a public shape"),
with the migration's shape on the owner's word of 2026-11-02 14:4xZ
(#594, option (a): the v1 record stays beside the token until a later
unlock opens it). Stage two (argon2id, #593) on the owner's dependency word
of 2026-11-02 14:4xZ: RustCrypto's `argon2`, new tokens at the reference's
default (section 5).

The reference is ts-stack at edf6e03,
`packages/wallet/wallet-toolbox/src/CWIStyleWalletManager.ts`, read with
`git show edf6e03:<path>`. Every line number below is that file's at that
commit unless another file is named. (The brief's numbers, :1665-1673 for
the salt, are an older layout; the issue's :1841 and :2328 are the pin's.)

## 1. Why

`CWIStyleWalletManager` salts its PBKDF2 with the profile's 16-byte id
(`src/managers/cwi_style_wallet_manager.rs`, `create_profile` and
`switch_profile`; `src/managers/CLAUDE.md` "Salt = profile.id"). The
reference draws a separate `passwordSalt = Random(32)` for every new
account (:1841) and every password change (:2328), records the KDF that
used it in the token (`passwordKdf`, :486), and writes the whole as its
UMP token. The profile id is a locator, public to anyone who holds the
file and to every listing; a salt is its own draw. This lane moves the
toolbox's key file onto the reference's UMP token format.

## 2. The reference's UMP token, field by field

`export interface UMPToken` at :415-498. Every byte field is a `number[]`.
The XOR of two 32-byte keys keys an AES-256-GCM `SymmetricKey`
(`SymmetricKey.encrypt` is the sdk's: a 32-byte IV, the ciphertext, the
16-byte tag).

| # | Field | Line | What it holds |
|---|-------|------|---------------|
| 0 | `passwordSalt` | :449 | the KDF salt; `Random(32)` at :1841 (new user) and :2328 (password change) |
| 1 | `passwordPresentationPrimary` | :419 | root primary key under `presentationKey XOR passwordKey` |
| 2 | `passwordRecoveryPrimary` | :424 | root primary key under `recoveryKey XOR passwordKey` |
| 3 | `presentationRecoveryPrimary` | :429 | root primary key under `presentationKey XOR recoveryKey` |
| 4 | `passwordPrimaryPrivileged` | :434 | root privileged key under `rootPrimaryKey XOR passwordKey` |
| 5 | `presentationRecoveryPrivileged` | :439 | root privileged key under `presentationKey XOR recoveryKey` |
| 6 | `presentationHash` | :444 | SHA-256 of the presentation key (the overlay lookup key, :662) |
| 7 | `recoveryHash` | :454 | SHA-256 of the recovery key (:683) |
| 8 | `presentationKeyEncrypted` | :459 | the presentation key wrapped under the root privileged key |
| 9 | `passwordKeyEncrypted` | :469 | the derived password key wrapped under the root privileged key |
| 10 | `recoveryKeyEncrypted` | :464 | the recovery key wrapped under the root privileged key |
| 11 | `profilesEncrypted?` | :475 | `JSON.stringify(profiles)` under `SymmetricKey(rootPrimaryKey)` (:2547-2552) |
| | `umpVersion?` | :480 | 3 for a token with KDF metadata (:157, :1874, :2593) |
| | `passwordKdf?` | :486-492 | `{ algorithm: 'pbkdf2-sha512' \| 'argon2id', iterations, memoryKiB?, parallelism?, hashLength? }`; absent on a legacy token |
| | `currentOutpoint?` | :497 | where the token sits on chain, once published |

The # column is the field's place on chain (`tokenFields`, :1229-1254)
and in the binary serialization (`serializeUMPToken`, :2648-2716); note
that 9 is the password key's copy and 10 the recovery key's, the reverse
of the interface's order.

"Wrapped" (fields 8 to 10) is `PrivilegedKeyManager.encrypt({ plaintext,
protocolID: [2, 'admin key wrapping'], keyID: '1' })` (:1856-1858,
:2568-2589), which is `ProtoWallet(rootPrivilegedKey).encrypt` with the
counterparty defaulting to `self` (`sdk/PrivilegedKeyManager.ts:361`):
a BRC-42 symmetric key derived from the root privileged key, then the same
AES-256-GCM.

`Profile` (:361-386) is `{ name, id: 16 bytes, primaryPad: 32,
privilegedPad: 32, createdAt: seconds }`; `isValidProfile` (:393-411)
checks those lengths, a name of 1 to 250 characters and a finite
`createdAt >= 0`. A profile's primary key is `rootPrimaryKey XOR
primaryPad`, its privileged key `rootPrivilegedKey XOR privilegedPad`
(`switchProfile`, :2267-2316); the default profile (id all zero) is the
root itself.

### The KDF

`derivePasswordKey` (:303-358): no metadata (a legacy token) is PBKDF2
HMAC-SHA512 at `PBKDF2_NUM_ROUNDS` 7777 (:83) with the token's salt, 32
bytes out; `pbkdf2-sha512` is the same at `passwordKdf.iterations`; the
output is 32 bytes whatever `hashLength` says (:320-327). `argon2id` uses
`iterations`, `memoryKiB`, `parallelism`, `hashLength` (defaults 7,
131072, 1, 32 at :88-91). `pbkdf2NativeOrWasm` (:244-293) is WebCrypto's
PBKDF2 with a hash-wasm fallback: the same function. `isValidKdfConfig`
(:103-122) bounds what a token may ask: PBKDF2 iterations 1 to 10,000,000
(:96), argon2 iterations to 20, memory to 262144 KiB, parallelism to 16,
`hashLength` to 64. The constructor's default for new tokens is argon2id
(:1677); `kdfConfig: { algorithm: 'pbkdf2-sha512' }` gives 7777 rounds
(:1678-1684). The constructor fills every field of `Required<KdfConfig>`,
so a reference-written PBKDF2 token's params read
`{"iterations":7777,"memoryKiB":131072,"parallelism":1,"hashLength":32}`.

### The binary layout

The token's at-rest form is binary, not JSON. `serializeUMPToken`
(:2648-2716):

```
u8 version            3 when umpVersion === 3 and passwordKdf, else 2
11 x (varint len, bytes)   fields 0..10 in the # order above
u8 profiles flag      1, then (varint len, bytes); or 0
-- version 3 only --
u8 kdf flag           1, then:
  u8 umpVersion
  (varint len, bytes) algorithm, UTF-8
  (varint len, bytes) JSON params, keys in the order
                      iterations, memoryKiB, parallelism, hashLength,
                      each only when defined
(varint len, bytes)   currentOutpoint, UTF-8
```

`deserializeUMPToken` (:2721-2817) reads versions 1 (no profiles flag), 2
and 3, refuses a field of length 0 or over 16 MiB, refuses KDF params
outside `isValidKdfConfig`, and requires an outpoint matching
`^[^\s.:]+[.:]\d+$` of at most 128 characters. The varint is Bitcoin's
(`Writer.writeVarIntNum`).

The on-chain form (`tokenFields` / `parseOutput`, :1229-1254,
:1376-1416) carries the same fields as PushDrop fields, with `[3]`,
the algorithm and the params JSON as fields 11 to 13 (12 to 14 when
profiles are present; `findKdfVersionFieldIndex`, :124-131).

### The reference's snapshot

`saveSnapshot` (:1960-2004): `u8 2 | 32-byte snapshotKey | 16-byte
activeProfileId | SymmetricKey(snapshotKey).encrypt(rootPrimaryKey(32) |
varint len | serializeUMPToken)`. `loadSnapshot` (:2012-2078) also reads
version 1 (no profile id). The snapshot key sits beside its ciphertext,
so a reference snapshot holds the root primary key in the clear in
effect: it is a session cache, not a password-protected file.

## 3. What the toolbox holds today (v1)

- `Profile { name, id (16), primary_pad (32), privileged_pad (32),
  created_at, updated_at }`, camelCase JSON, base64 bytes.
- `create_profile(name, password)`: draws a 32-byte primary key and a
  16-byte id; `derived = PBKDF2-HMAC-SHA512(password, salt = id,
  config.password_rounds)`; `primary_pad = primary_key XOR derived`.
- `switch_profile(id, password)`: `primary_key = primary_pad XOR
  PBKDF2(password, id)`; the wallet is built with it and the host's
  privileged key (`provide_privileged_key_manager`). A wrong password is
  not detected: it builds a different wallet.
- `save_snapshot()`: `32-byte key | SymmetricKey(key).encrypt(JSON {
  profiles, default_profile_id })`; `load_snapshot` reads it. This is the
  toolbox's key file: the bytes a host persists. Its key sits beside its
  ciphertext; the password protects the keys because only `pad XOR
  PBKDF2(password, id)` yields them.
- `UmpToken { version: 1, key_encrypted, profiles_encrypted }` and
  `WalletSnapshot { version: 2, snapshot_key, active_profile_id: String,
  encrypted_payload }` are public types no code writes or reads.
- `privileged_pad` is drawn and never used.

### The mapping

| v1 | the reference | the new key file |
|----|---------------|------------------|
| `PBKDF2(password, id)` | `passwordKey`, salt `passwordSalt` | opens the v1 record only; a fresh `passwordSalt` salts the new `passwordKey` |
| `primary_pad` | `Profile.primaryPad` | the token's pad is `primary XOR root` around a fresh random root, so the profile's key is unchanged; the v1 pad stays under `"v1"` until it is dropped (4.3) |
| `privileged_pad` | `Profile.privilegedPad` | kept, drawn when empty |
| id | `Profile.id` | kept; no longer a salt |
| (none) | presentation key, recovery key, root privileged key | drawn at the token's creation |
| `UmpToken` | `UMPToken` | the reference's fields and binary layout |
| `WalletSnapshot` | `saveSnapshot` layout | the reference's binary layout |

## 4. The new format

### 4.1 The token types (`src/managers/ump_token.rs`)

- `UmpToken`: the reference's fields under their snake_case names
  (`password_salt`, `password_presentation_primary`, ...,
  `profiles_encrypted: Option`, `ump_version: Option<u8>`,
  `password_kdf: Option<PasswordKdf>`, `current_outpoint: Option<String>`),
  with `to_bytes` and `from_bytes` in the reference's binary layout
  (section 2), versions 1, 2 and 3 read, 3 (or 2 for a legacy token)
  written. The format is binary, so the reference's names are the field
  names and the byte order is the layout; there is no JSON form.
- `PasswordKdf { algorithm, iterations, memory_kib, parallelism,
  hash_length }` with `KdfAlgorithm::{Pbkdf2Sha512, Argon2id}`, checked
  by the reference's `isValidKdfConfig` bounds; the params JSON written
  with the reference's key names and order.
- `WalletSnapshot`: the reference's snapshot layout (`version`,
  `snapshot_key`, a 16-byte `active_profile_id`, `encrypted_payload`) with
  `to_bytes` and `from_bytes`. The manager's key file does not use it (it
  would put the root key on disk under a key beside it); it is the
  reference's session snapshot for a host that wants one.
- `derive_password_key(token, password)`: the reference's
  `derivePasswordKey` (`src/managers/ump_kdf.rs`), PBKDF2-SHA512 at the
  token's iterations, 7777 when the token has no metadata, argon2id at
  the token's parameters (section 5).

The current outpoint: the toolbox publishes nothing (no interactor; rule
3), so its tokens carry `current_outpoint: None`. The reference has no
convention for a token not on chain: `createNewUser` publishes before
anything serializes (:1900), `serializeUMPToken` throws without an
outpoint (:2649-2651), `saveSnapshot` likewise (:1972), and
`deserializeUMPToken` refuses any outpoint not matching
`^[^\s.:]+[.:]\d+$` (:2786-2789), the empty one included. So the toolbox
writes `UNPUBLISHED_OUTPOINT`, `<64 zeros>.0`: the null txid at index 0,
which the reference's pattern takes and no transaction has (the null
txid marks a coinbase input; it is never a transaction's id). The toolbox
reads it back as `None` and, as the reference, refuses an empty or
malformed outpoint. The Rust-to-reference vector opens on the reference's
code with no locator set (section 7).

The params JSON: the reference's constructor fills every field of its
`Required<KdfConfig>` (:1679-1686), so a reference PBKDF2 token's params
read `{"iterations":7777,"memoryKiB":131072,"parallelism":1,"hashLength":32}`
whatever the algorithm; `PasswordKdf::pbkdf2` fills the same three, so
the toolbox writes those bytes too. The reference's writer is
deterministic there (a fixed key order, `JSON.stringify` of integers), so
a Rust token and a reference token at the same KDF differ in the salt,
the ciphertexts (fresh IVs and keys) and the outpoint, and nowhere else:
the version byte, the flags, `umpVersion`, the algorithm and the params
are byte-identical (`the_reference_written_v3_token_opens_to_its_recorded_root_keys`
compares the reference's params with `PasswordKdf::pbkdf2(7777)`'s).

### 4.2 The key file (v3)

The reference has no password-protected local file: its token lives on
chain and its snapshot is a cleartext-equivalent cache. The toolbox's key
file stays its own container, and every secret in it is a reference UMP
token:

```json
{
  "keyFileVersion": 3,
  "defaultProfileId": "<base64 16 bytes>" | null,
  "profiles": [
    {
      "name": "...", "id": "<base64 16>", "primaryPad": "<base64 32>",
      "privilegedPad": "<base64 32>", "createdAt": "<RFC 3339>", "updatedAt": "<RFC 3339>",
      "ump": { "presentationKey": "<base64 32>", "token": "<base64 serializeUMPToken>" },
      "v1": { "primaryPad": "<base64 32>" }
    }
  ]
}
```

- One token per profile. The toolbox's profiles carry their own
  passwords, where the reference has one password per account, so each
  toolbox profile is a reference account whose `profilesEncrypted` holds
  that one profile. The toolbox's key for profile p is `rootPrimaryKey
  XOR p.primaryPad`, the reference's derivation of a non-default profile
  (:2286); a reference host that opens the token and calls
  `switchProfile(p.id)` builds the same key.
- `presentationKey` is held in the file. The toolbox's unlock is the
  password alone (`switch_profile(id, password)`), so the presentation
  factor is the host's file, as v1's pad was. The token is the reference's
  and opens with the reference's presentation-and-password and
  presentation-and-recovery flows; a host that wants the presentation key
  off the disk is a later question, not this format's.
- The recovery key is drawn with the token and recoverable after an
  unlock (`recovery_key(id, password)`), unwrapped from field 10 with the
  root privileged key. The reference's manager hands the recovery key to
  its host twice: `recoveryKeySaver(recoveryKey)` at creation (:1840) and
  the public `getRecoveryKey()` (:2356-2361); `recovery_key` is the
  second, kept for that reason.
- The root privileged key is drawn with the token. The wallet the toolbox
  builds still takes the host's privileged key manager
  (`provide_privileged_key_manager`), unchanged; the token's privileged
  key wraps fields 8 to 10 and serves the reference's flows.
- The profile's fields are the clear index v1 already had (name, id, pads, times):
  `get_profiles` lists before an unlock. At an unlock the pads are read
  from the decrypted `profilesEncrypted`, never the clear copy.
- `"ump"` absent marks a v1 record carried until its own unlock (4.3);
  `"v1"` beside `"ump"` is the v1 record kept after its migrating unlock
  until a later unlock opens the token. The clear `primaryPad` of a record
  with a token is the token's pad, which yields no key without the
  token's root. The exports (`export_profile`, `export_profile_json`,
  `backup_all_profiles`) write the same record, so an exported v3 profile
  keeps its token (and its `"v1"` while kept); a v1 export (no `"ump"`)
  imports as a v1 record.
- New tokens: `argon2id` at the reference's defaults (iterations 7,
  memoryKiB 131072, parallelism 1, hashLength 32; section 5), `umpVersion`
  3, a 32-byte salt from `crate::rng::os_fill` (#191), and every key above
  drawn the same way. `config.password_rounds` is the v1 records'
  derivation alone. Before #593 they were `pbkdf2-sha512` at
  `config.password_rounds`; such a token still opens at its own KDF.
- `profilesEncrypted` is the reference's JSON (`name`, `id` and pads as
  byte arrays, `createdAt` in whole seconds) under the root primary key.

`save_snapshot` writes this container; `load_snapshot` reads it and v1.
`open_key_file(path)` reads a file (v1 or v3; an absent file is an empty
manager) and remembers the path; `save_key_file()` writes the container
to a sibling `<name>.tmp-<16 hex>` in the same directory, syncs it,
renames it over the file and syncs the directory, so a reader sees the
old file or the new one and the rename survives a crash. A sibling of
that shape left by an interrupted write holds key material, so
`open_key_file` overwrites it with zeros in place, syncs it and removes it
(best effort on a copy-on-write file system, where the overwrite may land
on new blocks). A writer in the key directory can already replace the key
file, so the cleanup never follows a link and never writes through a
shared inode: a symlink, or on unix a regular file with more than one
link, is removed by name alone, and off unix, where std gives no link
count, every sibling is removed unzeroed. The link count is read again on
the opened file, so a link made between the entry's read and the open is
removed by name, never written. A sibling the process cannot open (another
uid's leftover) is removed unzeroed and the open proceeds; an error from
the removal itself fails the open, as the key file's write would.

Key material in memory: the derived keys, the five factors, the opened
root, the presentation key (`UmpEntry` and the on-disk `UmpRecord`), the
kept v1 pad (`V1Record`), the decrypted profiles (`RefProfile`'s pads,
zeroed by its `Drop`), the decrypted v1 payload, the file's bytes read
and the encoded bytes written are each `Zeroizing` or zeroed on drop.
The factor wrapping derives the BRC-42 key itself and encrypts the
borrowed plaintext, where `ProtoWallet::encrypt` would have taken an owned
copy into `EncryptArgs` and dropped it unzeroed. The encoder writes into a
buffer sized ahead (8 KiB a profile) so serde does not grow it and free a
copy. Each copy's end on the write path: the base64 text of a pad or key,
encoded (`base64_bytes::serialize`) or decoded from
(`base64_bytes::deserialize`), is a `Zeroizing<String>` dropped once
serde has copied it into the buffer or the bytes out of it; the encoded
file is a `Zeroizing<Vec<u8>>` from the encoder to the write, the unlock's
commit and `save_key_file` alike, moved into std's blocking write, which
writes from it (tokio's file would copy it into a buffer of its own, freed
unzeroed), and zeroed when that write ends. Not covered: the key handed
to the host's wallet builder (its signature takes a `Vec<u8>`; that copy
is the host's), serde_json's scratch buffer for a string with escapes
(base64 has none), `save_snapshot`'s return (the caller's), the kernel's
page cache, and what bsv-rs and `ring` hold inside their own types.

### 4.3 The read path and the migration

- v3: `switch_profile(id, password)` derives the password key from the
  token's salt and KDF, opens `passwordPresentationPrimary` with
  `presentationKey XOR passwordKey`; a GCM tag that does not verify is a
  wrong password (`Error::AccessDenied`), and nothing is written.
- v1 (a `load_snapshot` of v1 bytes, or an `"ump"`-less record): opens
  as today, `pad XOR PBKDF2(password, id, config.password_rounds)` (the
  `use_pbkdf2: false` fallback kept for files written with it).
- The migration (#594, the owner's word of 2026-11-02 14:4xZ, option (a)).
  A v1 record has no password check, so the migrating unlock cannot tell
  a mistyped password from the right one. The token therefore joins the
  v1 record and does not replace it:
  1. The migrating unlock (a v1 record alone): v1 opens, `key = v1_pad XOR
     PBKDF2(password, id)`, and once the wallet is built a token is sealed
     around it: a fresh random root primary key, the token's pad `key XOR
     root` (so `root XOR pad`, the reference's derivation, is still the
     v1 key and the funds do not move), a fresh salt, a fresh password key
     at the configured KDF, fresh presentation, recovery and root
     privileged keys. The file is written with the token and the v1
     record (`"v1"`) both.
  2. A later unlock (both records): the token is tried before v1. If
     it opens, its GCM tag is the proof that this password is the one the
     token was sealed with, so two unlocks agreed; the v1 record leaves
     and the file is written with the token alone.
  3. If the token refuses at that later unlock, v1 opens (as it always
     did) and the token is re-sealed around that key under the arriving
     password, and the file is written with the re-sealed token beside
     the unchanged v1 record (the keeper's reading of 2026-11-02 19:5xZ,
     (ii)). The state "a token sealed with password X, the v1 record,
     password Y arrives" is the same whether X was a typo and Y the right
     password or X the right one and Y a typo: nothing in the file tells
     them apart, so the v1 record never leaves on a token that refused.
     Writing the re-seal makes the file follow the latest unlock: a typo
     at the migrating unlock is repaired on disk by the next right
     unlock, in this process or another (the round before held the
     re-seal in memory, so a typo stayed on disk until two right unlocks
     met in one process).
  The loss condition, the one case the v1 record is lost: the same wrong
  password at two consecutive unlocks seals the token under it at the
  earlier and opens it at the later, which drops the v1 record, and the
  right password is refused afterwards. A host closes it in its wallet
  builder: `switch_profile`'s call of the builder
  (`cwi_style_wallet_manager.rs`) comes between the derivation and the
  write, so a builder that
  checks the derived identity key against the storage's users and
  returns an error refuses the unlock with nothing written.
  With a key file open each write is a sibling, sync, rename and
  directory sync, before the wallet is installed; a failed write fails
  the unlock and leaves memory and file as they were. A failed unlock
  (unknown profile, no privileged key manager, a builder error) writes
  nothing at either step. The unlock reads its record before the KDF; the
  commit, under the profiles' write lock, installs the next record only
  while the profile still holds the record the unlock read, and refuses
  with `Error::InvalidOperation` otherwise (a delete or an import during
  the KDF, or another unlock of the profile that wrote before it: the caller
  retries, and the retry derives against the record now held; installing
  the later unlock's successor would overwrite the earlier's write, a
  dropped v1 record put back, and re-deriving under the write lock would
  hold every profile behind a KDF); the file then written is the manager's profiles as they are
  at the commit, the bytes `save_key_file` would write.
- A v1 file of several profiles migrates one profile per unlock: each
  profile has its own password. The file is v3 from the earliest migrating
  unlock on, with the not-yet-unlocked v1 records inside it.
- A reference-written token opens: v3 at its KDF (pbkdf2-sha512 at its
  `iterations`, argon2id at its four parameters), legacy (no metadata,
  the reference's version 2 serialization) at 7777. A record
  with a token alone is never re-written by an unlock.
- The salt: after step 2 the file holds nothing derived at the profile
  id. The token's root is a fresh draw, so no blob opens under
  `PBKDF2(password, id)` (the round before this fix sealed the token
  around the v1 derived key itself, so a guess at the profile-id salt
  could be tested against the token's GCM tags, which v1 never offered),
  and the clear pad is the token's, which yields no key without the
  root. Until step 2 the v1 record stays, and with it v1's exposure (the
  profile-id salt, a guess testable against any public key the wallet
  has shown); a host that never unlocks twice keeps it.

#### The factor model, and what an offline guess can test

The reference's token has three factors, any two of which open the root:
the password, the presentation key and the recovery key (fields 1 to 3).
The token itself is public (it sits on chain, found by
`presentationHash`); the presentation key is held off chain by the user
(a device, a WAB). A holder of the token alone cannot test a password
guess: field 1 needs `presentationKey XOR passwordKey`.

The toolbox's key file holds the presentation key beside the token, as
v1 held its pad: the unlock is the password alone. So the toolbox's file
has one factor. A holder of the file can test a password guess offline
against field 1's GCM tag, at one derivation of the token's KDF a guess
(argon2id at 128 MiB and 7 passes for a token written since #593, one
PBKDF2-SHA512 of 7777 rounds for one written before), with a per-token
random salt (no precomputation across files or profiles). v1 offered no tag, but a guess yields a candidate key whose
public key can be compared with any key the wallet has shown (its
identity key), so v1 was one factor too, at the profile-id salt. That
the toolbox file is one factor is a finding for the owner's items
(argon2id, #593, raises the cost of a guess; a presentation key off the
file is a host's design), not this change's to alter.

## 5. Argon2id (stage two, #593)

The reference's default for new tokens is argon2id (:1677) through
hash-wasm (`utility/hashWasm.ts`): `derivePasswordKey` (:330-347) calls
`argon2id({ password, salt, iterations, memorySize: memoryKiB,
parallelism, hashLength, outputType: 'binary' })`, version 0x13, no
secret, no associated data. The toolbox derives the same through
RustCrypto's `argon2` (`=0.5.3`, the owner's dependency word of
2026-11-02 14:4xZ; no default features, so no `password-hash` and no
`rand`; the `zeroize` feature), in `src/managers/ump_kdf.rs`. The memory
blocks (memoryKiB KiB, 128 MiB at the default) are the derivation's own
`Zeroizing` buffer, zeroed when it returns.

- The parameters are the token's, bounded as `isValidKdfConfig` bounds
  them (iterations 1 to 20, memoryKiB 1 to 262144, parallelism 1 to 16,
  hashLength 1 to 64, each present). Outside them the token is refused
  with the reference's text: `UMP token contains unsupported or unsafe
  KDF parameters.` at derivation (:316), `Serialized UMP token contains
  unsupported or unsafe KDF parameters.` at the reader (:2781).
- A `hashLength` other than 32 derives that many bytes, as hash-wasm
  does, and the open refuses it with the reference's `XOR length
  mismatch: 32 vs <n>` (:2437-2441): the reference throws there too, so
  no such token opens in either. A set inside the bounds that Argon2
  itself refuses (memory under 8 KiB a lane, output under 4 bytes;
  hash-wasm refuses the same) is an error naming argon2id.
- New tokens are argon2id at the reference's defaults, iterations 7,
  memoryKiB 131072, parallelism 1, hashLength 32 (:88-91), whose params
  JSON is `{"iterations":7,"memoryKiB":131072,"parallelism":1,"hashLength":32}`,
  the reference's byte for byte. The owner's word: the two wallets are
  interchangeable on the key file. The migration (4.3) and every re-seal
  write the same default. pbkdf2-sha512 tokens, the toolbox's before #593
  and the reference's with `kdfConfig: { algorithm: 'pbkdf2-sha512' }`,
  still open at their own iterations.
- The cost: each unlock of an argon2id token at the default allocates
  128 MiB; one derivation took under 0.4 s in the test profile on the
  lane's machine. The dev profile builds `argon2` and `blake2` at
  opt-level 3 so the tests derive at the default.

## 6. What a host sees

- The key file's version moves from v1 to 3 at the next save after this
  change or at the migrating unlock, whichever comes earlier.
- Since #593 a token the toolbox writes is argon2id at the reference's
  default; opening it needs an argon2id-capable toolbox. A toolbox of
  #560's stage one refuses that profile at derivation with its named
  reason (`Error::InvalidOperation`, "UMP token asks argon2id; this
  toolbox derives pbkdf2-sha512 only"); a host that may roll back keeps a
  copy of its key file before upgrading. A pbkdf2 token already in a file
  keeps its KDF until it is re-sealed. An older
  toolbox cannot read a v3 file (its `load_snapshot` takes the leading 32
  bytes as a key and fails the GCM tag); a host that may roll back keeps
  a copy of its v1 file before upgrading.
- A wrong password on a migrated profile is now an error
  (`Error::AccessDenied`) where v1 built another wallet.
- The public types that move: `UmpToken` (the reference's fields, no
  `new(key_encrypted, profiles_encrypted)`), `WalletSnapshot`
  (`active_profile_id: Vec<u8>` of 16 bytes, binary layout), both without
  their serde derives (the reference's bytes are their form), and the new
  `PasswordKdf`, `KdfAlgorithm` (`UNPUBLISHED_OUTPOINT` is crate-private:
  `to_bytes` writes it, `from_bytes` reads it as `None`). Three new
  manager methods: `open_key_file` and `save_key_file` (the migration
  re-writes the file it was opened from, so the manager must know the
  path) and `recovery_key` (the reference's `getRecoveryKey`, 4.2).
  Nothing else public moves.
- A host that never unlocks a migrated profile twice keeps the v1 record
  (and the profile-id salt) in its file beside the token.
- No host of this repository's line reads the key file today
  (bsv-wallet-cli at origin/beta has no `CWIStyleWalletManager` call);
  the CHANGELOG's host note is for bsv-wallet-cli#386.

## 7. The witnesses

In `src/managers/ump_token_tests.rs` (the token, the KDF and the
vectors both ways) and `src/managers/cwi_style_wallet_manager_tests.rs`
(the key file and the migration):

1. A new profile's token carries a 32-byte salt distinct from the
   profile id, `umpVersion` 3 and argon2id at the reference's default
   (#593); the salt is an OS draw. A migrated v1 record's token carries
   the same default.
2. A v1 file opens with its password; after the unlock the file holds
   `keyFileVersion` 3, a token with a fresh 32-byte salt and the v1
   record beside it, and the same profile key decrypts from the token; a
   second unlock opens the token and the file holds the token alone.
3. The #594 exit under (ii): a typo at the migrating unlock, then the
   right password re-seals the token on disk under it beside the intact
   v1 record, and the next right unlock drops v1 (w1); a wrong password
   beside v1 re-seals the token under it, the v1 record's bytes and the
   rest of the file unchanged, and the right one then opens through v1
   (w2); the loss condition, the same wrong password twice, drops v1 and
   the right one is refused (w3); typo, right, right across two
   instances completes the migration to the v1 primary key (w4). A
   failed unlock at either step (no privileged key manager) and a wrong
   password on a token alone leave the file byte-identical; a
   reference-written token is never re-written (w5).
4. After the drop, `PBKDF2(password, profile id)` opens no blob of the
   token and the clear pad yields no key with it; `PBKDF2(password,
   new salt)` opens the token to the v1 primary key. One changed byte of
   a v3 salt refuses the right password.
5. A profile deleted during its unlock refuses the commit and nothing is
   written; a stale write sibling is removed at the next open; the
   records' key material is zeroed on drop.
6. A reference legacy token (no KDF metadata) opens at 7777 rounds, and
   does not at another count.
7. The token's binary layout round-trips; the reference's bounds refuse
   a zero-length field, unsafe KDF params and an empty or malformed
   outpoint.
8. The reference's vectors (`test_vectors/keys/ump-token-reference.json`):
   a token written by the reference's own code under Node with recorded
   factors; the Rust reader recovers the recorded root primary and
   privileged keys, the profile's key and the recovery key, from both the
   v3 and the legacy serialization. And the other way: a token the Rust
   code writes is opened by the reference's `deserializeUMPToken`,
   `providePresentationKey` + `providePassword` and `switchProfile` under
   Node (`test_vectors/keys/ump-token-reference.mjs`), the recorded
   result beside it. The Rust token carries no outpoint (written as
   `UNPUBLISHED_OUTPOINT`) and the reference's reader takes it with no
   locator set. The reference's unlock needs a `UMPTokenInteractor`; the
   script stands one up locally (it answers the lookup with the token
   under test and records `buildAndSend` with a fixed outpoint), so no
   network is reached.
9. The argon2id vectors (#593, `ump-token-reference-argon2id.json`,
   `ump-token-rust-argon2id.json` and its `.result.json`): the
   reference's manager built with `kdfConfig` omitted, its default, under
   Node with hash-wasm 4.12.0, writes a token the Rust reader opens to the
   recorded keys, through `open_token` and through `switch_profile`; a
   token the Rust code writes at the same default opens on the
   reference's code (`node ump-token-reference.mjs verify argon2id`,
   eleven checks, all ok). argon2id at its own parameters opens and
   another parameter set derives another key; every bound refuses with
   the reference's text; a `hashLength` of 64 is refused with the XOR
   text; a set Argon2 refuses is an error.
