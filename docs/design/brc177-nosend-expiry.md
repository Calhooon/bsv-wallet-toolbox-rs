# BRC-177 noSend expiry and the reclaim quarantine at sync (#312)

A decision brief for the owner, no code. Labels: `[X]` read at the pin named
here; `[D]` told (the issue, its comments, the brief). Pins: the reference
ts-stack `edf6e03` (paths below are under `packages/wallet/wallet-toolbox/`,
written `WT/`); BRC-177 at BRCs `5119a9b` (`wallet/0177.md`, 437 lines); the
stack's charters at bsv-stack-lean `2c3f619`; the CLI at bsv-wallet-cli
`origin/beta` `0bd48af`; this crate at beta `02b1a87`. The Go reference
(`go-wallet-toolbox` at `e824899`) has no `noSendExpiry`, `brc177` or
`BRC-177` anywhere `[X]`.

## 1. The reference flow

### 1.1 What BRC-177 binds a wallet to

The label is the whole public surface: `p nosend expiry seconds|timestamp|blockheight <n>`
(`0177.md:27-34`); "No new wallet method, transaction field, or script
template is required" (`:17`), and "no new BRC-100 method or response field
and no new public action status" (`:296`). The binding sentences:

- `:53` "a wallet that does not support module ID `nosend` MUST reject the
  request rather than treating the label as ordinary metadata."
- `:55` the authoritative storage or monitor must establish support before
  prefunding.
- `:72-78` prefund: one dedicated anchor output, broadcast and accepted, then
  the protected action spends the anchor as its only automatic input, with no
  wallet change (`:79-81`).
- `:184` the deadline is persisted at activation; `:190` it is immutable.
- `:256-262` at the deadline: reject further `signAction`, check the action
  and anchor, reclaim the anchor to the `default` basket, retry until proven.
- `:265` a pre-signed reclaim lets a keyless storage monitor enforce it.
- `:275` `abortAction` on a released root starts the reclaim.
- `:294` "A reclaim output MUST remain unavailable for wallet coin selection
  until the wallet has validated proof that the reclaim won; synchronization
  alone is not proof." This is the sentence the quarantine serves.

The BRC at the stack's pin is revision 2 (`p nosend expiry never`, `p nosend
batch`, portable lifecycle statements, `:36`, `:194-216`, `:310-361`,
capability `brc177-nosend=2` at `:365`). The reference implements revision 1
only: its parser accepts three modes (`WT/src/utility/brc177NoSendExpiry.ts:108`)
and exactly one `p nosend ` label (`:93-98`), so `never` and `batch` are
rejected there, as `:368` asks of older implementations.

### 1.2 States

`Brc177NoSendExpiryState` (`brc177NoSendExpiry.ts:15-25`): `preparing`,
`unsigned`, `signed`, `revocation-requested`, `broadcast`, `reclaiming`,
`reclaimed`, `target-won`, `conflicted`, `cancelled`. Sync merges them by a
rank, not a clock (`:32-60`: preparing 0, unsigned 1, cancelled 2, signed 3,
revocation-requested 4, conflicted 5, broadcast 6, reclaiming 7, reclaimed 8,
target-won 9).

| from | to | writer | where |
|---|---|---|---|
| (none) | `preparing` | the door, protected kind | `WT/src/storage/methods/createAction.ts:730-737` |
| `preparing` | `unsigned` | `armNoSendExpiry` (reclaim stored) | `WT/src/storage/methods/noSendExpiry.ts:446-465` |
| `unsigned` | `signed` | `processAction` at release | `WT/src/storage/methods/processAction.ts:575-577`, CAS `:638` |
| `preparing`, `unsigned` | `cancelled` | abort, or the lifecycle when due | `WT/src/storage/StorageProvider.ts:766-770`; `noSendExpiryLifecycle.ts:254-258` |
| `signed` | `revocation-requested` | abort, or the lifecycle when due | `StorageProvider.ts:803-807`; `noSendExpiryLifecycle.ts:740-755` |
| `revocation-requested`, `conflicted` | `reclaiming` | lifecycle, anchor a UTXO, target `unknown` | `noSendExpiryLifecycle.ts:757-791`, `:382-389` |
| `revocation-requested` | `conflicted` | lifecycle, anchor not a UTXO | `:779-782` |
| `reclaiming` | `reclaimed` | lifecycle, the reclaim's proof stored | `:395-454` |
| any active | `broadcast`, `target-won` | lifecycle, the target observed or proven | `:141-191`, `:456-528` |

The lifecycle is `processNoSendExpiryLifecycle` (`noSendExpiryLifecycle.ts:885-896`,
896 lines in all), run by the Monitor's `TaskNoSendExpiry` every five seconds
and on each new header (`WT/src/monitor/tasks/TaskNoSendExpiry.ts:10-48`;
`WT/src/monitor/Monitor.ts:306,609`), on the active store only (`TaskNoSendExpiry.ts:37`).

### 1.3 Columns

Migration `2026-08-30-001 add brc177 nosend expiry state`
(`WT/src/storage/schema/KnexMigrations.ts:23`, body `:275-297`): thirteen
columns on `transactions`, every one `nullable()` (`:278-290`), and two
indexes, `(noSendExpiryState, noSendExpiryDeadline)` and
`(userId, noSendExpiryReclaimTxid)` (`:291-292`). This crate's
`docs/STORAGE-WIRE.md:223` lists the same thirteen names (it cites
`T/Transaction:40-54`; at `edf6e03` they sit at `TableTransaction.ts:38-51`).
The reference's column list without raw bytes names twelve of them, leaving out
`noSendExpiryReclaimRawTx` (`TableTransaction.ts:68-82`).

| column | written by | read by |
|---|---|---|
| `Mode`, `Value`, `Deadline`, `AnchorTxid`, `AnchorVout` | the door, `createAction.ts:732-737` | release check `processAction.ts:466-470`; `isDue` `noSendExpiryLifecycle.ts:79-85`; reclaim and winners `:763-770`, `:428-437` |
| `State` | every row of the table in 1.2 (by compare-and-set, `StorageKnex.ts:1132-1141`) | abort `StorageProvider.ts:713-716,800-838`; lifecycle pages `noSendExpiryLifecycle.ts:866-883`; both quarantines |
| `ReclaimTxid`, `ReclaimRawTx`, `ReclaimDerivationPrefix`, `ReclaimDerivationSuffix`, `ReclaimSatoshis` | `armNoSendExpiry`, `noSendExpiry.ts:455-465` | `insertReclaim` `noSendExpiryLifecycle.ts:292-380`; the quarantines; `protectNoSendExpiryReclaimInputOnFailure` `StorageProvider.ts:1101-1118` |
| `ReleasedAt` | `processAction.ts:577` | merge only (earliest wins, `EntityTransaction.ts:69-72`) |
| `ObservedAt` | the lifecycle's observation | merge only (`EntityTransaction.ts:73-76`) |

### 1.4 The flow and its callers

1. `Wallet.createAction` routes any `p nosend ` label to
   `createNoSendExpiryAction` (`WT/src/Wallet.ts:1162-1165`).
2. `createNoSendExpiryAction` (`WT/src/signer/methods/createNoSendExpiryAction.ts:195-313`):
   refuses unless the active storage advertises `brc177NoSendExpiry.version === 1`
   (`:200-203`; the capability is `WT/src/sdk/ActionBatch.interfaces.ts:15-17`,
   advertised at `StorageProvider.ts:428` when the store persists the
   lifecycle); `prepareNoSendExpiry` sizes the anchor (`:223`); the funding
   action is signed, processed and must be accepted (`:35-56`, `:229-230`);
   `activateNoSendExpiry` plans the protected action on the anchor (`:236-241`);
   the reclaim is built and signed to a fresh BRC-29 change output (`:98-146`),
   checked and stored by `armNoSendExpiry` (`:160-193`); then the action is
   signed and processed as `noSend`, or returned signable (`:275-312`).
3. The door's `brc177` metadata (internal, on the validated args,
   `brc177NoSendExpiry.ts:62-79`): `funding` adds the exact anchor output
   (`createAction.ts:810-815`); `protected` takes no other wallet input
   (`:1201-1205`) and no change (`:1183`, `:1705`).
4. `signAction` on a protected action rejects `sendWith` and
   `returnTXIDOnly` and forces `noSend` (`WT/src/signer/methods/signAction.ts:136-144`).
5. The storage methods: `prepareNoSendExpiry`, `activateNoSendExpiry`,
   `armNoSendExpiry` on the provider, the manager and the client
   (`WT/src/sdk/WalletStorage.interfaces.ts:89-91,192-200`;
   `StorageProvider.ts:474-501`; `WalletStorageManager.ts:540-565`;
   `StorageClientBase.ts:525-532`), each refused on an inactive store.
6. The permissions manager prices and shapes the request
   (`WT/src/WalletPermissionsManager.ts:93-115,798-816,4396`).

### 1.5 The abort codes and who answers them

`AbortActionStorageResult` adds four internal sentinels
(`StorageProvider.ts:148-151`), translated before `abortAction` returns
(`:906-926`); the public `AbortActionResult` stays `{ aborted }`:

| state at abort | sentinel | answer |
|---|---|---|
| `signed` (moved to `revocation-requested`, `:803-818`) or `revocation-requested` (`:820-825`) | `brc177-reclaim-requested` | runs the lifecycle once (`:908`); `aborted:false` if it ended `broadcast`, `target-won` or `conflicted`, else `true` (`:915-922`) |
| `reclaiming`, `reclaimed` | `brc177-reclaim-in-progress` | `aborted:true` (`:924`) |
| `cancelled` | `brc177-cancelled` | `aborted:true` (`:925`) |
| `broadcast`, `target-won`, `conflicted` | `brc177-target-protected` | `aborted:false` (`:926`) |
| `preparing`, `unsigned` | none | moved to `cancelled`, then the ordinary failure (`:766-771`) |

A row with a state on an inactive store throws `WERR_NOT_ACTIVE` (`:800-802`).
A failed reclaim keeps its anchor held instead of releasing it
(`:1101-1118`, called from `:1165-1168`).

### 1.6 The quarantines, exactly

Two in sync, one in the failure path above.

`quarantineUnresolvedBrc177Reclaim` (`WT/src/storage/schema/entities/EntityOutput.ts:11-26`):

```
if (!output.spendable || output.txid == null) return
target = one or none of findTransactions({ userId: output.userId,
                                           noSendExpiryReclaimTxid: output.txid })
if (target != null && target.noSendExpiryState !== 'reclaimed') output.spendable = false
```

Called on every incoming output: `mergeNew` after the ids are mapped and
before the insert (`:320-330`, call at `:327`), and `mergeExisting` when the
incoming row is newer, after `spendable` is copied and before the update
(`:332-361`, call at `:355`). The predicate reads the local store's target
row, so it sees the state the same chunk just merged: transactions come
before outputs in a chunk (`WT/src/storage/methods/getSyncChunk.ts:148-170`),
and the chunk carries whole transaction rows (`:157-161`). The hold is one way:
nothing in either merge sets `spendable` back to true.

`quarantineReclaimOutput` (`WT/src/storage/schema/entities/EntityTransaction.ts:125-141`),
called from `mergeExisting` (`:555-561`) when the local target is `reclaimed`
and the incoming row is `target-won` or carries a proof (`:518-520`): output
`vout 0` of the reclaim txid is set unspendable. The merge itself is by rank
(`:513-526`), so a later wall clock never regresses a state (`0177.md:294`).

The reference's own witnesses: `OutputTests.test.ts:352` ("reclaim outputs
stay quarantined when synchronized after a target winner") and
`TransactionTests.test.ts:280` ("sync advances lifecycle state but rejects
clock-skew regression"), both under `WT/src/storage/schema/entities/__tests/`.

## 2. What a TS-synced store does in the port today

The port has none of it `[X]`: `grep -rniE 'nosendexpiry|brc177|brc-177|reclaim' src`
finds nothing; the `transactions` table has no expiry column
(`src/storage/sqlx/migrations/001_initial.sql:108-124`; migrations 002 to 005
add none); `TableTransaction` has no field for them and ignores unknown
fields (`src/storage/entities/mod.rs:169-189`, `serde(default)` without
`deny_unknown_fields`), so an incoming chunk's thirteen values are dropped on
read. The charters name no expiry or reclaim word: `wallet.md` and
`storage.md` at `2c3f619` carry `nosend` only as a status (`wallet.md:127`);
a new word is the keeper's ruling.

The divergences, each with both sides:

1. **The coin the reference holds, the port offers.** A TS store B reached
   `reclaimed` (the reclaim output `spendable` true, `noSendExpiryLifecycle.ts:452`);
   a TS store C later stored the target's proof, so its row ranks
   `target-won` (`brc177NoSendExpiry.ts:55-56`). The port, syncing from B and
   C, receives the reclaim output with `spendable` true. The reference holds
   it (`EntityOutput.ts:23-24`, `EntityTransaction.ts:555-561`); the port's
   INSERT binds the incoming flag (`src/storage/sqlx/sync.rs:1994`) and its
   newer-row UPDATE does the same (`:1942`, `:1954`), and its transaction
   UPDATE carries no state (`:1796-1798`). The coin the chain already gave to
   the target is offered to the allocator. The same holds for any skew in
   which an output row says spendable while the local target is not
   `reclaimed`.
2. **Abort releases an exposed anchor.** A synced released target is a
   `nosend` row whose anchor input carries `spent_by` = the target. The port's
   abort accepts `nosend` (`src/storage/sqlx/abort_action.rs:53-59`) and
   releases every input it locked (`:648-664`, `spendable = 1, spent_by = NULL`).
   The reference never releases it: it requests a reclaim
   (`StorageProvider.ts:803-818`), as `0177.md:275` and `:377` require. A
   wallet that spends the released anchor races the recipient's signed bytes.
3. **No deadline is enforced.** A port that becomes the active store holds
   `signed` targets past their deadlines with nothing to reclaim them
   (`src/monitor/tasks/` has no counterpart of `TaskNoSendExpiry`). A synced
   `unsent` reclaim request would be posted by `send_waiting`
   (`src/monitor/tasks/send_waiting.rs:25`), and its output, inserted
   unspendable (`noSendExpiryLifecycle.ts:343-345`), is never released.
4. **The label is accepted as metadata.** Independently of sync, the port's
   door checks a label only for emptiness and length
   (`src/storage/sqlx/create_action/validation.rs:45-56`), so a
   `p nosend expiry seconds 300` request becomes an ordinary `noSend` action
   with ordinary change and no deadline: the case `0177.md:53` forbids. The
   reference on a store without the capability refuses
   (`createNoSendExpiryAction.ts:201-203`). The CLI's server takes BRC-100
   `createAction` from applications and has no `nosend` module, expiry or
   reclaim word (`git grep -i 'nosendexpiry|reclaim|brc.?177|expiry'` in its
   `src` at `0bd48af`: nothing); its `noSend` uses are statuses and flags
   (`src/commands/balance.rs:58`, `src/server/broadcast_follow_up.rs:73-76`).

Divergences 1 to 3 need a mixed deployment (a TS store that ran BRC-177
syncing into a port store); none is witnessed in a fixture here. Divergence 4
needs only an application that sends the label to a port wallet.

## 3. Three options

### (a) The divergence row now, BRC-177 as one 0.15 epic

Add D7 to `docs/ARCHITECTURE.md` section 7 citing `EntityOutput.ts:11-26`,
`EntityTransaction.ts:125-141,555-561`, `StorageProvider.ts:148-151,1101-1118`
and the four port lines of section 2; leave #312 open under the epic.

- Cost: one documentation commit. No schema, no public shape, no pin.
- Leaves open: all four divergences. Divergence 4 is a present breach of
  `0177.md:53` for any application, not only for mixed deployments.
- A variant, (a'), adds one behaviour change on 0.14: the door refuses any
  label starting `p nosend ` with a validation error, the reference's own
  answer on a store without the capability (`createNoSendExpiryAction.ts:201-203`)
  and BRC-111's rule (`0177.md:53`). One lane, about 30 lines and a fixture,
  no schema; the error is a new refusal of input that succeeds today, so it
  takes a CHANGELOG line.

### (b) The columns and the quarantine alone on 0.14

A migration `006` adding the thirteen nullable columns and the two indexes;
`TableTransaction` gains thirteen optional fields (a public struct, so a
public shape); sync reads and writes them (`fetch_transactions_for_sync`,
`upsert_transaction` at `sync.rs:1769-1852`) with the rank merge of
`EntityTransaction.ts:513-562`; both quarantines in `upsert_output`
(`sync.rs:1886-2016`) and in the transaction merge; `StorageClient` carries
the fields on the wire.

- What it protects: divergence 1, and only on a store that receives these
  rows from a TS store that ran BRC-177. Nothing in the port writes the
  columns; sync is their sole writer. On a port-only deployment it protects
  nothing.
- What it does not: divergences 2 to 4, unless the abort refuses any row with
  a state set (a small addition that answers `aborted:false` where the
  reference would reclaim: safe, not the same). Once the reference store
  reaches `reclaimed`, the port releases the coin only when a newer output row
  arrives, as in the reference.
- Cost: the schema (the owner's word), one public struct, about three lanes
  (migration and entity; sync merge and quarantines with fixtures; wire),
  and thirteen columns most of which the port stores and never reads.

### (c) The whole flow on 0.14

The lanes, each a PR, with the public shapes each moves:

| lane | content | public shape it moves |
|---|---|---|
| c1 schema | migration, thirteen fields, wire | `TableTransaction`; the schema |
| c2 sync | rank merge, both quarantines (as (b)) | none beyond c1 |
| c3 labels | parse and validate the three modes, refuse revision-2 forms | none (labels are the surface, `0177.md:17`) |
| c4 storage methods | `prepare_no_send_expiry`, `activate_no_send_expiry`, `arm_no_send_expiry`, a state compare-and-set, a capabilities read | the storage traits (all three implementors, CLAUDE.md "Trait hierarchy"); a capabilities type |
| c5 door | `brc177` metadata on the validated args: funding anchor output, protected anchor-only input, no change; release `unsigned` to `signed` in `process_action`; `sign_action` guards | the internal validated `CreateActionArgs`; no BRC-100 field |
| c6 signer | `create_no_send_expiry_action`: funding, broadcast and acceptance, BRC-29 reclaim pre-signed, arm | `Wallet` gains a routed path; no new method |
| c7 abort | the four sentinels, the inactive-store refusal, the held anchor on a failed reclaim | `AbortActionResult` unchanged; internal result enum |
| c8 monitor | `NoSendExpiryTask` and the lifecycle (896 lines in the reference), nudged on a new header | a new Monitor task and its options |
| c9 permissions | BRC-177 preflight in `WalletPermissionsManager` | its preflight shape |

- Cost: nine lanes, the schema, three trait additions, and a lifecycle about
  as large as the door. It ports revision 1 while the BRC at the stack's pin is
  revision 2; c3 must refuse `never` and `batch` as the reference does.
- The funding step broadcasts and needs acceptance (`0177.md:76`): every
  fixture of c6 and c8 runs on `MockWalletServices` and `mockito` (rule 3).

## 4. The keeper's reading and the recommendation

The keeper's word on the issue `[D]`: the design lanes are the crate's own and
run now; the owner's word comes with their brief, item 19.1 of 2026-10-28. The
captain's reading `[D]` (the issue's comment): record the divergence row now
and take BRC-177 as one epic, unless the owner wants it on 0.14. That is (a).

Against (a) alone: it records the sync divergence, which needs a mixed
deployment nobody has reported, and leaves divergence 4 standing, which needs
only one application sending a BRC-177 label to a port wallet. There the
port silently drops the promise the application asked for: the action keeps
its ordinary change reserved forever and no deadline is kept, while the
application believes the offer expires. That is the harm BRC-177 exists to
prevent (`0177.md:13-15`), and the BRC names the cheap answer (`:53`).

Against (b): it spends the schema, the owner's scarcest word, on the rarest
of the four cases, and leaves the port an active store that cannot keep a
deadline. Half the lifecycle in the schema invites the belief that the port
supports BRC-177.

Against (c) on 0.14: nine lanes and a lifecycle the size of the door for a
feature no consumer of this crate has asked for, ported at revision 1 while the
BRC moves to revision 2. It belongs in one epic with the BRC's revision fixed.

**Recommendation: (a'), the divergence row plus the door's refusal of
`p nosend ` labels on 0.14, and BRC-177 as one 0.15 epic.** The reason: the
refusal closes the only divergence a port-only wallet can reach today, it is
the reference's own behaviour on a store without the capability, and it
touches no schema, no pin and no public type; the sync quarantine then waits
for the columns, which wait for the flow that writes them, as the issue
already says. The divergence row should state plainly that a port store which
imports a TS store's BRC-177 rows can offer a held coin (divergence 1) and
release an exposed anchor on abort (divergence 2).

## 5. The exit tests

Each fixture is red on base `02b1a87` and green on the lane's tip; each guard
is shown to bite by a planted removal (rule 2).

**(a)** A test in the architecture tests (`oracle-56` shape) that
`docs/ARCHITECTURE.md` section 7 holds a row citing `EntityOutput.ts:11-26`.
**(a')** adds door fixtures in `src/storage/sqlx/create_action/tests/validation.rs`:

- `p nosend expiry seconds 300` with `noSend: true` is refused with a
  validation error and writes no row (red on base: the action is created);
- `p nosend expiry never`, `p nosend batch`, and two expiry labels are refused;
- `p nosendx`, `nosend expiry seconds 300` (no `p `) and `p other` are
  accepted as before (the prefix is exact, `brc177NoSendExpiry.ts:5,93`);
- planted: the refusal removed, the `seconds 300` fixture fails.

**(b)** sync fixtures in `src/storage/sqlx/sync.rs` tests, from a chunk shaped
as the reference's (`OutputTests.test.ts:352`, `TransactionTests.test.ts:280`):

- a new target row `{noSendExpiryState: 'reclaiming', noSendExpiryReclaimTxid: R}`
  and a new output `R:0` with `spendable: true` store `spendable = 0`
  (`mergeNew`, `EntityOutput.ts:327`);
- the same through a newer existing output row (`mergeExisting`, `:355`);
- state `reclaimed` keeps `spendable = 1`; another user's target does not hold
  it; an incoming `spendable: false` is unchanged;
- local `reclaimed` and an incoming `target-won` set `R:0` unspendable
  (`EntityTransaction.ts:555-561`);
- a newer incoming row with a lower rank does not regress the state;
- the migration on a 0.13 store leaves every existing row's thirteen columns
  null; an export carries the thirteen camelCase names (`STORAGE-WIRE.md:223`).

**(c)** all of (a') except the refusal of the three supported modes, all of
(b), and per lane: label vectors (the reference's
`WT/src/utility/__tests/brc177NoSendExpiry.test.ts`); the door's funding and
protected plans (anchor only, no change); `sign_action` refuses `sendWith`
and `returnTXIDOnly`; each row of the table in 1.5 as an abort fixture; the
lifecycle on `MockWalletServices`: a due `seconds` target moves to
`revocation-requested`, then `reclaiming` with an `unsent` reclaim request
when the target is `unknown` and the anchor a UTXO; a stored reclaim proof
moves it to `reclaimed`, the target `failed`, the reclaim output spendable;
an observed target moves to `broadcast` or `target-won`; an unreachable
service defers and never reclaims (`noSendExpiryLifecycle.ts:820-826`); a
`blockheight` deadline once triggered is not reactivated by a lower height
(`0177.md:218`); a store without the capability refuses before funding
(`createNoSendExpiryAction.ts:201-203`; the reference's
`test/Wallet/action/noSendExpiry.test.ts` and `TaskNoSendExpiry.test.ts`).
