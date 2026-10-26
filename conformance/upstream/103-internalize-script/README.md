# Toolbox #103: internalize accepts a failing spend

At ts-stack `edf6e03dad39bd7bc4fc6e73e64de3a2b6f805c9`, `internalizeAction`
checks AtomicBEEF structure and Merkle roots without executing the paying
transaction's unlocking scripts.

`vector.cjs` reproduces the toolbox fixture's payment with the reference's
`Wallet`, `WalletStorageManager`, and `StorageKnex` on SQLite `:memory:`.
Each case uses a fresh wallet. A local tracker accepts only the fixture's
synthetic root at height 100001. The parent locks 20,000 satoshis with
`02abcd87` (push `abcd`, `OP_EQUAL`); the payment sends 9,999 satoshis to the
fixture's BRC-29 derived P2PKH output. Only the unlocking push changes.

The program independently executes each spend with the reference's `Spend`
interpreter, calls `wallet.internalizeAction`, lists the default basket, and
reads the stored coin's spendable flag and transaction's received status.

| Case    | Unlocking script | Script valid | Accepted | Coin spendable |
| ------- | ---------------- | ------------ | -------- | -------------- |
| failing | `02abce`         | false        | true     | true           |
| valid   | `02abcd`         | true         | true     | true           |

Both stored transactions are `unproven` and `isOutgoing: false`. The reference
calls their managed output's purpose `change`. Spendable here means the stored
flag and inclusion in `listOutputs`; this vector does not attempt a later spend.

The keys are public throwaway fixture keys. No signing or chain access occurs.
Unexpected service methods and network connections fail immediately. A local
`postBeef` stub is supplied; the recorded runs never call it (`mockPosts: 0`).

## Run

Use Node 24.11 or newer and pnpm 10.33.2, the reference's package-manager pin.
The recorded run used Node v24.11.1. Start in a clean toolbox checkout containing
this directory. Dependency installation uses registry reads; the vector is offline.

```sh
VECTOR="$PWD/conformance/upstream/103-internalize-script/vector.cjs"
REFERENCE="$TMPDIR/ts-stack-edf6e03"
RUNS="$TMPDIR/upstream-103-runs"
if test -d /Volumes/Lanes; then
  RUNS=/Volumes/Lanes/out/bsv-wallet-toolbox-rs/upstream-103-vector
fi
mkdir -p "$RUNS"
git -C "$HOME/bsv/ts-stack" worktree add --detach "$REFERENCE" edf6e03
cd "$REFERENCE"
pnpm --filter '@bsv/wallet-toolbox...' install --frozen-lockfile \
  --ignore-scripts --store-dir "$HOME/bsv/targets/upstream-103-pnpm-store"
pnpm --filter @bsv/wallet-toolbox rebuild better-sqlite3
node "$VECTOR" "$PWD" > "$RUNS/run1.stdout" 2> "$RUNS/run1.stderr"
node "$VECTOR" "$PWD" > "$RUNS/run2.stdout" 2> "$RUNS/run2.stderr"
cmp "$RUNS/run1.stdout" "$RUNS/run2.stdout"
cat "$RUNS/run1.stdout"
wc -c "$RUNS/run1.stderr" "$RUNS/run2.stderr"
```

Reuse the detached checkout if it already exists and is clean at the exact pin.
The script checks the commit and clean status, and uses the installed reference
`esbuild` to compile its TypeScript sources in memory. It writes no build files
or database files. It does not import environment-dependent test setup helpers;
the storage setup follows `test/utils/TestUtilsWalletStorage.ts`.

`output.edf6e03.txt` is stdout verbatim from two byte-identical runs on
2026-10-26. Neither run emitted stderr. The date header records the actual UTC
run date, so a run on another day changes that header. No fields were stripped.

The checks were also exercised with temporary copies: replacing the tracker's
root predicate with `false` exited 1 with `The tx parameter must be valid
AtomicBEEF`; changing the failing input to `02abcd` exited 1 with `payment must
match the failing fixture`. Both changes were removed after the runs.

## Siblings and source reading

- Toolbox [issue #103](https://github.com/Calhooon/bsv-wallet-toolbox-rs/issues/103)
  and its comments record the ruling and the Rust fix at `603cecb`.
- [Failing fixture](../../../tests/fixtures/data/storage/internalize_refuses_a_failing_script.json)
  and [valid control](../../../tests/fixtures/data/storage/internalize_takes_a_valid_unproven_payment.json).
  Transaction construction follows [the seeder](../../../tests/fixtures/storage/seed.rs),
  including lock times 0, 1, 2 and the synthetic parent proof. The script asserts
  the failing fixture's exact txid and the BRC-29 locking bytes.
- The door tests are in `src/storage/sqlx/internalize_action.rs`, including
  `a_payment_whose_unlocking_script_fails_is_refused` and
  `the_verifier_refuses_the_failing_spend_and_takes_the_valid_one`.
- Stack scenario at `bsv-stack-lean@13203bf7f7401261a933aca7b2a97acd3c259ea0`:
  `corpus/scenarios/2026-10-24-wallet-internalize-unproven-payment-script-fails.md`.
- At `ts-stack@edf6e03`,
  `packages/wallet/wallet-toolbox/src/signer/methods/internalizeAction.ts:113`
  calls `ab.verify(chainTracker, false)`; storage repeats that check at
  `src/storage/methods/internalizeAction.ts:456` within the same package.
  `packages/sdk/src/transaction/Beef.ts:1020-1035` checks `verifyValid` and roots.

This vector witnesses T2 (checked evidence), T4 (the script's verdict), and
T6 (executable evidence). The captain sends this directory and its pinned output
to the stack keeper for the owner's upstream filing. No fix is proposed here.

Upstream: filed as [bsv-blockchain/ts-stack#1380](https://github.com/bsv-blockchain/ts-stack/issues/1380) under the owner's name on 2026-10-26 by the stack keeper, citing this vector at beta 82e822e, `internalizeAction.ts:456` and `Beef.ts:1020-1035` at edf6e03, and BRC-62 `0062.md:185`.
