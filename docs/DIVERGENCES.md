# The application-layer divergence register

The reference implementation is the TypeScript `@bsv/wallet-toolbox` and the
`@bsv/sdk` beside it (`~/bsv/ts-stack` at edf6e03). This crate conforms to it
wherever the reference decides something. The line between the registers:
a divergence with a filing cell (the other SDK's issue, or the toolbox's
reading with the charter line that rules it) is a row of `docs/ARCHITECTURE.md`
section 7 (D1 to D31, the gate's filing format), whether against a charter or
against the reference; a reorg divergence is a row of
`docs/REORG-DIVERGENCES.md` (R rows); a divergence whose behaviour of record
WAITS ON THE OWNER'S RULING, filed nowhere until he rules, is a row here, each
with the why, its scenario and its witness, numbered AD1, AD2 and so on, a
prefix of its own. When the owner rules, the row either goes (the code
re-converges on the reference) or moves to section 7 with its filing cell. A
row is deleted only by re-converging on the reference, never by deleting the
code; a row whose ruling is pending says so.

Conformed (no row needed): a merkle root a header service has at the proof's
height and that differs from the proof's root is refused, the reference's
invalid root (the port's `InvalidBeef`, class `InvalidParameter`); a tracker
that gives no answer in its word (a typed transport fault) never refutes a
root, as the reference's `isValidRootForHeight` throws and no verdict is read.

| # | Divergence | Reference behavior | Ours, and why | Where |
|---|---|---|---|---|
| AD1 | **A proof at a height the header service does not have** (#769, the keeper's reading of 2026-11-03 on LOW's L59 row; **the owner's ruling pending**, the keeper's recommendation: keep the hold) | `ChaintracksChainTracker.isValidRootForHeightCore` returns false when `fetchCanonicalHeader` answers null (ts-stack@edf6e03 `packages/wallet/wallet-toolbox/src/services/chaintracker/ChaintracksChainTracker.ts:189`), and `MerklePath.verify` reads that false as an invalid root (`packages/sdk/src/transaction/MerklePath.ts:499`): the payment is refused exactly as a wrong root is | The header client answers a null header as `NotFound`, an untyped tracker error, and the door holds: `Error::ServiceError` with `ChainTracker verification failed: ` and the tracker's text, the SDK's `WalletFault` of family `Service`, nothing written. The caller retries, and the same payment is admitted once the header service has the header. Why (the keeper's): unverifiable now is not invalid; a lagging store never refuses a valid payment; a lie is held until the node's word, never admitted | `storage/sqlx/beef_verification.rs` `tracker_fault`, `header_fault`; `services/providers/chaintracks_client.rs` `header_for_height`; witness `an_unknown_proof_height_holds_as_a_service_fault_and_admits_on_retry` (`storage/sqlx/internalize_no_header_tests.rs`) |

## AD1's scenario

- **The chain.** The payer's transaction is mined at height H and its BEEF
  carries the BUMP at H; the receiving wallet's header service sits one block
  behind, its tip at H - 1, so it has no header at H.
- **The steps.** The payment is internalized (`internalizeAction`, a wallet
  payment carrying that BEEF); then, once the header
  service has the header at H, it is internalized again.
- **The expected observations.** The reference refuses the call as an invalid
  root (the SDK's `MerklePath.ts:499` reading the false of
  `ChaintracksChainTracker.ts:189`), the refusal it gives a wrong root, and a
  host serves it as the caller's error. The port holds it as retryable:
  `Service error: ChainTracker verification failed: ...`, nothing written,
  which bsv-wallet-cli serves as its Service kind, 502 `SERVICE_ERROR`
  (`src/server/error.rs` at bsv-wallet-cli fa1e28f: `of_toolbox` :366,
  `of_fault` :295, `Kind::answer` :95). On the next try, with the header at
  H, the port admits the payment.
- **Beside it, the two causes that agree.** A typed transport fault from the
  tracker reaches the host as the SDK's `TransportFault` with its class,
  status and detail (`of_sdk` :282: Service, 502, retryable); a wrong root at
  a known height is `InvalidBeef`, class `InvalidParameter` (`of_toolbox`
  :354, `of_refusal` :130: Validation, 400). Witnesses beside AD1's:
  `a_tracker_transport_fault_reaches_the_host_as_its_typed_fault` and
  `a_wrong_root_at_a_known_height_is_refused`.
- **If the owner rules the reference's refusal**, that is a lane on the
  behaviour (the untyped tracker error at a missing height becomes the
  refusal), AD1 is deleted, and the witness flips with its fixture in its own
  commit. Nothing moves on beta before the word.
