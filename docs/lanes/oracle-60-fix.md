
# oracle-60-fix (#60, the cold review's fixes)

Repository: bsv-wallet-toolbox-rs. Your worktree is on branch `lane/oracle-60-fix`, cut from `lane/oracle-60-north-star` (one commit past the beta of #52: `docs/NORTH-STAR.md`, 269 lines, and `tools/scoreboard-write`, 216 lines). Read both whole, `.github/workflows/ci.yml`, `tools/release-tag-check-test.sh` (the sibling test convention), and issue #60 (`gh issue view 60`).

The cold review's findings and the captain's decisions, one commit each with its proof:
1. (must) No CI job calls the writer, so an edited table merges unchecked. Add a `scoreboard` job to `ci.yml` (ubuntu, Python 3 only, no cargo) running `tools/scoreboard-write --check`; not a required context (the captain adds it later); no job renamed. The write step stays with the producers (#54's bench job, #55's ratchet job) and the document says so in one sentence. Proof: `actionlint` clean (install under `~/bsv/targets/actionlint-tools` if absent; never `brew`); a planted edit inside the markers makes `--check` exit non-zero.
2. (must) `docs/NORTH-STAR.md:47-48` says `--check` fails when both JSON sources are absent; the tool passes with "0 source files" and preserves the cells, which is the right behaviour. Drop or correct the sentence to match the tool.
3. (captain's decision) The two links to `docs/lanes/README.md` (`:7`, `:196`) stay as they are: that file lands on beta through the captain's records PR before this one merges. Remove the lines 219-221 that hand the fix to the captain.
4. (must) A self-test `tools/scoreboard-write-test` (Python 3 or shell, in the sibling's convention) covering: unchanged on the bare tree; `--check` passes on the committed document; an edited observed cell fails; a tampered digest fails; stale sources fail then `--write` updates and recheck passes; a removed source preserves cells; the bad payloads the reviewer tried (negative, bool, fractional count, bad pin, bad date, bad case, non-map ladder, over 100 percent, duplicate key, array) each exit 1 with the document byte-identical; two writes byte-identical. The CI job runs it before `--check`. Proof: the test's run, its tail.
5. (should) A known metric found in the wrong source file (`files_over_800` inside `benches/baseline.json`) is dropped silently; fail with the key and the file it belongs to.

Gate: the self-test green; `tools/scoreboard-write --check` exit 0 on the committed document; `actionlint`; no em dash and no "first" in the diff.

Bounds: `docs/NORTH-STAR.md`, `tools/scoreboard-write`, `tools/scoreboard-write-test`, the one job in `ci.yml`. Two hours. No cargo command is needed for this lane; do not run the Rust gate.
