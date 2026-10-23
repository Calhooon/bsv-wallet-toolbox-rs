# oracle-60-north-star (#60)

Repository: bsv-wallet-toolbox-rs. Branch `lane/oracle-60-north-star` from `origin/beta`. Rules: `docs/lanes/README.md`. Read: issue #60, the epic #80 in full (`gh issue view 80`), issues #52 to #79 titles and exit tests (`gh issue list --label north-star --limit 50 --json number,title,body`), the CLI's document `git -C ~/bsv/bsv-wallet-cli show origin/beta:docs/NORTH-STAR.md` (if absent there, `origin/main`, or its #77), the stack's `~/bsv/bsv-stack-lean/docs/BSV-NORTH-STAR.md` (the six tests), the charters' section headings in `~/bsv/bsv-stack-lean/docs/charters/`.

The work: `docs/NORTH-STAR.md`, under 400 lines:
1. The crate's scoreboard row as a table whose numbers CI writes: door latency p99 at 1,000 and 10,000 coins, allocate-and-sign, ladder verdict p99 per rung and request count, BEEF verification at 10 MB, writer contention (door p99 with the monitor beside it), explorer calls per block, the code-health ratchets (files over 800, unwraps outside tests, mutation score, undocumented public items), the dependents' pin spread. Each cell today holds `measured 2026-10-23: <number>` where the issue gives one, else `not yet measured`. The table sits between `<!-- scoreboard:begin -->` and `<!-- scoreboard:end -->`; `tools/scoreboard-write` (one Python 3 script) fills it from `benches/baseline.json` and `tools/ratchet.json` when they exist and leaves a cell alone when its source is absent, so the bench and ratchet lanes plug in without editing this file. Run it on the tree (both sources absent today) and show the document unchanged.
2. The six phases with each issue's exit test in one line, and the phase's exit as the conjunction.
3. The never-without-the-owner list, verbatim from the epic.
4. The ranked research queue a lane takes its item from: Phase 0 items in wave order, then Phase 1 by dependency (#61 before #66; #63 with #56's lint), then the hot path; each item one line: number, title, what it unblocks, its stack-alignment label.
5. The loop in ten lines, and pointers (never restatements) to the charters by section and to `docs/lanes/README.md`.

The proof: `tools/scoreboard-write --check` exits zero on the committed document; a planted edit inside the markers makes it non-zero; revert. The markdown renders (no table broken: check with a quick `grep -c '^|'` and by eye).

Bounds: docs and the one tool only; no `src/`, no `Cargo.toml`. Four hours.
