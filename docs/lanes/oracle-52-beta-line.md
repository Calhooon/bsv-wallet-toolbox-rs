# oracle-52-beta-line (#52)

Repository: bsv-wallet-toolbox-rs. Branch `lane/oracle-52-beta-line` from `origin/beta`. Rules: `docs/lanes/README.md`. Read: `CLAUDE.md`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `RELEASING.md`, issue #52 (`gh issue view 52`).

The state: `beta` exists at v0.13.0 (e9857b8), protected with main's five required contexts (Test Suite (ubuntu-latest, stable), Clippy, Rustfmt, vectors, locks). CI runs on `main` only; `release.yml` accepts any `v*` tag that is on main.

The work, three commits:
1. `ci.yml`: `pull_request` and `push` on `[main, beta]`; the head comment names both protected branches and the five contexts.
2. `release.yml`: the tag check reads `vX.Y.Z-beta.N` as beta's (the tagged commit must be on `origin/beta`; `Cargo.toml` version must equal `X.Y.Z-beta.N`; the GitHub release is marked prerelease; `cargo publish` of a `-beta.N` version is a prerelease by semver) and a plain `vX.Y.Z` as main's (must be on `origin/main`; a plain tag on a commit beta holds but main does not is refused with a message naming the rule). Put the decision in `tools/release-tag-check.sh` (inputs: the tag, the manifest version, and the branches holding the commit, injectable so a test needs no git) and prove it with `tools/release-tag-check-test.sh`: plain tag on main passes; plain tag on beta only refused; beta tag on beta passes; beta tag on main only refused; tag and version mismatch refused; `v1.2.3-rc.1` refused (only `-beta.N` is a prerelease shape here). Run the test; its tail in the report. The workflow calls the script.
3. `RELEASING.md`: one section, "The two lines": `main` the stable line for quick fixes; `beta` the programme's line; `main` merges into `beta` after every stable patch; `beta` merges into `main` at a phase exit as the next minor, after the CLI's gate on a `vX.Y.0-beta.N` prerelease and a day of canary; a tag of any kind is the owner's word; the canary order (toolbox beta prerelease, then the CLI's beta builds against it, then the minor from main, then the CLI re-pins on its main as a patch; rollback is the previous pin). Who holds each word: the owner tags; the captain lands; the stack keeper holds the line.

Bounds: do not change `Cargo.toml`, the dependabot workflow, or any job's name (a renamed job drops a required context). Four hours.

Report per the rules; name for the captain: the exit test that only a real tag can run (a `-beta.1` tag publishing a prerelease) as `[D]`.
