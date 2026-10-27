# Releasing bsv-wallet-toolbox-rs

1. Every change reaches `main` through a pull request; `main` is protected, admins included, linear history.
2. The PR merges (rebase-merge) once its five required checks are green: `Test Suite (ubuntu-latest, stable)`, `Clippy`, `Rustfmt`, `vectors`, `locks`.
3. A release is a PR like any other: bump `version` in `Cargo.toml`, add the `CHANGELOG.md` heading, merge it green.
4. Tag the merged commit on `main`: `git tag vX.Y.Z <sha> && git push origin vX.Y.Z` (the tag must equal `Cargo.toml`'s version).
5. The tag runs `.github/workflows/release.yml`: `tools/release-tag-check.sh` checks the tag against the manifest and that the commit is on the tag's line (below), re-runs the CI jobs, then `cargo publish --dry-run` and `cargo publish`.
6. Nothing publishes from a laptop; no crates.io token is stored anywhere, in this repository or its secrets.
7. Publishing uses crates.io trusted publishing: the crate's Trusted Publishing setting names owner `Calhooon`, repository `bsv-wallet-toolbox-rs`, workflow `release.yml`, no environment.
8. A failed release job publishes nothing; fix through a PR, then the owner moves the tag to the fixed commit, or tags the next patch.
9. A published version is never re-published; a bad release is yanked on crates.io and superseded by the next patch.
10. Without the trusted-publishing setting on crates.io the publish step fails closed.

## The two lines

- `main` is the stable line, for quick fixes another project can take while the programme runs; its tags are plain `vX.Y.Z`.
- `beta` is the programme's line (epic #80); its tags are `vX.Y.Z-beta.N`, published as prereleases on crates.io (semver) and on GitHub.
- Both are protected with the same rules and the same five required checks; `ci.yml` runs on pull requests and pushes into either.
- `main` merges into `beta` after every stable patch, so beta never lacks a fix main shipped.
- `beta` merges into `main` only at the fleet's gate (the owner's word of 2026-10-27, verbatim on the stack's rulings page: "no merge to main until god tier and 110% confidence nothing breaks and we in conformance with other SDKs and all other sdk issues are logged on their repos"): every scoreboard row in `docs/NORTH-STAR.md` at its target with recorded evidence and all six phases exited; the gates and the production reads green; conformance with the other SDKs proven, the divergence table in `docs/ARCHITECTURE.md` empty or every row carrying a filing number on the other SDK's repository (ts-stack, go-sdk, py-sdk, the BRCs) or the word `ours` with its reading, nothing on it an unfiled finding against another SDK; then as the next minor, after the CLI's gate passes on a `vX.Y.0-beta.N` prerelease and a day of canary. Until then `beta` ships prereleases and `main` takes its own quick fixes. The word binds every crate's `main`, not this one alone.
- The release check refuses a plain tag on a commit beta holds but main does not, a beta tag on a commit beta does not hold, a tag that differs from `Cargo.toml`, and any other prerelease shape (`-rc.N` and the like).
- A tag of any kind is the owner's word.

The canary order at that merge:

1. The toolbox tags `vX.Y.0-beta.N` on `beta`; the prerelease publishes.
2. The CLI's beta builds against that prerelease and runs its gate; a day of canary follows.
3. `beta` merges into `main`, and the minor `vX.Y.0` is tagged on `main`.
4. The CLI re-pins to the minor on its `main`, as a patch of the CLI.

Rollback is the previous pin: the CLI returns to the version it pinned before, and the toolbox supersedes the bad version with the next patch (step 9).

Who holds each word: the owner tags; the captain lands PRs into `main` and `beta`; the stack keeper holds the line (a beta tag is never admitted to the stack's line).
