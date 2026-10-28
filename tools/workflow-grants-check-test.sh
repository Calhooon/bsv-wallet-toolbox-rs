#!/usr/bin/env bash
# The reusable workflow permission check, exercised on copies and small fixtures.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
check="$root/tools/workflow-grants-check.sh"
dir="$(mktemp -d "${TMPDIR:-${RUNNER_TEMP:?set TMPDIR}}/workflow-grants-check-test.XXXXXX")"
trap 'rm -rf "$dir"' EXIT
pass=0
fail=0

# expect <exit> <output substring> <name> <release fixture> <ci fixture>
expect() {
  local want="$1" text="$2" name="$3" out status=0
  out="$(bash "$check" "$4" "$5" 2>&1)" || status=$?
  if [ "$status" -eq "$want" ] && [[ "$out" == *"$text"* ]]; then
    echo "ok      $name (exit $status): $text"
    pass=$((pass + 1))
  else
    echo "FAILED  $name (exit $status, want $want): $out"
    fail=$((fail + 1))
  fi
}

cp "$root/.github/workflows/release.yml" "$dir/release.yml"
cp "$root/.github/workflows/ci.yml" "$dir/ci.yml"
python3 - "$dir" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1])
release = (root / "release.yml").read_text()
ci = (root / "ci.yml").read_text()
needle = "      pull-requests: read\n"
assert ci.count(needle) == 1, "expected one semver pull-requests permission"
(root / "issues.yml").write_text(ci.replace(needle, needle + "      issues: write\n"))
block = "    permissions:\n      contents: read\n      pull-requests: read\n"
assert release.count(block) == 1, "expected one CI caller permission block"
(root / "fallback.yml").write_text(release.replace(block, ""))


def caller(name, grant):
    (root / name).write_text("jobs:\n  ci:\n    uses: ./.github/workflows/ci.yml\n" + grant)


def callee(name, grant):
    (root / name).write_text("jobs:\n  sample:\n" + grant + "    runs-on: ubuntu-latest\n")


caller("write.yml", "    permissions:\n      contents: write\n")
caller("read-all.yml", "    permissions: read-all\n")
caller("write-all.yml", "    permissions: write-all\n")
caller("empty.yml", "    permissions: {}\n")
caller("absent.yml", "")
caller("quoted.yml", '    permissions: {"contents": "write"} # a grant\n')
caller("bad.yml", "    permissions: inherit\n")
caller("duplicate.yml", "    permissions:\n      contents: write\n      contents: read\n")
caller("none.yml", "    permissions:\n      contents: none\n")
callee("read.yml", "    permissions:\n      contents: read\n")
callee("read-all-ci.yml", "    permissions: read-all\n")
callee("write-ci.yml", "    permissions:\n      issues: write\n")
callee("none-ci.yml", "    permissions: {}\n")
callee("absent-ci.yml", "")
(root / "inherited.yml").write_text(
    "permissions:\n  contents: write\njobs:\n  ci:\n    uses: ./.github/workflows/ci.yml\n")
(root / "override.yml").write_text(
    "permissions: write-all\n" + (root / "empty.yml").read_text())
(root / "inherited-ci.yml").write_text(
    "permissions:\n  contents: read\njobs:\n  sample:\n    steps:\n      - run: |\n"
    "          permissions:\n            issues: write\n")
(root / "override-ci.yml").write_text(
    "permissions: write-all\n" + (root / "none-ci.yml").read_text())
(root / "no-caller.yml").write_text("jobs:\n  other:\n    uses: ./other.yml\n")
PY

expect 0 "workflow grants: ok" "the two live files" "$dir/release.yml" "$dir/ci.yml"
expect 1 "callee job semver requests issues: write; caller ci has no explicit grant for issues" \
  "an added job scope is refused" "$dir/release.yml" "$dir/issues.yml"
expect 1 "no explicit grant for pull-requests" "removing the caller job block is refused" \
  "$dir/fallback.yml" "$dir/ci.yml"
expect 0 "workflow grants: ok" "write covers read" "$dir/write.yml" "$dir/read.yml"
expect 0 "workflow grants: ok" "caller workflow fallback covers read" "$dir/inherited.yml" "$dir/read.yml"
expect 0 "workflow grants: ok" "callee fallback ignores script contents" "$dir/write.yml" "$dir/inherited-ci.yml"
expect 1 "no explicit grant for contents" "an empty job grant overrides the workflow" "$dir/override.yml" "$dir/read.yml"
expect 0 "workflow grants: ok" "an empty job ask overrides the workflow" "$dir/empty.yml" "$dir/override-ci.yml"
expect 0 "workflow grants: ok" "read-all covers read" "$dir/read-all.yml" "$dir/read.yml"
expect 1 "callee job sample requests issues: write" "read-all does not cover write" "$dir/read-all.yml" "$dir/write-ci.yml"
expect 0 "workflow grants: ok" "write-all covers write" "$dir/write-all.yml" "$dir/write-ci.yml"
expect 0 "workflow grants: ok" "write-all covers read-all" "$dir/write-all.yml" "$dir/read-all-ci.yml"
expect 1 "callee job sample requests *: read" "a scoped grant does not cover read-all" "$dir/write.yml" "$dir/read-all-ci.yml"
expect 1 "no explicit grant; token default is unknown" "an absent grant is refused" "$dir/absent.yml" "$dir/none-ci.yml"
expect 0 "workflow grants: ok" "an absent ask inherits the caller grant" "$dir/write.yml" "$dir/absent-ci.yml"
expect 0 "workflow grants: ok" "quoted inline scopes and comments" "$dir/quoted.yml" "$dir/read.yml"
expect 1 "unsupported permissions" "unsupported syntax is refused" "$dir/bad.yml" "$dir/read.yml"
expect 1 "duplicate field: contents" "duplicate scopes are refused" "$dir/duplicate.yml" "$dir/read.yml"
expect 1 "no explicit grant for contents" "none does not cover read" "$dir/none.yml" "$dir/read.yml"
expect 1 "no caller uses" "a missing reusable call is refused" "$dir/no-caller.yml" "$dir/read.yml"

echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
