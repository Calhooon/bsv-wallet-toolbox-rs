#!/usr/bin/env bash
# The cases of tools/release-tag-check.sh, with the branches passed by hand.
#
#   tools/release-tag-check-test.sh
set -uo pipefail

check="$(dirname "$0")/release-tag-check.sh"
pass=0
fail=0

# expect <want: ok|refused> <want stdout or refusal substring> <name> -- <args...>
expect() {
  local want="$1" text="$2" name="$3"
  shift 4
  local out status
  out="$(bash "$check" "$@" 2>&1)"
  status=$?
  if [ "$want" = ok ] && [ "$status" -eq 0 ] && [ "$out" = "$text" ]; then
    echo "ok      $name"
    pass=$((pass + 1))
  elif [ "$want" = refused ] && [ "$status" -eq 1 ] && [[ "$out" == *"$text"* ]]; then
    echo "ok      $name"
    pass=$((pass + 1))
  else
    echo "FAILED  $name (exit $status): $out"
    fail=$((fail + 1))
  fi
}

main_ok=$'line=main\nprerelease=false'
beta_ok=$'line=beta\nprerelease=true'

expect ok "$main_ok" "plain tag on main passes" -- v1.2.3 1.2.3 main
expect ok "$main_ok" "plain tag on a commit main and beta both hold passes" -- v1.2.3 1.2.3 main beta
expect refused "beta holds but main does not" "plain tag on beta only refused" -- v1.2.3 1.2.3 beta
expect ok "$beta_ok" "beta tag on beta passes" -- v1.3.0-beta.1 1.3.0-beta.1 beta
expect ok "$beta_ok" "beta tag on a commit beta and main both hold passes" -- v1.3.0-beta.2 1.3.0-beta.2 main beta
expect refused "main holds but beta does not" "beta tag on main only refused" -- v1.3.0-beta.1 1.3.0-beta.1 main
expect refused "does not match Cargo.toml version 1.2.4" "tag and version mismatch refused" -- v1.2.3 1.2.4 main
expect refused "does not match Cargo.toml version 1.3.0" "beta tag on a plain version refused" -- v1.3.0-beta.1 1.3.0 beta
expect refused "only -beta.N is a prerelease shape" "v1.2.3-rc.1 refused" -- v1.2.3-rc.1 1.2.3-rc.1 main beta
expect refused "only -beta.N is a prerelease shape" "v1.2.3-beta refused" -- v1.2.3-beta 1.2.3-beta beta
expect refused "only -beta.N is a prerelease shape" "tag without v refused" -- 1.2.3 1.2.3 main
expect refused "is not on main" "plain tag on neither line refused" -- v1.2.3 1.2.3
expect refused "is not on beta" "beta tag on neither line refused" -- v1.3.0-beta.1 1.3.0-beta.1 release/x

echo "${pass} passed, ${fail} failed"
[ "$fail" -eq 0 ]
