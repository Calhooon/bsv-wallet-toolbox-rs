#!/usr/bin/env bash
# The parsing of tools/dependents-check.sh (#252) on fixed test output: the
# TEXT-DIFF lines become text rows that never fail, and the FAILED lines, of
# every libtest shape (#257), name the failing row's tests and fail it.
#
#   tools/dependents-check-test.sh
set -uo pipefail

check="$(dirname "$0")/dependents-check.sh"
dir="$(mktemp -d "${TMPDIR:-${RUNNER_TEMP:?set TMPDIR}}/dependents-check-test.XXXXXX")"
trap 'rm -rf "$dir"' EXIT
pass=0
fail=0

# expect <want exit> <want output> <name> -- <file>
expect() {
  local want="$1" text="$2" name="$3" out status
  out="$(GITHUB_STEP_SUMMARY="" bash "$check" --parse "$5" --only bsv-wallet-cli 2>&1)"
  status=$?
  if [ "$status" -eq "$want" ] && [ "$out" = "$text" ]; then
    echo "ok      $name"
    pass=$((pass + 1))
  else
    echo "FAILED  $name (exit $status, want $want):"
    echo "$out"
    echo "want:"
    echo "$text"
    fail=$((fail + 1))
  fi
}

texts='TEXT-DIFF: 120-createAction.namedChange.json "Validation error: old" "Validation error: new"
TEXT-DIFF: 121-createAction.noSendChange.json "old words" "new words"'
oks='running 4 tests
test wire::replays_the_corpus ... ok
test wire::a_recording_is_not_a_replay ... ok
test spend_gate::tests::spends_inside_the_budget ... ok
test result: ok. 4 passed; 0 failed; 0 ignored'
fails='test spend_gate::tests::named_change_is_refused ... FAILED
test spend_gate::tests::named_change_names_the_output ... FAILED
test wire::fixture_120_replays ... FAILED
failures:
    spend_gate::tests::named_change_is_refused
test result: FAILED. 4 passed; 3 failed; 0 ignored'

printf '%s\n%s\n' "$oks" "$texts" >"$dir/text-only.log"
printf '%s\n%s\n%s\n' "$oks" "$texts" "$fails" >"$dir/failing.log"
printf '%s\n' "$oks" >"$dir/clean.log"
{
  for i in 1 2 3 4 5 6 7 8 9 10; do echo "test t$i ... FAILED"; done
} >"$dir/many.log"

rows_text='  bsv-wallet-cli   text differs: 120-createAction.namedChange.json (pending re-record at the pin)
  bsv-wallet-cli   text differs: 121-createAction.noSendChange.json (pending re-record at the pin)'

expect 0 "  bsv-wallet-cli   build: parsed                       test: ok
$rows_text" "two TEXT-DIFF lines are two text rows and no failure" -- "$dir/text-only.log"

expect 1 "  bsv-wallet-cli   build: parsed                       test: FAIL: spend_gate::tests::named_change_is_refused, spend_gate::tests::named_change_names_the_output, wire::fixture_120_replays
$rows_text" "three FAILED lines are one failing row naming the three tests" -- "$dir/failing.log"

expect 0 "  bsv-wallet-cli   build: parsed                       test: ok" \
  "ok lines alone are one passing row" -- "$dir/clean.log"

expect 1 "  bsv-wallet-cli   build: parsed                       test: FAIL: t1, t2, t3, t4, t5, t6, t7, t8 and 2 more" \
  "past eight failed tests the row counts the rest" -- "$dir/many.log"

# Every libtest failure shape is named (#257), each as libtest 1.x printed it
# for a scratch crate: the per-test line, then the `failures:` list.
cat >"$dir/unit.log" <<'EOF'
     Running unittests src/lib.rs (target/debug/deps/shapes-a7ea8d9008e3da9d)
test tests::unit_fails ... FAILED
test tests::result_err ... FAILED
test tests::passes ... ok
failures:
    tests::result_err
    tests::unit_fails
test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
EOF
cat >"$dir/integration.log" <<'EOF'
     Running tests/it.rs (target/debug/deps/it-3ca9749fdb0d5f26)
test integration_fails ... FAILED
failures:
    integration_fails
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
EOF
cat >"$dir/doctest.log" <<'EOF'
   Doc-tests shapes
test src/lib.rs - (line 1) ... FAILED
test src/lib.rs - module::item (line 5) ... FAILED
test src/lib.rs - module::cf (line 9) - compile fail ... FAILED
test src/lib.rs - module::nr (line 16) - compile ... FAILED
failures:
    src/lib.rs - (line 1)
    src/lib.rs - module::cf (line 9)
    src/lib.rs - module::item (line 5)
    src/lib.rs - module::nr (line 16)
test result: FAILED. 0 passed; 4 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.48s
EOF
cat >"$dir/should-panic.log" <<'EOF'
test tests::panics_not - should panic ... FAILED
test tests::panics_wrong - should panic ... FAILED
failures:
    tests::panics_not
    tests::panics_wrong
test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
EOF

expect 1 "  bsv-wallet-cli   build: parsed                       test: FAIL: tests::unit_fails, tests::result_err" \
  "a unit test and a test returning a result are named" -- "$dir/unit.log"

expect 1 "  bsv-wallet-cli   build: parsed                       test: FAIL: integration_fails" \
  "an integration test is named" -- "$dir/integration.log"

expect 1 "  bsv-wallet-cli   build: parsed                       test: FAIL: src/lib.rs - (line 1), src/lib.rs - module::item (line 5), src/lib.rs - module::cf (line 9), src/lib.rs - module::nr (line 16)" \
  "a doctest alone fails the row, named with its path and line; compile_fail and no_run modes dropped" -- "$dir/doctest.log"

expect 1 "  bsv-wallet-cli   build: parsed                       test: FAIL: tests::panics_not, tests::panics_wrong" \
  "a should_panic test is named without libtest's mode" -- "$dir/should-panic.log"

# The job summary carries the same rows as a table.
GITHUB_STEP_SUMMARY="$dir/summary.md" bash "$check" --parse "$dir/failing.log" --only bsv-wallet-cli >/dev/null
want_md='| dependent | build | test |
|---|---|---|
| bsv-wallet-cli | parsed | FAIL: spend_gate::tests::named_change_is_refused, spend_gate::tests::named_change_names_the_output, wire::fixture_120_replays |
| bsv-wallet-cli | text differs: 120-createAction.namedChange.json (pending re-record at the pin) | - |
| bsv-wallet-cli | text differs: 121-createAction.noSendChange.json (pending re-record at the pin) | - |'
if [ "$(cat "$dir/summary.md")" = "$want_md" ]; then
  echo "ok      the job summary carries the rows"
  pass=$((pass + 1))
else
  echo "FAILED  the job summary carries the rows:"
  cat "$dir/summary.md"
  fail=$((fail + 1))
fi

echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
