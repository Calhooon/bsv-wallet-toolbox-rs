#!/usr/bin/env bash
# Prove every test named in the invariants tables of docs/ARCHITECTURE.md
# (and docs/architecture/*.md when present) exists: `cargo test ... <name>
# -- --list` shows one test whose path ends in the name (a lib test is
# listed with its module path, so `--exact` would miss it). A row whose
# test column is `(no test)` is counted and skipped. Exit 1 when a named
# test is missing or more than one test ends in the name.
#
#   tools/arch-doc-tests.sh            # every row
#   tools/arch-doc-tests.sh --dry-run  # parse only, no cargo
#   ARCH_DOC_LIB_FEATURES=sqlite,remote \
#   ARCH_DOC_TEST_FEATURES=sqlite,remote,bench-support tools/arch-doc-tests.sh  # CI's form
#
# A row's test column is `name` for a lib test, or `name` (`--test file`)
# for an integration test. The invariants tables are the ones whose header
# row is `| invariant | charter | test |`. Section 7 must hold the D7 row
# with its reference citations and filing (#312), and a filing cell on
# every row (#343).
set -u
cd "$(dirname "$0")/.."
dry=0; [ "${1:-}" = "--dry-run" ] && dry=1
# ARCH_DOC_LIB_FEATURES names the cargo features for the `--lib` rows and
# ARCH_DOC_TEST_FEATURES those for the `--test` rows (CI passes its unit-test
# step's `sqlite,remote` and its integration step's `sqlite,remote,bench-support`
# so each row lists from a build already made); empty means the defaults.
lib_features="${ARCH_DOC_LIB_FEATURES:+ --features $ARCH_DOC_LIB_FEATURES}"
test_features="${ARCH_DOC_TEST_FEATURES:+ --features $ARCH_DOC_TEST_FEATURES}"
docs="docs/ARCHITECTURE.md"
[ -d docs/architecture ] && docs="$docs $(ls docs/architecture/*.md 2>/dev/null)"

rows=0; tested=0; untested=0; failed=0
while IFS= read -r line; do
  rows=$((rows + 1))
  cell="${line##*|}"                 # the text after the last pipe is empty
  cell="${line%|}"; cell="${cell##*|}"
  if printf '%s' "$cell" | grep -q '(no test)'; then
    untested=$((untested + 1)); continue
  fi
  name=$(printf '%s' "$cell" | grep -o '`[a-z0-9_]*`' | head -1 | tr -d '`')
  file=$(printf '%s' "$cell" | grep -o -- '--test [a-z0-9_]*' | head -1 | awk '{print $2}')
  if [ -z "$name" ]; then
    echo "NO NAME: $line"; failed=$((failed + 1)); continue
  fi
  tested=$((tested + 1))
  if [ -n "$file" ]; then cmd="cargo test --test $file$test_features $name -- --list"
  else cmd="cargo test --lib$lib_features $name -- --list"; fi
  if [ "$dry" = 1 ]; then echo "$cmd"; continue; fi
  out=$($cmd 2>/dev/null | grep -E "(^|::)$name: test\$")
  n=$(printf '%s\n' "$out" | grep -c ': test$')
  if [ "$n" = 1 ]; then echo "ok   $cmd -> $out"
  else echo "FAIL $cmd -> $n matches"; failed=$((failed + 1)); fi
done < <(awk '
  /^\| invariant \| charter \| test \|/ { table = 1; next }
  table && /^\|---/ { next }
  table && /^\|/ { print; next }
  { table = 0 }
' $docs)

# Section 7's D7 row (#312): the BRC-177 sync divergences, cited to the
# reference's quarantine and filed as ours.
d7=$(awk '/^## 7\. /{s=1; next} /^## /{s=0} s && /^\| D7 \|/' docs/ARCHITECTURE.md)
for cite in 'EntityOutput.ts:11-26' 'EntityTransaction.ts:125-141,555-561' \
  'StorageProvider.ts:148-151,1101-1118' 'Filing: ours (0.15 epic #352)'; do
  if printf '%s' "$d7" | grep -qF -- "$cite"; then echo "ok   section 7 D7 cites $cite"
  else echo "FAIL section 7 D7 does not cite $cite"; failed=$((failed + 1)); fi
done

# Section 7's filing column (#343): every row's last cell is the other SDK's
# issue (`<repo> #<n>`), `ours: ` (or `ours (#<n>): `, the toolbox's issue)
# with its reading, or `to file: <repo> `
# with the finding; an empty or missing cell fails, and so does a repository
# outside the other SDKs' list.
repo='(bsv-blockchain/)?(ts-stack|go-sdk|py-sdk|go-wallet-toolbox|BRCs)|(Calhooon/)?bsv-rs'
s7=$(awk '/^## 7\. /{s=1; next} /^## /{s=0} s' docs/ARCHITECTURE.md)
if printf '%s\n' "$s7" | grep -qxF '| # | the code | the charter | filing |'; then
  echo "ok   section 7 header names the filing column"
else echo "FAIL section 7 header has no filing column"; failed=$((failed + 1)); fi
filings=0
while IFS= read -r line; do
  id=$(printf '%s' "$line" | awk -F'|' '{gsub(/^ +| +$/, "", $2); print $2}')
  ncell=$(printf '%s' "$line" | awk -F'|' '{print NF - 2}')
  cell=$(printf '%s' "$line" | awk -F'|' '{gsub(/^ +| +$/, "", $5); print $5}')
  if [ "$ncell" != 4 ]; then
    echo "FAIL section 7 $id has $ncell cells, not 4"; failed=$((failed + 1))
  elif printf '%s' "$cell" | grep -qE "^$repo #[0-9]+\$"; then
    filings=$((filings + 1))
  elif printf '%s' "$cell" | grep -qE '^ours( \(#[0-9]+\))?: .+'; then
    filings=$((filings + 1))
  elif printf '%s' "$cell" | grep -qE "^to file: $repo[:,]? .+"; then
    filings=$((filings + 1)); echo "TODO section 7 $id: $cell"
  else
    echo "FAIL section 7 $id filing cell is not a filing: '$cell'"; failed=$((failed + 1))
  fi
done < <(printf '%s\n' "$s7" | grep -E '^\| D[0-9]+')
if [ "$filings" -gt 0 ]; then echo "ok   section 7 filing cells: $filings"
else echo "FAIL section 7 has no rows"; failed=$((failed + 1)); fi

echo "rows: $rows  with a test: $tested  (no test): $untested  failed: $failed"
[ "$failed" = 0 ]
