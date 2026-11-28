#!/usr/bin/env bash
# Build and test each dependent named in docs/DEPENDENTS.md against this tree.
#
# For every row of the check table (between the dependents-check markers):
#   1. clone the dependent at the branch matching the line under test: `--line L`
#      (in CI, GITHUB_BASE_REF on a pull request, GITHUB_REF_NAME on a push), the
#      dependent's default branch when an implicit line has no matching branch
#      or no line is named; an explicit missing branch fails (or copy a local
#      checkout given with --from; the source is never
#      written). The CLI pins this crate's prereleases on its beta only, so the
#      toolbox's beta builds the CLI's beta and the toolbox's main the CLI's main (#208);
#   2. rewrite its dependency on bsv-wallet-toolbox-rs to a path dependency on
#      this tree. A [patch.crates-io] entry would not apply when the dependent's
#      pin excludes this tree's version, so the dependency line itself is edited;
#   3. keep Cargo.lock honest: `cargo update -p bsv-wallet-toolbox-rs@<locked>`
#      moves that one package (and only what it newly needs); the lock's diff is
#      printed;
#   4. `cargo build --locked`, falling back to `cargo build` with a note when
#      the lock still needs a change (or the dependent commits no lock);
#   5. `cargo test --locked --no-fail-fast`, every target (`cargo test
#      --no-fail-fast` when the build was unlocked), with WIRE_TEXT_DIFF=report in its environment; a row's test
#      column may name a narrower command, with the reason in its reason column;
#   6. read the test output: every `TEXT-DIFF: <fixture file> ...` line is a row
#      `text differs` (a re-record pending at the pin, never a failure), and a
#      failing row names its failed tests, eight at most (#252).
#
# Usage: tools/dependents-check.sh [--ci] [--only CRATE] [--line L] [--from CRATE=DIR]...
#        tools/dependents-check.sh --parse FILE [--only CRATE]
#   --ci          run only the rows whose ci column is `yes`
#   --only CRATE  run one row
#   --line L      require and clone each dependent's branch L (default: the line CI
#                 runs on, GITHUB_BASE_REF or GITHUB_REF_NAME; missing implicit lines
#                 fall back to the default branch)
#   --from C=DIR  take crate C's repository from the local DIR (a copy is built; DIR is never written)
#   --parse FILE  read FILE as a dependent's captured test output and print its rows
#                 only (nothing is cloned or built); exits 1 when it names a failed test
# Environment: DEPENDENTS_WORK (scratch, default $RUNNER_TEMP or $TMPDIR),
#   DEPENDENTS_TARGET_DIR (cargo target root, default inside the scratch),
#   GITHUB_STEP_SUMMARY (when set, the rows are appended to it as a table).
# Exits non-zero when any dependent fails to build or test, and when a branch probe
# or a clone faults (a fault is a summary row, never a verdict).

set -euo pipefail

PKG=bsv-wallet-toolbox-rs
TREE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIST="$TREE/docs/DEPENDENTS.md"

ci=0
only=""
parse=""
line="${GITHUB_BASE_REF:-${GITHUB_REF_NAME:-}}"
explicit_line=0
declare -a froms=()
while [ $# -gt 0 ]; do
  case "$1" in
    --ci) ci=1 ;;
    --only) only="$2"; shift ;;
    --line) line="$2"; explicit_line=1; shift ;;
    --from) froms+=("$2"); shift ;;
    --parse) parse="$2"; shift ;;
    -h|--help) sed -n '2,/^set -euo/{ /^#/p; }' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

declare -a summary=()
status=0

# The rows a dependent's test output carries. A `TEXT-DIFF: <fixture file>
# <expected message> <actual message>` line is a fixture whose replayed refusal
# differs from it in the message text alone (bsv-wallet-cli #290): one row each,
# pending re-record at the pin, never a failure.
text_rows() {
  sed -n 's/^TEXT-DIFF: \([^ ]*\).*/\1/p' "$2" |
    while IFS= read -r fixture; do
      echo "$1|text differs: $fixture (pending re-record at the pin)|-"
    done
}

# The libtest `test NAME ... FAILED` lines, eight named, then how many more
# (#257). NAME is every shape: a unit or integration test's path, a doctest's
# `src/lib.rs - module::item (line 3)`, and a mode libtest appends (`tests::boom
# - should panic`, `... (line 9) - compile fail`) is dropped, so each name reads
# as the `failures:` list names it. The per-test line is read rather than that
# list: one line per failed test, in every target, whatever its shape.
failed_tests() {
  awk '/^test .+ \.\.\. FAILED$/ {
      name = substr($0, 6, length($0) - 5 - 11); sub(/ - [a-z_ ]+$/, "", name)
      n++; if (n <= 8) names = names (n > 1 ? ", " : "") name }
    END { if (n > 8) names = names " and " (n - 8) " more"; print names }' "$1"
}

# The rows, on the terminal and, in CI, in the job summary.
report() {
  local s c b t
  [ -z "${GITHUB_STEP_SUMMARY:-}" ] || printf '| dependent | build | test |\n|---|---|---|\n' >>"$GITHUB_STEP_SUMMARY"
  for s in ${summary[@]+"${summary[@]}"}; do
    IFS='|' read -r c b t <<<"$s"
    if [ "${b#text differs}" != "$b" ]; then
      printf '  %-16s %s\n' "$c" "$b"
    else
      printf '  %-16s build: %-28s test: %s\n' "$c" "$b" "$t"
    fi
    [ -z "${GITHUB_STEP_SUMMARY:-}" ] || printf '| %s | %s | %s |\n' "$c" "$b" "$t" >>"$GITHUB_STEP_SUMMARY"
  done
}

# A captured output read alone: the parsing's own proof (tools/dependents-check-test.sh).
if [ -n "$parse" ]; then
  crate="${only:-dependent}"
  failed="$(failed_tests "$parse")"
  if [ -n "$failed" ]; then
    summary+=("$crate|parsed|FAIL: $failed")
    status=1
  else
    summary+=("$crate|parsed|ok")
  fi
  while IFS= read -r r; do summary+=("$r"); done < <(text_rows "$crate" "$parse")
  report
  exit "$status"
fi

WORK="${DEPENDENTS_WORK:-${RUNNER_TEMP:-${TMPDIR:-}}}"
if [ -z "$WORK" ]; then
  echo "set DEPENDENTS_WORK or TMPDIR" >&2
  exit 2
fi
WORK="$WORK/dependents-check"

# A check with nothing to check is a failure, never a pass.
if [ ! -f "$LIST" ]; then
  echo "no table: $LIST is missing" >&2
  exit 2
fi
for marker in begin end; do
  if ! grep -qF "<!-- dependents-check:$marker -->" "$LIST"; then
    echo "no table: $LIST has no <!-- dependents-check:$marker --> marker" >&2
    exit 2
  fi
done

# The tree's own version, which the rewritten dependency must resolve to.
tree_version="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$TREE/Cargo.toml" | head -n1)"

from_for() {
  local f
  for f in ${froms[@]+"${froms[@]}"}; do
    if [ "${f%%=*}" = "$1" ]; then
      echo "${f#*=}"
      return
    fi
  done
}

# The rows of the check table: crate|clone|dir|ci|test
rows() {
  sed -n '/<!-- dependents-check:begin -->/,/<!-- dependents-check:end -->/p' "$LIST" |
    grep '^| ' | sed '1d' |
    awk -F'|' '{ for (i = 2; i <= 6; i++) { gsub(/^ +| +$/, "", $i) } print $2 "|" $3 "|" $4 "|" $5 "|" $6 }'
}

# Rewrite every dependency on $PKG in the given manifest to `path = "$TREE"`,
# keeping its other keys (features, optional, default-features, package).
rewrite_manifest() {
  local manifest="$1"
  PKG="$PKG" TREE="$TREE" perl -0pi -e '
    my ($pkg, $tree, $n) = ($ENV{PKG}, $ENV{TREE}, 0);
    sub fix {
      my ($head, $body) = @_;
      my ($name) = $head =~ /^[ \t]*([A-Za-z0-9_-]+)/;
      return "$head$body}" unless $name eq $pkg || $body =~ /\bpackage[ \t]*=[ \t]*"\Q$pkg\E"/;
      $body =~ s/[ \t]*\b(?:version|git|branch|tag|rev|registry|path)[ \t]*=[ \t]*"[^"]*"[ \t]*,?//g;
      $body =~ s/^[ \t,]+//;
      $body =~ s/[ \t,]+$//;
      $n++;
      return "$head path = \"$tree\"" . ($body eq "" ? " }" : ", $body }");
    }
    # name = { ..., package = "pkg", ... }  or  pkg = { ... }
    s/^([ \t]*[A-Za-z0-9_-]+[ \t]*=[ \t]*\{)([^\}\n]*)\}/fix($1, $2)/gme;
    # pkg = "version"
    $n += s/^([ \t]*)\Q$pkg\E([ \t]*=[ \t]*)"[^"]*"/$1$pkg$2\{ path = "$tree" \}/gm;
    die "a [dependencies.$pkg] table is not handled\n" if /^\[[^\]]*\.\Q$pkg\E\]/m;
    print STDERR "   rewrote $n dependency line(s)\n";
  ' "$manifest"
}

# "name version" for every package of a Cargo.lock, sorted.
lock_packages() {
  awk '/^name = /{ n = $3 } /^version = /{ v = $3; gsub(/"/, "", n); gsub(/"/, "", v); print n " " v }' "$1" | sort
}

run_one() {
  local crate="$1" clone="$2" dir="$3" testcmd="$4"
  local src dest from probe rc=0
  dest="$WORK/$crate"
  rm -rf "$dest"
  mkdir -p "$WORK"

  from="$(from_for "$crate")"
  if [ -n "$from" ]; then
    echo "== $crate: copying $from (the source is not touched)"
    rsync -a --exclude target --exclude .git "$from/" "$dest/"
  else
    # An explicit line must exist; an implicit missing line uses the default.
    # The probe's answer is a verdict only when it is a yes (0) or a no (2):
    # any other exit is a fault (host unreachable, auth), and a fault is not a verdict.
    probe=0
    if [ -n "$line" ] && { [ "$explicit_line" -eq 1 ] || [ "$line" != main ]; }; then
      git ls-remote --exit-code --heads "$clone" "refs/heads/$line" >/dev/null 2>&1 || probe=$?
    else
      probe=2
    fi
    case "$probe" in
      0)
        echo "== $crate: cloning $clone at its $line branch (the line under test)"
        git clone --quiet --depth 1 --branch "$line" "$clone" "$dest" || rc=$?
        ;;
      2)
        if [ "$explicit_line" -eq 1 ]; then
          echo "== $crate: no $line branch at $clone (explicit --line)"
          summary+=("$crate|no $line branch at $clone (explicit --line)|-")
          status=1
          return
        fi
        [ -n "$line" ] && [ "$line" != main ] && echo "   NOTE: $clone has no $line branch; its default branch stands in"
        echo "== $crate: cloning $clone at its default branch"
        git clone --quiet --depth 1 "$clone" "$dest" || rc=$?
        ;;
      *)
        echo "== $crate: the branch probe of $clone failed (git ls-remote exit $probe); a fault is not a verdict"
        summary+=("$crate|probe fault (exit $probe)|-")
        status=1
        return
        ;;
    esac
    if [ "$rc" -ne 0 ]; then
      echo "== $crate: the clone of $clone failed (git clone exit $rc); a fault is not a verdict"
      summary+=("$crate|clone fault (exit $rc)|-")
      status=1
      return
    fi
    echo "   at $(git -C "$dest" rev-parse --short HEAD) ($(git -C "$dest" rev-parse --abbrev-ref HEAD))"
  fi
  src="$dest/$dir"

  # Every manifest of the dependent's workspace that names this crate.
  local manifests
  manifests="$(cd "$dest" && grep -rlE --include=Cargo.toml "(^|[^A-Za-z0-9_-])$PKG([^A-Za-z0-9_-]|$)" . | grep -v '/target/' || true)"
  if [ -z "$manifests" ]; then
    echo "   no manifest names $PKG"
    summary+=("$crate|no dependency found|-")
    status=1
    return
  fi
  local m
  for m in $manifests; do
    echo "   $m:"
    rewrite_manifest "$dest/$m"
    grep -nE "$PKG|path = \"$TREE\"" "$dest/$m" | sed 's/^/     /'
  done

  export CARGO_TARGET_DIR="${DEPENDENTS_TARGET_DIR:-$WORK/target}/$crate"
  local note="" build="fail" test="not run" lock log="" failed r
  lock="$(dirname "$(cd "$src" && cargo locate-project --workspace --message-format plain)")/Cargo.lock"
  if [ -f "$lock" ]; then
    (
      cd "$src"
      local locked_version moved
      cp "$lock" "$WORK/$crate.lock.before"
      locked_version="$(grep -A1 "^name = \"$PKG\"$" "$lock" | sed -n 's/^version = "\(.*\)"$/\1/p' | head -n1)"
      if [ -z "$locked_version" ]; then
        echo "   NOTE: the lock holds no $PKG; cargo build resolves it"
        exit 0
      fi
      echo "   lock: $PKG $locked_version; cargo update -p $PKG@$locked_version"
      if ! cargo update -p "$PKG@$locked_version" 2>&1 | sed 's/^/     /'; then
        echo "   NOTE: cargo update -p refused; cargo build resolves the lock"
      fi
      echo "   lock diff (package and version; < before, > after):"
      moved="$(diff <(lock_packages "$WORK/$crate.lock.before") <(lock_packages "$lock") | grep '^[<>]' || true)"
      awk '{ print "     " $0 }' <<<"${moved:-none}"
    ) || true
  fi

  # The rows built here must not inherit this repository's -Dwarnings.
  if [ ! -f "$lock" ]; then
    note="no committed Cargo.lock; built unlocked"
    echo "   NOTE: $note"
    if (cd "$src" && unset RUSTFLAGS && cargo build); then
      build="ok (unlocked)"
    else
      build="FAIL"
    fi
  elif (cd "$src" && unset RUSTFLAGS && cargo build --locked); then
    build="ok (--locked)"
  else
    note="--locked build failed; retried unlocked"
    echo "   NOTE: $note"
    if (cd "$src" && unset RUSTFLAGS && cargo build); then
      build="ok (unlocked)"
    else
      build="FAIL"
    fi
  fi

  # Two bsv-rs in one graph is the usual cause of a failed build here: the
  # dependent pins a bsv-rs this tree's does not match.
  local sdks
  sdks="$(cd "$src" && cargo tree --prefix none -e normal 2>/dev/null |
    sed -n 's/^bsv-rs v\([^ ]*\).*/\1/p' | sort -u | paste -sd' ' - || true)"
  if [ "$(wc -w <<<"$sdks")" -gt 1 ]; then
    echo "   NOTE: bsv-rs $sdks in the graph"
    note="${note:+$note; }bsv-rs $sdks"
  fi

  # The rewrite took: the dependency resolves to this tree.
  if [ "$build" != "FAIL" ]; then
    if (cd "$src" && cargo tree -i "$PKG@$tree_version" --depth 0 2>/dev/null) | grep -qF "($TREE)"; then
      echo "   resolves $PKG v$tree_version ($TREE)"
    else
      echo "   ERROR: $PKG does not resolve to $TREE"
      build="FAIL (not the tree)"
    fi
    local dupes
    dupes="$(cd "$src" && cargo tree -d --depth 0 2>/dev/null | grep -c "^$PKG v" || true)"
    if [ "$dupes" -gt 1 ]; then
      echo "   NOTE: $dupes copies of $PKG in the graph (a dependency of the dependent pins another)"
      note="${note:+$note; }$dupes copies of $PKG"
    fi
  fi

  # Every target by default: a test the build does not reach is not a test. Past
  # a failing target too (--no-fail-fast), so the row names every failed test.
  if [ "${build#ok}" != "$build" ]; then
    local what="cargo test --locked --no-fail-fast"
    if [ -n "$testcmd" ] && [ "$testcmd" != "default" ]; then
      what="${testcmd//\`/}"
      note="${note:+$note; }narrowed: $what"
    elif [ "$build" = "ok (unlocked)" ]; then
      what="cargo test --no-fail-fast"
    fi
    echo "   $what"
    # The knob for every row, not the CLI's alone: a dependent whose tests do not
    # read WIRE_TEXT_DIFF ignores an unknown variable.
    log="$WORK/$crate.test.log"
    if (cd "$src" && unset RUSTFLAGS && export WIRE_TEXT_DIFF=report && eval "$what") 2>&1 | tee "$log"; then
      test="ok ($what)"
    else
      test="FAIL ($what)"
      failed="$(failed_tests "$log")"
      [ -z "$failed" ] || test="$test: $failed"
    fi
  fi

  [ "${build#ok}" != "$build" ] && [ "${test#ok}" != "$test" ] || status=1
  summary+=("$crate|$build|$test${note:+ [$note]}")
  [ -z "$log" ] || while IFS= read -r r; do summary+=("$r"); done < <(text_rows "$crate" "$log")
}

any=0
while IFS='|' read -r crate clone dir inci testcmd; do
  [ -n "$crate" ] || continue
  if [ -n "$only" ] && [ "$crate" != "$only" ]; then continue; fi
  if [ "$ci" -eq 1 ] && [ "$inci" != "yes" ]; then
    summary+=("$crate|skipped in CI ($inci)|-")
    continue
  fi
  any=1
  run_one "$crate" "$clone" "$dir" "$testcmd"
done < <(rows)

if [ "$any" -eq 0 ]; then
  if [ -n "$only" ]; then
    echo "empty selection: no row named $only to run in $LIST" >&2
  elif [ "$ci" -eq 1 ]; then
    echo "empty selection: no row of $LIST has ci yes" >&2
  else
    echo "empty selection: the table in $LIST has no rows" >&2
  fi
  exit 2
fi

echo
echo "dependents of $PKG v$tree_version ($TREE):"
report
exit "$status"
