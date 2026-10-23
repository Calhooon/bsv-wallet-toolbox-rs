#!/usr/bin/env bash
# Build and test each dependent named in docs/DEPENDENTS.md against this tree.
#
# For every row of the check table (between the dependents-check markers):
#   1. clone the dependent at its default branch into a scratch directory
#      (or copy a local checkout given with --from; the source is never written);
#   2. rewrite its dependency on bsv-wallet-toolbox-rs to a path dependency on
#      this tree. A [patch.crates-io] entry would not apply when the dependent's
#      pin excludes this tree's version, so the dependency line itself is edited;
#   3. keep Cargo.lock honest: `cargo update -p bsv-wallet-toolbox-rs@<locked>`
#      moves that one package (and only what it newly needs); the lock's diff is
#      printed;
#   4. `cargo build --locked`, falling back to `cargo build` with a note when
#      the lock still needs a change (or the dependent commits no lock);
#   5. `cargo test --lib` (`--bins` with a note when the crate has no library).
#
# Usage: tools/dependents-check.sh [--ci] [--only CRATE] [--from CRATE=DIR]...
#   --ci          run only the rows whose ci column is `yes`
#   --only CRATE  run one row
#   --from C=DIR  take crate C's repository from the local DIR (a copy is built; DIR is never written)
# Environment: DEPENDENTS_WORK (scratch, default $RUNNER_TEMP or $TMPDIR),
#   DEPENDENTS_TARGET_DIR (cargo target root, default inside the scratch).
# Exits non-zero when any dependent fails to build or test.

set -euo pipefail

PKG=bsv-wallet-toolbox-rs
TREE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIST="$TREE/docs/DEPENDENTS.md"
WORK="${DEPENDENTS_WORK:-${RUNNER_TEMP:-${TMPDIR:-}}}"
if [ -z "$WORK" ]; then
  echo "set DEPENDENTS_WORK or TMPDIR" >&2
  exit 2
fi
WORK="$WORK/dependents-check"

ci=0
only=""
declare -a froms=()
while [ $# -gt 0 ]; do
  case "$1" in
    --ci) ci=1 ;;
    --only) only="$2"; shift ;;
    --from) froms+=("$2"); shift ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
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

# The rows of the check table: crate|clone|dir|ci
rows() {
  sed -n '/<!-- dependents-check:begin -->/,/<!-- dependents-check:end -->/p' "$LIST" |
    grep '^| ' | sed '1d' |
    awk -F'|' '{ for (i = 2; i <= 5; i++) { gsub(/^ +| +$/, "", $i) } print $2 "|" $3 "|" $4 "|" $5 }'
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

declare -a summary=()
status=0

run_one() {
  local crate="$1" clone="$2" dir="$3"
  local src dest from
  dest="$WORK/$crate"
  rm -rf "$dest"
  mkdir -p "$WORK"

  from="$(from_for "$crate")"
  if [ -n "$from" ]; then
    echo "== $crate: copying $from (the source is not touched)"
    rsync -a --exclude target --exclude .git "$from/" "$dest/"
  else
    echo "== $crate: cloning $clone at its default branch"
    git clone --quiet --depth 1 "$clone" "$dest"
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
  local note="" build="fail" test="not run" lock
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

  if [ "${build#ok}" != "$build" ]; then
    local has_lib
    has_lib="$(cd "$src" && cargo metadata --no-deps --format-version 1 |
      jq --arg m "$(pwd -P)/Cargo.toml" '[.packages[] | select(.manifest_path == $m) | .targets[] | select(.kind | index("lib") or index("rlib"))] | length')"
    local what="--lib"
    if [ "$has_lib" -eq 0 ]; then
      what="--bins"
      note="${note:+$note; }no library; tested --bins"
      echo "   NOTE: no library target; cargo test --bins"
    fi
    if (cd "$src" && unset RUSTFLAGS && cargo test $what); then
      test="ok ($what)"
    else
      test="FAIL ($what)"
    fi
  fi

  [ "${build#ok}" != "$build" ] && [ "${test#ok}" != "$test" ] || status=1
  summary+=("$crate|$build|$test${note:+ [$note]}")
}

any=0
while IFS='|' read -r crate clone dir inci; do
  [ -n "$crate" ] || continue
  if [ -n "$only" ] && [ "$crate" != "$only" ]; then continue; fi
  if [ "$ci" -eq 1 ] && [ "$inci" != "yes" ]; then
    summary+=("$crate|skipped in CI ($inci)|-")
    continue
  fi
  any=1
  run_one "$crate" "$clone" "$dir"
done < <(rows)

if [ -n "$only" ] && [ "$any" -eq 0 ]; then
  echo "no row named $only in $LIST" >&2
  exit 2
fi

echo
echo "dependents of $PKG v$tree_version ($TREE):"
for s in ${summary[@]+"${summary[@]}"}; do
  IFS='|' read -r c b t <<<"$s"
  printf '  %-16s build: %-28s test: %s\n' "$c" "$b" "$t"
done
exit "$status"
