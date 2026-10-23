#!/usr/bin/env bash
# The release tag's verdict (RELEASING.md, "The two lines").
#
#   tools/release-tag-check.sh <tag> <manifest-version> [<branch> ...]
#
# <branch> ... names the branches that hold the tagged commit (release.yml
# asks git; a test passes them by hand, so the decision needs no repository).
#
#   vX.Y.Z         main's: the commit must be on main.
#   vX.Y.Z-beta.N  beta's: the commit must be on beta; a prerelease.
#   anything else  refused (only -beta.N is a prerelease shape here).
#
# The tag without its `v` must equal the manifest version. On a pass it prints
# `line=<main|beta>` and `prerelease=<true|false>` (lines for GITHUB_OUTPUT)
# and exits 0; on a refusal it prints an ::error:: line and exits 1.
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <tag> <manifest-version> [<branch> ...]" >&2
  exit 2
fi

tag="$1"
version="$2"
shift 2

refuse() {
  echo "::error::$*"
  exit 1
}

holds() {
  local b
  for b in ${branches[@]+"${branches[@]}"}; do
    [ "$b" = "$1" ] && return 0
  done
  return 1
}

branches=("$@")
num='(0|[1-9][0-9]*)'

if [[ "$tag" =~ ^v$num\.$num\.$num$ ]]; then
  line=main
  prerelease=false
elif [[ "$tag" =~ ^v$num\.$num\.$num-beta\.$num$ ]]; then
  line=beta
  prerelease=true
else
  refuse "tag ${tag} is neither vX.Y.Z (main's) nor vX.Y.Z-beta.N (beta's); only -beta.N is a prerelease shape here"
fi

if [ "${tag#v}" != "$version" ]; then
  refuse "tag ${tag} does not match Cargo.toml version ${version}"
fi

if ! holds "$line"; then
  if [ "$line" = main ] && holds beta; then
    refuse "tag ${tag} is a plain tag on a commit beta holds but main does not; a plain vX.Y.Z is main's, a beta commit takes vX.Y.Z-beta.N"
  fi
  if [ "$line" = beta ] && holds main; then
    refuse "tag ${tag} is a beta tag on a commit main holds but beta does not; vX.Y.Z-beta.N is beta's, a main commit takes a plain vX.Y.Z"
  fi
  refuse "tag ${tag} is ${line}'s, and the tagged commit is not on ${line}; tag a commit that merged through a PR"
fi

echo "line=${line}"
echo "prerelease=${prerelease}"
