#!/usr/bin/env bash
# Check release.yml's reusable CI call against every ci.yml job's permissions.
# Usage: tools/workflow-grants-check.sh [release.yml ci.yml]
# The reader handles permission maps, read-all and write-all using Python's
# standard library. Unsupported permission syntax fails closed.
set -euo pipefail

if [ "$#" -ne 0 ] && [ "$#" -ne 2 ]; then
  echo "usage: $0 [release.yml ci.yml]" >&2
  exit 2
fi
root="$(cd "$(dirname "$0")/.." && pwd)"
python3 - "${1:-$root/.github/workflows/release.yml}" "${2:-$root/.github/workflows/ci.yml}" <<'PY'
import re
import sys
from pathlib import Path

LEVEL = {"none": 0, "read": 1, "write": 2}


def uncomment(text):
    quote = None
    escaped = False
    for i, char in enumerate(text):
        if escaped:
            escaped = False
        elif char == "\\" and quote == '"':
            escaped = True
        elif char == quote:
            quote = None
        elif char in "\"'" and quote is None:
            quote = char
        elif char == "#" and quote is None and (i == 0 or text[i - 1].isspace()):
            return text[:i].rstrip()
    return text.rstrip()


def scalar(value):
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def mapping(lines):
    """Read direct fields only; nested steps and script bodies stay opaque."""
    result = {}
    if not lines:
        return result
    indent = lines[0][0]
    i = 0
    while i < len(lines):
        depth, text = lines[i]
        match = re.fullmatch(r"([\w-]+|'[^']+'|\"[^\"]+\"):\s*(.*)", text)
        if depth != indent or match is None:
            raise ValueError(f"unsupported mapping entry: {text}")
        key, value = scalar(match[1]), match[2]
        end = i + 1
        while end < len(lines) and lines[end][0] > indent:
            end += 1
        if key in result:
            raise ValueError(f"duplicate field: {key}")
        result[key] = (value, lines[i + 1:end])
        i = end
    return result


def workflow(path):
    lines = []
    for raw in Path(path).read_text().splitlines():
        text = uncomment(raw.lstrip(" "))
        if not text or text in ("---", "..."):
            continue
        lines.append((len(raw) - len(raw.lstrip(" ")), text))
    fields = mapping(lines)
    jobs_value, jobs_lines = fields.get("jobs", ("", []))
    if jobs_value or not jobs_lines:
        raise ValueError(f"{path}: expected a jobs mapping")
    jobs = {}
    for name, (value, body) in mapping(jobs_lines).items():
        if value or not body:
            raise ValueError(f"{path}: expected a mapping for job {name}")
        jobs[name] = mapping(body)
    return fields, jobs


def permissions(block):
    if block is None:
        return None
    value, body = block
    value = scalar(value)
    if value in ("read-all", "write-all") and not body:
        return {"*": value.removesuffix("-all")}
    if value.startswith("{") and value.endswith("}") and not body:
        body = [(0, item.strip()) for item in value[1:-1].split(",") if item.strip()]
    elif value or not body:
        raise ValueError(f"unsupported permissions: {value!r}")
    result = {}
    for scope, (level, nested) in mapping(body).items():
        level = scalar(level)
        if nested or level not in LEVEL or scope == "*":
            raise ValueError(f"unsupported permission: {scope}: {level}")
        result[scope] = level
    return result


def effective(job, fields):
    if "permissions" in job:
        return permissions(job["permissions"]), "job"
    return permissions(fields.get("permissions")), "workflow fallback"


def describe(grants):
    if grants is None:
        return "no explicit grant"
    return ", ".join(f"{scope}: {level}" for scope, level in grants.items()) or "{}"


def check(release_path, ci_path):
    release, callers = workflow(release_path)
    ci, jobs = workflow(ci_path)
    callers = {name: job for name, job in callers.items()
               if scalar(job.get("uses", ("", []))[0]) == "./.github/workflows/ci.yml"}
    if not callers:
        raise ValueError("no caller uses ./.github/workflows/ci.yml")
    findings = []
    for caller_name, caller in callers.items():
        grant, source = effective(caller, release)
        print(f"caller {caller_name} grants {describe(grant)} ({source})")
        if grant is None:
            findings.append(f"caller {caller_name}: no explicit grant; token default is unknown")
        for name, job in jobs.items():
            asks, source = effective(job, ci)
            print(f"callee {name} requests {describe(asks)} ({source})")
            if asks is None:
                # With no callee override, GitHub keeps the caller's grant.
                continue
            for scope, level in asks.items():
                given = (grant or {}).get(scope, (grant or {}).get("*", "none"))
                if LEVEL[level] <= LEVEL[given]:
                    continue
                detail = (f"grants {scope}: {given}" if given != "none" else
                          f"has no explicit grant for {scope}")
                findings.append(f"callee job {name} requests {scope}: {level}; "
                                f"caller {caller_name} {detail}")
    for finding in findings:
        print(f"::error::{finding}")
    if findings:
        return 1
    print("workflow grants: ok")
    return 0


try:
    sys.exit(check(*sys.argv[1:]))
except (OSError, ValueError) as error:
    print(f"::error::{error}", file=sys.stderr)
    sys.exit(1)
PY
