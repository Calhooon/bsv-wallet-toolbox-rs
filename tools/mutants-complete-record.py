#!/usr/bin/env python3
"""Write the complete Mutants run's record from its ledger; see docs/census/.

Reads the run's ledger (catalog.json, base.txt, runs/iN/mutants.out/outcomes.json,
state/done/N, logs/rounds.log, logs/load.log, logs/baseline.log) and writes
docs/census/mutants-complete-2026-11-02.{md,json} and the mutation cell's source
in tools/ratchet.json (metrics.mutation_score_pct and its mutation_score
section), which tools/scoreboard-write reads. Counting is tools/mutants-score's
own collect() with the skipped baseline accepted; score = caught / (caught +
missed + timeout), unviable excluded. Floors are the base commit's
tools/mutants-floor.json, read and never written.

The run moved between two hosts; a mutant's host is the segment holding its
state/done/N file's mtime (its completion time), the segments bounded by the
captain's and the lanes' host lines in logs/rounds.log.

    python3 tools/mutants-complete-record.py [--ledger DIR]
"""

import argparse
import datetime
import importlib.machinery
import importlib.util
import json
from collections import Counter
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
LEDGER = Path("/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-mutants-complete-box")
DAY = "2026-11-02"
STEM = f"docs/census/mutants-complete-{DAY}"
DOORS = ("src/storage/sqlx/create_action/must_spend.rs",
         "src/storage/sqlx/create_action/own_word.rs",
         "src/storage/sqlx/create_action/review_beef.rs")
# (host, cores, lane, the rounds.log text opening the segment, the text closing it).
# None opens at the log's top or closes at its end.
SEGMENTS = (
    ("box", 18, "p5-mutants-complete-box", None, "captain: the box lane stopped"),
    ("studio", 10, "p5-mutants-complete-studio-b", "studio lane (p5-mutants-complete-studio-b)",
     "captain: the Studio lane stopped"),
    ("box", 18, "p5-mutants-complete-box-c", "lane p5-mutants-complete-box-c:", None),
)
# The rounds-log lines that switched RUST_TEST_THREADS=2 on and off; a round takes the
# driver as it stands at its opening.
THREADS = (("RUST_TEST_THREADS=2 (the box load rule)", 2), ("RUST_TEST_THREADS=2 removed", None),
           ("RUST_TEST_THREADS=2 put back", 2))
# The first burst of beef-rejected-*.hex dumps into $TMPDIR (pid 93567, 22:18:42Z to 22:23:05Z;
# pid 41370 wrote the second, 23:21:07Z to 23:24:40Z, by the dumps' own millisecond stamps):
# about 1.3 million files, so a test that reads the folder whole runs minutes from then on.
DUMPS_FROM = datetime.datetime(2026, 11, 1, 22, 18, 42, tzinfo=datetime.timezone.utc)
MEASURED = ("measured on the box on the unmutated base after the run, 2026-11-02T07:03Z to 07:15Z, with "
            "$TMPDIR at 1,411,380 entries: the lib pass 253.06 s under RUST_TEST_THREADS=2 and 195.16 s at the "
            "default, the one test alone 171.55 s; logs baseline-threads-{2,default}.log and one-test.log under "
            "/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-mutants-complete-box-c/")
HOSTS = {"box": "the box (18 cores, rustc 1.98.1, SDKROOT the Xcode 26.3 SDK)",
         "studio": "the Studio (10 cores, its default toolchain and SDK)"}


def tool(name):
    loader = importlib.machinery.SourceFileLoader(name, str(ROOT / "tools" / name))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def stamp(line):
    return datetime.datetime.fromisoformat(line.split()[0].replace("Z", "+00:00"))


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def pct(c):
    d = c["caught"] + c["missed"] + c["timeout"]
    return round(100 * c["caught"] / d, 2) if d else None


def first(text):
    return text.strip().splitlines()[0]


def segments(rounds):
    """Each segment's host, cores, lane, opening and closing stamps from the rounds log."""
    out = []
    for host, cores, lane, opens, closes in SEGMENTS:
        start = stamp(rounds[0]) if opens is None else stamp(next(l for l in rounds if opens in l))
        # The log's last stamp is whole seconds; a done file's mtime is not.
        end = stamp(rounds[-1]) + datetime.timedelta(seconds=1) if closes is None else stamp(next(l for l in rounds if closes in l))
        out.append({"host": host, "cores": cores, "lane": lane, "start": start, "end": end})
    return out


def threads(rounds):
    """The switches of RUST_TEST_THREADS as (stamp, value), value None for the default."""
    return [(stamp(next(l for l in rounds if text in l)), value) for text, value in THREADS]


def openings(ledger):
    """Every round's opening: its stamp and one-minute load. The load is load.log's line
    stamped within five seconds of the opening (the box lane's early lines carry no stamp),
    else the rounded figure the rounds log wrote, else None."""
    rounds = [l for l in (ledger / "logs/rounds.log").read_text().splitlines() if "round start" in l]
    loads = [(stamp(l), float(l.split("load averages:")[1].split()[0]))
             for l in (ledger / "logs/load.log").read_text().splitlines() if l.startswith("20")]
    out = []
    for l in rounds:
        t = stamp(l)
        near = [v for s, v in loads if abs((s - t).total_seconds()) <= 5]
        words = l.split()
        logged = float(words[words.index("load") + 1].rstrip(",")) if "load" in words else None
        out.append({"stamp": t, "load_1m": near[0] if near else logged})
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=LEDGER)
    ledger = parser.parse_args().ledger
    score = tool("mutants-score")
    catalog = json.loads((ledger / "catalog.json").read_text())
    base = (ledger / "base.txt").read_text().strip()
    floors = json.loads(subprocess.run(["git", "-C", str(ROOT), "show", f"{base}:tools/mutants-floor.json"],
                                       capture_output=True, text=True, check=True).stdout)["files"]
    paths = [ledger / "runs" / f"i{n}" / "mutants.out" / "outcomes.json" for n in range(len(catalog))]
    missing = [n for n, p in enumerate(paths) if not p.exists()]
    documents = [(str(p), json.loads(p.read_text())) for p in paths if p.exists()]
    counts, seen, problems = score.collect(documents, skipped_baseline=True)
    expected = {m["name"]: m["file"] for m in catalog}
    absent = sorted(set(expected) - seen)

    rounds = (ledger / "logs/rounds.log").read_text().splitlines()
    segs = segments(rounds)
    opens = openings(ledger)
    switches = threads(rounds)
    verdicts = {}
    for _, doc in documents:
        for row in doc["outcomes"]:
            if row["scenario"] != "Baseline":
                verdicts[row["scenario"]["Mutant"]["name"]] = row["summary"]
    mutants = []
    for n, m in enumerate(catalog):
        done = ledger / "state/done" / str(n)
        if not done.exists():
            problems.append(f"catalog index {n} has no state/done entry")
            continue
        t = datetime.datetime.fromtimestamp(done.stat().st_mtime, datetime.timezone.utc)
        seg = next((i for i, s in enumerate(segs) if s["start"] <= t <= s["end"]), None)
        if seg is None:
            problems.append(f"catalog index {n} finished at {iso(t)}, outside every host segment")
        o = max((o for o in opens if o["stamp"] <= t), key=lambda o: o["stamp"])
        mutants.append({"n": n, "file": m["file"], "name": m["name"], "verdict": verdicts.get(m["name"]),
                        "seconds": float(done.read_text().split()[1]), "finished": t, "segment": seg,
                        "host": segs[seg]["host"] if seg is not None else None,
                        "round_opened": o["stamp"], "load_1m": o["load_1m"],
                        "threads": next((v for s, v in reversed(switches) if s <= o["stamp"]), None)})
    for i, s in enumerate(segs):
        s["mutants"] = sum(1 for m in mutants if m["segment"] == i)
        s["verdicts"] = Counter(m["verdict"] for m in mutants if m["segment"] == i)

    survivors, timeouts = {}, {}
    for m in mutants:
        if m["verdict"] == "MissedMutant":
            survivors.setdefault(m["file"], []).append(m["name"])
        elif m["verdict"] == "Timeout":
            timeouts.setdefault(m["file"], []).append(m["name"])
    studio = [m for m in mutants if m["host"] == "studio" and m["verdict"] == "Timeout"]
    two = [m for m in mutants if m["threads"] == 2 and m["verdict"] == "Timeout"]

    totals = Counter()
    files = []
    for f in sorted(set(expected.values())):
        c = counts.get(f, Counter())
        totals.update(c)
        wanted = sum(1 for v in expected.values() if v == f)
        floor = floors.get(f, {}).get("floor")
        s = pct(c)
        if sum(c.values()) != wanted:
            verdict = "incomplete"
        elif f not in floors:
            verdict = "no floor entry"
        elif floor is None:
            verdict = "floor pending"
        elif s is None:
            verdict = "no viable mutants"
        elif c["caught"] * 100 < floor * (c["caught"] + c["missed"] + c["timeout"]):
            verdict = "below floor"
        else:
            verdict = "at or above floor"
        # Below the floor by the Studio's timeouts alone: at or above it with them left out.
        def below_by(timed):
            k = sum(1 for m in timed if m["file"] == f)
            return k, (verdict == "below floor" and k > 0
                       and c["caught"] * 100 >= floor * (c["caught"] + c["missed"] + c["timeout"] - k))
        st, by_studio = below_by(studio)
        tt, by_two = below_by(two)
        hosts = Counter(m["host"] for m in mutants if m["file"] == f)
        files.append({"file": f, "mutants": wanted, "caught": c["caught"], "missed": c["missed"],
                      "timeout": c["timeout"], "unviable": c["unviable"], "score": s,
                      "floor": floor, "verdict": verdict, "hosts": dict(sorted(hosts.items(), key=lambda kv: str(kv[0]))),
                      "studio_timeouts": st, "below_by_studio_timeouts": by_studio,
                      "threads_2_timeouts": tt, "below_by_threads_2_timeouts": by_two})

    complete = not missing and not absent and not problems
    overall = pct(totals)
    seconds = [m["seconds"] for m in mutants]
    baseline = next(l for l in (ledger / "logs/baseline.log").read_text().splitlines()
                    if l.startswith("test result"))
    hours = {h: round(sum((s["end"] - s["start"]).total_seconds() for s in segs if s["host"] == h) / 3600, 2)
             for h in HOSTS}
    run = {
        "base": base,
        "catalog": f"cargo mutants --list --json > catalog.json, in a copy of the base (git archive {base[:8]}); "
                   f"the set of .cargo/mutants.toml: {len(catalog)} mutants over {len(set(expected.values()))} files",
        "baseline": "CARGO_TARGET_DIR=$HOME/bsv/targets/p5-mutants-complete-box/w1 cargo test --lib on the box, "
                    "rule 10's proxy assignments inline: " + baseline,
        "command": "per catalog index N, worker K, in the scratch copy wK of the base (reset to the base before "
                   "each run): HTTP_PROXY=http://127.0.0.1:9 HTTPS_PROXY=http://127.0.0.1:9 "
                   "ALL_PROXY=http://127.0.0.1:9 NO_PROXY=127.0.0.1,localhost,::1 no_proxy=127.0.0.1,localhost,::1 "
                   "CARGO_TARGET_DIR=$HOME/bsv/targets/p5-mutants-complete-box/wK CARGO_BUILD_JOBS=2 "
                   "RUST_TEST_THREADS=2 [SDKROOT=<Xcode 26.3's MacOSX.sdk> on the box] cargo mutants --in-place "
                   "--baseline skip --timeout 270 --build-timeout 400 "
                   f"--shard N/{len(catalog)} --sharding round-robin --output runs/iN",
        "driver": "round.py in the ledger (one foreground round per shell call, every round's date -u in "
                  "logs/rounds.log and its opening uptime in logs/load.log; eight workers when the one-minute "
                  "load at a round's opening is 50 or under, four above; a run still going at a round's deadline "
                  "is killed, its output removed and the index run again whole in a later round: it had no outcome)",
        "scoring": "tools/mutants-score collect(), skipped baseline; score = caught / (caught + missed + timeout), "
                   "unviable excluded; the per-file table also as tools/mutants-score --catalog catalog.json "
                   "--baseline-skipped --report-only over every outcomes file",
        "floors": f"tools/mutants-floor.json at the base {base[:8]}, read only",
        "oracle": "the Mutants workflow's: cargo test --lib (additional_cargo_args in .cargo/mutants.toml)",
        "started": iso(segs[0]["start"]), "finished": iso(segs[-1]["end"]),
        "host_hours": hours,
        "mutant_seconds": {"sum": round(sum(seconds)), "max": round(max(seconds))},
        "rounds": len(opens),
        "hosts": HOSTS,
        "test_threads": [{"from": iso(s), "RUST_TEST_THREADS": v or "default"} for s, v in switches],
        "timeouts_by_threads": {str(v or "default"): dict(sorted(Counter(m["verdict"] for m in mutants
                                                                      if m["threads"] == v).items()))
                                for v in (None, 2)},
        "by_tmpdir_and_threads": {f"{when}, RUST_TEST_THREADS={v or 'default'}": dict(sorted(Counter(
            m["verdict"] for m in mutants if m["threads"] == v
            and (m["finished"] >= DUMPS_FROM) == (when == "after the dumps")).items()))
            for when in ("before the dumps", "after the dumps") for v in (None, 2)},
        "segments": [{"host": s["host"], "cores": s["cores"], "lane": s["lane"], "start": iso(s["start"]),
                      "end": iso(s["end"]), "mutants": s["mutants"],
                      "verdicts": dict(sorted(s["verdicts"].items()))} for s in segs],
        "toolchain": "box: " + first(subprocess.run(["rustc", "-V"], capture_output=True, text=True).stdout)
                     + "; the Studio's toolchain is not in the ledger",
        "tool_version": first(subprocess.run(["cargo", "mutants", "--version"], capture_output=True,
                                             text=True).stdout),
        "lockfile": "Cargo.lock (no lock is committed; resolved once by cargo fetch, copied to every worker)",
        "logs": str(ledger) + ": logs/rounds.log, logs/load.log, logs/baseline.log, runs/i<N>.log, "
                "runs/i<N>/mutants.out, state/done/<N>",
    }
    head = {"caught": totals["caught"], "missed": totals["missed"], "timeout": totals["timeout"],
            "unviable": totals["unviable"], "mutants": len(catalog), "tested": len(seen)}
    record = {
        "producer": "tools/mutants-complete-record.py over the run's ledger (lanes p5-mutants-complete-box, "
                    "p5-mutants-complete-studio-b, p5-mutants-complete-box-c)",
        "scoreboard": ["mutation_score_pct"],
        "complete": complete,
        "problems": problems + ([f"catalog indices with no outcome: {len(missing)}"] if missing else []),
        "run": run,
        "cells": [dict(cell="mutation_score_pct", value=overall, unit="percent",
                       scope="the Mutants workflow's set, every catalogued mutant", **head)]
                 + [dict(cell=f"mutants.file.{r['file']}.score", value=r["score"], unit="percent", **{
                     k: v for k, v in r.items() if k not in ("file", "score")}) for r in files],
        "studio_timeouts": [{"n": m["n"], "name": m["name"], "round_opened": iso(m["round_opened"]),
                             "load_1m": m["load_1m"]} for m in studio],
        "survivors": survivors,
        "timeouts": timeouts,
        "mutants": [[m["n"], m["verdict"], m["host"], m["threads"] or 0, m["seconds"]] for m in mutants],
    }
    write_json(ROOT / f"{STEM}.json", record)
    write_md(ROOT / f"{STEM}.md", record, files, head, overall, run)
    if complete:
        ratchet(overall, head, run)
    print(f"tested {len(seen)}/{len(catalog)}; score {overall}; complete {complete}; "
          f"problems {len(record['problems'])}")


def write_json(path, record):
    """Two-space JSON, each cell, list and survivor file on one line and eight mutants a line
    (the 800-line rule); `mutants` rows are [catalog index, verdict, host, RUST_TEST_THREADS (0 the default),
    seconds]."""
    lines = ["{"]
    keys = list(record)
    for i, key in enumerate(keys):
        value, comma = record[key], "," if i < len(keys) - 1 else ""
        if key in ("cells", "studio_timeouts"):
            lines.append(f'  "{key}": [')
            lines += ["    " + json.dumps(c) + ("," if j < len(value) - 1 else "") for j, c in enumerate(value)]
            lines.append("  ]" + comma)
        elif key == "mutants":
            rows = [", ".join(json.dumps(m) for m in value[j:j + 8]) for j in range(0, len(value), 8)]
            lines.append('  "mutants": [')
            lines += ["    " + r + ("," if j < len(rows) - 1 else "") for j, r in enumerate(rows)]
            lines.append("  ]" + comma)
        elif key in ("survivors", "timeouts"):
            lines.append(f'  "{key}": {{')
            lines += [f"    {json.dumps(f)}: {json.dumps(v)}" + ("," if j < len(value) - 1 else "")
                      for j, (f, v) in enumerate(value.items())]
            lines.append("  }" + comma)
        else:
            body = json.dumps(value, indent=2).replace("\n", "\n  ")
            lines.append(f"  {json.dumps(key)}: {body}{comma}")
    lines.append("}")
    path.write_text("\n".join(lines) + "\n")


def cell(v):
    return "n/a" if v is None else f"{v}%"


def short(name):
    loc, change = name.split(": ", 1)
    return f"{':'.join(loc.split(':')[1:])} {change}"


def write_md(path, record, files, head, overall, run):
    viable = head["caught"] + head["missed"] + head["timeout"]
    below = [r for r in files if r["verdict"] == "below floor"]
    nofloor = [r for r in files if r["verdict"] == "no floor entry"]
    segs = run["segments"]
    seg_lines = [f"- {HOSTS[s['host']]}, lane `{s['lane']}`, {s['start']} to {s['end']}: {s['mutants']} mutants "
                 f"({', '.join(f'{v} {k}' for k, v in s['verdicts'].items())})" for s in segs]
    out = [f"# The complete Mutants run of {DAY}", "",
           f"Base `{run['base'][:8]}` (the run spans {run['started']} to {run['finished']} on two hosts: "
           f"{run['host_hours']['box']} hours on the box and {run['host_hours']['studio']} on the Studio by the "
           f"rounds' `date -u`). Every value below is written by `tools/mutants-complete-record.py` from the run's "
           f"ledger; the machine-readable cells, the run's commands, times, hosts, toolchain, tool version, "
           f"logs and every mutant's verdict and host are [mutants-complete-{DAY}.json](mutants-complete-{DAY}.json). "
           f"The ledger is on the lane volume under `{LEDGER}/` (`catalog.json`, `runs/`, `state/`, `logs/`). "
           f"The scoreboard cell `mutation_score_pct` in `docs/NORTH-STAR.md` is written from this run by "
           f"`tools/scoreboard-write`.", "",
           "## Method", "",
           f"`{run['tool_version']}` over the Mutants workflow's set (`.cargo/mutants.toml`: "
           f"{head['mutants']} mutants over {len(files)} files), every catalogued mutant once, one mutant per "
           f"`--in-place --baseline skip` invocation after one green `cargo test --lib` on the base "
           f"({run['baseline'].split(': ', 1)[1]}); test timeout 270 s, build timeout 400 s; eight workers "
           f"(four when a round opened above a one-minute load of 50), each in its own scratch copy and target "
           f"directory, `CARGO_BUILD_JOBS=2`, `RUST_TEST_THREADS=2` in the rounds the Timeouts section names "
           f"(the default in the rest); the oracle is the workflow's (`--lib`). "
           f"A timeout is a timeout, never retried into a kill; a run killed at a round's deadline had no "
           f"outcome and ran again whole. Scored by `tools/mutants-score` (`collect()`, skipped baseline): "
           f"score = caught / (caught + missed + timeout), unviable excluded. Floors are the base's "
           f"`tools/mutants-floor.json`, reported only; no floor is written by this run.", "",
           "The hosts, by segment (a mutant's host is the segment holding its completion time, the mtime of "
           "its `state/done/N`):", ""] + seg_lines + ["",
           f"Toolchain: {run['toolchain']}.", "",
           "## Totals", "",
           "| Mutants | Tested | Viable | Caught | Missed | Timeout | Unviable | Score |",
           "|---:|---:|---:|---:|---:|---:|---:|---:|",
           f"| {head['mutants']} | {head['tested']} | {viable} | {head['caught']} | {head['missed']} | "
           f"{head['timeout']} | {head['unviable']} | {cell(overall)} |", "",
           f"Files below their floor: {len(below)}. Files with no floor entry: {len(nofloor)}.", "",
           "## Per file", "",
           "| File | Host | Mutants | Caught | Missed | Timeout | Unviable | Score | Floor | Verdict |",
           "|---|---|---:|---:|---:|---:|---:|---:|---:|---|"]
    for r in files:
        door = " (door)" if r["file"] in DOORS else ""
        host = ", ".join(f"{h} {n}" for h, n in r["hosts"].items())
        out.append(f"| `{r['file']}`{door} | {host} | {r['mutants']} | {r['caught']} | {r['missed']} | "
                   f"{r['timeout']} | {r['unviable']} | {cell(r['score'])} | "
                   f"{'none' if r['floor'] is None else r['floor']} | {r['verdict']} |")
    out += ["", "## Timeouts", "",
            "Each named here took longer than the 270 s test timeout under its mutation (a slow test or a "
            "loop the mutation made); counted as not caught.", ""]
    for f, names in record["timeouts"].items():
        out.append(f"- `{f}`: " + "; ".join(f"`{short(n)}`" for n in names))
    if not record["timeouts"]:
        out.append("None.")
    out += ["", "### The Studio's timeouts", "",
            "Each timeout the Studio segment ran, with the one-minute load at its round's opening "
            "(`logs/load.log`):", ""]
    out += [f"- `{short(t['name'])}` (`{t['name'].split(':')[0]}`), catalog {t['n']}, round opened "
            f"{t['round_opened']} at load {t['load_1m']}" for t in record["studio_timeouts"]] or ["None."]
    by = [r["file"] for r in files if r["below_by_studio_timeouts"]]
    out += ["", "Files below their floor by the Studio's timeouts alone (at or above it with them left out): "
            + (", ".join(f"`{f}`" for f in by) if by else "none") + "."]
    tb = run["timeouts_by_threads"]
    out += ["", "### The timeouts under RUST_TEST_THREADS=2", "",
            "The driver's environment set `RUST_TEST_THREADS=2` for the rounds opened from "
            + ", ".join(f"{s['from']} ({s['RUST_TEST_THREADS']})" for s in run["test_threads"])
            + ". Verdicts at the default: " + ", ".join(f"{v} {k}" for k, v in tb["default"].items())
            + "; under 2 threads: " + ", ".join(f"{v} {k}" for k, v in tb["2"].items()) + ". The caught "
            "mutants' test phases under 2 threads run to within seconds of the 270 s test timeout (the "
            "`phase_results` in each `outcomes.json`), so a timeout there need not be a hang.", "",
            f"The second cause: from {iso(DUMPS_FROM)} two test processes wrote about 1.3 million "
            "`beef-rejected-*.hex` dumps (`describe_invalid_beef` in `create_action/beef_graph.rs`) into "
            "`$TMPDIR`, which every worker shares; `wallet::txid_only_tests::immediate_broadcast_writes_no_beef_dump` "
            "reads that folder whole twice (`beef_dumps()`), so on the unmutated base it alone took minutes "
            f"after ({MEASURED}). Verdicts by the dumps and the threads: "
            + "; ".join(f"{k}: " + ", ".join(f"{n} {v}" for v, n in c.items())
                        for k, c in run["by_tmpdir_and_threads"].items()) + ".", "",
            "Files below their floor by the 2-thread timeouts alone (at or above it with them left out): "
            + (", ".join(f"`{r['file']}`" for r in files if r["below_by_threads_2_timeouts"]) or "none") + "."]
    out += ["", "## Survivors", "",
            "Every missed mutant, by file, as `line:column` and the mutation (the full names are in the "
            "JSON's `survivors`). A survivor is a figure here; the triage is the hardening lanes' (#534, #535, "
            "#536).", ""]
    for f, names in record["survivors"].items():
        door = " (door)" if f in DOORS else ""
        out.append(f"- `{f}`{door} ({len(names)}): " + "; ".join(f"`{short(n)}`" for n in names))
    if record["problems"]:
        out += ["", "## Problems", ""] + [f"- {p}" for p in record["problems"]]
    path.write_text("\n".join(out) + "\n")


def ratchet(overall, head, run):
    path = ROOT / "tools/ratchet.json"
    data = json.loads(path.read_text())
    data["metrics"]["mutation_score_pct"] = overall
    data["mutation_score"] = {
        "method": f"{run['tool_version']} over .cargo/mutants.toml's set (the Mutants workflow's set: "
                  f"{head['mutants']} mutants), every catalogued mutant once, --in-place --baseline skip after a "
                  f"green `cargo test --lib` on {run['base'][:8]}, one catalog mutant per run (--shard N/"
                  f"{head['mutants']} --sharding round-robin), --timeout 270 --build-timeout 400, rule 10's proxy "
                  "variables; scored by tools/mutants-score (its collect(), --baseline-skipped); score = caught / "
                  "(caught + missed + timeout), unviable excluded",
        "caught": head["caught"], "missed": head["missed"], "timeout": head["timeout"],
        "unviable": head["unviable"],
        "hosts": {s["lane"]: f"{s['host']}, {s['mutants']} mutants" for s in run["segments"]},
        "source": f"{STEM}.{{md,json}} (the per-file table, every survivor and every mutant's host)",
        "measured_at": DAY,
    }
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
