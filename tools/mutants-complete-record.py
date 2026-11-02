#!/usr/bin/env python3
"""Write the complete Mutants run's record from its ledger; see docs/census/.

Reads the run's ledger (catalog.json, base.txt, runs/iN/mutants.out/outcomes.json,
state/done/N, logs/rounds.log, logs/load.log, logs/baseline.log, and the artifact
run's state/artifact-timeout/N and artifact/runs/iN) and writes
docs/census/mutants-complete-2026-11-02.{md,json} and the mutation cell's source
in tools/ratchet.json (metrics.mutation_score_pct and its mutation_score
section), which tools/scoreboard-write reads. Counting is tools/mutants-score's
own collect() with the skipped baseline accepted; score = caught / (caught +
missed + timeout), unviable excluded. Floors are the base commit's
tools/mutants-floor.json, read and never written.

The run moved between two hosts; a mutant's host is the segment holding its
state/done/N file's mtime (its completion time), the segments bounded by the
captain's and the lanes' host lines in logs/rounds.log.

The artifact: the run's 288 timeouts, most of them a test reading a temp folder
of 1.4 million dumps, were moved to state/artifact-timeout/N (their runs to
artifact/runs/iN) and run again under the restored environment (lane
p5-mutants-complete-box-d); the record's figure is the repaired one, and its
section "The artifact" carries the run as it stood.

    python3 tools/mutants-complete-record.py [--ledger DIR]
    python3 tools/mutants-complete-record.py --check   # the md against the json; exit 1 on a difference
"""

import argparse
import datetime
import importlib.machinery
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
LEDGER = Path("/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-mutants-complete-box")
DAY = "2026-11-02"
STEM = f"docs/census/mutants-complete-{DAY}"
ORACLE = "docs/census/mutants-oracle-2026-11-01.json"
DOORS = ("src/storage/sqlx/create_action/must_spend.rs",
         "src/storage/sqlx/create_action/own_word.rs",
         "src/storage/sqlx/create_action/review_beef.rs")
# (host, cores, lane, the rounds.log text opening the segment, the text closing it).
# None opens at the log's top or closes at its end.
SEGMENTS = (
    ("box", 18, "p5-mutants-complete-box", None, "captain: the box lane stopped"),
    ("studio", 10, "p5-mutants-complete-studio-b", "studio lane (p5-mutants-complete-studio-b)",
     "captain: the Studio lane stopped"),
    ("box", 18, "p5-mutants-complete-box-c", "lane p5-mutants-complete-box-c:", "lane p5-mutants-complete-box-d:"),
    ("box", 18, "p5-mutants-complete-box-d", "lane p5-mutants-complete-box-d:", None),
)
# The rounds-log lines that switched RUST_TEST_THREADS=2 on and off; a round takes the
# driver as it stands at its opening.
THREADS = (("RUST_TEST_THREADS=2 (the box load rule)", 2), ("RUST_TEST_THREADS=2 removed", None),
           ("RUST_TEST_THREADS=2 put back", 2), ("RUST_TEST_THREADS removed for the repair", None))
# The opening burst of beef-rejected-*.hex dumps into $TMPDIR (pid 93567, 22:18:42Z to 22:23:05Z;
# pid 41370 wrote the second, 23:21:07Z to 23:24:40Z, by the dumps' own millisecond stamps):
# about 1.3 million files, so a test that reads the folder whole runs minutes from then on.
DUMPS_FROM = datetime.datetime(2026, 11, 1, 22, 18, 42, tzinfo=datetime.timezone.utc)
MEASURED = ("measured on the box on the unmutated base after the run, 2026-11-02T07:03Z to 07:15Z, with "
            "$TMPDIR at 1,411,380 entries: the lib pass 253.06 s under RUST_TEST_THREADS=2 and 195.16 s at the "
            "default, the one test alone 171.55 s; logs baseline-threads-{2,default}.log and one-test.log under "
            "/Volumes/Lanes/out/bsv-wallet-toolbox-rs/p5-mutants-complete-box-c/")
WITNESS = "wallet::txid_only_tests::immediate_broadcast_writes_no_beef_dump"
# The repair's own figures (logs/environment-restored.log in the ledger).
PILE = {
    "cause": "a mutant turned describe_invalid_beef's dump (create_action/beef_graph.rs at the base) into an "
             "unbounded loop; two mutated test processes (pid 93567, 2026-11-01T22:18:42Z to 22:23:05Z; pid "
             "41370, 23:21:07Z to 23:24:40Z) wrote beef-rejected-*.hex into the shared $TMPDIR, and "
             f"{WITNESS} reads that folder whole twice (beef_dumps())",
    "folder": "/Volumes/Lanes/tmp",
    "files": 1398520, "entries": 1411380, "counted": "2026-11-02T07:43:48Z",
}
CLEARING = {
    "command": "find /Volumes/Lanes/tmp -maxdepth 1 -name 'beef-rejected-*.hex' -type f -delete",
    "at": "2026-11-02T07:49:00Z", "removed": 1398520, "entries_left": 12861,
    "free_before": "246Gi", "free_after": "250Gi",
}
ENVIRONMENT = {
    "at": "2026-11-02T07:50:29Z",
    "tree": "the pristine scratch copy of the base (no difference from git archive 42559923 save Cargo.lock), "
            "the default test threads, rule 10's proxy assignments inline, one-minute load 10.42",
    "witness_s": 0.08, "lib_s": 25.64, "witness_s_pile": 171.55, "lib_s_pile": 253.06,
    "lib_result": "test result: ok. 1730 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 25.64s",
}
# Each segment's worker rule, as its driver (round.py and its kept copies in the ledger) states it.
RULES = {
    "p5-mutants-complete-box": "eight by round.py.before-throttle-2240 until the captain's throttle to four at "
                               "22:39:24Z; from 00:03:36Z eight when the one-minute load at a round's opening is "
                               "50 or under, else four",
    "p5-mutants-complete-studio-b": "eight when the one-minute load at a round's opening is 50 or under, else four",
    "p5-mutants-complete-box-c": "eight when the one-minute load at a round's opening is 50 or under, else four",
    "p5-mutants-complete-box-d": "four when the one-minute load at a round's opening is 55 or under, else two",
}
# The rounds log names its worker count from 00:03:36Z on; before, round.py held a fixed count, eight until
# the captain's throttle line (THREADS[0]) and four after it.
FIXED = (8, 4)
HOSTS = {"box": "the box (18 cores, rustc 1.98.1, SDKROOT the Xcode 26.3 SDK)",
         "studio": "the Studio (10 cores, rustc 1.95.0 by a reading after the run, its default SDK)"}
# The ledger did not record the Studio's toolchain; this is the host's own reading after the run.
STUDIO_RUSTC = ("rustc 1.95.0, not in the ledger: `rustc --version` read on the Studio after the run "
                "(2026-11-02), and the version the record of #571 (docs/census/mutants-oracle-2026-11-01.md) "
                "names for its run on the same host on 2026-11-01")
THE = {"box": "the box", "studio": "the Studio"}
KEYS = ("caught", "missed", "timeout", "unviable")


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


def head_line(text):
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
        out.append({"stamp": t, "load_1m": near[0] if near else logged,
                    "round_load": None if logged is None else int(logged)})
    return out


def over_time(run):
    """The tests a run's log names as running over 60 seconds, those of them with no result line before
    the 270 s timeout, and what the log shows: a test that never finished, where the unmutated base's
    whole lib pass took ENVIRONMENT's lib_s, is a loop or a wait the mutation made; one that finished
    is a slow test."""
    lines = "".join(p.read_text(errors="replace") for p in sorted((run / "mutants.out/log").glob("*.log"))).splitlines()
    over = [l[len("test "):].split(" has been running")[0] for l in lines if "has been running for over 60" in l]
    unfinished = [t for t in over if not any(l.startswith(f"test {t} ... ") for l in lines)]
    kind = ("the log names no test over 60 s" if not over
            else "a loop or a wait the mutation made" if unfinished else "a slow test")
    return {"tests_over_60s": over, "unfinished": unfinished, "kind": kind}


def workers(rounds, segs, switches):
    """Each segment's spans of one worker count and one RUST_TEST_THREADS, by the rounds that opened in it.
    A span runs from its first round's opening (the segment's own opening for its first) to the next
    span's; the last to the segment's last round end, or its close when a stop ended it mid-round."""
    throttle = switches[0][0]
    ends = [stamp(l) for l in rounds if "round end" in l]
    opened = []
    for l in rounds:
        if "round start" in l:
            t, words = stamp(l), l.split()
            n = int(words[words.index("workers,") - 1]) if "workers," in words else FIXED[t > throttle]
            opened.append((t, n, next((v for s, v in reversed(switches) if s <= t), None)))
    out = []
    for seg in segs:
        spans = []
        for t, n, v in (o for o in opened if seg["start"] <= o[0] < seg["end"]):
            if not spans or (spans[-1]["workers"], spans[-1]["threads"]) != (n, v):
                spans.append({"workers": n, "threads": v, "from": t if spans else seg["start"], "rounds": 0})
            spans[-1]["rounds"] += 1
        last = max((t for t in ends if seg["start"] <= t < seg["end"]), default=None)
        for i, sp in enumerate(spans):
            sp["to"] = (spans[i + 1]["from"] if i + 1 < len(spans)
                        else last if last and last > max(o[0] for o in opened if o[0] < seg["end"]) else seg["end"])
        out.append({"lane": seg["lane"], "host": seg["host"], "rule": RULES[seg["lane"]], "spans": [
            {"workers": sp["workers"], "RUST_TEST_THREADS": sp["threads"] or "default", "from": iso(sp["from"]),
             "to": iso(sp["to"]), "hours": round((sp["to"] - sp["from"]).total_seconds() / 3600, 2),
             "rounds": sp["rounds"]} for sp in spans]})
    return out


def verdicts_of(documents):
    out = {}
    for _, doc in documents:
        for row in doc["outcomes"]:
            if row["scenario"] != "Baseline":
                out[row["scenario"]["Mutant"]["name"]] = row["summary"]
    return out


def tally(c):
    return {k: c[k] for k in KEYS} | {"score": pct(c)}


def floor_verdict(c, wanted, floor, listed):
    s = pct(c)
    if sum(c.values()) != wanted:
        return "incomplete"
    if not listed:
        return "no floor entry"
    if floor is None:
        return "floor pending"
    if s is None:
        return "no viable mutants"
    if c["caught"] * 100 < floor * (c["caught"] + c["missed"] + c["timeout"]):
        return "below floor"
    return "at or above floor"


def build(ledger):
    score = tool("mutants-score")
    catalog = json.loads((ledger / "catalog.json").read_text())
    base = (ledger / "base.txt").read_text().strip()
    floors = json.loads(subprocess.run(["git", "-C", str(ROOT), "show", f"{base}:tools/mutants-floor.json"],
                                       capture_output=True, text=True, check=True).stdout)["files"]
    moved = {int(p.name) for p in (ledger / "state/artifact-timeout").iterdir()}
    outcome = "mutants.out/outcomes.json"
    paths = [ledger / "runs" / f"i{n}" / outcome for n in range(len(catalog))]
    art_paths = [ledger / "artifact/runs" / f"i{n}" / outcome if n in moved else p for n, p in enumerate(paths)]
    missing = [n for n, p in enumerate(paths) if not p.exists()]
    documents = [(str(p), json.loads(p.read_text())) for p in paths if p.exists()]
    art_documents = [(str(p), json.loads(p.read_text())) for p in art_paths if p.exists()]
    counts, seen, problems = score.collect(documents, skipped_baseline=True)
    art_counts, art_seen, art_problems = score.collect(art_documents, skipped_baseline=True)
    problems += [f"artifact: {p}" for p in art_problems]
    expected = {m["name"]: m["file"] for m in catalog}
    absent = sorted(set(expected) - seen) + sorted(set(expected) - art_seen)

    rounds = (ledger / "logs/rounds.log").read_text().splitlines()
    segs = segments(rounds)
    opens = openings(ledger)
    switches = threads(rounds)
    verdicts, art_verdicts = verdicts_of(documents), verdicts_of(art_documents)

    def place(n, m, path, verdict):
        if not path.exists():
            problems.append(f"catalog index {n} has no {path.parent.name} entry")
            return None
        t = datetime.datetime.fromtimestamp(path.stat().st_mtime, datetime.timezone.utc)
        seg = next((i for i, s in enumerate(segs) if s["start"] <= t <= s["end"]), None)
        if seg is None:
            problems.append(f"catalog index {n} finished at {iso(t)}, outside every host segment")
        # The round a run ran in is the last opened by its start (its completion less its seconds): the box
        # lane's early driver wrote state/done at the round's end, the next round's opening stamp.
        seconds = float(path.read_text().split()[1])
        began = t - datetime.timedelta(seconds=seconds)
        o = max((o for o in opens if o["stamp"] <= began), key=lambda o: o["stamp"])
        return {"n": n, "file": m["file"], "name": m["name"], "verdict": verdict,
                "seconds": seconds, "finished": t, "segment": seg,
                "host": segs[seg]["host"] if seg is not None else None,
                "round_opened": o["stamp"], "load_1m": o["load_1m"], "round_load": o["round_load"],
                "threads": next((v for s, v in reversed(switches) if s <= o["stamp"]), None)}

    mutants = [place(n, m, ledger / "state/done" / str(n), verdicts.get(m["name"])) for n, m in enumerate(catalog)]
    art = [place(n, m, ledger / "state/artifact-timeout" / str(n), art_verdicts.get(m["name"]))
           if n in moved else mutants[n] for n, m in enumerate(catalog)]
    if None in mutants or None in art:
        raise SystemExit("\n".join(problems))
    # A run lies inside the round it is placed in: its start (completion less its seconds) is not before
    # the round's opening.
    problems += [f"catalog index {m['n']} started before its round opened" for m in mutants
                 if (m["finished"] - m["round_opened"]).total_seconds() < m["seconds"]]
    problems += [f"artifact index {m['n']} is {m['verdict']}, not Timeout" for m in art
                 if m["n"] in moved and m["verdict"] != "Timeout"]
    problems += [f"Timeout at index {m['n']} not run again" for m in art
                 if m["verdict"] == "Timeout" and m["n"] not in moved]

    def seg_rows(view):
        return [{"host": s["host"], "cores": s["cores"], "lane": s["lane"], "start": iso(s["start"]),
                 "end": iso(s["end"]), "mutants": sum(1 for m in view if m["segment"] == i),
                 "verdicts": dict(sorted(Counter(m["verdict"] for m in view if m["segment"] == i).items()))}
                for i, s in enumerate(segs)]

    def by_file(view, verdict):
        out = {}
        for m in view:
            if m["verdict"] == verdict:
                out.setdefault(m["file"], []).append(m["name"])
        return dict(sorted(out.items()))

    def timed_out(view, segs, moved):
        """Each timeout of the figure, by file, with its host segment and its round's opening load."""
        out = {}
        for m in view:
            if m["verdict"] == "Timeout":
                out.setdefault(m["file"], []).append({
                    "n": m["n"], "name": m["name"], "host": m["host"], "lane": segs[m["segment"]]["lane"],
                    "repair": m["n"] in moved, "round_opened": iso(m["round_opened"]),
                    "round_load": m["round_load"], "load_1m": m["load_1m"], "seconds": m["seconds"],
                    "finished": iso(m["finished"]), **over_time(ledger / "runs" / f"i{m['n']}")})
        return dict(sorted(out.items()))

    studio = [m for m in art if m["host"] == "studio" and m["verdict"] == "Timeout"]
    two = [m for m in art if m["threads"] == 2 and m["verdict"] == "Timeout"]
    totals, art_totals = Counter(), Counter()
    files, art_files = [], []
    for f in sorted(set(expected.values())):
        c, a = counts.get(f, Counter()), art_counts.get(f, Counter())
        totals.update(c)
        art_totals.update(a)
        wanted = sum(1 for v in expected.values() if v == f)
        floor = floors.get(f, {}).get("floor")
        hosts = Counter(m["host"] for m in mutants if m["file"] == f)
        # A verdict rests on the file's timeouts when it changes with them counted as caught.
        caught = c + Counter({"caught": c["timeout"], "timeout": -c["timeout"]})
        left_out = c - Counter({"timeout": c["timeout"]})
        files.append({"file": f, "mutants": wanted, **{k: c[k] for k in KEYS}, "score": pct(c), "floor": floor,
                      "verdict": floor_verdict(c, wanted, floor, f in floors),
                      "hosts": dict(sorted(hosts.items(), key=lambda kv: str(kv[0]))),
                      "score_timeouts_left_out": pct(left_out), "score_timeouts_caught": pct(caught),
                      "verdict_rests_on_timeouts": c["timeout"] > 0 and floor_verdict(
                          caught, wanted, floor, f in floors) != floor_verdict(c, wanted, floor, f in floors)})
        verdict = floor_verdict(a, wanted, floor, f in floors)

        # Below the floor by the Studio's timeouts alone: at or above it with them left out.
        def below_by(timed):
            k = sum(1 for m in timed if m["file"] == f)
            return k, (verdict == "below floor" and k > 0
                       and a["caught"] * 100 >= floor * (a["caught"] + a["missed"] + a["timeout"] - k))
        st, by_studio = below_by(studio)
        tt, by_two = below_by(two)
        art_files.append({"file": f, **{k: a[k] for k in KEYS}, "score": pct(a), "verdict": verdict,
                          "studio_timeouts": st, "below_by_studio_timeouts": by_studio,
                          "threads_2_timeouts": tt, "below_by_threads_2_timeouts": by_two})

    complete = not missing and not absent and not problems
    seconds = [m["seconds"] for m in mutants]
    baseline = next(l for l in (ledger / "logs/baseline.log").read_text().splitlines()
                    if l.startswith("test result"))
    hours = {h: round(sum((s["end"] - s["start"]).total_seconds() for s in segs if s["host"] == h) / 3600, 2)
             for h in HOSTS}
    repair = segs[-1]
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
                   "[RUST_TEST_THREADS=2 in the rounds test_threads names] [TMPDIR=<scratch>/tmp-wK, emptied at "
                   "each reset, in the repair's rounds] [SDKROOT=<Xcode 26.3's MacOSX.sdk> on the box] "
                   "cargo mutants --in-place --baseline skip --timeout 270 --build-timeout 400 "
                   f"--shard N/{len(catalog)} --sharding round-robin --output runs/iN",
        "driver": "round.py in the ledger (one foreground round per shell call, every round's date -u in "
                  "logs/rounds.log and its opening uptime in logs/load.log; the worker count per segment as "
                  "workers names it; a run "
                  "still going at a round's deadline is killed, its output removed and the index run again whole "
                  "in a later round: it had no outcome)",
        "repair": f"the artifact run's {len(moved)} Timeout entries moved from state/done to "
                  "state/artifact-timeout (their runs to artifact/runs) and run again by the same driver and "
                  f"command, {iso(repair['start'])} to {iso(repair['end'])} (lane {repair['lane']}), the default "
                  "test threads and each worker its own TMPDIR; a re-run's verdict replaces its timeout",
        "scoring": "tools/mutants-score collect(), skipped baseline; score = caught / (caught + missed + timeout), "
                   "unviable excluded; the per-file table also as tools/mutants-score --catalog catalog.json "
                   "--baseline-skipped --report-only over every outcomes file",
        "floors": f"tools/mutants-floor.json at the base {base[:8]}, read only",
        "oracle": "the Mutants workflow's: cargo test --lib (additional_cargo_args in .cargo/mutants.toml)",
        "started": iso(segs[0]["start"]), "finished": iso(segs[-1]["end"]),
        "host_hours": hours,
        "mutant_seconds": {"sum": round(sum(seconds)), "max": round(max(seconds))},
        "rounds": len(opens),
        "workers": workers(rounds, segs, switches),
        "hosts": HOSTS,
        "test_threads": [{"from": iso(s), "RUST_TEST_THREADS": v or "default"} for s, v in switches],
        "segments": seg_rows(mutants),
        "toolchain": "box: " + head_line(subprocess.run(["rustc", "-V"], capture_output=True, text=True).stdout)
                     + "; the Studio: " + STUDIO_RUSTC,
        "tool_version": head_line(subprocess.run(["cargo", "mutants", "--version"], capture_output=True,
                                                 text=True).stdout),
        "lockfile": "Cargo.lock (no lock is committed; resolved once by cargo fetch, copied to every worker)",
        "logs": str(ledger) + ": logs/rounds.log, logs/load.log, logs/baseline.log, logs/environment-restored.log, "
                "runs/i<N>.log, runs/i<N>/mutants.out, state/done/<N>, state/artifact-timeout/<N>, artifact/runs/",
    }
    head = {k: totals[k] for k in KEYS} | {"mutants": len(catalog), "tested": len(seen)}
    witness = sum(1 for n in moved for log in (ledger / "artifact/runs" / f"i{n}" / "mutants.out/log").glob("*.log")
                  if f"{WITNESS.split('::')[-1]} has been running" in log.read_text(errors="replace"))
    oracle = json.loads((ROOT / ORACLE).read_text())
    doors = [{"file": f, "this_lib": tally(counts.get(f, Counter())), "artifact_lib": tally(art_counts.get(f, Counter())),
              "oracle_571_lib": {k: oracle["oracles"]["narrow"]["files"][f][k] for k in KEYS + ("score",)},
              "oracle_571_wide": {k: oracle["oracles"]["wide"]["files"][f][k] for k in KEYS + ("score",)}}
             for f in DOORS]
    artifact = {
        "figure": pct(art_totals), **{k: art_totals[k] for k in KEYS},
        "rerun": dict(sorted(Counter(mutants[n]["verdict"] for n in moved).items())),
        "witness_over_60s": witness,
        "pile": PILE, "clearing": CLEARING, "environment": ENVIRONMENT,
        "timeouts_by_threads": {str(v or "default"): dict(sorted(Counter(m["verdict"] for m in art
                                                                      if m["threads"] == v).items()))
                                for v in (None, 2)},
        "by_tmpdir_and_threads": {f"{when}, RUST_TEST_THREADS={v or 'default'}": dict(sorted(Counter(
            m["verdict"] for m in art if m["threads"] == v
            and (m["finished"] >= DUMPS_FROM) == (when == "after the dumps")).items()))
            for when in ("before the dumps", "after the dumps") for v in (None, 2)},
        "segments": seg_rows(art),
        "files": art_files,
        "studio_timeouts": [{"n": m["n"], "name": m["name"], "round_opened": iso(m["round_opened"]),
                             "load_1m": m["load_1m"]} for m in studio],
        "timeouts": by_file(art, "Timeout"),
    }
    overall = pct(totals)
    record = {
        "producer": "tools/mutants-complete-record.py over the run's ledger (lanes p5-mutants-complete-box, "
                    "p5-mutants-complete-studio-b, p5-mutants-complete-box-c, p5-mutants-complete-box-d)",
        "scoreboard": ["mutation_score_pct"],
        "complete": complete,
        "problems": problems + ([f"catalog indices with no outcome: {len(missing)}"] if missing else []),
        "standing": [
            f"The base is {base}.",
            "The artifact cannot recur on beta past #563 (merged as 6afdfe47; its d4f86b7c removed the dump "
            "from describe_invalid_beef), and the repair's driver gives each worker its own TMPDIR.",
            f"The figure {overall} is the base's complete figure under the stated method: every catalogued "
            "mutant has one verdict, every timeout of the artifact run was run again under the restored "
            "environment, and a slow folder can only turn a verdict into a timeout, so the caught and missed "
            "verdicts of the rounds the pile reached stand.",
        ],
        "run": run,
        "cells": [dict(cell="mutation_score_pct", value=overall, unit="percent",
                       scope="the Mutants workflow's set, every catalogued mutant", **head)]
                 + [dict(cell=f"mutants.file.{r['file']}.score", value=r["score"], unit="percent", **r)
                    for r in files],
        "doors": doors,
        "artifact": artifact,
        "survivors": by_file(mutants, "MissedMutant"),
        "timeouts": timed_out(mutants, segs, moved),
        "mutants": [{"n": m["n"], "verdict": m["verdict"], "host": m["host"], "threads": m["threads"] or 0,
                     "seconds": m["seconds"]} | ({"artifact_timeout": True} if m["n"] in moved else {})
                    for m in mutants],
    }
    return record


def dump(value, depth=0, key=None):
    """Two-space JSON: the top two levels of objects expanded, a list of objects one object a line,
    `mutants` eight rows a line (the 800-line rule), everything deeper on one line."""
    pad, inner = "  " * depth, "  " * (depth + 1)
    if key == "mutants" and isinstance(value, list):
        rows = [", ".join(json.dumps(m) for m in value[j:j + 8]) for j in range(0, len(value), 8)]
        return "[\n" + ",\n".join(inner + r for r in rows) + "\n" + pad + "]"
    if isinstance(value, dict) and value and depth < 2:
        items = [f"{inner}{json.dumps(k)}: {dump(v, depth + 1, k)}" for k, v in value.items()]
        return "{\n" + ",\n".join(items) + "\n" + pad + "}"
    if isinstance(value, list) and value and all(isinstance(v, dict) for v in value):
        return "[\n" + ",\n".join(inner + json.dumps(v) for v in value) + "\n" + pad + "]"
    return json.dumps(value)


def cell(v):
    return "n/a" if v is None else f"{v}%"


def short(name):
    loc, change = name.split(": ", 1)
    return f"{':'.join(loc.split(':')[1:])} {change}"


def counted(c):
    return ", ".join(f"{v} {k}" for k, v in c.items())


def named(record_timeouts):
    out = [f"- `{f}`: " + "; ".join(f"`{short(n)}`" for n in names) for f, names in record_timeouts.items()]
    return out or ["None."]


def render_md(record):
    """The md, from the json record alone (so --check can compare them)."""
    run, art = record["run"], record["artifact"]
    head = record["cells"][0]
    overall = head["value"]
    files = record["cells"][1:]
    viable = head["caught"] + head["missed"] + head["timeout"]
    art_viable = art["caught"] + art["missed"] + art["timeout"]
    below = [r for r in files if r["verdict"] == "below floor"]
    nofloor = [r for r in files if r["verdict"] == "no floor entry"]

    def seg_lines(segs):
        return [f"- {HOSTS[s['host']]}, lane `{s['lane']}`, {s['start']} to {s['end']}: {s['mutants']} mutants "
                f"({counted(s['verdicts'])})" for s in segs]
    out = [f"# The complete Mutants run of {DAY}", "",
           f"Base `{run['base'][:8]}` (the run spans {run['started']} to {run['finished']} on two hosts: "
           f"{run['host_hours']['box']} hours on the box and {run['host_hours']['studio']} on the Studio by the "
           f"rounds' `date -u`). Every value below is written by `tools/mutants-complete-record.py` from the run's "
           f"ledger; the machine-readable cells, the run's commands, times, hosts, toolchain, tool version, "
           f"logs and every mutant's verdict and host are [mutants-complete-{DAY}.json](mutants-complete-{DAY}.json). "
           f"The ledger is on the lane volume under `{LEDGER}/` (`catalog.json`, `runs/`, `state/`, `logs/`, "
           f"`artifact/`). The scoreboard cell `mutation_score_pct` in `docs/NORTH-STAR.md` is written from this "
           f"run by `tools/scoreboard-write`.", "",
           f"The figure is the repaired one, {cell(overall)}: the run before the repair scored "
           f"{cell(art['figure'])}, its {art['timeout']} timeouts an artifact of a temp folder of "
           f"{art['pile']['files']:,} dumps; each was run again under the restored environment "
           "(the section The artifact).", "",
           "## Standing", ""] + [f"- {s}" for s in record["standing"]] + ["",
           "## Method", "",
           f"`{run['tool_version']}` over the Mutants workflow's set (`.cargo/mutants.toml`: "
           f"{head['mutants']} mutants over {len(files)} files), every catalogued mutant once, one mutant per "
           f"`--in-place --baseline skip` invocation after one green `cargo test --lib` on the base "
           f"({run['baseline'].split(': ', 1)[1]}); test timeout 270 s, build timeout 400 s; the workers by segment "
           f"below, each in its own scratch copy and target directory, `CARGO_BUILD_JOBS=2`, "
           f"`RUST_TEST_THREADS=2` in the rounds named below (the default in the rest); the oracle is the "
           f"workflow's (`--lib`). "
           f"A timeout is a timeout, never retried into a kill; a run killed at a round's deadline had no "
           f"outcome and ran again whole. Scored by `tools/mutants-score` (`collect()`, skipped baseline): "
           f"score = caught / (caught + missed + timeout), unviable excluded. Floors are the base's "
           f"`tools/mutants-floor.json`, reported only; no floor is written by this run.", "",
           f"The repair: {run['repair']}.", "",
           "The workers, by segment and by the rounds that opened in it (`logs/rounds.log`; hours from a span's "
           "first opening to the next span's, the last to the segment's last round end or to the stop that "
           "ended it mid-round):", ""] + [
           f"- {THE[w['host']]}, lane `{w['lane']}`, by its rule {w['rule']}: " + "; then ".join(
               f"{sp['workers']} workers at {sp['RUST_TEST_THREADS']} test threads, {sp['from']} to {sp['to']} "
               f"({sp['hours']} h, {sp['rounds']} round{'s' * (sp['rounds'] != 1)})" for sp in w["spans"]) for w in run["workers"]] + ["",
           "The hosts, by segment (a mutant's host is the segment holding its completion time, the mtime of "
           "its `state/done/N`):", ""] + seg_lines(run["segments"]) + ["",
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
    rests = [r for r in files if r["verdict_rests_on_timeouts"]]
    out += ["", "A verdict rests on timeouts when it changes with the file's timeouts counted as caught (the "
            "json's `verdict_rests_on_timeouts`, beside `score_timeouts_left_out` and `score_timeouts_caught`). "
            + (" ".join(f"`{r['file']}` is {r['verdict']} at {cell(r['score'])} with its {r['timeout']} "
                        f"timeouts ({sum(t['repair'] for t in record['timeouts'][r['file']])} of them the repair "
                        f"segment's), {cell(r['score_timeouts_left_out'])} with them left out and "
                        f"{cell(r['score_timeouts_caught'])} with them caught, against a floor of {r['floor']}: "
                        "its verdict rests on those timeouts alone." for r in rests)
               if rests else "No file's verdict rests on its timeouts.")
            + " Every other file's verdict stands with its timeouts counted either way."]
    out += ["", "## The door files beside #571", "",
            "The three door files under this run's `--lib` oracle (base `42559923`), beside "
            f"[{Path(ORACLE).name}]({Path(ORACLE).name}) (#571, base `83a5b049`): its `--lib` run and its widened "
            "oracle (`.cargo/mutants-doors.toml`).", "",
            "| File | Run | Caught | Missed | Timeout | Unviable | Score |",
            "|---|---|---:|---:|---:|---:|---:|"]
    for d in record["doors"]:
        for label, key in (("#571, widened", "oracle_571_wide"), ("#571, `--lib`", "oracle_571_lib"),
                           ("this run, `--lib`", "this_lib"), ("the artifact, `--lib`", "artifact_lib")):
            c = d[key]
            out.append(f"| `{d['file']}` | {label} | {c['caught']} | {c['missed']} | {c['timeout']} | "
                       f"{c['unviable']} | {cell(c['score'])} |")
    out += ["", "## Timeouts", "",
            "Each named here took longer than the 270 s test timeout under its mutation in the restored "
            "environment; counted as not caught. Each carries its host segment, the round it ran in (its "
            "opening by `logs/rounds.log`) and the one-minute load at that opening (the `round start` line's "
            "figure, with `logs/load.log`'s uptime beside it), its run time (`state/done/N`) and its completion "
            "(that file's mtime). A timeout from the repair segment says so.", ""]
    rows = [t for ts in record["timeouts"].values() for t in ts]
    out += [f"Of the {len(rows)}: {sum(t['repair'] for t in rows)} from the repair segment, every one on "
            + " and ".join(sorted({THE[t["host"]] for t in rows})) + "; the round loads run from "
            f"{min(t['round_load'] for t in rows)} to {max(t['round_load'] for t in rows)}. By each run's log "
            "(`runs/iN/mutants.out/log`), a test the log names as running over 60 s with no result line before "
            "the 270 s timeout, where the unmutated base's whole `cargo test --lib` took "
            f"{record['artifact']['environment']['lib_s']} s in the restored environment, is a loop or a wait the "
            "mutation made; one with its result line is a slow test: "
            + counted(Counter(t["kind"] for t in rows)) + ". The test named is the log's first that never "
            "finished (every name in the json's `tests_over_60s` and `unfinished`).", ""] if rows else []
    for f, rows in record["timeouts"].items():
        out.append(f"- `{f}` ({len(rows)}):")
        out += [f"  - `{short(t['name'])}`, catalog {t['n']}: {t['host']}, lane `{t['lane']}`"
                f"{' (the repair segment)' if t['repair'] else ''}; round opened {t['round_opened']} at load "
                f"{t['round_load']} ({t['load_1m']} in `logs/load.log`); {t['seconds']} s, done {t['finished']}; "
                f"{t['kind']}" + (f": `{t['unfinished'][0]}` never finished ({len(t['tests_over_60s'])} over 60 s, "
                                  f"{len(t['unfinished'])} unfinished)" if t["unfinished"] else "")
                for t in rows]
    out += [] if record["timeouts"] else ["None."]
    out += ["", "## Survivors", "",
            "Every missed mutant, by file, as `line:column` and the mutation (the full names are in the "
            "JSON's `survivors`). A survivor is a figure here; the triage is the hardening lanes' (#534, #535, "
            "#536).", ""]
    for f, names in record["survivors"].items():
        door = " (door)" if f in DOORS else ""
        out.append(f"- `{f}`{door} ({len(names)}): " + "; ".join(f"`{short(n)}`" for n in names))
    pile, clear, env = art["pile"], art["clearing"], art["environment"]
    out += ["", "## The artifact", "",
            f"The run before the repair (lanes `p5-mutants-complete-box`, `p5-mutants-complete-studio-b`, "
            f"`p5-mutants-complete-box-c`) scored {cell(art['figure'])}: {art['caught']} caught, {art['missed']} "
            f"missed, {art['timeout']} timeout, {art['unviable']} unviable ({art_viable} viable). The record "
            "of that run said its figure could not be trusted; the cause and the repair follow.", "",
            f"The pile: {pile['cause']}. In `{pile['folder']}` at {pile['counted']}: {pile['files']:,} "
            f"`beef-rejected-*.hex` files of {pile['entries']:,} entries. On the unmutated base the folder made "
            f"the one test take minutes ({MEASURED}). In {art['witness_over_60s']} of the artifact's "
            f"{art['timeout']} timeout logs the witness `{WITNESS}` is named as running over 60 seconds.", "",
            f"The clearing: `{clear['command']}` at {clear['at']}, after counting: {clear['removed']:,} removed, "
            f"{clear['entries_left']:,} entries left, free space {clear['free_before']} before and "
            f"{clear['free_after']} after (`df -h`).", "",
            f"The environment restored, at {env['at']} on {env['tree']}: the witness alone {env['witness_s']} s "
            f"(beside {env['witness_s_pile']} s under the pile) and the full `cargo test --lib` {env['lib_s']} s "
            f"(beside {env['lib_s_pile']} s under the pile at RUST_TEST_THREADS=2): {env['lib_result']}.", "",
            f"The re-run of the {art['timeout']}: {counted(art['rerun'])}. A mutant that timed out again "
            "keeps its timeout (the method's verdict).", "",
            "The artifact run's hosts, by segment:", ""] + seg_lines(art["segments"][:-1]) + ["",
            "The three door files in the artifact run: "
            + "; ".join(f"`{d['file']}` {d['artifact_lib']['caught']} caught, {d['artifact_lib']['missed']} "
                        f"survivors, {d['artifact_lib']['timeout']} timeout" for d in record["doors"]) + ".", "",
            "### The artifact's timeouts by file", "",
            "| File | Caught | Missed | Timeout | Unviable | Score |",
            "|---|---:|---:|---:|---:|---:|"]
    out += [f"| `{r['file']}`{' (door)' if r['file'] in DOORS else ''} | {r['caught']} | {r['missed']} | "
            f"{r['timeout']} | {r['unviable']} | {cell(r['score'])} |" for r in art["files"] if r["timeout"]]
    out += ["", "Each by name:", ""] + named(art["timeouts"])
    out += ["", "### The Studio's timeouts", "",
            "Each timeout the Studio segment ran, with the one-minute load at its round's opening "
            "(`logs/load.log`):", ""]
    out += [f"- `{short(t['name'])}` (`{t['name'].split(':')[0]}`), catalog {t['n']}, round opened "
            f"{t['round_opened']} at load {t['load_1m']}" for t in art["studio_timeouts"]] or ["None."]
    by = [r["file"] for r in art["files"] if r["below_by_studio_timeouts"]]
    out += ["", "Files below their floor by the Studio's timeouts alone (at or above it with them left out): "
            + (", ".join(f"`{f}`" for f in by) if by else "none") + "."]
    tb = art["timeouts_by_threads"]
    out += ["", "### The timeouts under RUST_TEST_THREADS=2", "",
            "The driver's environment set `RUST_TEST_THREADS=2` for the rounds opened from "
            + ", ".join(f"{s['from']} ({s['RUST_TEST_THREADS']})" for s in run["test_threads"])
            + ". Verdicts at the default: " + counted(tb["default"]) + "; under 2 threads: " + counted(tb["2"])
            + ". Verdicts by the dumps and the threads: "
            + "; ".join(f"{k}: " + ", ".join(f"{n} {v}" for v, n in c.items())
                        for k, c in art["by_tmpdir_and_threads"].items()) + ".", "",
            "Files below their floor by the 2-thread timeouts alone (at or above it with them left out): "
            + (", ".join(f"`{r['file']}`" for r in art["files"] if r["below_by_threads_2_timeouts"]) or "none")
            + "."]
    if record["problems"]:
        out += ["", "## Problems", ""] + [f"- {p}" for p in record["problems"]]
    return "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=LEDGER)
    parser.add_argument("--check", action="store_true", help="render the md from the json; exit 1 on a difference")
    args = parser.parse_args()
    md, js = ROOT / f"{STEM}.md", ROOT / f"{STEM}.json"
    if args.check:
        record = json.loads(js.read_text())
        if md.read_text() != render_md(record) or js.read_text() != dump(record) + "\n":
            print(f"{STEM}.md or .json differs from the json's own rendering; run the script over the ledger")
            return 1
        print(f"{STEM}.md matches its json")
        return 0
    record = build(args.ledger)
    js.write_text(dump(record) + "\n")
    md.write_text(render_md(record))
    head = record["cells"][0]
    if record["complete"]:
        ratchet(head, record["run"])
    print(f"tested {head['tested']}/{head['mutants']}; score {head['value']}; artifact "
          f"{record['artifact']['figure']}; complete {record['complete']}; problems {len(record['problems'])}")
    return 0


def ratchet(head, run):
    path = ROOT / "tools/ratchet.json"
    data = json.loads(path.read_text())
    data["metrics"]["mutation_score_pct"] = head["value"]
    data["mutation_score"] = {
        "method": f"{run['tool_version']} over .cargo/mutants.toml's set (the Mutants workflow's set: "
                  f"{head['mutants']} mutants), every catalogued mutant once, --in-place --baseline skip after a "
                  f"green `cargo test --lib` on {run['base'][:8]}, one catalog mutant per run (--shard N/"
                  f"{head['mutants']} --sharding round-robin), --timeout 270 --build-timeout 400, rule 10's proxy "
                  "variables; the artifact run's timeouts run again under the restored environment; scored by "
                  "tools/mutants-score (its collect(), --baseline-skipped); score = caught / "
                  "(caught + missed + timeout), unviable excluded",
        "caught": head["caught"], "missed": head["missed"], "timeout": head["timeout"],
        "unviable": head["unviable"],
        "hosts": {s["lane"]: f"{s['host']}, {s['mutants']} mutants" for s in run["segments"]},
        "source": f"{STEM}.{{md,json}} (the per-file table, every survivor, every mutant's host and the artifact)",
        "measured_at": DAY,
    }
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    sys.exit(main())
