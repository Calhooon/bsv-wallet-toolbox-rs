#!/usr/bin/env python3
"""The A8 census rows from the profiles of examples/a8_census.rs.

Reads DIR/<scene>-<size>-r<run>.{json.gz,json.syms.json,out} for three runs
of each scene and size, takes each function's median share over the three
(a run where it is absent counts zero) and writes census.json and census.md
to OUT. A row per function above 2 percent per scene and size, the rest one
`other` row; `input_size` is the size of the same scene at which the
function's median share is largest. Measures only: no candidate, no ranking.

    a8_census_rows.py DIR OUT MACHINE.json
"""

import importlib.util
import json
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "profile", os.path.join(HERE, "a8_census_profile.py"))
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)

SCENES = [
    ("create", [1, 10, 100], "inputs",
     "createAction: allocation of N funded coins (one shared proven parent), "
     "the signer over them, the process writes, no_send"),
    ("process", [1, 10, 100], "inputs",
     "createAction as above with the post to MockWalletServices (processAction's broadcast)"),
    ("internalize", [1], "output",
     "storage internalizeAction of the door bench's proven basket insertion"),
    ("beef-internalize", [1, 4, 10], "MB",
     "wallet internalizeAction of an atomic BEEF: a proven parent and an unproven "
     "child of about N MB with one signed P2PKH input (stream verification, the word pass)"),
    ("beef-create", [1, 4, 10], "MB",
     "createAction naming the same child's output 0 with the BEEF as inputBEEF "
     "(the caller BEEF pre-check), sign_and_process false"),
]
SYSTEM = {
    "libsystem_kernel.dylib": "the kernel (libsystem_kernel)",
    "libsystem_malloc.dylib": "the allocator (libsystem_malloc)",
    "libsystem_platform.dylib": "libsystem_platform (memmove, memset, memcmp)",
    "libsystem_pthread.dylib": "pthread (libsystem_pthread)",
}
RUST = {
    "k256": "secp256k1 (RustCrypto k256 0.14)",
    "primeorder": "secp256k1 (RustCrypto primeorder, k256's point tables)",
    "wnaf": "secp256k1 (wnaf 0.14, k256's multi-scalar multiplication)",
    "crypto_bigint": "crypto-bigint 0.7 (k256's modular inversion)",
    "sha2": "sha2 (SHA-256, aarch64 SHA extensions)",
    "tokio": "tokio",
    "flume": "flume (sqlx-sqlite's worker channel)",
    "parking_lot": "parking_lot",
    "futures_task": "futures",
}


def crate_and_library(name, lib, binary):
    if lib in SYSTEM or lib != binary:
        return "system", SYSTEM.get(lib, lib)
    head = name.lstrip("<").split("::")
    root = head[0]
    if root == "bsv_rs":
        return "bsv-rs", "bsv-rs " + "::".join(head[1:3])
    if root == "bsv_wallet_toolbox_rs":
        return "bsv-wallet-toolbox-rs", "toolbox " + "::".join(head[1:4])
    if root.startswith("sqlx"):
        return "sqlx", root.replace("_", "-")
    if root in ("core", "alloc", "std"):
        return "std", "Rust std (" + root + ")"
    if len(head) == 1:
        return "libsqlite3-sys", "SQLite (C, bundled by libsqlite3-sys 0.30)"
    return root.replace("_", "-"), RUST.get(root, root.replace("_", "-"))


def main(directory, out, machine_path):
    machine = json.load(open(machine_path))
    rows, totals = [], []
    for scene, sizes, unit, description in SCENES:
        medians = {}
        for size in sizes:
            runs = []
            for run in (1, 2, 3):
                base = os.path.join(directory, f"{scene}-{size}-r{run}")
                result = profile.analyse(base + ".json.gz", base + ".out")
                runs.append((base, result))
            names = {f["function"] for _, r in runs for f in r["functions"]}
            shares = {}
            for name in names:
                values = [next((f["share_pct"] for f in r["functions"]
                                if f["function"] == name), 0.0) for _, r in runs]
                shares[name] = statistics.median(values)
            meta = {}
            for _, r in runs:
                for f in r["functions"]:
                    lib, callers = meta.setdefault(f["function"], (f["lib"], {}))
                    for caller, n in f["via"]:
                        callers[caller] = callers.get(caller, 0) + n
            per_call = lambda key: statistics.median(r[key] / r["iterations"] for _, r in runs)
            totals.append({
                "scene": f"{scene} ({size} {unit})", "iterations": runs[0][1]["iterations"],
                "wall_ms_per_call": per_call("door_wall_ms"),
                "cpu_ms_per_call": per_call("cpu_ms"),
                "unplaced_cpu_pct": statistics.median(
                    100 * r["wait_cpu_ms"] / r["cpu_ms"] for _, r in runs),
                "on_cpu_samples": [r["on_cpu_samples"] for _, r in runs],
                "reset_alignment": [round(r["main_in_window_under_reset"], 3) for _, r in runs],
                "runs": [os.path.basename(b) + ".json.gz" for b, _ in runs],
            })
            medians[size] = (shares, meta, runs)
        for size in sizes:
            shares, meta, runs = medians[size]
            kept = 0.0
            for name, share in sorted(shares.items(), key=lambda kv: -kv[1]):
                if share <= 2.0:
                    continue
                kept += share
                lib, via = meta[name]
                crate, library = crate_and_library(name, lib, "a8_census")
                peak = max(sizes, key=lambda s: medians[s][0].get(name, 0.0))
                row = {
                    "crate": crate, "function": name, "share_pct": round(share, 2),
                    "scene": f"{scene} ({size} {unit})", "input_size": f"{peak} {unit}",
                    "library": library,
                    "run": {
                        "profiles": [os.path.basename(b) + ".json.gz" for b, _ in runs],
                        "command": "samply record --save-only --unstable-presymbolicate "
                                   f"--rate 2000 -o <profile> -- a8_census {scene} {size} "
                                   f"{runs[0][1]['iterations']}",
                    },
                }
                top = sorted(via.items(), key=lambda kv: -kv[1])[:3]
                if top and top[0][0] is not None:
                    row["via"] = [{"caller": c, "samples_in_three_runs": n} for c, n in top]
                rows.append(row)
            rows.append({
                "crate": "other", "function": "other", "share_pct": round(100 - kept, 2),
                "scene": f"{scene} ({size} {unit})", "input_size": f"{size} {unit}",
                "library": "every function at or below 2 percent",
                "run": {"profiles": [os.path.basename(b) + ".json.gz" for b, _ in runs]},
            })
    census = {"machine": machine, "scenes": [
        {"scene": s, "sizes": z, "unit": u, "description": d} for s, z, u, d in SCENES],
        "totals": totals, "rows": rows}
    json.dump(rows, open(os.path.join(out, "census.json"), "w"), indent=1)
    json.dump(census, open(os.path.join(out, "census-full.json"), "w"), indent=1)
    write_markdown(os.path.join(out, "census.md"), census)
    print(json.dumps({"rows": len(rows), "totals": len(totals)}))


def cell(text):
    return str(text).replace("|", "\\|")


def write_markdown(path, census):
    m = census["machine"]
    lines = [
        "# A8 census: bsv-wallet-toolbox-rs door scenes and the large BEEF path",
        "",
        f"Machine: {m['chip']}, {m['cores']} cores ({m['performance_cores']} performance, "
        f"{m['efficiency_cores']} efficiency), macOS {m['macos']}, {m['rustc']}, {m['samply']}. "
        f"Load: {m['load_average_during_runs']}. Build: {m['build']}. Base: {m['base']}.",
        "",
        "## Scenes",
        "",
    ]
    for scene in census["scenes"]:
        sizes = ", ".join(str(z) for z in scene["sizes"])
        lines.append(f"- `{scene['scene']}` ({sizes} {scene['unit']}): {scene['description']}.")
    lines += [
        "",
        "## Scene totals (median of three runs, per door call)",
        "",
        "Wall is the door call's elapsed time; CPU is every thread's CPU time inside the "
        "call windows (samply's `threadCPUDelta`). Unplaced is the part of that CPU charged "
        "to samples whose leaf is a blocking wait: bursts shorter than the 0.5 ms interval "
        "before a thread blocked, which no on-CPU sample places. Alignment is the share of "
        "in-window main-thread samples under the between-call reset (near zero when the "
        "windows and the profile clock agree).",
        "",
        "| Scene | Calls per run | Wall ms | CPU ms | Unplaced CPU % | On-CPU samples | Alignment | Profiles |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for t in census["totals"]:
        lines.append(
            f"| {t['scene']} | {t['iterations']} | {t['wall_ms_per_call']:.3f} | "
            f"{t['cpu_ms_per_call']:.3f} | {t['unplaced_cpu_pct']:.1f} | "
            f"{', '.join(map(str, t['on_cpu_samples']))} | "
            f"{', '.join(map(str, t['reset_alignment']))} | {', '.join(t['runs'])} |")
    lines += [
        "",
        "## Rows",
        "",
        "Share is the function's self time (on-CPU samples whose leaf is the function's "
        "native symbol; an inlined callee counts as its caller) over the scene's on-CPU "
        "samples, the median of three runs. Input size is the size of the same scene at "
        "which the function's median share is largest. Via is the function's nearest "
        "callers outside its own crate, by samples summed over the three runs.",
        "",
        "| Scene | Share % | Crate | Function | Input size | Library | Via |",
        "| --- | ---: | --- | --- | --- | --- | --- |",
    ]
    for r in census["rows"]:
        via = "; ".join(f"{v['caller']} ({v['samples_in_three_runs']})" for v in r.get("via", []))
        lines.append(
            f"| {r['scene']} | {r['share_pct']:.2f} | {cell(r['crate'])} | `{cell(r['function'])}` | "
            f"{r['input_size']} | {cell(r['library'])} | {cell(via)} |")
    lines += [
        "",
        "Each row's run: its three profiles under `profiles/` and the command "
        "`samply record --save-only --unstable-presymbolicate --rate 2000 -o <profile> -- "
        "a8_census <scene> <size> <calls>` (in `census.json`). Rows are measured only: no "
        "candidate is named and the order is by share within a scene.",
        "",
    ]
    open(path, "w").write("\n".join(lines))


if __name__ == "__main__":
    main(*sys.argv[1:4])
