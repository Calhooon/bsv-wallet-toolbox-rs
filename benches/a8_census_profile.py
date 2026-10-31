#!/usr/bin/env python3
"""Self time per function from a samply profile of examples/a8_census.rs.

Reads a profile saved with `samply record --save-only --unstable-presymbolicate`
(Firefox Profiler format plus the `.syms.json` sidecar) and the scene's JSON
report (its door-call windows in epoch milliseconds). Every thread's samples
inside a window count. A sample whose leaf is a blocking kernel wait (`IDLE`)
is off CPU and is left out; every other sample is one interval of on-CPU
time for its leaf frame's native symbol (an inlined callee counts as its
caller). The CPU total is the windows' sum of `threadCPUDelta`; the part of
it charged to idle samples (`wait_cpu_ms`) is CPU spent in bursts shorter than
the interval before a thread blocked, which no sample places. Prints one
JSON object: the totals and every symbol's share of the on-CPU samples.

    a8_census_profile.py PROFILE.json.gz REPORT.json
"""

import bisect
import gzip
import json
import sys


IDLE = {
    "semaphore_wait_trap", "__psynch_cvwait", "__psynch_mutexwait", "kevent",
    "kevent64", "kevent_id", "mach_msg2_trap", "mach_msg_trap", "__semwait_signal",
    "__select", "__workq_kernreturn", "__ulock_wait", "__ulock_wait2", "poll",
    "__pthread_kill",
}


STD = ("core::", "alloc::", "std::")


def load_symbols(syms_path):
    data = json.load(open(syms_path))
    strings = data["string_table"]
    tables = {}
    for lib in data["data"]:
        rows = sorted(lib["symbol_table"], key=lambda row: row["rva"])
        tables[lib["debug_name"]] = (
            [row["rva"] for row in rows],
            [
                (
                    row["rva"] + row["size"],
                    strings[row["symbol"]],
                    " ".join(strings[f["function"]] for f in row.get("frames", [])),
                )
                for row in rows
            ],
        )
    return tables


def symbol(tables, lib, address):
    """The native symbol at `address`, and its rows' inlined frame names. An
    address the sidecar does not place (samply symbolizes no system library
    but dyld, pthread, the kernel and platform, so every malloc address is
    one) counts as its library's one `LIB (unsymbolized)` row."""
    starts, rows = tables.get(lib, ([], []))
    at = bisect.bisect_right(starts, address) - 1
    if at >= 0 and address < rows[at][0]:
        return rows[at][1], rows[at][2]
    return f"{lib} (unsymbolized)", ""


def analyse(profile_path, report_path):
    profile = json.load(gzip.open(profile_path))
    tables = load_symbols(profile_path[: -len(".gz")] + ".syms.json")
    report = json.load(open(report_path))
    windows = sorted(report["windows_epoch_ms"])
    starts = [w[0] for w in windows]
    origin = profile["meta"]["startTime"]
    libs = [lib["debugName"] for lib in profile["libs"]]

    def inside(time):
        at = bisect.bisect_right(starts, origin + time) - 1
        return at >= 0 and origin + time <= windows[at][1]

    binary = profile["meta"]["product"]
    vias = {}
    self_n, lib_of, on_cpu, cpu_us, idle, wait_us = {}, {}, 0, 0, 0, 0
    reset_in, main_in, reset_out, main_out = 0, 0, 0, 0
    for thread in profile["threads"]:
        frames, stacks = thread["frameTable"], thread["stackTable"]
        funcs, resources = thread["funcTable"], thread["resourceTable"]
        cache = {}

        def leaf(stack):
            if stack not in cache:
                frame = stacks["frame"][stack]
                resource = funcs["resource"][frames["func"][frame]]
                lib = libs[resources["lib"][resource]] if resource is not None else "?"
                cache[stack] = (lib, *symbol(tables, lib, frames["address"][frame]))
            return cache[stack]

        def under_reset(stack):
            while stack is not None:
                # The reset is inlined into its block_on monomorph.
                if "::reset::{async_fn_env" in leaf(stack)[2]:
                    return True
                stack = stacks["prefix"][stack]
            return False

        def via(stack, own):
            """The nearest ancestor in the binary outside std, core, alloc and
            the leaf's own crate (`own`, its leading path segment)."""
            stack = stacks["prefix"][stack]
            while stack is not None:
                lib, name, _ = leaf(stack)
                head = name.lstrip("<")
                if lib == binary and not head.startswith(STD) and not head.startswith(own):
                    return name
                stack = stacks["prefix"][stack]
            return None

        rows = thread["samples"]
        for stack, time, delta in zip(rows["stack"], rows["time"], rows["threadCPUDelta"]):
            if stack is None:
                continue
            if not inside(time):
                if thread.get("isMainThread"):
                    main_out += 1
                    reset_out += under_reset(stack)
                continue
            cpu_us += delta or 0
            if thread.get("isMainThread"):
                main_in += 1
                reset_in += under_reset(stack)
            lib, name, _ = leaf(stack)
            if name in IDLE:
                idle += 1
                wait_us += delta or 0
                continue
            self_n[name] = self_n.get(name, 0) + 1
            lib_of[name] = lib
            own = name.lstrip("<").split("::")[0] + "::" if lib == binary else "\0"
            callers = vias.setdefault(name, {})
            caller = via(stack, own)
            callers[caller] = callers.get(caller, 0) + 1
            on_cpu += 1

    interval = profile["meta"]["interval"]
    rows = sorted(self_n.items(), key=lambda item: -item[1])
    return {
        "scene": report["scene"],
        "size": report["size"],
        "iterations": report["iterations"],
        "door_wall_ms": report["door_wall_ms"],
        "window_ms": sum(w[1] - w[0] for w in windows),
        "cpu_ms": cpu_us / 1e3,
        "wait_cpu_ms": wait_us / 1e3,
        "on_cpu_samples": on_cpu,
        "on_cpu_ms": on_cpu * interval,
        "idle_samples": idle,
        "interval_ms": interval,
        # The windows' alignment with the profile's clock: in-window main
        # thread samples under the between-call reset (near zero when aligned).
        # Out of the windows the same share is the reset's, well above zero.
        "main_in_window_under_reset": reset_in / main_in if main_in else None,
        "main_out_of_window_under_reset": reset_out / main_out if main_out else None,
        "functions": [
            {"function": name, "lib": lib_of[name], "samples": n,
             "share_pct": 100.0 * n / on_cpu,
             # Its nearest callers outside its own crate, by samples.
             "via": sorted(vias.get(name, {}).items(), key=lambda kv: -kv[1])[:3]}
            for name, n in rows
        ],
    }


if __name__ == "__main__":
    print(json.dumps(analyse(*sys.argv[1:3])))
