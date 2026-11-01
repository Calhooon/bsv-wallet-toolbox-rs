#!/usr/bin/env python3
"""Render the A8 record's bench tables from its JSON; see docs/a8/.

Each table in docs/a8/cycle-1-beef-bytes.md sits between a pair of markers,
`<!-- a8-record:NAME:begin -->` and `<!-- a8-record:NAME:end -->`, and is
rendered from one key of docs/a8/cycle-1-beef-bytes.json, so the record's
figures and the json cannot drift. With no argument the md is rewritten;
with --check nothing is written and the exit is 1 when the md differs.

    python3 tools/a8-record.py [--check]
"""

import argparse
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parent.parent
RECORD = ROOT / "docs/a8/cycle-1-beef-bytes.md"
FIGURES = ROOT / "docs/a8/cycle-1-beef-bytes.json"
# name: (json key, a single build or an off and on pair)
TABLES = {
    "base": ("bench_base_f59fce2b", False),
    "round-1": ("bench_round_1_interleaved", True),
    "round-2": ("bench_round_2_interleaved", True),
    "doors": ("bench_other_scenes_round_1", True),
    "tip": ("bench_tip_74994d79", True),
}
FIELDS = (("sha", "sha2 %"), ("memmove", "memmove %"),
          ("ms", "wall ms"), ("cpu_ms", "on-CPU ms"))


def decimals(section):
    """The most decimal places any figure of the section carries."""
    places = 0
    for row in section.values():
        for cell in (row.values() if "off" in row else [row]):
            for value in cell.values():
                text = repr(float(value))
                places = max(places, len(text.split(".")[1].rstrip("0")))
    return max(places, 2)


def render(section, pair):
    width = decimals(section)

    def cell(value):
        return f"{value:,.{width}f}"

    if pair:
        head = ["scene"] + [f"{label} {build}" for _, label in FIELDS
                            for build in ("off", "on")]
        rows = [[scene] + [cell(figures[build][field]) for field, _ in FIELDS
                           for build in ("off", "on")]
                for scene, figures in section.items()]
    else:
        head = ["scene"] + [label for _, label in FIELDS]
        rows = [[scene] + [cell(figures[field]) for field, _ in FIELDS]
                for scene, figures in section.items()]
    lines = ["| " + " | ".join(head) + " |",
             "|" + "---|" + "---:|" * (len(head) - 1)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def rendered(text, figures):
    for name, (key, pair) in TABLES.items():
        begin = f"<!-- a8-record:{name}:begin -->"
        end = f"<!-- a8-record:{name}:end -->"
        pattern = re.compile(re.escape(begin) + r"\n.*?" + re.escape(end), re.S)
        if len(pattern.findall(text)) != 1:
            sys.exit(f"a8-record: {RECORD.name} needs one {begin} ... {end} pair")
        table = render(figures[key], pair)
        text = pattern.sub(lambda _: f"{begin}\n{table}\n{end}", text)
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true",
                        help="exit 1 when the md's tables differ from the json")
    args = parser.parse_args()
    text = RECORD.read_text()
    new = rendered(text, json.loads(FIGURES.read_text()))
    if args.check:
        if new != text:
            print(f"a8-record: {RECORD.relative_to(ROOT)} differs from "
                  f"{FIGURES.name}; run python3 tools/a8-record.py")
            return 1
        print("a8-record: the tables match the json")
        return 0
    RECORD.write_text(new)
    return 0


if __name__ == "__main__":
    sys.exit(main())
