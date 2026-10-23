"""The ratchet's built-in duplication count (#55), used by tools/ratchet.

A file's lines are taken with comments removed (the caller blanks them) and
whitespace collapsed; blank lines are dropped. Every window of WINDOW
consecutive remaining lines is a location; a window whose text appears at more
than one location in src/ is duplicated, and the count is the number of such
locations. Overlapping windows are separate locations, so a repeated run of
n lines counts n - WINDOW + 1 windows at each copy.

The pairs name the largest duplicated runs: each location of a repeated window
is paired with the window's earliest location (by file, then line), a pair is
extended while the windows after both still match, and the ten longest pairs
are listed as `file:line` to `file:line` with their length in kept lines.
"""

import re

WINDOW = 6
TOP = 10
SPACE = re.compile(r"\s+")


def kept_lines(code):
    """[(line number, normalized text)] of the non-blank lines of `code`."""
    out = []
    for n, line in enumerate(code.split("\n"), 1):
        norm = SPACE.sub(" ", line).strip()
        if norm:
            out.append((n, norm))
    return out


def scan(files):
    """`files`: [(relative path, comment-free text)], in a fixed order."""
    lines = {rel: kept_lines(code) for rel, code in files}
    where = {}
    for rel, kept in lines.items():
        for i in range(len(kept) - WINDOW + 1):
            key = "\n".join(t for _, t in kept[i : i + WINDOW])
            where.setdefault(key, []).append((rel, i))
    groups = [locs for locs in where.values() if len(locs) > 1]
    count = sum(len(locs) for locs in groups)

    def same(a, b):
        (fa, ia), (fb, ib) = a, b
        ka, kb = lines[fa], lines[fb]
        if ia + WINDOW > len(ka) or ib + WINDOW > len(kb):
            return False
        return all(ka[ia + k][1] == kb[ib + k][1] for k in range(WINDOW))

    pairs = []
    for locs in groups:
        locs = sorted(locs)
        head = locs[0]
        for other in locs[1:]:
            back_a, back_b = (head[0], head[1] - 1), (other[0], other[1] - 1)
            if back_a[1] >= 0 and back_b[1] >= 0 and same(back_a, back_b):
                continue  # not the start of the run; the earlier window holds it
            n = 1
            while same((head[0], head[1] + n), (other[0], other[1] + n)):
                n += 1
            pairs.append((n + WINDOW - 1, head, other))
    pairs.sort(key=lambda p: (-p[0], p[1], p[2]))

    def at(loc):
        rel, i = loc
        return f"{rel}:{lines[rel][i][0]}"

    return {
        "method": f"built-in: windows of {WINDOW} non-blank lines, comments removed, "
        "whitespace collapsed; locations whose window repeats",
        "count": count,
        "windows": sum(max(len(k) - WINDOW + 1, 0) for k in lines.values()),
        "top_pairs": [f"{at(a)} = {at(b)} ({n} lines)" for n, a, b in pairs[:TOP]],
    }
