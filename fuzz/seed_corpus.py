#!/usr/bin/env python3
"""Seed fuzz/corpus/<target>/ from the fixtures and vectors already in the repo.

Run from the repository root: python3 fuzz/seed_corpus.py
Each seed is named by its source (file stem, then the case). The bytes come
from the repo's fixtures and vectors (the arcade and broadcast_memory seeds
fill a few placeholders such as the txid and the clock): a provider seed is its fuzzed-status byte (the index
of the status in fuzz/src/lib.rs STATUSES) and then the recorded body.
"""

import glob
import json
import os
import struct

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS = os.path.join(ROOT, "fuzz", "corpus")
# The index of each status in fuzz/src/lib.rs STATUSES (no 429, no 5xx).
STATUS_BYTE = {200: 0, 201: 4, 202: 5, 400: 6, 404: 7, 409: 8, 422: 9, 460: 10, 465: 11}
PROVIDER_TARGET = {
    "WhatsOnChain": "whatsonchain",
    "Bitails": "bitails",
    "BananaBlocks": "bananablocks",
    "Chaintracks": "chaintracks",
    "BHS": "bhs",
}
TXID = "11" * 32
# The memory row an Arcade word is recorded as (the vector's own classes:
# seen, mined, failure; every other word is the broadcaster holding it).
MEMORY_WORD = {
    "SEEN_ON_NETWORK": "seen",
    "SEEN_MULTIPLE_NODES": "seen",
    "MINED": "mined",
    "IMMUTABLE": "mined",
    "REJECTED": "rejected",
    "DOUBLE_SPEND_ATTEMPTED": "rejected",
}


def write(target, name, data):
    os.makedirs(os.path.join(CORPUS, target), exist_ok=True)
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in name)
    with open(os.path.join(CORPUS, target, safe), "wb") as f:
        f.write(data)


def rel(path):
    return os.path.relpath(path, ROOT)


def stem(path):
    return rel(path).replace("/", "_").rsplit(".", 1)[0]


def hex_fields(obj, keys, out):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and isinstance(v, str):
                try:
                    out.append(bytes.fromhex(v))
                except ValueError:
                    pass
            hex_fields(v, keys, out)
    elif isinstance(obj, list):
        for v in obj:
            hex_fields(v, keys, out)


def body_bytes(answer):
    if "json" in answer:
        return json.dumps(answer["json"]).encode()
    body = answer.get("body", "")
    return body.encode() if isinstance(body, str) else json.dumps(body).encode()


def ladders():
    for path in sorted(glob.glob(os.path.join(ROOT, "tests/fixtures/data/ladders/**/*.json"), recursive=True)):
        fixture = json.load(open(path))
        for provider in fixture.get("providers", []):
            target = PROVIDER_TARGET.get(provider["name"])
            for i, route in enumerate(provider.get("routes", [])):
                for j, answer in enumerate(route.get("answers", [])):
                    status = STATUS_BYTE.get(answer.get("status"))
                    if target is None or status is None:
                        continue
                    body = body_bytes(answer)
                    write(target, f"{stem(path)}_{i}_{j}", bytes([status]) + body)
                    if b"txOrId" in body:
                        write("tsc_proof", f"{stem(path)}_{i}_{j}", struct.pack("<I", 900000) + body)


def tsc_vectors():
    path = os.path.join(ROOT, "test_vectors/transactions/merkle_path.json")
    for case in json.load(open(path))["tsc_proof_to_merkle_path_test_vectors"]:
        inputs = case["inputs"]
        proof = {"index": inputs.get("index"), "nodes": inputs.get("nodes"), "txOrId": inputs.get("txid")}
        height = inputs.get("blockHeight") or 0
        write("tsc_proof", f"{stem(path)}_{case['id']}", struct.pack("<I", height) + json.dumps(proof).encode())


def bumps():
    found = []
    for path in sorted(glob.glob(os.path.join(ROOT, "tests/fixtures/data/**/*.json"), recursive=True)):
        out = []
        hex_fields(json.load(open(path)), {"merkle_path", "merklePath", "bump"}, out)
        for i, data in enumerate(out):
            write("merkle_path", f"{stem(path)}_{i}", data)
            found.append(data)
    return found


def beefs():
    rows = sorted(glob.glob(os.path.join(ROOT, "tests/vectors/broadcast_body/*.bin")))
    for path in rows:
        data = open(path, "rb").read()
        for target in ("beef_door", "broadcast_memory"):
            write(target, stem(path), data)


def arcade(bump):
    path = os.path.join(ROOT, "tests/vectors/arcade_status_verdicts.json")
    vectors = json.load(open(path))
    subs = {"{TXID}": TXID, "{COMPETITOR}": "22" * 32, "{NOW}": "2026-10-26T00:00:00Z", "{BUMP}": bump.hex()}

    def fill(body):
        text = json.dumps(body)
        for k, v in subs.items():
            text = text.replace(k, v)
        return text.encode()

    for case in vectors["cases"]:
        body = fill(case["body"])
        status = STATUS_BYTE.get(case.get("http") or 200)
        if status is not None:
            for target in ("arc", "arcade"):
                write(target, f"{stem(path)}_{case['name']}", bytes([status]) + body)
        frame = b"id: 1\nevent: status\ndata: " + body + b"\n\n"
        write("arcade_sse", f"{stem(path)}_{case['name']}", bytes([len(frame) // 2 % 256]) + frame)
        word = MEMORY_WORD.get(case["body"].get("txStatus"), "accepted")
        rows = f"{TXID},ArcadeV2,{word},1799990000\n{TXID},network,{word},1700000000\n"
        write("broadcast_memory", f"{stem(path)}_{case['name']}", rows.encode())


def main():
    ladders()
    tsc_vectors()
    found = bumps()
    beefs()
    arcade(found[0] if found else b"")
    for target in sorted(os.listdir(CORPUS)):
        print(target, len(os.listdir(os.path.join(CORPUS, target))))


if __name__ == "__main__":
    main()
