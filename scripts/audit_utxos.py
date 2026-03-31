#!/usr/bin/env python3 -u
"""
Audit spendable UTXOs in Alice's wallet against on-chain state.

Checks each spendable output via WhatsOnChain to see if it has been
spent on-chain. Outputs that are spent on-chain but still marked
spendable in the DB are the result of a feedback loop bug.

Usage:
    # Dry-run (default) — shows what would be fixed, no DB writes
    python3 audit_utxos.py --dry-run

    # Audit a single basket (for testing)
    python3 audit_utxos.py --dry-run --basket default

    # Actually fix the DB
    python3 audit_utxos.py --apply

Dependencies: Python stdlib only (sqlite3, urllib, json, time, argparse)
"""

import argparse
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

DB_PATH = "/Users/johncalhoun/bsv/_archived/bsv-wallet-cli-old/wallet.db"
WOC_BASE = "https://api.whatsonchain.com/v1/bsv/main"
REQUEST_INTERVAL = 0.34          # seconds between WoC requests (~3 req/s)
MAX_RETRIES = 3
RETRY_BACKOFF = 2                # seconds between retries
RATE_LIMIT_BACKOFF = 10          # seconds to wait on 429
PROGRESS_EVERY = 100             # print progress every N outputs


# --------------------------------------------------------------------------- #
# WhatsOnChain helpers
# --------------------------------------------------------------------------- #

def check_spent_on_chain(txid: str, vout: int) -> bool | None:
    """
    Returns True if the output is spent, False if unspent, None on error.
    """
    url = f"{WOC_BASE}/tx/{txid}/out/{vout}/spent"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "bsv-worm-audit/1.0"})
            resp = urllib.request.urlopen(req, timeout=15)
            # 200 with data means spent
            body = resp.read()
            if body and len(body) > 2:
                # WoC returns the spending tx details as JSON
                return True
            # Empty 200 — treat as unspent (shouldn't happen but be safe)
            return False

        except urllib.error.HTTPError as e:
            if e.code == 404:
                # 404 means not spent
                return False
            if e.code == 429:
                print(f"    Rate limited (429), backing off {RATE_LIMIT_BACKOFF}s...")
                time.sleep(RATE_LIMIT_BACKOFF)
                continue
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            print(f"    HTTP error {e.code} for {txid}:{vout} after {MAX_RETRIES} attempts")
            return None

        except (urllib.error.URLError, OSError) as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            print(f"    Network error for {txid}:{vout} after {MAX_RETRIES} attempts: {e}")
            return None

    return None


# --------------------------------------------------------------------------- #
# Main audit logic
# --------------------------------------------------------------------------- #

def run_audit(db_path: str, apply: bool, basket_filter: str | None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # ----- Build query -----
    query = """
        SELECT o.output_id, o.satoshis, o.vout, t.txid, b.name AS basket_name
        FROM outputs o
        JOIN transactions t ON o.transaction_id = t.transaction_id
        LEFT JOIN output_baskets b ON o.basket_id = b.basket_id
        WHERE o.spendable = 1
          AND o.basket_id IS NOT NULL
    """
    params = []
    if basket_filter:
        query += " AND b.name = ?"
        params.append(basket_filter)
    query += " ORDER BY b.name, o.output_id"

    rows = conn.execute(query, params).fetchall()
    total = len(rows)

    if total == 0:
        print("No spendable outputs found matching criteria.")
        conn.close()
        return

    mode = "APPLY" if apply else "DRY-RUN"
    print(f"\n{'=' * 60}")
    print(f"UTXO Audit — {mode} mode")
    print(f"Database: {db_path}")
    print(f"Total outputs to check: {total}")
    if basket_filter:
        print(f"Basket filter: {basket_filter}")
    print(f"{'=' * 60}\n")

    # ----- Per-basket accumulators -----
    baskets: dict[str, dict] = {}

    def get_basket(name: str) -> dict:
        if name not in baskets:
            baskets[name] = {
                "checked": 0,
                "unspent": 0,
                "spent_fixed": 0,
                "errors": 0,
                "sats_fixed": 0,
            }
        return baskets[name]

    # ----- Walk outputs -----
    fixed_ids: list[int] = []

    for i, row in enumerate(rows):
        output_id = row["output_id"]
        satoshis = row["satoshis"]
        vout = row["vout"]
        txid = row["txid"]
        basket = row["basket_name"] or "(unknown)"

        b = get_basket(basket)
        b["checked"] += 1

        # Progress
        if (i + 1) % PROGRESS_EVERY == 0 or (i + 1) == total:
            print(f"  Progress: {i + 1}/{total}  (basket: {basket})")

        # Skip outputs without a txid (shouldn't happen, but be defensive)
        if not txid:
            b["errors"] += 1
            print(f"    SKIP: output_id={output_id} has no txid")
            continue

        # Check on-chain
        spent = check_spent_on_chain(txid, vout)

        if spent is None:
            b["errors"] += 1
        elif spent:
            b["spent_fixed"] += 1
            b["sats_fixed"] += satoshis
            fixed_ids.append(output_id)
            action = "FIXED" if apply else "WOULD FIX"
            print(f"  {action}: output_id={output_id} txid={txid}:{vout} basket={basket} sats={satoshis} — spent on-chain")
        else:
            b["unspent"] += 1

        # Rate limit
        time.sleep(REQUEST_INTERVAL)

    # ----- Apply fixes -----
    if apply and fixed_ids:
        print(f"\nApplying {len(fixed_ids)} fixes to database...")
        cursor = conn.cursor()
        for oid in fixed_ids:
            cursor.execute(
                "UPDATE outputs SET spendable = 0, updated_at = datetime('now') WHERE output_id = ?",
                (oid,),
            )
        conn.commit()
        print(f"Done. {len(fixed_ids)} outputs marked as not spendable.")
    elif not apply and fixed_ids:
        print(f"\nDRY-RUN: {len(fixed_ids)} outputs would be fixed. Re-run with --apply to write changes.")

    conn.close()

    # ----- Summary -----
    print(f"\n{'=' * 60}")
    print("PER-BASKET SUMMARY")
    print(f"{'=' * 60}")

    overall_checked = 0
    overall_unspent = 0
    overall_spent = 0
    overall_errors = 0
    overall_sats = 0

    for name in sorted(baskets.keys()):
        b = baskets[name]
        overall_checked += b["checked"]
        overall_unspent += b["unspent"]
        overall_spent += b["spent_fixed"]
        overall_errors += b["errors"]
        overall_sats += b["sats_fixed"]

        label = "Spent on-chain (FIXED)" if apply else "Spent on-chain (WOULD FIX)"
        print(f"\nBasket: {name}")
        print(f"  Total checked:  {b['checked']}")
        print(f"  Still unspent:  {b['unspent']}")
        print(f"  {label}: {b['spent_fixed']}")
        print(f"  Check errors:   {b['errors']}")
        print(f"  Sats affected:  {b['sats_fixed']}")

    label = "Spent on-chain (FIXED)" if apply else "Spent on-chain (WOULD FIX)"
    print(f"\n{'=' * 60}")
    print("OVERALL SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total checked:  {overall_checked}")
    print(f"  Still unspent:  {overall_unspent}")
    print(f"  {label}: {overall_spent}")
    print(f"  Check errors:   {overall_errors}")
    print(f"  Sats in bad-state outputs: {overall_sats}")
    print()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(
        description="Audit wallet UTXOs against on-chain state via WhatsOnChain"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show what would be fixed without modifying DB (default)",
    )
    group.add_argument(
        "--apply",
        action="store_true",
        help="Actually write fixes to the database",
    )
    parser.add_argument(
        "--basket",
        type=str,
        default=None,
        help="Audit only a specific basket (e.g. 'default', 'worm-state')",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=DB_PATH,
        help=f"Path to wallet SQLite database (default: {DB_PATH})",
    )

    args = parser.parse_args()

    # --apply overrides the default --dry-run
    apply = args.apply

    if apply:
        print("*** APPLY MODE — database will be modified ***")
        print("Press Ctrl+C within 3 seconds to abort...")
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            print("\nAborted.")
            sys.exit(1)

    run_audit(db_path=args.db, apply=apply, basket_filter=args.basket)


if __name__ == "__main__":
    main()
