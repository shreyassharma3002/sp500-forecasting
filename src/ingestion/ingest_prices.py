"""
Entry point: download S&P 500 constituents + prices into the landing Volume.

Modes:
  auto        -> 'full' if the Volume has no price files yet, else 'incremental'
  full        -> 5 years of history (first run + weekly refresh)
  incremental -> last 7 days (daily runs; overlaps are deduplicated in Silver)
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd

# Make sibling module importable whether run as a Job task or from the editor
try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
except NameError:
    sys.path.insert(0, os.getcwd())

from yf_client import fetch_prices_batch, get_constituents  # noqa: E402

# ---- Config ----
LANDING_ROOT     = "/Volumes/workspace/sp500/landing"
PRICES_DIR       = f"{LANDING_ROOT}/prices"
CONSTITUENTS_DIR = f"{LANDING_ROOT}/constituents"
BATCH_SIZE       = 50      # avoids yfinance timeouts
MAX_FAIL_RATIO   = 0.10    # fail the run if >10% of tickers fail
PERIODS          = {"full": "5y", "incremental": "7d"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest_prices")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["auto", "full", "incremental"], default="auto")
    args, _ = parser.parse_known_args()  # ignore extra args injected by the runtime
    return args


def resolve_mode(mode: str) -> str:
    if mode != "auto":
        return mode
    has_files = os.path.isdir(PRICES_DIR) and any(
        f.endswith(".parquet") for f in os.listdir(PRICES_DIR)
    )
    return "incremental" if has_files else "full"


def main() -> None:
    mode   = resolve_mode(parse_args().mode)
    period = PERIODS[mode]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log.info("Run %s | mode=%s | period=%s", run_id, mode, period)

    os.makedirs(PRICES_DIR, exist_ok=True)
    os.makedirs(CONSTITUENTS_DIR, exist_ok=True)

    # 1. Constituents (fail fast: without tickers there is nothing to do)
    constituents = get_constituents()
    constituents.to_parquet(f"{CONSTITUENTS_DIR}/{run_id}_constituents.parquet", index=False)
    tickers = constituents["ticker"].tolist()
    log.info("Constituents: %d tickers across %d sectors",
             len(tickers), constituents["sector"].nunique())

    # 2. Prices in batches
    failed, rows = [], 0
    for i in range(0, len(tickers), BATCH_SIZE):
        batch_no = i // BATCH_SIZE
        batch = tickers[i:i + BATCH_SIZE]
        try:
            df = fetch_prices_batch(batch, period)
            got = set(df["ticker"]) if not df.empty else set()
            failed += [t for t in batch if t not in got]
            if not df.empty:
                df.to_parquet(f"{PRICES_DIR}/{run_id}_{mode}_batch_{batch_no:03d}.parquet", index=False)
                rows += len(df)
            log.info("Batch %02d: %d rows, %d/%d tickers ok", batch_no, len(df), len(got), len(batch))
        except Exception as e:
            log.error("Batch %02d failed entirely: %s", batch_no, e)
            failed += batch
        time.sleep(1)

    # 3. Quality gate: stop the Job before bad data flows downstream
    fail_ratio = len(failed) / len(tickers)
    log.info("Done | rows=%d | failed=%d (%.1f%%)", rows, len(failed), fail_ratio * 100)
    if failed:
        log.warning("Failed tickers: %s", failed[:25])
    if fail_ratio > MAX_FAIL_RATIO:
        raise RuntimeError(f"{fail_ratio:.0%} of tickers failed (limit {MAX_FAIL_RATIO:.0%})")


if __name__ == "__main__":
    main()