"""
Helpers for downloading S&P 500 data from Wikipedia and yfinance.
Pure pandas: no Spark. Everything returns clean, typed DataFrames
ready to be written as parquet to the landing Volume.
"""
import io
import logging
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf

log = logging.getLogger(__name__)

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
PRICE_COLS = ["open", "high", "low", "close", "volume"]
MARKET_TZ = ZoneInfo("America/New_York")


def utc_now_str() -> str:
    """UTC timestamp as a string (avoids pandas nanosecond timestamps in parquet)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_constituents() -> pd.DataFrame:
    """Current S&P 500 members with company name and sector."""
    resp = requests.get(
        WIKI_URL,
        headers={"User-Agent": "Mozilla/5.0 (sp500-forecasting portfolio project)"},
        timeout=30,
    )
    resp.raise_for_status()

    table = pd.read_html(io.StringIO(resp.text))[0]
    df = table[["Symbol", "Security", "GICS Sector", "GICS Sub-Industry"]].rename(
        columns={
            "Symbol": "ticker",
            "Security": "company",
            "GICS Sector": "sector",
            "GICS Sub-Industry": "sub_industry",
        }
    )
    # yfinance uses '-' instead of '.' (BRK.B -> BRK-B)
    df["ticker"] = df["ticker"].str.strip().str.replace(".", "-", regex=False)
    df["ingested_at"] = utc_now_str()

    # Sanity check: the S&P 500 has ~503 share classes
    if not 480 <= len(df) <= 520:
        raise ValueError(f"Unexpected constituent count: {len(df)}")
    return df


def drop_incomplete_today(df: pd.DataFrame) -> pd.DataFrame:
    """Remove today's bar if the US market hasn't closed yet (partial-day prices)."""
    now_et = datetime.now(MARKET_TZ)
    closed = (now_et.hour, now_et.minute) >= (16, 30)   # 4:00pm close + 30 min buffer
    if not closed:
        df = df[df["date"] < now_et.strftime("%Y-%m-%d")]
    return df


def fetch_prices_batch(tickers: list[str], period: str, retries: int = 2) -> pd.DataFrame:
    """
    Download daily adjusted prices for a batch of tickers.
    Returns long format: one row per (date, ticker).
    """
    raw = pd.DataFrame()
    for attempt in range(1, retries + 2):
        try:
            raw = yf.download(
                tickers,
                period=period,
                interval="1d",
                group_by="ticker",
                auto_adjust=True,   # split/dividend-adjusted prices
                threads=True,
                progress=False,
            )
            break
        except Exception as e:
            log.warning("Batch download attempt %d failed: %s", attempt, e)
            time.sleep(5 * attempt)

    if raw.empty:
        return pd.DataFrame()

    # Wide (ticker x field columns) -> long (one row per date+ticker)
    frames = []
    available = set(raw.columns.get_level_values(0))
    for t in tickers:
        if t not in available:
            continue
        sub = raw[t].dropna(how="all").reset_index()
        sub["ticker"] = t
        frames.append(sub)
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df.columns = [c.lower() for c in df.columns]

    # Known fixes (see project learnings):
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")  # no nanosecond timestamps
    for c in PRICE_COLS:
        df[c] = df[c].astype("float64")                              # stable dtypes across batches

    df = df.dropna(subset=["close"])
    df = drop_incomplete_today(df)                                   # no partial trading days
    df["ingested_at"] = utc_now_str()
    return df[["date", "ticker", *PRICE_COLS, "ingested_at"]]