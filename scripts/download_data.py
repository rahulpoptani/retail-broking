#!/usr/bin/env python3
from datetime import date, timedelta
from pathlib import Path

import yfinance as yf
import pandas as pd

# --- Configuration ---
SYMBOLS   = ["TCS.NS", "RELIANCE.NS", "HDFCBANK.NS"]
INTERVAL  = "1m"        # 1m 2m 5m 15m 30m 1h 1d 1wk 1mo
START     = "2026-06-01"
END       = "2026-06-13"
# ---------------------

DATA_DIR = Path(__file__).parent.parent / "data"


def date_range(start: str, end: str):
    current = date.fromisoformat(start)
    stop    = date.fromisoformat(end)
    while current < stop:
        yield current
        current += timedelta(days=1)


def download_day(symbol: str, day: date) -> pd.DataFrame | None:
    next_day = day + timedelta(days=1)
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=str(day), end=str(next_day), interval=INTERVAL, auto_adjust=True)
        if df.empty:
            return None
        df.index = df.index.tz_localize(None) if df.index.tzinfo is not None else df.index
        df.index.name = "Datetime"
        df = df[["Open", "High", "Low", "Close", "Volume"]]
        df["Symbol"] = symbol
        return df
    except Exception as e:
        print(f"    Error on {day}: {e}")
        return None


def csv_path(symbol: str) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_symbol = symbol.replace(".", "_").replace("/", "_").replace("^", "")
    return DATA_DIR / f"{safe_symbol}_{INTERVAL}.csv"


def append_to_csv(df: pd.DataFrame, filepath: Path):
    write_header = not filepath.exists()
    df.to_csv(filepath, mode="a", header=write_header)


def main():
    days = list(date_range(START, END))
    print(f"\n{len(SYMBOLS)} symbol(s) | interval={INTERVAL} | {START} to {END} ({len(days)} day(s))\n")

    for symbol in SYMBOLS:
        filepath = csv_path(symbol)
        total_rows = 0
        print(f"  {symbol}")

        for day in days:
            df = download_day(symbol, day)
            if df is None:
                print(f"    {day}: no data (weekend/holiday?)")
                continue
            append_to_csv(df, filepath)
            total_rows += len(df)
            print(f"    {day}: {len(df)} rows appended")

        print(f"  -> {total_rows} total rows saved to {filepath.name}\n")


if __name__ == "__main__":
    main()
