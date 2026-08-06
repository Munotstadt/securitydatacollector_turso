"""Collector_YahooWeekday_0600-2000_UTC: läuft Mo-Fr 06:00-20:00 UTC,
jeweils zur Minute 15/35/55 (siehe Workflow).
Holt Kurse für alle Securities mit Collector = 4 und Ticker IS NOT NULL aus security_master,
schreibt Intraday-Kurse direkt nach security_prices in Turso."""

import os
import sys
import time
from datetime import datetime, timezone

import yfinance as yf
import libsql_client

COLLECTOR_ID = 4
SOURCE_NAME = "YahooWeekday_0600-2000_UTC"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


def get_client():
    url = os.environ["TURSO_DATABASE_URL"]
    token = os.environ["TURSO_AUTH_TOKEN"]
    return libsql_client.create_client_sync(url=url, auth_token=token)


def fetch_securities(client):
    rs = client.execute(
        "SELECT SecurityID, Ticker FROM security_master "
        "WHERE Collector = ? AND Ticker IS NOT NULL",
        [COLLECTOR_ID],
    )
    return [(row[0], row[1]) for row in rs.rows]


def fetch_price_with_retry(ticker):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            t = yf.Ticker(ticker)
            price = t.fast_info["last_price"]
            if price is None:
                raise ValueError("last_price ist None")
            return float(price)
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SECONDS * attempt
                print(f"  Versuch {attempt} für {ticker} fehlgeschlagen ({e}), warte {wait}s...")
                time.sleep(wait)
    raise last_error


def main():
    client = get_client()
    securities = fetch_securities(client)
    print(f"{len(securities)} Securities mit Collector={COLLECTOR_ID} gefunden.")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    added, errors = 0, 0

    for security_id, ticker in securities:
        try:
            price = fetch_price_with_retry(ticker)
            client.execute(
                """INSERT OR IGNORE INTO security_prices
                   (SecurityID, Price, PriceDate, Source, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [security_id, price, now_iso, SOURCE_NAME, now_iso],
            )
            added += 1
            print(f"  OK     SecurityID={security_id} Ticker={ticker} Price={price}")
        except Exception as e:
            errors += 1
            print(f"  FEHLER SecurityID={security_id} Ticker={ticker}: {e}")

    client.close()
    print(f"Fertig: {added} Kurse eingefügt, {errors} Fehler.")

    if added == 0 and errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
