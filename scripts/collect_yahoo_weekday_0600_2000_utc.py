"""Collector_YahooWeekday_0600-2000_UTC: läuft Mo-Fr 06:00-20:00 UTC,
jeweils zur Minute 15/35/55 (siehe Workflow).
Holt Kurse für alle Securities mit Collector = 4 und Ticker IS NOT NULL aus security_master,
schreibt Intraday-Kurse direkt nach security_prices in Turso."""

import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import yfinance as yf
import libsql_client

COLLECTOR_ID = 4
SOURCE_NAME = "YahooWeekday_0600-2000_UTC"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


def get_client():
    url = os.environ.get("TURSO_DATABASE_URL", "")
    token = os.environ.get("TURSO_AUTH_TOKEN", "")

    if not url or not token:
        print("FEHLER: TURSO_DATABASE_URL oder TURSO_AUTH_TOKEN fehlt.")
        sys.exit(1)

    parsed = urlparse(url)
    print(f"DIAGNOSE: Verbinde zu Host = {parsed.hostname}, Schema = {parsed.scheme}")

    if url.startswith("libsql://"):
        url = url.replace("libsql://", "https://", 1)

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

    tables_rs = client.execute("SELECT name FROM sqlite_master WHERE type='table'")
    print("DIAGNOSE: Vorhandene Tabellen:", [row[0] for row in tables_rs.rows])

    count_before = client.execute("SELECT COUNT(*) FROM security_prices").rows[0][0]
    print(f"DIAGNOSE: Zeilen in security_prices VOR dem Lauf: {count_before}")

    securities = fetch_securities(client)
    print(f"{len(securities)} Securities mit Collector={COLLECTOR_ID} gefunden.")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    added, errors = 0, 0

    # NUR DEN ERSTEN Security testen, ohne OR IGNORE, um den echten Fehler zu sehen
    if securities:
        security_id, ticker = securities[0]
        price = fetch_price_with_retry(ticker)
        print(f"DIAGNOSE: Teste Insert OHNE 'OR IGNORE' für SecurityID={security_id}, "
              f"Ticker={ticker}, Price={price}, PriceDate={now_iso}")
        try:
            result = client.execute(
                """INSERT INTO security_prices
                   (SecurityID, Price, PriceDate, Source, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [security_id, price, now_iso, SOURCE_NAME, now_iso],
            )
            print(f"DIAGNOSE: Insert erfolgreich! rows_affected = {result.rows_affected}, "
                  f"last_insert_rowid = {result.last_insert_rowid}")
        except Exception as e:
            print(f"DIAGNOSE: ECHTER FEHLER beim Insert: {type(e).__name__}: {e}")
            client.close()
            sys.exit(1)

    count_after = client.execute("SELECT COUNT(*) FROM security_prices").rows[0][0]
    print(f"DIAGNOSE: Zeilen in security_prices NACH dem Testinsert: {count_after}")

    client.close()


if __name__ == "__main__":
    main()
