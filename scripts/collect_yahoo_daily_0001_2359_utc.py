"""Collector_YahooDaily_0001-2359_UTC: läuft täglich (7 Tage/Woche), rund um die Uhr,
jeweils zur Minute 18/48 (siehe Workflow).
Holt Kurse für alle Securities mit Collector = 2 und Ticker IS NOT NULL aus security_master,
schreibt Intraday-Kurse direkt nach security_prices in Turso (via HTTP-API)."""

import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import yfinance as yf

COLLECTOR_ID = 2
SOURCE_NAME = "YahooDaily_0001-2359_UTC"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


class TursoClient:
    def __init__(self, url, token):
        parsed = urlparse(url)
        host = parsed.hostname
        print(f"DIAGNOSE: Verbinde zu Host = {host}")
        self.endpoint = f"https://{host}/v2/pipeline"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    @staticmethod
    def _encode_args(args):
        encoded = []
        for a in args or []:
            if a is None:
                encoded.append({"type": "null"})
            elif isinstance(a, bool):
                encoded.append({"type": "integer", "value": str(int(a))})
            elif isinstance(a, int):
                encoded.append({"type": "integer", "value": str(a)})
            elif isinstance(a, float):
                encoded.append({"type": "float", "value": a})
            else:
                encoded.append({"type": "text", "value": str(a)})
        return encoded

    def execute(self, sql, args=None):
        body = {"requests": [
            {"type": "execute", "stmt": {"sql": sql, "args": self._encode_args(args)}},
            {"type": "close"},
        ]}
        resp = requests.post(self.endpoint, headers=self.headers, json=body, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        first = data["results"][0]
        if first.get("type") == "error":
            raise RuntimeError(f"Turso SQL error: {first['error'].get('message')}")

        result = first["response"]["result"]
        rows = [[cell.get("value") for cell in row] for row in result.get("rows", [])]
        affected = int(result.get("affected_row_count", 0))
        return rows, affected


def get_client():
    url = os.environ.get("TURSO_DATABASE_URL", "")
    token = os.environ.get("TURSO_AUTH_TOKEN", "")
    if not url or not token:
        print("FEHLER: TURSO_DATABASE_URL oder TURSO_AUTH_TOKEN fehlt.")
        sys.exit(1)
    return TursoClient(url, token)


def fetch_securities(client):
    rows, _ = client.execute(
        "SELECT SecurityID, Ticker FROM security_master "
        "WHERE Collector = ? AND Ticker IS NOT NULL",
        [COLLECTOR_ID],
    )
    return [(row[0], row[1]) for row in rows]


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
    added, skipped, errors = 0, 0, 0

    for security_id, ticker in securities:
        try:
            price = fetch_price_with_retry(ticker)
            _, affected = client.execute(
                """INSERT OR IGNORE INTO security_prices
                   (SecurityID, Price, PriceDate, Source, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [security_id, price, now_iso, SOURCE_NAME, now_iso],
            )
            if affected == 0:
                skipped += 1
                print(f"  SKIP   SecurityID={security_id} Ticker={ticker} Price={price}")
            else:
                added += 1
                print(f"  OK     SecurityID={security_id} Ticker={ticker} Price={price}")
        except Exception as e:
            errors += 1
            print(f"  FEHLER SecurityID={security_id} Ticker={ticker}: {e}")

    count_after, _ = client.execute("SELECT COUNT(*) FROM security_prices")
    print(f"Fertig: {added} neu eingefügt, {skipped} übersprungen, {errors} Fehler.")
    print(f"Zeilen in security_prices insgesamt: {count_after[0][0]}")

    if added == 0 and errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
