"""Diagnose-Variante: testet EINEN Insert ohne OR IGNORE, um den echten
Constraint-Fehler sichtbar zu machen (Turso HTTP-API, kein libsql-client)."""

import os
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import yfinance as yf

COLLECTOR_ID = 4
SOURCE_NAME = "YahooWeekday_0600-2000_UTC"


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

        print(f"DIAGNOSE: Rohe API-Antwort: {data}")  # <-- zeigt uns ALLES

        first = data["results"][0]
        if first.get("type") == "error":
            raise RuntimeError(f"Turso SQL error: {first['error'].get('message')}")

        result = first["response"]["result"]
        rows = [[cell.get("value") for cell in row] for row in result.get("rows", [])]
        affected = int(result.get("affected_row_count", 0))
        return rows, affected


def main():
    url = os.environ.get("TURSO_DATABASE_URL", "")
    token = os.environ.get("TURSO_AUTH_TOKEN", "")
    client = TursoClient(url, token)

    rows, _ = client.execute(
        "SELECT SecurityID, Ticker FROM security_master WHERE Collector = ? AND Ticker IS NOT NULL",
        [COLLECTOR_ID],
    )
    security_id, ticker = rows[0][0], rows[0][1]

    t = yf.Ticker(ticker)
    price = float(t.fast_info["last_price"])
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print(f"DIAGNOSE: Teste INSERT (OHNE OR IGNORE) für SecurityID={security_id}, "
          f"Ticker={ticker}, Price={price}, PriceDate={now_iso}")

    try:
        _, affected = client.execute(
            """INSERT INTO security_prices (SecurityID, Price, PriceDate, Source, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            [security_id, price, now_iso, SOURCE_NAME, now_iso],
        )
        print(f"DIAGNOSE: Insert erfolgreich! affected_row_count = {affected}")
    except Exception as e:
        print(f"DIAGNOSE: ECHTER FEHLER beim Insert: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
