"""One-Time Migration: liest security_prices.csv aus dem Repo-Root und
schreibt alle Zeilen nach Turso (Tabelle security_prices).

CSV-Format: SecurityID,SecurityName,Price,PriceDate,Source,Created_at
PriceDate/Created_at im CSV: DD.MM.YYYY HH:MM:SS (Swiss-Format)
-> wird umgewandelt in ISO: YYYY-MM-DD HH:MM:SS (Turso-Schema-Vorgabe)

SecurityName aus dem CSV wird ignoriert (security_prices kennt nur SecurityID).
MainID ist im CSV nicht vorhanden -> bleibt NULL.

NUR EINMALIG AUSFÜHREN (workflow_dispatch, kein Cron)."""

import csv
import os
import sys
from urllib.parse import urlparse

import requests

CSV_PATH = "security_prices.csv"
BATCH_SIZE = 200  # Statements pro HTTP-Request an Turso


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
        rows, affected, _ = self.batch_execute([(sql, args)])[0]
        return rows, affected

    def batch_execute(self, statements):
        """statements: Liste von (sql, args)-Tupeln. Läuft in EINEM HTTP-Request."""
        requests_body = [
            {"type": "execute", "stmt": {"sql": sql, "args": self._encode_args(args)}}
            for sql, args in statements
        ]
        requests_body.append({"type": "close"})

        resp = requests.post(self.endpoint, headers=self.headers,
                              json={"requests": requests_body}, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for i, _ in enumerate(statements):
            item = data["results"][i]
            if item.get("type") == "error":
                results.append((None, None, item["error"].get("message")))
                continue
            result = item["response"]["result"]
            rows = [[cell.get("value") for cell in row] for row in result.get("rows", [])]
            affected = int(result.get("affected_row_count", 0))
            results.append((rows, affected, None))
        return results


def get_client():
    url = os.environ.get("TURSO_DATABASE_URL", "")
    token = os.environ.get("TURSO_AUTH_TOKEN", "")
    if not url or not token:
        print("FEHLER: TURSO_DATABASE_URL oder TURSO_AUTH_TOKEN fehlt.")
        sys.exit(1)
    return TursoClient(url, token)


def swiss_to_iso(swiss_str):
    """'27.07.2026 17:41:38' -> '2026-07-27 17:41:38'"""
    swiss_str = swiss_str.strip()
    date_part, time_part = swiss_str.split(" ", 1)
    day, month, year = date_part.split(".")
    return f"{year}-{month}-{day} {time_part}"


def read_csv_rows():
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"{len(rows)} Zeilen in {CSV_PATH} gefunden.")
    return rows


def main():
    client = get_client()
    csv_rows = read_csv_rows()

    count_before, _ = client.execute("SELECT COUNT(*) FROM security_prices")
    print(f"Zeilen in security_prices VOR Migration: {count_before[0][0]}")

    statements = []
    parse_errors = 0

    for row in csv_rows:
        try:
            security_id = int(row["SecurityID"])
            price = float(row["Price"])
            price_date = swiss_to_iso(row["PriceDate"])
            source = row["Source"].strip()
            created_at = swiss_to_iso(row["Created_at"])
        except Exception as e:
            parse_errors += 1
            print(f"  PARSE-FEHLER bei Zeile {row}: {e}")
            continue

        statements.append((
            """INSERT OR IGNORE INTO security_prices
               (SecurityID, Price, PriceDate, Source, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            [security_id, price, price_date, source, created_at],
        ))

    print(f"{len(statements)} gültige Zeilen zum Import vorbereitet ({parse_errors} Parse-Fehler übersprungen).")

    added, skipped, db_errors = 0, 0, 0

    for i in range(0, len(statements), BATCH_SIZE):
        batch = statements[i:i + BATCH_SIZE]
        try:
            results = client.batch_execute(batch)
            for _, affected, error_msg in results:
                if error_msg:
                    db_errors += 1
                    print(f"  DB-FEHLER: {error_msg}")
                elif affected == 0:
                    skipped += 1
                else:
                    added += 1
        except Exception as e:
            db_errors += len(batch)
            print(f"  BATCH-FEHLER (Zeilen {i}-{i+len(batch)}): {e}")

        print(f"  Batch {i // BATCH_SIZE + 1}: {i + len(batch)}/{len(statements)} verarbeitet "
              f"(bisher: {added} eingefügt, {skipped} übersprungen, {db_errors} Fehler)")

    count_after, _ = client.execute("SELECT COUNT(*) FROM security_prices")

    print("=" * 60)
    print(f"MIGRATION ABGESCHLOSSEN")
    print(f"  Neu eingefügt:      {added}")
    print(f"  Übersprungen (Dup): {skipped}")
    print(f"  Parse-Fehler:       {parse_errors}")
    print(f"  DB-Fehler:          {db_errors}")
    print(f"  Zeilen VOR:         {count_before[0][0]}")
    print(f"  Zeilen NACH:        {count_after[0][0]}")
    print("=" * 60)

    if db_errors > 0 and added == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
