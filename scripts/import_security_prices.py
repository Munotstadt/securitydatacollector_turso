#!/usr/bin/env python3
"""
Importiert eine SecurityID;Price;PriceDate;Source (oder komma-getrennte) CSV-Datei
in die bereits bestehende Turso-Tabelle `security_prices`.

Tabellen-Schema (bereits vorhanden):
  CREATE TABLE security_prices (
      id integer PRIMARY KEY AUTOINCREMENT,
      SecurityID integer NOT NULL,
      Price real NOT NULL,
      PriceDate text NOT NULL,           -- Format: YYYY-MM-DD oder YYYY-MM-DD HH:MM:SS
      Source text,
      created_at text DEFAULT ...,
      ... FK auf security_master, CHECKs auf PriceDate-Format und Source-Laenge
  );
  CREATE UNIQUE INDEX idx_security_prices_unique ON security_prices (SecurityID, PriceDate);

Da bereits ein UNIQUE INDEX auf (SecurityID, PriceDate) existiert, wird
"INSERT OR IGNORE" verwendet: bereits vorhandene Kombinationen werden
automatisch uebersprungen, kein Fehler, kein Duplikat.

Das PriceDate-Feld in den hochgeladenen CSVs liegt im Format dd.mm.yyyy vor
und wird vor dem Insert auf yyyy-mm-dd (das von der CHECK-Constraint
verlangte Format) umgestellt.

Benoetigte Umgebungsvariablen:
  TURSO_DATABASE_URL   z.B. libsql://<db-name>-<org>.turso.io
  TURSO_AUTH_TOKEN     Turso Auth-Token

Aufruf:
  python scripts/import_security_prices.py <pfad-zur-csv-datei>
"""

import csv
import os
import re
import sys

from libsql_client import create_client_sync

TABLE = "security_prices"
BATCH_SIZE = 500

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$")
DMY_DATE_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")


def normalize_date(value: str) -> str:
    """Konvertiert dd.mm.yyyy nach yyyy-mm-dd. Laesst bereits-ISO-Werte unveraendert."""
    value = value.strip()
    if ISO_DATE_RE.match(value):
        return value
    m = DMY_DATE_RE.match(value)
    if m:
        day, month, year = m.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    raise ValueError(f"Unbekanntes Datumsformat: {value!r}")


def detect_delimiter(sample: str) -> str:
    return ";" if sample.count(";") >= sample.count(",") else ","


def read_rows(path: str):
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        sample = f.readline()
        f.seek(0)
        delimiter = detect_delimiter(sample)
        reader = csv.DictReader(f, delimiter=delimiter)
        fieldmap = {name.strip().lower(): name for name in reader.fieldnames or []}

        def col(row, key):
            name = fieldmap.get(key.lower())
            return (row.get(name) or "").strip() if name else ""

        rows = []
        errors = []
        for line_no, row in enumerate(reader, start=2):
            sid = col(row, "SecurityID")
            price = col(row, "Price")
            price_date = col(row, "PriceDate")
            source = col(row, "Source") or None
            if not sid or not price_date or not price:
                continue
            try:
                iso_date = normalize_date(price_date)
            except ValueError as e:
                errors.append(f"Zeile {line_no}: {e}")
                continue
            if source and len(source) > 128:
                source = source[:128]
            rows.append((int(sid), float(price), iso_date, source))
        return rows, errors


def main():
    if len(sys.argv) < 2:
        print("Usage: import_security_prices.py <csv-path>", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    url = os.environ["TURSO_DATABASE_URL"]
    auth_token = os.environ["TURSO_AUTH_TOKEN"]

    # libsql-client versucht bei "libsql://" oder "wss://" per WebSocket (Hrana)
    # zu verbinden. Das schlaegt auf manchen Runnern/Netzwerken (z.B. GitHub
    # Actions) mit einem WSServerHandshakeError fehl. Der HTTP-Modus
    # (Schema "https://") ist zuverlaessiger und wird deshalb erzwungen.
    if url.startswith("libsql://"):
        url = "https://" + url[len("libsql://") :]
    elif url.startswith("wss://"):
        url = "https://" + url[len("wss://") :]

    rows, errors = read_rows(csv_path)
    print(f"{len(rows)} gueltige Zeilen aus {csv_path} gelesen.")
    if errors:
        print(f"{len(errors)} Zeilen mit Fehlern uebersprungen:")
        for e in errors[:20]:
            print(f"  {e}")

    if not rows:
        print("Keine Zeilen zu importieren.")
        return

    client = create_client_sync(url=url, auth_token=auth_token)
    try:
        # Bekannte SecurityIDs aus security_master laden. Zeilen mit einer
        # SecurityID, die dort nicht existiert, wuerden die FOREIGN KEY
        # Constraint verletzen und (da client.batch() transaktional ist) den
        # gesamten Batch abbrechen -- deshalb werden sie vorab herausgefiltert.
        result = client.execute("SELECT SecurityID FROM security_master")
        known_ids = {r[0] for r in result.rows}

        valid_rows = [r for r in rows if r[0] in known_ids]
        unknown_ids = sorted({r[0] for r in rows if r[0] not in known_ids})

        if unknown_ids:
            print(
                f"{len(rows) - len(valid_rows)} Zeilen uebersprungen, da "
                f"{len(unknown_ids)} SecurityID(s) nicht in security_master existieren:"
            )
            print(f"  {unknown_ids}")

        if not valid_rows:
            print("Keine gueltigen Zeilen (mit bekannter SecurityID) zu importieren.")
            return

        insert_sql = f"""
            INSERT OR IGNORE INTO {TABLE} (SecurityID, Price, PriceDate, Source)
            VALUES (?, ?, ?, ?)
        """

        inserted = 0
        failed = 0
        for i in range(0, len(valid_rows), BATCH_SIZE):
            chunk = valid_rows[i : i + BATCH_SIZE]
            statements = [(insert_sql, list(row)) for row in chunk]
            try:
                results = client.batch(statements)
                inserted += sum(1 for res in results if getattr(res, "rows_affected", 0))
            except Exception as e:
                # Falls doch ein Batch fehlschlaegt (z.B. anderer
                # Constraint-Verstoss), Zeile fuer Zeile einzeln versuchen,
                # damit ein einzelner problematischer Datensatz nicht den
                # gesamten restlichen Import blockiert.
                print(f"  Batch fehlgeschlagen ({e}), verarbeite Zeilen einzeln...")
                for row in chunk:
                    try:
                        res = client.execute(insert_sql, list(row))
                        if getattr(res, "rows_affected", 0):
                            inserted += 1
                    except Exception as row_err:
                        failed += 1
                        print(f"    Zeile uebersprungen {row}: {row_err}")
            print(f"  {min(i + BATCH_SIZE, len(valid_rows))}/{len(valid_rows)} verarbeitet...")

        skipped = len(valid_rows) - inserted - failed
        print(
            f"Fertig: {inserted} neue Zeilen eingefuegt, {skipped} Duplikate/bereits "
            f"vorhanden uebersprungen, {failed} Zeilen mit Fehler, "
            f"{len(rows) - len(valid_rows)} wegen unbekannter SecurityID uebersprungen."
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
