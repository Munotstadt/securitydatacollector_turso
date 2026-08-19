#!/usr/bin/env python3
"""
cleanup_prices_weekly.py

Wöchentlicher Clean-up-Job für die Tabelle `security_prices` in Turso.

Hintergrund: verschiedene Collector-Läufe (Sources) erzeugen für dieselbe
Security an einem Tag mehrere Kurs-Einträge (z.B. die Yahoo-Collectoren,
die mehrmals täglich laufen). Für Kurse, die älter als RETENTION_DAYS
(12) Tage sind, werden diese Einträge auf EINEN Eintrag pro Tag,
Security UND Source reduziert - erhalten bleibt jeweils der letzte
(chronologisch späteste) Preis des Tages. Mehrere Sources ergeben
weiterhin mehrere Einträge pro Tag (es wird NICHT über Sources hinweg
zusammengefasst - "1 Eintrag/Tag/Security/Source", nicht "1/Tag/Security").

Rows, die nicht älter als RETENTION_DAYS Tage sind
(date(PriceDate) >= heute - 12 Tage), werden nicht angefasst - dort
bleiben alle Intraday-Einträge vollständig erhalten.

Die eigentliche Kompaktierung passiert serverseitig in einem einzigen
DELETE-Statement (Turso/libSQL unterstützt Window-Functions), damit nicht
potenziell hunderttausende Zeilen über die HTTP-API hin- und hergeschickt
werden müssen.

Sicherheits-/Verifikations-Schritte:
  - Vor dem DELETE wird berechnet, wie viele Zeilen nach der Kompaktierung
    übrig bleiben SOLLTEN (Anzahl distincter (SecurityID, Source, Tag)-
    Gruppen unter den Kandidaten) und wie viele Zeilen das DELETE folglich
    entfernen sollte (expected_deleted).
  - Nach dem DELETE wird die von Turso zurückgemeldete tatsächliche
    affected_row_count mit expected_deleted verglichen; bei Abweichung
    wird das im Log-Detail vermerkt (WARNING), der Lauf aber nicht als
    ERROR gewertet, da das DELETE selbst bereits gelaufen ist.
  - DRY_RUN=1 (Env-Var) bzw. der Workflow-Input `dry_run` führt keine
    Löschung aus, sondern loggt nur, was gelöscht/behalten würde -
    nützlich für einen ersten Testlauf.

Läuft wöchentlich, siehe .github/workflows/Cleanup_Prices_Weekly.yml.

Env vars:
  TURSO_DATABASE_URL, TURSO_AUTH_TOKEN  - wie alle anderen Skripte in
                                           diesem Repo.
  DRY_RUN                               - "1"/"true" = nichts löschen,
                                           nur simulieren/loggen.
"""

import os
import sys
import time
from urllib.parse import urlparse

import requests

from collector_run_log import log_run

RUN_LOG_LABEL = "Price Cleanup Weekly"
RETENTION_DAYS = 12
DRY_RUN = os.environ.get("DRY_RUN", "").strip().lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Turso HTTP client - gleiches Muster wie in den collect_*.py Skripten
# dieses Repos (execute() gibt (rows, affected_row_count) zurück).
# ---------------------------------------------------------------------------
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

    def execute(self, sql, args=None, timeout=60):
        body = {"requests": [
            {"type": "execute", "stmt": {"sql": sql, "args": self._encode_args(args)}},
            {"type": "close"},
        ]}
        resp = requests.post(self.endpoint, headers=self.headers, json=body, timeout=timeout)
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


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
COUNT_TOTAL_SQL = "SELECT COUNT(*) FROM security_prices"

COUNT_CANDIDATES_SQL = (
    "SELECT COUNT(*) FROM security_prices WHERE date(PriceDate) < date('now', ?)"
)

# Anzahl distincter (SecurityID, Source, Tag)-Gruppen unter den Kandidaten
# = Anzahl Zeilen, die nach der Kompaktierung übrig bleiben (sollten).
COUNT_GROUPS_SQL = """
SELECT COUNT(*) FROM (
    SELECT 1
    FROM security_prices
    WHERE date(PriceDate) < date('now', ?)
    GROUP BY SecurityID, Source, date(PriceDate)
)
"""

# Behält pro (SecurityID, Source, Tag) nur die Zeile mit dem spätesten
# PriceDate (bei exaktem Gleichstand: höchste rowid) - alle anderen
# Zeilen dieser Kandidaten-Menge werden gelöscht.
DELETE_SQL = """
DELETE FROM security_prices
WHERE date(PriceDate) < date('now', ?)
  AND rowid NOT IN (
    SELECT rowid FROM (
        SELECT rowid,
               ROW_NUMBER() OVER (
                   PARTITION BY SecurityID, Source, date(PriceDate)
                   ORDER BY PriceDate DESC, rowid DESC
               ) AS rn
        FROM security_prices
        WHERE date(PriceDate) < date('now', ?)
    )
    WHERE rn = 1
  )
"""


def main():
    started = time.monotonic()
    client = get_client()
    cutoff_arg = f"-{RETENTION_DAYS} days"

    total_before, _ = client.execute(COUNT_TOTAL_SQL)
    total_before = total_before[0][0]

    candidates_rows, _ = client.execute(COUNT_CANDIDATES_SQL, [cutoff_arg])
    candidate_count = candidates_rows[0][0]

    groups_rows, _ = client.execute(COUNT_GROUPS_SQL, [cutoff_arg])
    would_keep = groups_rows[0][0]
    expected_deleted = candidate_count - would_keep

    print(f"Gesamt security_prices: {total_before} Zeilen.")
    print(f"Kandidaten (älter als {RETENTION_DAYS} Tage): {candidate_count} Zeilen, "
          f"davon {would_keep} bereits 1/Tag/Security/Source (bleiben in jedem Fall).")
    print(f"Erwartete Löschungen: {expected_deleted}.")

    if DRY_RUN:
        deleted = 0
        print("DRY RUN: es wird NICHTS gelöscht (DRY_RUN gesetzt).")
    else:
        _, deleted = client.execute(DELETE_SQL, [cutoff_arg, cutoff_arg], timeout=120)
        print(f"DELETE ausgeführt: {deleted} Zeilen gelöscht.")

    total_after, _ = client.execute(COUNT_TOTAL_SQL)
    total_after = total_after[0][0]

    duration = time.monotonic() - started

    warning = ""
    if not DRY_RUN and deleted != expected_deleted:
        warning = (f" WARNUNG: erwartete {expected_deleted} Löschungen, "
                   f"tatsächlich {deleted}.")
        print(warning.strip())

    print(f"Fertig in {duration:.1f}s. Zeilen in security_prices insgesamt: "
          f"{total_after} (vorher: {total_before}).")

    status = "OK" if not warning else "ERROR"
    prefix = "DRY RUN - " if DRY_RUN else ""
    detail = (f"{prefix}{candidate_count} candidates, {would_keep} kept "
              f"(1/day/security/source), {expected_deleted} expected deletions, "
              f"{total_before}->{total_after} rows total, {duration:.1f}s{warning}")
    log_run(RUN_LOG_LABEL, status, deleted if not DRY_RUN else expected_deleted, detail)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log_run(RUN_LOG_LABEL, "ERROR", 0, f"Unhandled exception: {e}")
        raise
