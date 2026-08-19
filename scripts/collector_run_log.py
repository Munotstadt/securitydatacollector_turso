"""
collector_run_log.py

Shared Helper, den jeder Collector dieses Repos (securitydatacollector_turso)
am Ende seines Laufs aufruft, um einen Eintrag in die Tabelle
`collector_runs` der Turso-Datenbank zu schreiben.

Analog zu run_log.py im Schwester-Repo securitydatacollector - dort wird
eine CSV-Zeile vorbereitet und von einem commit_and_push-Skript an
data/collector_runs.csv angehaengt. Hier gibt es kein Git-Commit-Schema
(die Collectoren dieses Repos schreiben ohnehin direkt per HTTP-API in
Turso), daher schreibt log_run() den Eintrag direkt und synchron in die
Turso-Tabelle `collector_runs`:

    CREATE TABLE collector_runs (
        collectortype text,
        runat text,
        status text,
        datapoints text,
        detail text,
        trigger text,
        runid integer,
        runnumber integer
    );

Trigger-Art und Run-ID/-Nummer kommen NICHT von uns berechnet, sondern
direkt aus den von GitHub Actions automatisch gesetzten Umgebungsvariablen
(GITHUB_EVENT_NAME, GITHUB_RUN_ID, GITHUB_RUN_NUMBER) - kein zusaetzlicher
API-Call noetig, und ausserhalb von Actions (z.B. lokaler Testlauf) sind
diese Variablen einfach leer.

Bewusst unabhaengig von den TursoClient-Implementierungen der einzelnen
Collector-Skripte (die sich in Rueckgabeform leicht unterscheiden) - dieses
Modul spricht die Turso HTTP-API (/v2/pipeline) direkt an und braucht nur
TURSO_DATABASE_URL / TURSO_AUTH_TOKEN aus der Umgebung.

log_run() wirft NIE eine Exception: ein fehlgeschlagener Log-Schreibvorgang
darf weder einen sonst erfolgreichen Collector-Lauf zum Scheitern bringen,
noch den eigentlichen Fehler eines fehlgeschlagenen Laufs verschleiern.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

ZURICH = ZoneInfo("Europe/Zurich")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS collector_runs (
    collectortype text,
    runat text,
    status text,
    datapoints text,
    detail text,
    trigger text,
    runid integer,
    runnumber integer
)
"""

INSERT_SQL = (
    "INSERT INTO collector_runs "
    "(collectortype, runat, status, datapoints, detail, trigger, runid, runnumber) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)


def _normalize_turso_url(url):
    url = url.rstrip("/")
    if url.startswith("libsql://"):
        return "https://" + url[len("libsql://"):]
    if url.startswith("wss://"):
        return "https://" + url[len("wss://"):]
    return url


def _to_arg(v):
    if v is None or v == "":
        return {"type": "null"}
    if isinstance(v, bool):
        return {"type": "integer", "value": "1" if v else "0"}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    return {"type": "text", "value": str(v)}


def now_zurich_str():
    return datetime.now(ZURICH).strftime("%d.%m.%Y %H:%M:%S")


def trigger_label():
    """GITHUB_EVENT_NAME ist z.B. 'schedule' (Cron) oder 'workflow_dispatch'
    (manuell ueber den Actions-Tab auf GitHub ausgeloest)."""
    event = os.environ.get("GITHUB_EVENT_NAME", "")
    if event == "workflow_dispatch":
        return "Manual"
    if event == "schedule":
        return "Scheduled"
    return event or "Unknown"


def _int_env(name):
    val = os.environ.get(name, "")
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def log_run(collector_type, status, data_points, detail=""):
    """Schreibt EINEN Eintrag in die Turso-Tabelle collector_runs.

    collector_type: z.B. "Yahoo Weekday 0400-2200 UTC" (siehe Aufrufer)
    status: "OK" oder "ERROR"
    data_points: Anzahl verarbeiteter/geschriebener Datenpunkte (wird als
        Text gespeichert, analog zur Detail-Spalte im Schwester-Repo, die
        z.B. auch "3148 daily-compacted / -5 monthly-compacted" enthalten kann)
    detail: optionaler Freitext (z.B. Fehleranzahl/-meldung)
    """
    try:
        url = os.environ.get("TURSO_DATABASE_URL", "")
        token = os.environ.get("TURSO_AUTH_TOKEN", "")
        if not url or not token:
            print("[run_log] TURSO_DATABASE_URL/TURSO_AUTH_TOKEN fehlt - "
                  "collector_runs-Eintrag uebersprungen.")
            return

        endpoint = f"{_normalize_turso_url(url)}/v2/pipeline"
        args = [
            collector_type,
            now_zurich_str(),
            status,
            str(data_points),
            detail or "",
            trigger_label(),
            _int_env("GITHUB_RUN_ID"),
            _int_env("GITHUB_RUN_NUMBER"),
        ]
        body = {
            "requests": [
                {"type": "execute", "stmt": {"sql": CREATE_TABLE_SQL, "args": []}},
                {"type": "execute", "stmt": {"sql": INSERT_SQL, "args": [_to_arg(a) for a in args]}},
                {"type": "close"},
            ]
        }
        resp = requests.post(
            endpoint,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=body,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        for r in data.get("results", []):
            if r.get("type") == "error":
                print(f"[run_log] Turso-Fehler beim Schreiben des collector_runs-Eintrags: {r.get('error')}")
                return
        print(f"[run_log] collector_runs-Eintrag geschrieben: "
              f"{collector_type} / {status} / {data_points}"
              + (f" ({detail})" if detail else ""))
    except Exception as e:
        print(f"[run_log] Konnte collector_runs-Eintrag nicht schreiben (ignoriert): {e}")
