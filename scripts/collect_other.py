"""
Collector_Other: für Securities, die in security_master mit Collector=3
(Collector_Other) geflaggt sind und deren Kurs nicht über Yahoo Finance
bezogen werden kann (z.B. SNB-Zinssätze, Bank-APIs, HTML-Scraping-Quellen).

Da diese Quellen sehr heterogen sind (unterschiedliche APIs/Formate je
Security), wird hier pro SecurityName eine eigene Fetch-Funktion registriert.
Neue "Other"-Securities in security_master brauchen zusätzlich einen Eintrag
in FETCHERS bzw. FETCHERS_BY_ID unten.

Schreibt direkt nach security_prices in Turso (via HTTP-API).
Läuft mit derselben Cadence wie der Weekday-Collector (siehe Workflow).
"""

import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

COLLECTOR_ID = 3  # Collector_Other in security_parameter

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


# ---------------------------------------------------------------------------
# Turso HTTP-Client (identisch zum Muster der Yahoo-Collectoren)
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


def fetch_active_securities(client):
    """Alle Securities mit Collector = 3 (Collector_Other) aus security_master."""
    rows, _ = client.execute(
        "SELECT SecurityID, SecurityName FROM security_master WHERE Collector = ?",
        [COLLECTOR_ID],
    )
    return [(row[0], row[1]) for row in rows]


def strip_html(html: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "\n", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text


# ---------------------------------------------------------------------------
# Quelle 1: SNB RSS-Feed (R10 - Rendite Bundesobligationen 10J)
# ---------------------------------------------------------------------------

def fetch_snb_r10():
    url = "https://www.snb.ch/public/de/rss/interestRates"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        xml = resp.read().decode("utf-8", errors="replace")
    for item in re.findall(r"<item>(.*?)</item>", xml, re.DOTALL):
        if re.search(r"<cb:rateName>\s*R10\s*</cb:rateName>", item):
            value = float(re.search(r"<cb:value>\s*([\d,.\-]+)\s*</cb:value>", item).group(1).replace(",", "."))
            return value, None
    raise ValueError("R10 nicht im SNB RSS-Feed gefunden")


# ---------------------------------------------------------------------------
# Quelle 2: Raiffeisen API (Hypothekarzinsen Winterthur)
# ---------------------------------------------------------------------------

RAIFFEISEN_API_URL = "https://api.raiffeisen.ch/loan-product-service/v1/products"
RAIFFEISEN_BANK_CODE = "1485"

RAIFFEISEN_DURATION_MAP = {
    12: "Raiffeisen Winterthur Hypothek 1 Jahr Zinssatz",
    60: "Raiffeisen Winterthur Hypothek 5 Jahr Zinssatz",
    120: "Raiffeisen Winterthur Hypothek 10 Jahr Zinssatz",
    180: "Raiffeisen Winterthur Hypothek 15 Jahr Zinssatz",
}

_raiffeisen_rates_cache = None  # Cache pro Skript-Lauf: 1 API-Call statt 4


def _get_raiffeisen_rates():
    global _raiffeisen_rates_cache
    if _raiffeisen_rates_cache is not None:
        return _raiffeisen_rates_cache
    req = urllib.request.Request(RAIFFEISEN_API_URL, headers={
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-rai-bankcode": RAIFFEISEN_BANK_CODE,
        "x-rai-channel": "INFORMATION",
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.raiffeisen.ch/winterthur/de/privatkunden/"
                   "wohnen-und-hypotheken/hypothekarzinsen.html",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    fixed = next(p for p in data if p.get("type") == "FIXED")
    rates_by_months = {
        v["durationInMonths"]: v["rate"]
        for v in fixed["variants"]
        if v["durationInMonths"] in RAIFFEISEN_DURATION_MAP
    }
    _raiffeisen_rates_cache = {
        name: rates_by_months[months]
        for months, name in RAIFFEISEN_DURATION_MAP.items()
        if months in rates_by_months
    }
    return _raiffeisen_rates_cache


def _make_raiffeisen_rate_fetcher(security_name):
    def _fetch():
        rates = _get_raiffeisen_rates()
        if security_name not in rates:
            raise ValueError(f"'{security_name}' nicht in Raiffeisen-API-Antwort enthalten")
        return rates[security_name], None
    return _fetch


# ---------------------------------------------------------------------------
# Quelle 3: onvista (STOXX Europe 600 EUR NR)
# ---------------------------------------------------------------------------

ONVISTA_API_URL = "https://api.onvista.de/api/v1/instruments/INDEX/1544657/quote?idNotation=&range=D1"


def fetch_onvista_stoxx():
    req = urllib.request.Request(ONVISTA_API_URL, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=15) as r:
        d = json.loads(r.read())
    price = float(d.get("last") or d.get("previousLast"))
    return price, "EUR"


# ---------------------------------------------------------------------------
# Quelle 4: Raiffeisen Futura II Fonds (boerse.raiffeisen.ch, HTML-Scraping)
# ---------------------------------------------------------------------------

def _make_raiffeisen_futura_fetcher(fund_id):
    def _fetch():
        url = f"https://boerse.raiffeisen.ch/fonds/detail/{fund_id}?exchangeid=393"
        req = urllib.request.Request(url, headers=DEFAULT_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        text = strip_html(html)

        price_match = re.search(r"\bCHF\s*\n+\s*([\d']+[.,]\d+)", text)
        if not price_match:
            raise ValueError(
                f"Preis-Muster auf der Raiffeisen-Fondsseite (fund_id={fund_id}) "
                "nicht gefunden - Seitenlayout hat sich evtl. geändert."
            )
        price = float(price_match.group(1).replace("'", "").replace(",", "."))
        if price <= 0:
            raise ValueError(f"Unplausibler Preis für fund_id={fund_id}.")
        return price, "CHF"
    return _fetch


# ---------------------------------------------------------------------------
# Quelle 5: Raiffeisen Börse - Goldvreneli 20 Fr. (Ankaufspreis)
# ---------------------------------------------------------------------------

RAIFFEISEN_EDELMETALLE_URL = "https://boerse.raiffeisen.ch/edelmetalle"


def fetch_raiffeisen_vreneli_ankauf():
    req = urllib.request.Request(RAIFFEISEN_EDELMETALLE_URL, headers=DEFAULT_HEADERS)
    with urllib.request.urlopen(req, timeout=20) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    text = strip_html(html)

    match = re.search(
        r"20 Fr\.\s*Vreneli.*?([\d']+[.,]\d+)\s*\n+\s*([\d']+[.,]\d+)",
        text, re.DOTALL,
    )
    if not match:
        raise ValueError(
            "Preis-Muster für '20 Fr. Vreneli' auf boerse.raiffeisen.ch/edelmetalle "
            "nicht gefunden - Seitenlayout hat sich evtl. geändert."
        )
    ankauf = float(match.group(1).replace("'", "").replace(",", "."))
    if ankauf <= 0:
        raise ValueError("Unplausibler Ankaufspreis - Extraktion vermutlich fehlgeschlagen.")
    return ankauf, "CHF"


# ---------------------------------------------------------------------------
# Quelle 6: iShares NAV-Export
# ---------------------------------------------------------------------------

ISHARES_MONTHS = {
    "Jan.": 1, "Feb.": 2, "März": 3, "Apr.": 4, "Mai": 5, "Juni": 6,
    "Juli": 7, "Aug.": 8, "Sept.": 9, "Okt.": 10, "Nov.": 11, "Dez.": 12,
}


def _make_ishares_nav_fetcher(download_url):
    def _fetch():
        req = urllib.request.Request(download_url, headers=DEFAULT_HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read().decode("utf-8-sig", errors="replace")

        sheet_match = re.search(
            r'<ss:Worksheet ss:Name="([^"]*[Hh]istor[^"]*)">(.*?)</ss:Worksheet>',
            content, re.DOTALL,
        )
        if not sheet_match:
            names = re.findall(r'<ss:Worksheet ss:Name="([^"]+)"', content)
            raise ValueError(f"Kein Historie-Sheet gefunden. Vorhandene Sheets: {names}")

        sheet_name, sheet_body = sheet_match.group(1), sheet_match.group(2)
        rows = re.findall(r"<ss:Row>(.*?)</ss:Row>", sheet_body, re.DOTALL)

        def row_cells(row):
            return re.findall(r'<ss:Data ss:Type="(String|Number)">([^<]*)</ss:Data>', row)

        header_idx = None
        nav_col = None
        currency_col = None
        for i, row in enumerate(rows):
            cells = row_cells(row)
            nav_positions = [j for j, (t, v) in enumerate(cells) if t == "String" and "nav" in v.lower()]
            if nav_positions:
                header_idx = i
                nav_col = nav_positions[0]
                currency_positions = [j for j, (t, v) in enumerate(cells) if t == "String" and "curren" in v.lower() or v.strip().lower() in ("währung",)]
                currency_col = currency_positions[0] if currency_positions else None
                break

        data_row = None
        if header_idx is not None and header_idx + 1 < len(rows):
            candidate = rows[header_idx + 1]
            candidate_cells = row_cells(candidate)
            if nav_col < len(candidate_cells) and candidate_cells[nav_col][0] == "Number":
                data_row = candidate

        if data_row is None:
            preview = []
            for row in rows[:6]:
                preview.append([v for _t, v in row_cells(row)])
            raise ValueError(
                f"Keine gültige NAV-Datenzeile im Sheet '{sheet_name}' gefunden. "
                f"Erste Zeilen zur Diagnose: {preview}"
            )

        cells = row_cells(data_row)
        nav_str = cells[nav_col][1]
        currency = cells[currency_col][1].strip() if currency_col is not None and currency_col < len(cells) else None
        try:
            nav = float(nav_str)
        except ValueError:
            raise ValueError(f"NAV-Wert nicht numerisch: {nav_str!r}")
        return nav, currency
    return _fetch


# ---------------------------------------------------------------------------
# Registry: SecurityName -> (Fetch-Funktion, Source-Label)
# ---------------------------------------------------------------------------

FETCHERS = {
    "Rendite Bundesobligationen Eidgenossenschaft 10 Jahre (%)": (fetch_snb_r10, "SNB"),
    "STOXX Europe 600 EUR NR": (fetch_onvista_stoxx, "onvista"),
    "Raiffeisen Winterthur Hypothek 1 Jahr Zinssatz": (
        _make_raiffeisen_rate_fetcher("Raiffeisen Winterthur Hypothek 1 Jahr Zinssatz"), "RB Winterthur"),
    "Raiffeisen Winterthur Hypothek 5 Jahr Zinssatz": (
        _make_raiffeisen_rate_fetcher("Raiffeisen Winterthur Hypothek 5 Jahr Zinssatz"), "RB Winterthur"),
    "Raiffeisen Winterthur Hypothek 10 Jahr Zinssatz": (
        _make_raiffeisen_rate_fetcher("Raiffeisen Winterthur Hypothek 10 Jahr Zinssatz"), "RB Winterthur"),
    "Raiffeisen Winterthur Hypothek 15 Jahr Zinssatz": (
        _make_raiffeisen_rate_fetcher("Raiffeisen Winterthur Hypothek 15 Jahr Zinssatz"), "RB Winterthur"),
    "Raiffeisen Futura II - Systematic Invest Equity (Vorsorge)": (
        _make_raiffeisen_futura_fetcher("114426954"), "Raiffeisen Börse"),
    "Raiffeisen Futura II - Systematic Invest Equity B (Samantha)": (
        _make_raiffeisen_futura_fetcher("114426952"), "Raiffeisen Börse"),
    "Gold Vreneli (CHF 20)": (fetch_raiffeisen_vreneli_ankauf, "Raiffeisen Börse"),
}

FETCHERS_BY_ID = {
    # Falls künftig SecurityID-basierte Zuordnung nötig ist (z.B. bei
    # Umbenennungen in security_master), hier eintragen.
}


def main():
    client = get_client()
    active = fetch_active_securities(client)
    print(f"{len(active)} Securities mit Collector={COLLECTOR_ID} gefunden.")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    added, skipped, errors = 0, 0, 0

    for security_id, name in active:
        entry = FETCHERS_BY_ID.get(security_id) or FETCHERS.get(name)
        if not entry:
            print(f"[WARN] Keine Fetch-Funktion registriert für '{name}' "
                  f"(SecurityID {security_id}) - bitte in collect_other.py ergänzen.")
            errors += 1
            continue

        fetch_fn, source_label = entry
        try:
            price, _currency = fetch_fn()
            _, affected = client.execute(
                """INSERT OR IGNORE INTO security_prices
                   (SecurityID, Price, PriceDate, Source, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [security_id, round(price, 5), now_iso, source_label, now_iso],
            )
            if affected == 0:
                skipped += 1
                print(f"  SKIP   {name} (SecurityID={security_id}): {price}")
            else:
                added += 1
                print(f"  OK     {name} (SecurityID={security_id}): {price}")
        except Exception as e:
            errors += 1
            print(f"  FEHLER {name} (SecurityID={security_id}): {e}")

    count_after, _ = client.execute("SELECT COUNT(*) FROM security_prices")
    print(f"Fertig: {added} neu eingefügt, {skipped} übersprungen, {errors} Fehler.")
    print(f"Zeilen in security_prices insgesamt: {count_after[0][0]}")

    if added == 0 and errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
