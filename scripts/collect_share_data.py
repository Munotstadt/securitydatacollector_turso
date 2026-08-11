#!/usr/bin/env python3
"""
collect_share_data.py

Collects fundamental data via yfinance's Ticker.info (market cap, P/E ratios,
dividend yield, beta, 52-week range, margins, etc.) for every Share in
security_master (Instrument = 19), and writes it into the generic
security_data table in Turso (munotstadtsecuritydb).

Same generic-lookup pattern as collect_fund_data.py:
  - Fundamental metric KEYS (marketCap, trailingPE, dividendYield, beta, ...)
    are a bounded vocabulary -> resolved to a security_parameter.ParameterID
    via FieldNameID (ParaTable='security_data', ParaField='Fundamental'),
    new ones auto-created on first sight.

Run schedule: weekly (fundamentals don't change intraday) -
see .github/workflows/collect_share_data.yml.

Env vars required (same secrets already used elsewhere in this repo):
  TURSO_DATABASE_URL
  TURSO_AUTH_TOKEN
"""

import os
import sys
import time
import traceback
from datetime import datetime, timezone

import requests
import yfinance as yf

TURSO_DATABASE_URL = os.environ["TURSO_DATABASE_URL"].rstrip("/")
if TURSO_DATABASE_URL.startswith("libsql://"):
    TURSO_DATABASE_URL = "https://" + TURSO_DATABASE_URL[len("libsql://"):]
elif TURSO_DATABASE_URL.startswith("wss://"):
    TURSO_DATABASE_URL = "https://" + TURSO_DATABASE_URL[len("wss://"):]
TURSO_AUTH_TOKEN = os.environ["TURSO_AUTH_TOKEN"]

SOURCE = "yfinance"
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
PARA_TABLE = "security_data"
DATA_TYPE = "Fundamental"
INSTRUMENT_SHARES_ID = 19  # security_parameter.ParameterID for Instrument='Shares'

# yfinance .info keys worth keeping - everything else in the dict is either
# redundant, a long text blob (business summary), or rarely useful for a
# tracking dashboard. Extend this list if more metrics are needed later.
FUNDAMENTAL_KEYS = [
    "marketCap", "enterpriseValue",
    "trailingPE", "forwardPE", "priceToBook", "priceToSalesTrailing12Months",
    "pegRatio", "enterpriseToRevenue", "enterpriseToEbitda",
    "dividendYield", "dividendRate", "payoutRatio", "fiveYearAvgDividendYield",
    "beta", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "fiftyDayAverage", "twoHundredDayAverage",
    "profitMargins", "operatingMargins", "grossMargins", "ebitdaMargins",
    "returnOnAssets", "returnOnEquity",
    "revenueGrowth", "earningsGrowth", "earningsQuarterlyGrowth",
    "totalRevenue", "totalDebt", "totalCash", "freeCashflow", "operatingCashflow",
    "debtToEquity", "currentRatio", "quickRatio",
    "trailingEps", "forwardEps", "bookValue",
    "sharesOutstanding", "floatShares", "heldPercentInsiders", "heldPercentInstitutions",
    "shortRatio", "shortPercentOfFloat",
    "targetMeanPrice", "targetHighPrice", "targetLowPrice", "recommendationMean",
    "numberOfAnalystOpinions",
]


class TursoClient:
    def __init__(self, database_url, token):
        self.base_url = f"{database_url}/v2/pipeline"
        self.token = token

    @staticmethod
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

    @staticmethod
    def _from_cell(cell):
        if cell is None or cell.get("type") == "null":
            return None
        if cell["type"] == "integer":
            return int(cell["value"])
        if cell["type"] == "float":
            return float(cell["value"])
        return cell["value"]

    def batch(self, statements):
        requests_payload = [
            {"type": "execute", "stmt": {"sql": sql, "args": [self._to_arg(a) for a in args]}}
            for sql, args in statements
        ]
        requests_payload.append({"type": "close"})

        resp = requests.post(
            self.base_url,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            json={"requests": requests_payload},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        results = []
        for r in data.get("results", []):
            if r.get("type") == "error":
                raise RuntimeError(f"Turso error: {r.get('error')}")
            resp_body = r.get("response")
            if not resp_body or resp_body.get("type") != "execute":
                continue
            result = resp_body["result"]
            cols = [c["name"] for c in result.get("cols", [])]
            rows = [
                {cols[i]: self._from_cell(cell) for i, cell in enumerate(row)}
                for row in result.get("rows", [])
            ]
            results.append({
                "rows": rows,
                "last_insert_rowid": result.get("last_insert_rowid"),
                "affected": result.get("affected_row_count", 0),
            })
        return results

    def query(self, sql, args=()):
        return self.batch([(sql, args)])[0]["rows"]

    def execute(self, sql, args=()):
        result = self.batch([(sql, args)])[0]
        return result["last_insert_rowid"]

    def execute_many(self, statements, chunk_size=250):
        for i in range(0, len(statements), chunk_size):
            self.batch(statements[i: i + chunk_size])


_param_cache = {}


def resolve_parameter_id(db, para_field, parameter_name):
    key = (PARA_TABLE, para_field, parameter_name)
    if key in _param_cache:
        return _param_cache[key]

    rows = db.query(
        "SELECT ParameterID FROM security_parameter WHERE ParaTable=? AND ParaField=? AND ParameterName=?",
        (PARA_TABLE, para_field, parameter_name),
    )
    if rows:
        pid = rows[0]["ParameterID"]
    else:
        pid = db.execute(
            "INSERT INTO security_parameter (ParameterName, ParaTable, ParaField, created_at) VALUES (?,?,?,datetime('now'))",
            (parameter_name, PARA_TABLE, para_field),
        )
        print(f"    + new security_parameter: ParaField='{para_field}' ParameterName='{parameter_name}' -> ParameterID={pid}")

    _param_cache[key] = pid
    return pid


def facts_from_info(info):
    facts = []
    for key in FUNDAMENTAL_KEYS:
        val = info.get(key)
        if val is None:
            continue
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            facts.append({"field_name": key, "numeric_value": float(val), "field_value": None})
        else:
            facts.append({"field_name": key, "numeric_value": None, "field_value": str(val)})
    return facts


def facts_to_statements(db, security_id, facts):
    statements = []
    for f in facts:
        field_name_id = resolve_parameter_id(db, DATA_TYPE, f["field_name"])
        statements.append((
            "INSERT INTO security_data (SecurityID, DataType, FieldNameID, FieldName, FieldValue, NumericValue, Unit, AsOfDate, Source) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (security_id, DATA_TYPE, field_name_id, None, f["field_value"], f["numeric_value"], None, TODAY, SOURCE),
        ))
    return statements


def collect_for_security(db, ticker_symbol, security_id):
    t = yf.Ticker(ticker_symbol)
    info = t.info
    if not info:
        return []
    facts = facts_from_info(info)
    return facts_to_statements(db, security_id, facts)


def main():
    db = TursoClient(TURSO_DATABASE_URL, TURSO_AUTH_TOKEN)

    securities = db.query(
        """
        SELECT SecurityID, SecurityName, Ticker
        FROM security_master
        WHERE Instrument = ?
          AND Ticker IS NOT NULL AND Ticker != ''
        """,
        (INSTRUMENT_SHARES_ID,),
    )
    print(f"Found {len(securities)} Share securities with a Ticker.")

    ok, failed, skipped = 0, 0, 0
    for sec in securities:
        security_id, name, ticker = sec["SecurityID"], sec["SecurityName"], sec["Ticker"]
        try:
            statements = collect_for_security(db, ticker, security_id)
            if not statements:
                print(f"  SKIP  {name} ({ticker}) — no info returned")
                skipped += 1
                continue
            db.execute_many(statements)
            print(f"  OK    {name} ({ticker}) — {len(statements)} rows")
            ok += 1
        except Exception as e:
            print(f"  FAIL  {name} ({ticker}): {e}", file=sys.stderr)
            traceback.print_exc()
            failed += 1
        time.sleep(1.5)  # be polite to Yahoo's undocumented endpoints

    print(f"\nDone. ok={ok} failed={failed} skipped={skipped} total={len(securities)}")
    print(f"security_parameter entries created/resolved this run: {len(_param_cache)}")
    if failed and failed == len(securities):
        sys.exit(1)


if __name__ == "__main__":
    main()
