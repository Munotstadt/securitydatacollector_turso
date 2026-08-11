#!/usr/bin/env python3
"""
collect_fund_data.py

Collects comprehensive fund-level data via yfinance's Ticker.funds_data
(Top Holdings, Asset Classes, Sector Weightings, Bond Holdings/Ratings,
Fund Overview & Operations) for every ETF/Fund in security_master, and
writes it into the generic security_data table in Turso
(munotstadtsecuritydb).

Field-name handling follows the same "generic lookup" pattern as the rest
of the Munotstadt suite:
  - Bounded/categorical field names (sector names, asset classes, bond
    rating buckets, fund overview/operations metric keys) are resolved to
    a security_parameter.ParameterID via FieldNameID — new ones are
    auto-created on first sight (ParaTable='security_data', ParaField=DataType).
  - Unbounded field names (top holding / equity / bond holding issuer
    names) stay as free text in FieldName.

Run schedule: weekly (fund composition data doesn't change intraday) —
see .github/workflows/collect_fund_data.yml.

Env vars required (same secrets already used elsewhere in this repo):
  TURSO_DATABASE_URL   e.g. "https://munotstadtsecuritydb-munotstadt.aws-eu-west-1.turso.io"
  TURSO_AUTH_TOKEN     Database-scoped auth token
"""

import os
import sys
import time
import traceback
from datetime import datetime, timezone

import requests
import yfinance as yf

TURSO_DATABASE_URL = os.environ["TURSO_DATABASE_URL"].rstrip("/")
TURSO_AUTH_TOKEN = os.environ["TURSO_AUTH_TOKEN"]

SOURCE = "yfinance"
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
PARA_TABLE = "security_data"

# DataTypes whose FieldName is a bounded/reusable vocabulary -> parameter-linked (FieldNameID).
# Everything else (TopHolding, EquityHolding, BondHolding) stays free text (FieldName).
CATEGORICAL_DATA_TYPES = {"AssetClass", "SectorWeight", "BondRating", "FundOverview", "FundOperations", "Fundamental"}


# ---------------------------------------------------------------------------
# Turso: direct HTTP /v2/pipeline client using a pre-provisioned DB URL +
# auth token (same TURSO_DATABASE_URL / TURSO_AUTH_TOKEN secrets already
# configured in this repo). No Platform API / token-minting involved.
# (libsql-client has known bugs against Turso's HTTP API — use raw HTTP,
# consistent with the rest of the Munotstadt suite.)
# ---------------------------------------------------------------------------
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
        """
        statements: list of (sql, args) tuples.
        Returns list of result dicts: {"rows": [...], "last_insert_rowid": int|None, "affected": int}
        """
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
        """Single write statement; returns last_insert_rowid."""
        result = self.batch([(sql, args)])[0]
        return result["last_insert_rowid"]

    def execute_many(self, statements, chunk_size=250):
        """Write helper: splits into chunks to keep each HTTP call reasonably sized."""
        for i in range(0, len(statements), chunk_size):
            self.batch(statements[i : i + chunk_size])


# ---------------------------------------------------------------------------
# security_parameter resolve-or-create (with in-run cache)
# ---------------------------------------------------------------------------
_param_cache = {}  # (ParaTable, ParaField, ParameterName) -> ParameterID


def resolve_parameter_id(db, para_field, parameter_name):
    """Looks up (ParaTable=PARA_TABLE, ParaField=para_field, ParameterName=parameter_name)
    in security_parameter; creates it if missing. Cached per process run."""
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


# ---------------------------------------------------------------------------
# yfinance extraction -> intermediate "fact" dicts (resolved to SQL later)
# ---------------------------------------------------------------------------
def facts_from_dataframe(df, data_type, name_col_candidates, value_col_candidates, unit="%"):
    """Generic DataFrame -> list of fact dicts {field_name, numeric_value, unit}."""
    facts = []
    if df is None or df.empty:
        return facts

    df = df.reset_index()
    name_col = next((c for c in name_col_candidates if c in df.columns), df.columns[0])
    value_col = next((c for c in value_col_candidates if c in df.columns), None)

    for _, row in df.iterrows():
        field_name = str(row[name_col])
        numeric_value = None
        if value_col is not None:
            try:
                numeric_value = float(row[value_col])
                if unit == "%" and abs(numeric_value) <= 1.0:
                    numeric_value *= 100  # yfinance often returns fractions (0.084 -> 8.4%)
            except (TypeError, ValueError):
                numeric_value = None
        facts.append({
            "data_type": data_type, "field_name": field_name, "field_value": None,
            "numeric_value": numeric_value, "unit": unit if numeric_value is not None else None,
        })
    return facts


def facts_from_dict(d, data_type):
    """Generic dict -> list of fact dicts (used for fund_overview / fund_operations)."""
    facts = []
    if not d:
        return facts
    for key, val in d.items():
        if val is None:
            continue
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            facts.append({"data_type": data_type, "field_name": key, "field_value": None, "numeric_value": float(val), "unit": None})
        else:
            facts.append({"data_type": data_type, "field_name": key, "field_value": str(val), "numeric_value": None, "unit": None})
    return facts


def facts_to_statements(db, security_id, facts):
    """Resolves each fact to a final INSERT statement, using FieldNameID for
    categorical DataTypes (resolving/creating the security_parameter row) and
    plain FieldName text for unbounded ones (Holdings)."""
    statements = []
    for f in facts:
        if f["data_type"] in CATEGORICAL_DATA_TYPES:
            field_name_id = resolve_parameter_id(db, f["data_type"], f["field_name"])
            statements.append((
                "INSERT INTO security_data (SecurityID, DataType, FieldNameID, FieldName, FieldValue, NumericValue, Unit, AsOfDate, Source) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (security_id, f["data_type"], field_name_id, None, f["field_value"], f["numeric_value"], f["unit"], TODAY, SOURCE),
            ))
        else:
            statements.append((
                "INSERT INTO security_data (SecurityID, DataType, FieldNameID, FieldName, FieldValue, NumericValue, Unit, AsOfDate, Source) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (security_id, f["data_type"], None, f["field_name"], f["field_value"], f["numeric_value"], f["unit"], TODAY, SOURCE),
            ))
    return statements


def collect_for_security(db, ticker_symbol, security_id):
    """Returns a list of (sql, args) INSERT tuples for one security."""
    t = yf.Ticker(ticker_symbol)
    fd = t.funds_data
    if fd is None:
        return []

    facts = []

    # Top holdings (DataFrame: index=symbol/name, column like 'Holding Percent') — free text, unbounded
    facts += facts_from_dataframe(
        fd.top_holdings, "TopHolding",
        name_col_candidates=["Symbol", "Name", "index"],
        value_col_candidates=["Holding Percent", "holdingPercent"],
    )

    # Asset classes — bounded vocabulary
    asset_classes = fd.asset_classes
    if isinstance(asset_classes, dict):
        facts += facts_from_dict({k: (v * 100 if isinstance(v, (int, float)) else v) for k, v in asset_classes.items()}, "AssetClass")
    else:
        facts += facts_from_dataframe(asset_classes, "AssetClass", name_col_candidates=["index"], value_col_candidates=[0, "Value"])

    # Sector weightings — bounded vocabulary (GICS-ish sectors)
    sector_weightings = fd.sector_weightings
    if isinstance(sector_weightings, dict):
        facts += facts_from_dict({k: (v * 100 if isinstance(v, (int, float)) else v) for k, v in sector_weightings.items()}, "SectorWeight")
    else:
        facts += facts_from_dataframe(sector_weightings, "SectorWeight", name_col_candidates=["index"], value_col_candidates=[0, "Value"])

    # Bond ratings — bounded vocabulary; bond/equity holdings — free text, unbounded
    facts += facts_from_dataframe(fd.bond_ratings, "BondRating", name_col_candidates=["index", "Rating"], value_col_candidates=[0, "Value"])
    facts += facts_from_dataframe(fd.bond_holdings, "BondHolding", name_col_candidates=["index"], value_col_candidates=[0, "Value"])
    facts += facts_from_dataframe(fd.equity_holdings, "EquityHolding", name_col_candidates=["index"], value_col_candidates=[0, "Value"])

    # Fund overview + operations — metric KEYS are a bounded vocabulary (category, totalAssets, yield, ...)
    facts += facts_from_dict(fd.fund_overview, "FundOverview")
    facts += facts_from_dict(fd.fund_operations, "FundOperations")

    # Description as a single text row under a fixed 'description' key
    if fd.description:
        facts.append({"data_type": "FundOverview", "field_name": "description", "field_value": str(fd.description)[:2000], "numeric_value": None, "unit": None})

    return facts_to_statements(db, security_id, facts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    db = TursoClient(TURSO_DATABASE_URL, TURSO_AUTH_TOKEN)

    securities = db.query(
        """
        SELECT sm.SecurityID, sm.SecurityName, sm.Ticker
        FROM security_master sm
        JOIN security_parameter ip ON ip.ParameterID = sm.Instrument
        WHERE ip.ParaTable = 'security_master' AND ip.ParaField = 'Instrument'
          AND ip.ParameterName = 'ETF/Funds'
          AND sm.Ticker IS NOT NULL AND sm.Ticker != ''
        """
    )
    print(f"Found {len(securities)} ETF/Fund securities with a Ticker.")

    ok, failed, skipped = 0, 0, 0
    for sec in securities:
        security_id, name, ticker = sec["SecurityID"], sec["SecurityName"], sec["Ticker"]
        try:
            statements = collect_for_security(db, ticker, security_id)
            if not statements:
                print(f"  SKIP  {name} ({ticker}) — no funds_data returned")
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
        sys.exit(1)  # only hard-fail the job if literally everything failed


if __name__ == "__main__":
    main()
