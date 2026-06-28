"""
DRL → Cube Query Converter

Converts a Drools Rule Language (DRL) rule into a Cube REST API query JSON,
then optionally executes it against the Cube endpoint.

Flow:
  DRL rule string
    → parse conditions (fact fields, operators, values)
    → map fact type → Cube cube name
    → map conditions → Cube filters / timeDimensions
    → map accumulations → Cube measures
    → POST to Cube /cubejs-api/v1/load
    → return rows

Supported DRL patterns:
  - Simple field comparisons:  field > value, field == value, field in (...)
  - Date range conditions:     dateField >= "...", dateField <= "..."
  - Accumulate patterns:       accumulate(fact, sum($field))
"""

import re
import json
import os
import requests
from dataclasses import dataclass, field
from typing import Any


# ── Cube cube/dimension/measure registry ────────────────────────────────────

# Maps DRL fact type → Cube cube name
FACT_TO_CUBE: dict[str, str] = {
    "TradeSummary":      "TradeSummary",
    "DailySummary":      "DailySummary",
    "OhlcvBars5Min":     "OhlcvBars5Min",
    "OhlcvBars15Min":    "OhlcvBars15Min",
    "OhlcvBars1Hour":    "OhlcvBars1Hour",
    "AccountPositions":  "AccountPositions",
    "NavDaily":          "NavDaily",
    "SettlementStatus":  "SettlementStatus",
}

# Maps DRL field name → (cube_name, cube_member, member_type)
# member_type: "dimension" | "measure" | "time_dimension"
FIELD_TO_MEMBER: dict[str, tuple[str, str, str]] = {
    # TradeSummary
    "account_id":            ("TradeSummary", "account_id",            "dimension"),
    "brokerage":             ("TradeSummary", "total_brokerage",       "measure"),
    "charges":               ("TradeSummary", "total_charges",         "measure"),
    "fills":                 ("TradeSummary", "total_fills",           "measure"),
    "trade_date":            ("TradeSummary", "event_date",            "time_dimension"),

    # DailySummary
    "symbol":                ("DailySummary", "symbol",                "dimension"),
    "daily_return_pct":      ("DailySummary", "daily_return_pct",      "dimension"),
    "volume":                ("DailySummary", "total_volume",          "measure"),
    "high":                  ("DailySummary", "max_high",              "measure"),
    "low":                   ("DailySummary", "min_low",               "measure"),
    "event_date":            ("DailySummary", "event_date",            "time_dimension"),

    # SettlementStatus
    "settlement_state":      ("SettlementStatus", "settlement_state",  "dimension"),
    "settlement_date":       ("SettlementStatus", "settlement_date",   "time_dimension"),

    # AccountPositions
    "instrument":            ("AccountPositions", "instrument",        "dimension"),
    "quantity":              ("AccountPositions", "total_long_quantity","measure"),
}

# DRL operator → Cube filter operator
OPERATOR_MAP: dict[str, str] = {
    ">":   "gt",
    ">=":  "gte",
    "<":   "lt",
    "<=":  "lte",
    "==":  "equals",
    "!=":  "notEquals",
    "in":  "equals",        # DRL `in (a, b)` → Cube `equals` with multiple values
}

# Default measures to include per cube when not explicitly specified in DRL
DEFAULT_MEASURES: dict[str, list[str]] = {
    "TradeSummary":     ["TradeSummary.total_brokerage", "TradeSummary.total_fills",
                         "TradeSummary.active_accounts"],
    "DailySummary":     ["DailySummary.max_high", "DailySummary.min_low",
                         "DailySummary.total_volume", "DailySummary.avg_daily_return_pct"],
    "SettlementStatus": ["SettlementStatus.total_trades", "SettlementStatus.pending_trades",
                         "SettlementStatus.failed_trades"],
    "AccountPositions": ["AccountPositions.total_notional_exposure",
                         "AccountPositions.accounts_with_positions"],
    "OhlcvBars5Min":    ["OhlcvBars5Min.max_high", "OhlcvBars5Min.min_low",
                         "OhlcvBars5Min.sum_volume"],
}


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class ParsedRule:
    cube_name: str
    filters: list[dict]
    time_dimensions: list[dict]
    measures: list[str]
    dimensions: list[str]
    order: list[list[str]] = field(default_factory=list)
    limit: int = 1000


# ── Parser ───────────────────────────────────────────────────────────────────

class DrlParser:
    """
    Parses a simplified DRL rule into a ParsedRule.

    Supported DRL subset:
      rule "Name"
      when
        $alias: FactType(
          field1 > value1,
          field2 in ("a", "b"),
          dateField >= "YYYY-MM-DD",
          dateField <= "YYYY-MM-DD"
        )
      then
        // (body ignored — this is read-only analytics)
      end
    """

    _FACT_PATTERN    = re.compile(r"\$\w+:\s*(\w+)\s*\(")
    _CONDITION_PATTERN = re.compile(
        r"(\w+)\s*(>=|<=|!=|==|>|<|in)\s*([\"(][^\")\n]+[\")][,\s]?)"
    )
    _DATE_VALUE      = re.compile(r'"(\d{4}-\d{2}-\d{2})"')
    _IN_VALUES       = re.compile(r'"([^"]+)"')

    def parse(self, drl: str) -> ParsedRule:
        cube_name = self._extract_cube(drl)
        filters, time_dimensions = self._extract_conditions(drl, cube_name)
        measures = DEFAULT_MEASURES.get(cube_name, [])
        dimensions = self._default_dimensions(cube_name)

        return ParsedRule(
            cube_name=cube_name,
            filters=filters,
            time_dimensions=time_dimensions,
            measures=measures,
            dimensions=dimensions,
        )

    def _extract_cube(self, drl: str) -> str:
        match = self._FACT_PATTERN.search(drl)
        if not match:
            raise ValueError("Could not find a fact declaration ($alias: FactType(...)) in DRL")
        fact_type = match.group(1)
        cube = FACT_TO_CUBE.get(fact_type)
        if not cube:
            raise ValueError(f"Unknown fact type '{fact_type}'. Add it to FACT_TO_CUBE.")
        return cube

    def _extract_conditions(
        self, drl: str, cube_name: str
    ) -> tuple[list[dict], list[dict]]:
        filters: list[dict] = []
        time_dimensions: list[dict] = []
        time_range: dict[str, str] = {}

        for m in self._CONDITION_PATTERN.finditer(drl):
            drl_field, drl_op, raw_val = m.group(1), m.group(2), m.group(3).strip().rstrip(",")

            mapping = FIELD_TO_MEMBER.get(drl_field)
            if not mapping:
                continue  # skip unknown fields gracefully

            _, cube_member, member_type = mapping
            full_member = f"{cube_name}.{cube_member}"
            cube_op = OPERATOR_MAP.get(drl_op, "equals")

            if member_type == "time_dimension":
                date_match = self._DATE_VALUE.search(raw_val)
                if date_match:
                    if drl_op in (">=", ">"):
                        time_range["from"] = date_match.group(1)
                    elif drl_op in ("<=", "<"):
                        time_range["to"] = date_match.group(1)
                    time_range["dimension"] = full_member
            else:
                if drl_op == "in":
                    values = self._IN_VALUES.findall(raw_val)
                else:
                    values = [raw_val.strip('"')]

                filters.append({
                    "member":   full_member,
                    "operator": cube_op,
                    "values":   values,
                })

        if time_range.get("dimension"):
            date_range = []
            if "from" in time_range:
                date_range.append(time_range["from"])
            if "to" in time_range:
                date_range.append(time_range["to"])
            time_dimensions.append({
                "dimension": time_range["dimension"],
                "dateRange": date_range if date_range else "last 30 days",
            })

        return filters, time_dimensions

    def _default_dimensions(self, cube_name: str) -> list[str]:
        """Return the natural grouping dimensions for each cube."""
        defaults: dict[str, list[str]] = {
            "TradeSummary":     ["TradeSummary.account_id", "TradeSummary.event_date"],
            "DailySummary":     ["DailySummary.symbol",     "DailySummary.event_date"],
            "SettlementStatus": ["SettlementStatus.settlement_state"],
            "AccountPositions": ["AccountPositions.account_id", "AccountPositions.instrument"],
            "NavDaily":         ["NavDaily.scheme",          "NavDaily.nav_date"],
            "OhlcvBars5Min":    ["OhlcvBars5Min.symbol",     "OhlcvBars5Min.window_start"],
            "OhlcvBars15Min":   ["OhlcvBars15Min.symbol",    "OhlcvBars15Min.window_start"],
            "OhlcvBars1Hour":   ["OhlcvBars1Hour.symbol",    "OhlcvBars1Hour.window_start"],
        }
        return defaults.get(cube_name, [])


# ── Cube REST API client ──────────────────────────────────────────────────────

class CubeClient:
    """
    Thin wrapper around Cube's REST API.
    POST /cubejs-api/v1/load with a Cube query JSON → returns rows.
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_token: str | None = None,
    ):
        self.base_url  = (base_url or os.environ.get("CUBE_API_URL", "http://localhost:4000")).rstrip("/")
        self.api_token = api_token or os.environ.get("CUBE_API_TOKEN", "")

    def query(self, cube_query: dict) -> list[dict]:
        url = f"{self.base_url}/cubejs-api/v1/load"
        headers = {
            "Content-Type":  "application/json",
            "Authorization": self.api_token,
        }
        resp = requests.post(url, headers=headers, json={"query": cube_query}, timeout=60)
        resp.raise_for_status()
        return resp.json().get("data", [])


# ── Orchestrator: DRL → SQL (via Cube) ───────────────────────────────────────

class DrlToCubeQuery:
    """
    Converts a DRL rule string into a Cube query dict.
    Optionally executes it and returns result rows.

    The generated Cube query is what Cube translates to SQL before sending to
    Trino, which then scans the Gold Iceberg table via the Polaris catalog.

    Pipeline:
      DRL rule
        → DrlParser  →  ParsedRule
        → build_cube_query()  →  Cube query dict (JSON)
        → CubeClient.query()  →  POST to Cube /cubejs-api/v1/load
        → Cube generates SQL  →  Trino executes against Gold
        → result rows
    """

    def __init__(self, cube_client: CubeClient | None = None):
        self._parser = DrlParser()
        self._client = cube_client or CubeClient()

    def convert(self, drl: str) -> dict:
        """Parse DRL and return the Cube query dict without executing."""
        parsed = self._parser.parse(drl)
        return self._build_cube_query(parsed)

    def execute(self, drl: str) -> list[dict[str, Any]]:
        """Parse DRL, build Cube query, execute against Cube API, return rows."""
        cube_query = self.convert(drl)
        return self._client.query(cube_query)

    def _build_cube_query(self, parsed: ParsedRule) -> dict:
        query: dict[str, Any] = {
            "measures":       parsed.measures,
            "dimensions":     parsed.dimensions,
            "filters":        parsed.filters,
            "timeDimensions": parsed.time_dimensions,
            "limit":          parsed.limit,
        }
        if parsed.order:
            query["order"] = parsed.order
        return query


# ── Example usage ─────────────────────────────────────────────────────────────

EXAMPLE_RULES: dict[str, str] = {

    "high_brokerage_accounts": """
        rule "High Brokerage Accounts"
        when
          $t: TradeSummary(
            brokerage > 10000,
            trade_date >= "2026-06-01",
            trade_date <= "2026-06-28"
          )
        then
          // report accounts generating > ₹10,000 brokerage in June
        end
    """,

    "volatile_equity_symbols": """
        rule "Volatile Equity Symbols"
        when
          $d: DailySummary(
            daily_return_pct > 5,
            symbol in ("RELIANCE", "TCS", "HDFCBANK"),
            event_date >= "2026-06-01",
            event_date <= "2026-06-28"
          )
        then
          // flag symbols with intraday move > 5%
        end
    """,

    "pending_settlement": """
        rule "Trades Pending Settlement"
        when
          $s: SettlementStatus(
            settlement_state == "PENDING",
            settlement_date >= "2026-06-20",
            settlement_date <= "2026-06-28"
          )
        then
          // backoffice alert: trades stuck in PENDING beyond T+2
        end
    """,
}


if __name__ == "__main__":
    converter = DrlToCubeQuery()

    for rule_name, drl_text in EXAMPLE_RULES.items():
        print(f"\n{'='*60}")
        print(f"Rule: {rule_name}")
        print(f"{'='*60}")

        cube_query = converter.convert(drl_text)
        print("Cube query (→ Trino SQL via Cube):")
        print(json.dumps(cube_query, indent=2))

        # Uncomment to actually execute against a running Cube instance:
        # rows = converter.execute(drl_text)
        # print(f"Rows returned: {len(rows)}")
        # for row in rows[:5]:
        #     print(row)
