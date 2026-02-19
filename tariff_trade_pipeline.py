"""
tariff_trade_pipeline.py
========================
Generates four CSVs and updates a historical annual tariff rate dataset:

  Datawrapper-ready (rows = months, columns = years):
    1. monthly_customs_duties.csv         – customs duties revenue, in billions
    2. effective_tariff_rate.csv          – customs duties as % of BOP-basis goods imports
    3. goods_trade_balance_cumulative.csv – calendar-year cumulative goods trade balance

  Underlying data (long format, one row per year-month):
    4. monthly_underlying_data.csv        – customs duties, goods imports, goods exports (billions)

  Historical dataset update:
    - Reads data-Wb0HH.csv (annual effective tariff rate series)
    - Recalculates 2025 as: sum(2025 duties) / sum(2025 imports) across all available months
    - Adds 2026 YTD as: sum(2026 duties) / sum(2026 imports) for months where both are present
    - Writes data-Wb0HH_updated.csv

Data sources:
  - Customs duties: Treasury Fiscal Data API (MTS Table 4), pulled automatically
  - Goods imports/exports (for tariff rate & underlying data):
      FT-900 Exhibit 12 (Not Seasonally Adjusted, BOP basis)
      → https://www.census.gov/foreign-trade/Press-Release/current_press_release/exh12.xlsx
      → Place in the same folder as this script (auto-detected by filename containing "exh12")
  - Goods trade balance (for trade balance chart):
      FT-900 Exhibit 1 (Seasonally Adjusted, BOP basis)
      → https://www.census.gov/foreign-trade/Press-Release/current_press_release/exh1.xlsx
      → Place in the same folder as this script (auto-detected by filename containing "exh1")
      → Falls back to Exhibit 12 NSA data if Exhibit 1 is not present

Usage:
  pip install requests pandas openpyxl
  python tariff_trade_pipeline.py
"""

import re
import sys
import requests
import pandas as pd
from datetime import date
from pathlib import Path


# ── Configuration ──────────────────────────────────────────────────────────────

START_YEAR = 2024
END_YEAR   = 2026

# Directory to search for FT-900 exhibit files (default: same folder as this script)
FT900_DIR  = Path(__file__).parent

# Output directory
OUTPUT_DIR = Path(__file__).parent

# Historical annual tariff rate dataset to update
HISTORICAL_FILE = Path(__file__).parent / "data-Wb0HH.csv"

# API base URLs
MTS_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

# MTS Table 4 classification labels — update if Treasury renames a line
CUSTOMS_LABEL        = "Customs Duties"
TOTAL_RECEIPTS_LABEL = "Total Receipts"

MONTH_NAMES = {
    "january":1, "february":2, "march":3, "april":4,
    "may":5, "june":6, "july":7, "august":8,
    "september":9, "october":10, "november":11, "december":12,
}
MONTH_LABELS = {v: k.capitalize()[:3] for k, v in MONTH_NAMES.items()}
MONTH_ORDER  = list(MONTH_LABELS.values())  # ['Jan','Feb',...,'Dec']

# Column indices in exh12.xlsx (0-based, confirmed from file inspection)
# Layout: Period | Balance(BOP, Census) | Exports(BOP, NetAdj, Census) | Imports(BOP, NetAdj, Census)
COL_EXH12_BALANCE_BOP = 1
COL_EXH12_EXPORTS_BOP = 3
COL_EXH12_IMPORTS_BOP = 6

# Column indices in exh1.xlsx (0-based, confirmed from FT-900 PDF)
# Layout: Period | Balance(Total, Goods/BOP, Services) | Exports(Total, Goods/BOP, NetAdj, Census)
#                | Imports(Total, Goods/BOP, NetAdj, Census)
COL_EXH1_BALANCE_BOP = 2
COL_EXH1_EXPORTS_BOP = 5
COL_EXH1_IMPORTS_BOP = 9


# ── 1. Treasury MTS API ────────────────────────────────────────────────────────

def fetch_mts_receipts() -> pd.DataFrame:
    """
    Pulls MTS Table 4 from the Fiscal Data API.
    Returns DataFrame with columns: year, month, customs_duties_bn, total_receipts_bn
    """
    print("Fetching MTS receipts from Fiscal Data API...")

    params = {
        "fields": (
            "record_calendar_year,record_calendar_month,"
            "classification_desc,current_month_net_rcpt_amt"
        ),
        "filter": (
            f"record_calendar_year:gte:{START_YEAR},"
            f"record_calendar_year:lte:{END_YEAR}"
        ),
        "sort":      "record_calendar_year,record_calendar_month",
        "page[size]": 10000,
        "format":    "json",
    }

    try:
        resp = requests.get(
            f"{MTS_BASE}/v1/accounting/mts/mts_table_4",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ERROR: {e}")
        return pd.DataFrame()

    data = resp.json().get("data", [])
    if not data:
        print("  WARNING: MTS API returned no rows.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["year"]      = df["record_calendar_year"].astype(int)
    df["month"]     = df["record_calendar_month"].astype(int)
    # current_month_net_rcpt_amt is in raw dollars (not millions despite API docs)
    # Net = gross customs duties minus refunds
    df["amount_dollars"] = pd.to_numeric(df["current_month_net_rcpt_amt"], errors="coerce")
    df["amount_mn"] = df["amount_dollars"] / 1_000_000  # convert to millions
    df["desc"]      = df["classification_desc"].str.strip()

    # Customs Duties row
    customs = (
        df[df["desc"] == CUSTOMS_LABEL][["year", "month", "amount_mn"]]
        .rename(columns={"amount_mn": "customs_duties_mn"})
    )

    # Total Receipts row
    totals = (
        df[df["desc"] == TOTAL_RECEIPTS_LABEL][["year", "month", "amount_mn"]]
        .rename(columns={"amount_mn": "total_receipts_mn"})
    )

    if customs.empty:
        available = sorted(df["desc"].unique().tolist())
        print(
            f"  WARNING: '{CUSTOMS_LABEL}' not found in MTS Table 4.\n"
            f"  Available labels: {available}\n"
            f"  → Update CUSTOMS_LABEL in config."
        )

    out = customs.merge(totals, on=["year", "month"], how="outer")
    out["customs_duties_bn"] = out["customs_duties_mn"] / 1_000
    out["total_receipts_bn"] = out["total_receipts_mn"] / 1_000

    print(f"  Got {len(out)} month-rows from MTS.")
    return out[["year", "month", "customs_duties_mn", "customs_duties_bn", "total_receipts_bn"]]


# ── 2. FT-900 Exhibit 12 Parser ────────────────────────────────────────────────

def find_ft900_file(directory: Path) -> Path:
    """Auto-detect exh12.xlsx in the given directory."""
    for pattern in ["*exh12*.xlsx", "*exh12*.xls", "*EXH12*.xlsx", "*Exh12*.xlsx"]:
        matches = list(directory.glob(pattern))
        if matches:
            chosen = max(matches, key=lambda p: p.stat().st_mtime)
            print(f"  Found: {chosen.name}")
            return chosen
    raise FileNotFoundError(
        f"Could not find FT-900 Exhibit 12 in: {directory}\n\n"
        f"Download from:\n"
        f"  https://www.census.gov/foreign-trade/Press-Release/current_press_release/exh12.xlsx\n"
        f"Save as 'exh12.xlsx' in: {directory}"
    )


def parse_ft900_exhibit12(filepath: Path) -> pd.DataFrame:
    """
    Parse FT-900 Exhibit 12 (Not Seasonally Adjusted U.S. Trade in Goods).
    Returns DataFrame: year, month, goods_exports_bop_mn, goods_imports_bop_mn,
                       goods_balance_bop_mn
    """
    print(f"  Reading: {filepath.name}")
    raw = pd.read_excel(filepath, sheet_name=0, header=None, dtype=str)

    def parse_val(s):
        if pd.isna(s) or str(s).strip() in ("", "-", "nan"):
            return None
        try:
            return float(str(s).strip().replace(",", ""))
        except ValueError:
            return None

    records = []
    current_year = None

    for i, row in raw.iterrows():
        period = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""

        if re.match(r"^\d{4}$", period):
            current_year = int(period)
            continue

        if current_year is None:
            continue

        # Skip annual/YTD summary rows
        if re.match(r"^jan", period.lower()) and "-" in period.lower():
            continue

        period_clean = re.sub(r"\s*\(R\)", "", period, flags=re.IGNORECASE).strip()
        month_num = MONTH_NAMES.get(period_clean.lower())
        if month_num is None:
            continue

        records.append({
            "year":                  current_year,
            "month":                 month_num,
            "goods_exports_bop_mn":  parse_val(row.iloc[COL_EXH12_EXPORTS_BOP]),
            "goods_imports_bop_mn":  parse_val(row.iloc[COL_EXH12_IMPORTS_BOP]),
            "goods_balance_bop_mn":  parse_val(row.iloc[COL_EXH12_BALANCE_BOP]),
        })

    df = pd.DataFrame(records).sort_values(["year", "month"]).reset_index(drop=True)
    print(f"    Extracted {len(df)} monthly rows ({df['year'].min()}–{df['year'].max()})")
    return df


# ── 2b. FT-900 Exhibit 1 Parser (Seasonally Adjusted, for trade balance chart) ──

def find_ft900_exh1(directory: Path) -> Path | None:
    """
    Auto-detect exh1.xlsx in the given directory.
    Returns None (rather than raising) if not found, since it is optional.
    """
    # Match exh1.xlsx but NOT exh10, exh11, exh12, etc.
    for pattern in ["exh1.xlsx", "exh1.xls", "EXH1.xlsx", "Exh1.xlsx",
                    "*_exh1.xlsx", "*-exh1.xlsx"]:
        matches = list(directory.glob(pattern))
        if matches:
            chosen = max(matches, key=lambda p: p.stat().st_mtime)
            print(f"  Found Exhibit 1: {chosen.name}")
            return chosen
    return None


def parse_ft900_exhibit1(filepath: Path) -> pd.DataFrame:
    """
    Parse FT-900 Exhibit 1 (Seasonally Adjusted U.S. International Trade in Goods and Services).
    Extracts the Goods (BOP basis) columns only.

    Column layout (0-based), confirmed from FT-900 PDF:
      0  Period
      1  Balance – Total
      2  Balance – Goods (BOP basis)   ← COL_EXH1_BALANCE_BOP
      3  Balance – Services
      4  Exports – Total
      5  Exports – Goods (BOP basis)   ← COL_EXH1_EXPORTS_BOP
      6  Exports – Net Adjustments
      7  Exports – Census Basis
      8  Imports – Total
      9  Imports – Goods (BOP basis)   ← COL_EXH1_IMPORTS_BOP
      10 Imports – Net Adjustments
      11 Imports – Census Basis

    Returns DataFrame: year, month, goods_exports_bop_sa_mn, goods_imports_bop_sa_mn,
                       goods_balance_bop_sa_mn
    """
    print(f"  Reading: {filepath.name}")
    raw = pd.read_excel(filepath, sheet_name=0, header=None, dtype=str)

    def parse_val(s):
        if pd.isna(s) or str(s).strip() in ("", "-", "nan"):
            return None
        try:
            return float(str(s).strip().replace(",", ""))
        except ValueError:
            return None

    records = []
    current_year = None

    for _, row in raw.iterrows():
        period = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""

        if re.match(r"^\d{4}$", period):
            current_year = int(period)
            continue

        if current_year is None:
            continue

        # Skip YTD summary rows (e.g. "Jan. - Dec.", "Jan. - Sep.")
        if re.match(r"^jan", period.lower()) and "-" in period.lower():
            continue

        # Skip "August data as published last month:" and similar footnote rows
        if "data as published" in period.lower():
            continue

        period_clean = re.sub(r"\s*\(R\)", "", period, flags=re.IGNORECASE).strip()
        # Handle abbreviated month names used in exh1 (e.g. "January" or "Jan.")
        period_clean = period_clean.rstrip(".")
        month_num = MONTH_NAMES.get(period_clean.lower())
        if month_num is None:
            continue

        # Guard: need enough columns
        if len(row) <= COL_EXH1_IMPORTS_BOP:
            continue

        records.append({
            "year":                     current_year,
            "month":                    month_num,
            "goods_exports_bop_sa_mn":  parse_val(row.iloc[COL_EXH1_EXPORTS_BOP]),
            "goods_imports_bop_sa_mn":  parse_val(row.iloc[COL_EXH1_IMPORTS_BOP]),
            "goods_balance_bop_sa_mn":  parse_val(row.iloc[COL_EXH1_BALANCE_BOP]),
        })

    df = pd.DataFrame(records).sort_values(["year", "month"]).reset_index(drop=True)
    if df.empty:
        print("    WARNING: No monthly rows extracted from Exhibit 1.")
    else:
        print(f"    Extracted {len(df)} monthly rows ({df['year'].min()}–{df['year'].max()})")
    return df


# ── 3. Build Datawrapper-ready CSVs + underlying data ─────────────────────────

def pivot_for_datawrapper(df, value_col):
    """Pivot long → wide: Month rows, year columns."""
    df = df.copy()
    df["Month"] = df["month"].map(MONTH_LABELS)
    pivot = df.pivot(index="Month", columns="year", values=value_col)
    pivot.columns = [str(c) for c in pivot.columns]
    pivot = pivot.reindex(MONTH_ORDER)
    pivot.index.name = "Month"
    return pivot


def build_outputs(mts: pd.DataFrame, trade: pd.DataFrame, trade_sa: pd.DataFrame | None = None):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    trade = trade[trade["year"].between(START_YEAR, END_YEAR)].copy()

    # millions -> billions for trade data
    for col in ["goods_exports_bop_mn", "goods_imports_bop_mn", "goods_balance_bop_mn"]:
        trade[col.replace("_mn", "_bn")] = (trade[col] / 1_000).round(3)

    if mts.empty:
        merged = trade.copy()
        merged["customs_duties_mn"] = None
        merged["customs_duties_bn"] = None
        merged["total_receipts_bn"] = None
    elif trade.empty:
        merged = mts.copy()
        for col in ["goods_exports_bop_bn", "goods_imports_bop_bn", "goods_balance_bop_bn"]:
            merged[col] = None
    else:
        merged = mts.merge(trade, on=["year", "month"], how="outer")

    merged = merged.sort_values(["year", "month"]).reset_index(drop=True)

    # ── CSV 1: Monthly customs duties (rows=months, cols=years)
    duties = merged.dropna(subset=["customs_duties_bn"])
    p1 = OUTPUT_DIR / "monthly_customs_duties.csv"
    pivot_for_datawrapper(duties, "customs_duties_bn").to_csv(p1)
    print(f"  → {p1.name}")

    # ── CSV 2: Effective tariff rate
    # customs_duties_mn (millions, from MTS) / goods_imports_bop_mn (millions, from exh12)
    # Units cancel — result is a pure ratio × 100 = percent
    rate_df = merged.dropna(subset=["customs_duties_mn", "goods_imports_bop_mn"]).copy()
    rate_df["effective_tariff_rate_pct"] = (
        rate_df["customs_duties_mn"] / rate_df["goods_imports_bop_mn"] * 100
    ).round(4)
    p2 = OUTPUT_DIR / "effective_tariff_rate.csv"
    pivot_for_datawrapper(rate_df, "effective_tariff_rate_pct").to_csv(p2)
    print(f"  → {p2.name}")

    # ── CSV 3: Cumulative goods trade balance (SA if available, else NSA)
    if trade_sa is not None and not trade_sa.empty:
        trade_sa_filt = trade_sa[trade_sa["year"].between(START_YEAR, END_YEAR)].copy()
        trade_sa_filt["goods_balance_bop_sa_bn"] = (
            trade_sa_filt["goods_balance_bop_sa_mn"] / 1_000
        ).round(3)
        bal_src = trade_sa_filt[["year", "month", "goods_balance_bop_sa_bn"]].dropna(
            subset=["goods_balance_bop_sa_bn"]
        )
        balance_col = "goods_balance_bop_sa_bn"
        balance_label = "SA"
    else:
        bal_src = merged.dropna(subset=["goods_balance_bop_bn"])[
            ["year", "month", "goods_balance_bop_bn"]
        ].copy()
        balance_col = "goods_balance_bop_bn"
        balance_label = "NSA"

    bal_src["cumulative_balance_bn"] = (
        bal_src.groupby("year")[balance_col].cumsum().round(3)
    )
    p3 = OUTPUT_DIR / "goods_trade_balance_cumulative.csv"
    pivot_for_datawrapper(bal_src, "cumulative_balance_bn").to_csv(p3)
    print(f"  → {p3.name}  ({balance_label})")

    # ── CSV 4: Monthly underlying data (long format, one row per year-month)
    # Only rows where at least customs duties OR trade data is present.
    underlying = merged[
        merged[["customs_duties_bn", "goods_imports_bop_bn", "goods_exports_bop_bn"]]
        .notna().any(axis=1)
    ][["year", "month",
       "customs_duties_bn",
       "goods_imports_bop_bn",
       "goods_exports_bop_bn"]].copy()
    underlying["month_label"] = underlying.apply(
        lambda r: date(int(r["year"]), int(r["month"]), 1).strftime("%b %Y"), axis=1
    )
    underlying = underlying[["year", "month", "month_label",
                              "customs_duties_bn",
                              "goods_imports_bop_bn",
                              "goods_exports_bop_bn"]]
    p4 = OUTPUT_DIR / "monthly_underlying_data.csv"
    underlying.to_csv(p4, index=False, float_format="%.4f")
    print(f"  → {p4.name}")

    return p1, p2, p3, p4


# ── 4. Update historical annual tariff rate dataset ────────────────────────────

def update_historical(underlying_path: Path):
    """
    Reads monthly_underlying_data.csv and updates the historical annual
    effective tariff rate dataset (data-Wb0HH.csv).

    Annual rate = sum(customs_duties_bn) / sum(goods_imports_bop_bn) * 100
      - 2025: all months available in underlying data
      - 2026: only months where BOTH customs_duties_bn AND goods_imports_bop_bn are present
    """
    if not HISTORICAL_FILE.exists():
        print(f"  WARNING: Historical file not found ({HISTORICAL_FILE.name}) — skipping update.")
        return None

    historical = pd.read_csv(HISTORICAL_FILE)
    # Strip BOM and whitespace from column names
    historical.columns = historical.columns.str.strip().str.lstrip("\ufeff")
    value_col = historical.columns[1]

    underlying = pd.read_csv(underlying_path)

    results = {}

    for year in [2025, 2026]:
        yr_data = underlying[underlying["year"] == year].copy()

        if year == 2026:
            # Only months where both duties and imports are present
            yr_data = yr_data.dropna(subset=["customs_duties_bn", "goods_imports_bop_bn"])

        if yr_data.empty:
            print(f"  {year}: no complete data available — row not written.")
            continue

        total_duties  = yr_data["customs_duties_bn"].sum()
        total_imports = yr_data["goods_imports_bop_bn"].sum()

        if total_imports == 0:
            print(f"  {year}: imports sum to zero — skipping.")
            continue

        rate = round(total_duties / total_imports * 100, 2)
        n_months = len(yr_data)
        label = "full year" if year == 2025 else f"YTD {n_months}-month"
        print(f"  {year} ({label}): duties={total_duties:.3f}B  imports={total_imports:.3f}B  → rate={rate}%")
        results[year] = rate

    if not results:
        print("  No updates to write.")
        return None

    # Remove stale rows for years being updated, then append fresh values
    updated = historical[~historical["Year"].isin(results.keys())].copy()
    new_rows = pd.DataFrame({
        "Year":    list(results.keys()),
        value_col: list(results.values()),
    })
    updated = (
        pd.concat([updated, new_rows], ignore_index=True)
        .sort_values("Year")
        .reset_index(drop=True)
    )

    out_path = OUTPUT_DIR / "data-Wb0HH_updated.csv"
    updated.to_csv(out_path, index=False)
    print(f"  → {out_path.name}  ({len(updated)} rows, "
          f"{int(updated['Year'].min())}–{int(updated['Year'].max())})")

    print("\n  Recent rows:")
    print(updated.tail(5).to_string(index=False))

    return out_path


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("╔══ Tariff & Trade Data Pipeline ══╗")
    print(f"   Coverage : {START_YEAR}–{END_YEAR}")
    print(f"   Output   : {OUTPUT_DIR}/\n")

    print("[1/3] Fetching MTS customs duties...")
    mts = fetch_mts_receipts()

    print("\n[2/3] Parsing FT-900 trade data...")
    ft900_path = find_ft900_file(FT900_DIR)
    trade = parse_ft900_exhibit12(ft900_path)

    exh1_path = find_ft900_exh1(FT900_DIR)
    if exh1_path is not None:
        trade_sa = parse_ft900_exhibit1(exh1_path)
        print("  Trade balance chart will use Exhibit 1 (seasonally adjusted).")
    else:
        trade_sa = None
        print("  Exhibit 1 not found — trade balance chart will use Exhibit 12 (not seasonally adjusted).")
        print("  To use SA data, download exh1.xlsx from:")
        print("  https://www.census.gov/foreign-trade/Press-Release/current_press_release/exh1.xlsx")

    if mts.empty and trade.empty:
        print("\nERROR: Both data sources empty. Check network and config.")
        sys.exit(1)

    print("\n[3/3] Writing CSVs...")
    p1, p2, p3, p4 = build_outputs(mts, trade, trade_sa)

    print("\n[4/4] Updating historical tariff rate dataset...")
    update_historical(p4)

    print("\n╚══ Done ══╝")

    print("\n── monthly_underlying_data.csv ──")
    print(pd.read_csv(p4).to_string(index=False))


if __name__ == "__main__":
    main()
