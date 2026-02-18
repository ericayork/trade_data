"""
tariff_trade_pipeline.py
========================
Generates three Datawrapper-ready CSVs (rows = months, columns = years):
  1. monthly_customs_duties.csv         – customs duties revenue, in billions
  2. effective_tariff_rate.csv          – customs duties as % of BOP-basis goods imports
  3. goods_trade_balance_cumulative.csv – calendar-year cumulative goods trade balance

Data sources:
  - Customs duties: Treasury Fiscal Data API (MTS Table 4), pulled automatically
  - Goods imports/exports/balance: FT-900 Exhibit 12 (Not Seasonally Adjusted, BOP basis)
    → Download from: https://www.census.gov/foreign-trade/Press-Release/current_press_release/exh12.xlsx
    → Place in the same folder as this script (auto-detected by filename containing "exh12")

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

# Directory to search for FT-900 Exhibit 12 (default: same folder as this script)
FT900_DIR  = Path(__file__).parent

# Output directory
OUTPUT_DIR = Path(__file__).parent

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
COL_BALANCE_BOP = 1
COL_EXPORTS_BOP = 3
COL_IMPORTS_BOP = 6


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
            "classification_desc,current_month_gross_rcpt_amt"
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
    df["amount_mn"] = pd.to_numeric(df["current_month_gross_rcpt_amt"], errors="coerce")
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
    return out[["year", "month", "customs_duties_bn", "total_receipts_bn"]]


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
            "goods_exports_bop_mn":  parse_val(row.iloc[COL_EXPORTS_BOP]),
            "goods_imports_bop_mn":  parse_val(row.iloc[COL_IMPORTS_BOP]),
            "goods_balance_bop_mn":  parse_val(row.iloc[COL_BALANCE_BOP]),
        })

    df = pd.DataFrame(records).sort_values(["year", "month"]).reset_index(drop=True)
    print(f"    Extracted {len(df)} monthly rows ({df['year'].min()}–{df['year'].max()})")
    return df


# ── 3. Build Datawrapper-ready CSVs ───────────────────────────────────────────
#
# All three CSVs: rows = months (Jan–Dec), columns = years
#

def pivot_for_datawrapper(df, value_col):
    """Pivot long → wide: Month rows, year columns."""
    df = df.copy()
    df["Month"] = df["month"].map(MONTH_LABELS)
    pivot = df.pivot(index="Month", columns="year", values=value_col)
    pivot.columns = [str(c) for c in pivot.columns]
    pivot = pivot.reindex(MONTH_ORDER)
    pivot.index.name = "Month"
    return pivot


def build_outputs(mts: pd.DataFrame, trade: pd.DataFrame):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    trade = trade[trade["year"].between(START_YEAR, END_YEAR)].copy()

    # millions -> billions for trade data
    for col in ["goods_exports_bop_mn", "goods_imports_bop_mn", "goods_balance_bop_mn"]:
        trade[col.replace("_mn", "_bn")] = (trade[col] / 1_000).round(3)

    merged = (
        mts.merge(trade, on=["year", "month"], how="outer")
        .sort_values(["year", "month"])
        .reset_index(drop=True)
    )

    # ── CSV 1: Monthly customs duties (rows=months, cols=years)
    duties = merged.dropna(subset=["customs_duties_bn"])
    p1 = OUTPUT_DIR / "monthly_customs_duties.csv"
    pivot_for_datawrapper(duties, "customs_duties_bn").to_csv(p1)
    print(f"  → {p1.name}")

    # ── CSV 2: Effective tariff rate
    rate_df = merged.dropna(subset=["customs_duties_bn", "goods_imports_bop_bn"]).copy()
    rate_df["Effective Tariff Rate"] = (
        rate_df["customs_duties_bn"] / rate_df["goods_imports_bop_bn"] * 100
    ).round(4)
    p2 = OUTPUT_DIR / "effective_tariff_rate.csv"
    pivot_for_datawrapper(rate_df, "Effective Tariff Rate").to_csv(p2)
    print(f"  → {p2.name}")

    # ── CSV 3: Cumulative goods trade balance
    bal_df = merged.dropna(subset=["goods_balance_bop_bn"]).copy()
    bal_df["cumulative_balance_bn"] = (
        bal_df.groupby("year")["goods_balance_bop_bn"].cumsum().round(3)
    )
    p3 = OUTPUT_DIR / "goods_trade_balance_cumulative.csv"
    pivot_for_datawrapper(bal_df, "cumulative_balance_bn").to_csv(p3)
    print(f"  → {p3.name}")

    return p1, p2, p3


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("╔══ Tariff & Trade Data Pipeline ══╗")
    print(f"   Coverage : {START_YEAR}–{END_YEAR}")
    print(f"   Output   : {OUTPUT_DIR}/\n")

    print("[1/3] Fetching MTS customs duties...")
    mts = fetch_mts_receipts()

    print("\n[2/3] Parsing FT-900 Exhibit 12 (BOP-basis trade data)...")
    ft900_path = find_ft900_file(FT900_DIR)
    trade = parse_ft900_exhibit12(ft900_path)

    if mts.empty and trade.empty:
        print("\nERROR: Both data sources empty. Check network and config.")
        sys.exit(1)

    print("\n[3/3] Writing CSVs...")
    p1, p2, p3 = build_outputs(mts, trade)

    print("\n╚══ Done ══╝")

    # Quick preview
    for label, path in [
        ("monthly_customs_duties.csv",         p1),
        ("effective_tariff_rate.csv",           p2),
        ("goods_trade_balance_cumulative.csv",  p3),
    ]:
        print(f"\n── {label} ──")
        print(pd.read_csv(path).to_string())


if __name__ == "__main__":
    main()
