# trp_core.py
from __future__ import annotations

import calendar
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

# ----------------------------
# Config
# ----------------------------

FISCAL_QUARTERS = {
    "Q1": [4, 5, 6],      # Apr–Jun
    "Q2": [7, 8, 9],      # Jul–Sep
    "Q3": [10, 11, 12],   # Oct–Dec
    "Q4": [1, 2, 3],      # Jan–Mar
}

PROGRAM_RBW = "RBW"
PROGRAM_CARPOOL = "CARPOOL"
PROGRAM_RAD = "RAD"
PROGRAM_AFV = "AFV"

EXPECTED_OUTPUT_FILES = [
    "cleaned_master.csv",
    "lunch_report.csv",
    "winner_report.csv",
    "run_log.json",
]

# ----------------------------
# Helpers: reading files
# ----------------------------

def read_table(path: str) -> pd.DataFrame:
    """
    Reads CSV or Excel based on extension.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {path}")

    ext = p.suffix.lower()
    if ext in [".csv"]:
        return pd.read_csv(path)
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    # fallback: try csv
    return pd.read_csv(path)

# ----------------------------
# Helpers: cleaning
# ----------------------------

def _safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x)

def clean_email(email: str) -> str:
    email = _safe_str(email).strip().lower()
    email = re.sub(r"\s+", "", email)
    return email

def clean_badge(badge: str) -> str:
    badge = _safe_str(badge).strip().upper()
    badge = re.sub(r"[^A-Z0-9]", "", badge)
    return badge

def smart_title(name: str) -> str:
    """
    Title-case but keep apostrophes/hyphens reasonably intact.
    """
    name = _safe_str(name).strip()
    name = re.sub(r"\s+", " ", name)

    def _fix_token(tok: str) -> str:
        parts = re.split(r"([\'-])", tok)
        parts = [p.capitalize() if p not in ["'", "-"] else p for p in parts]
        return "".join(parts)

    tokens = name.split(" ")
    tokens = [_fix_token(t) for t in tokens if t]
    return " ".join(tokens)

def parse_datetime(series: pd.Series) -> pd.Series:
    """
    Robust datetime parsing. Coerces invalid to NaT.
    """
    return pd.to_datetime(series, errors="coerce")

# ----------------------------
# Loaders: map each file to standard columns
# ----------------------------

def load_rbw(path: str) -> pd.DataFrame:
    df = read_table(path)
    out = pd.DataFrame({
        "name_raw": df.get("Name"),
        "badge_raw": df.get("Badge ID Number"),
        "email_raw": df.get("Email"),
        "created_raw": df.get("Created"),
    })
    out["program"] = PROGRAM_RBW
    out["created_date"] = parse_datetime(out["created_raw"])
    return out

def load_carpool(path: str) -> pd.DataFrame:
    df = read_table(path)
    out = pd.DataFrame({
        "name_raw": df.get("Name"),
        "badge_raw": df.get("Badge ID Number"),
        "email_raw": df.get("Email"),
        "created_raw": df.get("Created"),
    })
    out["program"] = PROGRAM_CARPOOL
    out["created_date"] = parse_datetime(out["created_raw"])
    return out

def load_rad(path: str) -> pd.DataFrame:
    df = read_table(path)
    out = pd.DataFrame({
        "name_raw": df.get("Name"),
        "badge_raw": df.get("Badge ID Number"),
        "email_raw": df.get("Microchip Email"),
        "created_raw": df.get("Refuel Date and Time"),
    })
    out["program"] = PROGRAM_RAD
    out["created_date"] = parse_datetime(out["created_raw"])
    return out

def load_afv(path: str) -> pd.DataFrame:
    df = read_table(path)
    out = pd.DataFrame({
        "name_raw": df.get("Name"),
        "badge_raw": df.get("Badge #"),
        "email_raw": df.get("Email"),
        "created_raw": pd.Series([None] * len(df)),
    })
    out["program"] = PROGRAM_AFV
    out["created_date"] = pd.NaT
    return out

def fiscal_quarter_label(dt: Optional[pd.Timestamp]) -> Optional[str]:
    if pd.isna(dt):
        return None
    m = int(dt.month)
    y = int(dt.year)

    if m in FISCAL_QUARTERS["Q1"]:
        return f"Q1-{y}"
    if m in FISCAL_QUARTERS["Q2"]:
        return f"Q2-{y}"
    if m in FISCAL_QUARTERS["Q3"]:
        return f"Q3-{y}"
    if m in FISCAL_QUARTERS["Q4"]:
        return f"Q4-{y}"
    return None

def standardize(master: pd.DataFrame) -> pd.DataFrame:
    master = master.copy()
    master["name"] = master["name_raw"].apply(smart_title)
    master["badge_id"] = master["badge_raw"].apply(clean_badge)
    master["email"] = master["email_raw"].apply(clean_email)

    master["has_badge"] = master["badge_id"].astype(str).str.len() > 0
    master["has_email"] = master["email"].astype(str).str.contains("@", na=False)

    master["year"] = master["created_date"].dt.year
    master["month"] = master["created_date"].dt.month
    master["ym"] = master["created_date"].dt.to_period("M").astype(str)

    master["fiscal_q"] = master.apply(lambda r: fiscal_quarter_label(r["created_date"]), axis=1)

    cols = [
        "program",
        "name", "badge_id", "email",
        "created_date", "year", "month", "ym", "fiscal_q",
        "has_badge", "has_email",
        "name_raw", "badge_raw", "email_raw", "created_raw",
    ]
    return master[cols]

# ----------------------------
# Reporting period logic
# ----------------------------

def prev_year_month(y: int, m: int) -> Tuple[int, int]:
    return (y - 1, 12) if m == 1 else (y, m - 1)

def last_business_day(y: int, m: int) -> date:
    last_day = calendar.monthrange(y, m)[1]
    d = date(y, m, last_day)
    while d.weekday() >= 5:  # Sat/Sun
        d = date.fromordinal(d.toordinal() - 1)
    return d

def reporting_month_for_run(run_dt: datetime, cutoff_day: int = 15) -> Tuple[int, int]:
    """
    Monthly reporting period selection:
    - If run day <= cutoff_day: previous month
    - Else: current month ONLY if run day is last business day of month; otherwise previous month
    """
    y, m, d = run_dt.year, run_dt.month, run_dt.day

    if d <= cutoff_day:
        return prev_year_month(y, m)

    if run_dt.date() == last_business_day(y, m):
        return (y, m)

    return prev_year_month(y, m)

# ----------------------------
# Filtering helpers
# ----------------------------

def quarter_months(q: str) -> List[int]:
    q = q.upper()
    if q not in FISCAL_QUARTERS:
        raise ValueError(f"Quarter must be one of {list(FISCAL_QUARTERS.keys())}")
    return FISCAL_QUARTERS[q]

def filter_month(master: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    df = master.copy()
    df = df[df["year"].notna() & df["month"].notna()]
    return df[(df["year"].astype(int) == int(year)) & (df["month"].astype(int) == int(month))]

def filter_quarter(master: pd.DataFrame, q: str, year: int) -> pd.DataFrame:
    months = quarter_months(q)
    df = master.copy()
    df = df[df["year"].notna() & df["month"].notna()]
    df = df[(df["year"].astype(int) == int(year)) & (df["month"].astype(int).isin(months))]
    return df

def most_recent_month_from_rbw_carpool(master: pd.DataFrame) -> Tuple[int, int]:
    df = master[master["program"].isin([PROGRAM_RBW, PROGRAM_CARPOOL])].copy()
    df = df.dropna(subset=["created_date"])
    if df.empty:
        raise ValueError("No dated records found in RBW/CARPOOL to determine most recent month.")
    max_dt = df["created_date"].max()
    return int(max_dt.year), int(max_dt.month)

def most_recent_month_in_quarter(master: pd.DataFrame, q: str, year: int) -> Tuple[int, int]:
    df = filter_quarter(master[master["program"].isin([PROGRAM_RBW, PROGRAM_CARPOOL])], q, year)
    df = df.dropna(subset=["created_date"])
    if df.empty:
        raise ValueError(f"No RBW/CARPOOL activity found in {q} {year}.")
    max_dt = df["created_date"].max()
    return int(max_dt.year), int(max_dt.month)

# ----------------------------
# Rewards: lunches
# ----------------------------

def lunches_from_trips(n: int) -> int:
    if n < 5:
        return 0
    if 5 <= n <= 8:
        return 1
    if 9 <= n <= 12:
        return 2
    if 13 <= n <= 15:
        return 3
    if 16 <= n <= 19:
        return 4
    return 5  # 20–31 => 5 (cap)

def calculate_lunch_report(month_df: pd.DataFrame) -> pd.DataFrame:
    df = month_df[month_df["program"].isin([PROGRAM_RBW, PROGRAM_CARPOOL])].copy()
    df = df[df["badge_id"].astype(str).str.len() > 0]

    trip_counts = (
        df.groupby(["badge_id"], as_index=False)
          .agg(trips=("badge_id", "size"),
               name=("name", "first"),
               email=("email", "first"))
    )

    trip_counts["lunches"] = trip_counts["trips"].apply(lunches_from_trips)
    trip_counts = trip_counts.sort_values(["lunches", "trips"], ascending=[False, False]).reset_index(drop=True)
    return trip_counts[["name", "badge_id", "email", "trips", "lunches"]]

# ----------------------------
# Drawings: winners
# ----------------------------

def unique_pool(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["key"] = x["badge_id"]
    x.loc[x["key"].astype(str).str.len() == 0, "key"] = x["email"]
    x = x[x["key"].astype(str).str.len() > 0]
    x = x.drop_duplicates(subset=["key"]).reset_index(drop=True)
    return x

def draw_winners(pool_df: pd.DataFrame, n: int, seed: int) -> pd.DataFrame:
    if len(pool_df) == 0 or n <= 0:
        return pool_df.iloc[0:0].copy()
    n = min(n, len(pool_df))
    return pool_df.sample(n=n, replace=False, random_state=seed).copy()

def run_monthly_drawing(month_df: pd.DataFrame, exclude_keys: set, seed: int) -> Tuple[pd.DataFrame, set]:
    participants = month_df[month_df["program"].isin([PROGRAM_RBW, PROGRAM_CARPOOL])].copy()
    pool = unique_pool(participants)
    pool = pool[~pool["key"].isin(exclude_keys)].reset_index(drop=True)

    winners = draw_winners(pool, n=8, seed=seed)
    winners["program_award"] = "MONTHLY_RBW_CARPOOL"

    new_exclude = set(winners["key"].tolist())
    return winners[["program_award", "name", "badge_id", "email", "key"]], exclude_keys.union(new_exclude)

def run_rad_drawing(quarter_df: pd.DataFrame, exclude_keys: set, seed: int) -> Tuple[pd.DataFrame, set]:
    rad = quarter_df[quarter_df["program"] == PROGRAM_RAD].copy()
    pool = unique_pool(rad)
    pool = pool[~pool["key"].isin(exclude_keys)].reset_index(drop=True)

    winners = draw_winners(pool, n=4, seed=seed)
    winners["program_award"] = "QUARTERLY_RAD"

    new_exclude = set(winners["key"].tolist())
    return winners[["program_award", "name", "badge_id", "email", "key"]], exclude_keys.union(new_exclude)

def run_afv_drawing(master: pd.DataFrame, exclude_keys: set, seed: int) -> Tuple[pd.DataFrame, set]:
    afv = master[master["program"] == PROGRAM_AFV].copy()
    pool = unique_pool(afv)
    pool = pool[~pool["key"].isin(exclude_keys)].reset_index(drop=True)

    winners = draw_winners(pool, n=2, seed=seed)
    winners["program_award"] = "QUARTERLY_AFV"

    new_exclude = set(winners["key"].tolist())
    return winners[["program_award", "name", "badge_id", "email", "key"]], exclude_keys.union(new_exclude)

# ----------------------------
# Seeds / Audit
# ----------------------------

def build_run_seed(run_type: str, year: int, month: Optional[int], quarter: Optional[str]) -> int:
    """
    Deterministic seed stable across machines/sessions (unlike Python's hash()).
    """
    base = f"{run_type}|{year}|{month or ''}|{quarter or ''}"
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)  # 32-bit-ish

# ----------------------------
# Output writing
# ----------------------------

def ensure_outputs_dir(outdir: str) -> None:
    os.makedirs(outdir, exist_ok=True)

def write_outputs(outdir: str, cleaned_master: pd.DataFrame, lunch_report: pd.DataFrame, winners_report: pd.DataFrame, run_log: Dict) -> None:
    ensure_outputs_dir(outdir)

    cleaned_master.to_csv(os.path.join(outdir, "cleaned_master.csv"), index=False)
    lunch_report.to_csv(os.path.join(outdir, "lunch_report.csv"), index=False)
    winners_report.to_csv(os.path.join(outdir, "winner_report.csv"), index=False)

    with open(os.path.join(outdir, "run_log.json"), "w", encoding="utf-8") as f:
        json.dump(run_log, f, indent=2, default=str)

def assert_expected_outputs(outdir: str) -> None:
    missing = []
    for fname in EXPECTED_OUTPUT_FILES:
        p = os.path.join(outdir, fname)
        if not os.path.exists(p):
            missing.append(p)
    if missing:
        raise FileNotFoundError("Expected output files missing:\n" + "\n".join(missing))