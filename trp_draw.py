import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import tkinter as tk
from tkinter import filedialog, simpledialog, messagebox
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


# ----------------------------
# Helpers: cleaning
# ----------------------------

def _safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x)

def clean_email(email: str) -> str:
    email = _safe_str(email).strip().lower()
    # optional: collapse internal spaces just in case
    email = re.sub(r"\s+", "", email)
    return email

def clean_badge(badge: str) -> str:
    badge = _safe_str(badge).strip().upper()
    # remove spaces and non-alphanum (keeps IDs like C12933 clean)
    badge = re.sub(r"[^A-Z0-9]", "", badge)
    return badge

def smart_title(name: str) -> str:
    """
    Title-case but keep apostrophes/hyphens reasonably intact.
    """
    name = _safe_str(name).strip()
    name = re.sub(r"\s+", " ", name)

    def _fix_token(tok: str) -> str:
        # handle O'NEIL, SMITH-JONES
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
    df = pd.read_csv(path)
    # expected columns: Name, Badge ID Number, Email, Created, Mode of Transportation
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
    df = pd.read_csv(path)
    # expected columns: Name, Badge ID Number, Email, Created, Carpool Partner, Carpool Tag Number
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
    df = pd.read_csv(path)
    # expected columns: Name, Badge ID Number, Microchip Email, Refuel Date and Time, Receipt Image
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
    df = pd.read_csv(path)
    # expected columns include: Name, Badge #, Email, ... (no date)
    out = pd.DataFrame({
        "name_raw": df.get("Name"),
        "badge_raw": df.get("Badge #"),
        "email_raw": df.get("Email"),
        "created_raw": pd.Series([None] * len(df)),
    })
    out["program"] = PROGRAM_AFV
    out["created_date"] = pd.NaT
    return out


def standardize(master: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning rules and create canonical columns.
    """
    master = master.copy()
    master["name"] = master["name_raw"].apply(smart_title)
    master["badge_id"] = master["badge_raw"].apply(clean_badge)
    master["email"] = master["email_raw"].apply(clean_email)

    # basic validity flags (helpful for auditing)
    master["has_badge"] = master["badge_id"].astype(str).str.len() > 0
    master["has_email"] = master["email"].astype(str).str.contains("@", na=False)

    # derive time fields where created_date exists
    master["year"] = master["created_date"].dt.year
    master["month"] = master["created_date"].dt.month
    master["ym"] = master["created_date"].dt.to_period("M").astype(str)

    # fiscal quarter label
    master["fiscal_q"] = master.apply(lambda r: fiscal_quarter_label(r["created_date"]), axis=1)

    # keep just what we need + raw fields for audit
    cols = [
        "program",
        "name", "badge_id", "email",
        "created_date", "year", "month", "ym", "fiscal_q",
        "has_badge", "has_email",
        "name_raw", "badge_raw", "email_raw", "created_raw",
    ]
    return master[cols]


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
    # Q4 spans Jan–Mar, still belongs to fiscal year of that calendar year label (per your definition)
    if m in FISCAL_QUARTERS["Q4"]:
        return f"Q4-{y}"
    return None


# ----------------------------
# Run period logic
# ----------------------------

def most_recent_month_from_rbw_carpool(master: pd.DataFrame) -> Tuple[int, int]:
    """
    Return (year, month) for most recent activity date among RBW + CARPOOL.
    """
    df = master[master["program"].isin([PROGRAM_RBW, PROGRAM_CARPOOL])].copy()
    df = df.dropna(subset=["created_date"])
    if df.empty:
        raise ValueError("No dated records found in RBW/CARPOOL to determine most recent month.")
    max_dt = df["created_date"].max()
    return int(max_dt.year), int(max_dt.month)


def quarter_months(q: str) -> List[int]:
    q = q.upper()
    if q not in FISCAL_QUARTERS:
        raise ValueError(f"Quarter must be one of {list(FISCAL_QUARTERS.keys())}")
    return FISCAL_QUARTERS[q]


def filter_quarter(master: pd.DataFrame, q: str, year: int) -> pd.DataFrame:
    months = quarter_months(q)
    df = master.copy()
    df = df[df["year"].notna() & df["month"].notna()]
    df = df[(df["year"].astype(int) == int(year)) & (df["month"].astype(int).isin(months))]
    return df


def most_recent_month_in_quarter(master: pd.DataFrame, q: str, year: int) -> Tuple[int, int]:
    df = filter_quarter(master[master["program"].isin([PROGRAM_RBW, PROGRAM_CARPOOL])], q, year)
    df = df.dropna(subset=["created_date"])
    if df.empty:
        raise ValueError(f"No RBW/CARPOOL activity found in {q} {year}.")
    max_dt = df["created_date"].max()
    return int(max_dt.year), int(max_dt.month)


def filter_month(master: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    df = master.copy()
    df = df[df["year"].notna() & df["month"].notna()]
    return df[(df["year"].astype(int) == int(year)) & (df["month"].astype(int) == int(month))]


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
    # 20–31 => 5 (cap)
    return 5


def calculate_lunch_report(month_df: pd.DataFrame) -> pd.DataFrame:
    """
    month_df should already be filtered to a single month.
    Uses RBW + CARPOOL logs only.
    Trip count = rows per badge_id across both programs.
    """
    df = month_df[month_df["program"].isin([PROGRAM_RBW, PROGRAM_CARPOOL])].copy()
    # Keep only rows with a badge_id (badge is the primary key)
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
    """
    Make a unique person pool. Primary key = badge_id; fallback = email.
    """
    x = df.copy()
    x["key"] = x["badge_id"]
    x.loc[x["key"].astype(str).str.len() == 0, "key"] = x["email"]
    x = x[x["key"].astype(str).str.len() > 0]
    x = x.drop_duplicates(subset=["key"]).reset_index(drop=True)
    return x


def draw_winners_from_pool(pool_df: pd.DataFrame, n: int, rng: pd.core.groupby.generic.SeriesGroupBy = None, seed: int = 0) -> pd.DataFrame:
    """
    Randomly sample n rows from pool_df (without replacement).
    Deterministic with seed for audit.
    """
    if len(pool_df) == 0 or n <= 0:
        return pool_df.iloc[0:0].copy()

    n = min(n, len(pool_df))
    winners = pool_df.sample(n=n, replace=False, random_state=seed).copy()
    return winners


def run_monthly_drawing(month_df: pd.DataFrame, exclude_keys: set, seed: int) -> Tuple[pd.DataFrame, set]:
    participants = month_df[month_df["program"].isin([PROGRAM_RBW, PROGRAM_CARPOOL])].copy()
    pool = unique_pool(participants)

    pool["key"] = pool["badge_id"]
    pool.loc[pool["key"].astype(str).str.len() == 0, "key"] = pool["email"]
    pool = pool[~pool["key"].isin(exclude_keys)].reset_index(drop=True)

    winners = draw_winners_from_pool(pool, n=8, seed=seed)
    winners["program_award"] = "MONTHLY_RBW_CARPOOL"
    new_exclude = set(winners["key"].tolist())
    return winners[["program_award", "name", "badge_id", "email", "key"]], exclude_keys.union(new_exclude)


def run_rad_drawing(quarter_df: pd.DataFrame, exclude_keys: set, seed: int) -> Tuple[pd.DataFrame, set]:
    rad = quarter_df[quarter_df["program"] == PROGRAM_RAD].copy()
    pool = unique_pool(rad)
    pool["key"] = pool["badge_id"]
    pool.loc[pool["key"].astype(str).str.len() == 0, "key"] = pool["email"]
    pool = pool[~pool["key"].isin(exclude_keys)].reset_index(drop=True)

    winners = draw_winners_from_pool(pool, n=4, seed=seed)
    winners["program_award"] = "QUARTERLY_RAD"
    new_exclude = set(winners["key"].tolist())
    return winners[["program_award", "name", "badge_id", "email", "key"]], exclude_keys.union(new_exclude)


def run_afv_drawing(afv_master: pd.DataFrame, exclude_keys: set, seed: int) -> Tuple[pd.DataFrame, set]:
    afv = afv_master[afv_master["program"] == PROGRAM_AFV].copy()
    pool = unique_pool(afv)
    pool["key"] = pool["badge_id"]
    pool.loc[pool["key"].astype(str).str.len() == 0, "key"] = pool["email"]
    pool = pool[~pool["key"].isin(exclude_keys)].reset_index(drop=True)

    winners = draw_winners_from_pool(pool, n=2, seed=seed)
    winners["program_award"] = "QUARTERLY_AFV"
    new_exclude = set(winners["key"].tolist())
    return winners[["program_award", "name", "badge_id", "email", "key"]], exclude_keys.union(new_exclude)


# ----------------------------
# IO: outputs
# ----------------------------

def ensure_outputs_dir(outdir: str) -> None:
    os.makedirs(outdir, exist_ok=True)

def write_outputs(outdir: str, cleaned_master: pd.DataFrame, lunch_report: pd.DataFrame, winners: pd.DataFrame, run_log: Dict) -> None:
    ensure_outputs_dir(outdir)

    cleaned_master.to_csv(os.path.join(outdir, "cleaned_master.csv"), index=False)
    lunch_report.to_csv(os.path.join(outdir, "lunch_report.csv"), index=False)
    winners.to_csv(os.path.join(outdir, "winner_report.csv"), index=False)

    with open(os.path.join(outdir, "run_log.json"), "w", encoding="utf-8") as f:
        json.dump(run_log, f, indent=2, default=str)

# ======================================================
# MAIN EXECUTION FUNCTION (CALLED BY GUI)
# ======================================================
def run_drawings(month_df, master, quarterly=False, quarter_df=None, seed=0):
    """
    Wrapper that returns a winners dataframe with columns:
    program_award, name, badge_id, email
    """
    winners_all = []
    exclude = set()

    # --- Monthly RBW/Carpool (8 winners) ---
    participants = month_df[month_df["program"].isin(["RBW", "CARPOOL"])].copy()
    pool = unique_pool(participants)  # you should already have unique_pool()
    pool = pool[~pool["key"].isin(exclude)].reset_index(drop=True)

    w_m = draw(pool, 8, seed=seed + 1)  # you should already have draw()
    w_m["program_award"] = "MONTHLY_RBW_CARPOOL"
    exclude |= set(w_m["key"].tolist())
    winners_all.append(w_m[["program_award", "name", "badge_id", "email"]])

    if quarterly:
        if quarter_df is None:
            raise ValueError("quarter_df is required when quarterly=True")

        # --- RAD (4 winners from quarter activity) ---
        rad = quarter_df[quarter_df["program"] == "RAD"].copy()
        pool_r = unique_pool(rad)
        pool_r = pool_r[~pool_r["key"].isin(exclude)].reset_index(drop=True)

        w_r = draw(pool_r, 4, seed=seed + 2)
        w_r["program_award"] = "QUARTERLY_RAD"
        exclude |= set(w_r["key"].tolist())
        winners_all.append(w_r[["program_award", "name", "badge_id", "email"]])

        # --- AFV (2 winners from roster) ---
        afv = master[master["program"] == "AFV"].copy()
        pool_a = unique_pool(afv)
        pool_a = pool_a[~pool_a["key"].isin(exclude)].reset_index(drop=True)

        w_a = draw(pool_a, 2, seed=seed + 3)
        w_a["program_award"] = "QUARTERLY_AFV"
        winners_all.append(w_a[["program_award", "name", "badge_id", "email"]])

    import pandas as pd
    return pd.concat(winners_all, ignore_index=True) if winners_all else pd.DataFrame(
        columns=["program_award", "name", "badge_id", "email"]
    )

def run_trp(
    rbw_path,
    carpool_path,
    rad_path,
    afv_path,
    outdir,
    mode,
    quarter=None,
    year=None,
    status_cb=None
):
    """
    Main automation entry point.
    Called by trp_gui.py
    """

    def status(msg):
        print(msg)
        if callable(status_cb):
            status_cb(msg)

    status("Loading files...")

    rbw = load_rbw(rbw_path)
    carpool = load_carpool(carpool_path)
    rad = load_rad(rad_path)
    afv = load_afv(afv_path)

    status("Cleaning + merging datasets...")
    master = standardize(pd.concat([rbw, carpool, rad, afv]))

    # -------------------------
    # MONTHLY
    # -------------------------
    if mode == "monthly":
        status("Detecting most recent month...")
        year, month = most_recent_month_from_rbw_carpool(master)

        month_df = filter_month(master, year, month)

        status("Calculating lunches...")
        lunch_report = calculate_lunch_report(month_df)

        status("Drawing monthly winners...")
        # winners = run_drawings(month_df, master, quarterly=False)
        winners = run_drawings(...)
    # -------------------------
    # QUARTERLY
    # -------------------------
    else:
        status(f"Running quarterly logic: {quarter} {year}")

        my, mm = most_recent_month_in_quarter(master, quarter, year)

        month_df = filter_month(master, my, mm)
        quarter_df = filter_quarter(master, quarter, year)

        lunch_report = calculate_lunch_report(month_df)

        winners = run_drawings(
            month_df,
            master,
            quarterly=True,
            quarter_df=quarter_df
        )

    # -------------------------
    # OUTPUT
    # -------------------------
    status("Writing outputs...")
    write_outputs(outdir, master, lunch_report, winners)

    status("Run complete.")

    return outdir
# ----------------------------
# Main
# ----------------------------

def build_run_seed(run_type: str, year: int, month: Optional[int], q: Optional[str]) -> int:
    """
    Deterministic seed that changes by run period.
    """
    base = f"{run_type}|{year}|{month if month else ''}|{q if q else ''}"
    return abs(hash(base)) % (2**31 - 1)

def select_files_gui():
    """
    Opens file dialogs for selecting TRP input files.
    Returns dictionary of file paths.
    """

    root = tk.Tk()
    root.withdraw()  # hide empty window

    messagebox.showinfo(
        "TRP Automation",
        "Select the required program files."
    )

    files = {}

    files["rbw"] = filedialog.askopenfilename(
        title="Select RBW File",
        filetypes=[("CSV files", "*.csv")]
    )

    files["carpool"] = filedialog.askopenfilename(
        title="Select Carpool File",
        filetypes=[("CSV files", "*.csv")]
    )

    files["rad"] = filedialog.askopenfilename(
        title="Select RAD File",
        filetypes=[("CSV files", "*.csv")]
    )

    files["afv"] = filedialog.askopenfilename(
        title="Select AFV File",
        filetypes=[("CSV files", "*.csv")]
    )

    # validate selections
    for key, value in files.items():
        if not value:
            messagebox.showerror("Error", f"{key.upper()} file not selected.")
            raise SystemExit("File selection cancelled.")

    return files

def select_run_type_gui():
    root = tk.Tk()
    root.withdraw()

    choice = messagebox.askyesno(
        "Run Type",
        "Run MONTHLY?\n\nYes = Monthly\nNo = Quarterly"
    )

    if choice:
        return {"mode": "monthly"}

    # Quarterly prompts
    quarter = simpledialog.askstring(
        "Quarter",
        "Enter Fiscal Quarter (Q1, Q2, Q3, Q4):"
    )

    year = simpledialog.askinteger(
        "Year",
        "Enter Year (e.g., 2026):"
    )

    if not quarter or not year:
        raise SystemExit("Quarterly information missing.")

    return {
        "mode": "quarterly",
        "quarter": quarter.upper(),
        "year": year
    }

def main():
    # ---------- GUI INPUT ----------
    files = select_files_gui()
    run_config = select_run_type_gui()

    rbw_path = files["rbw"]
    carpool_path = files["carpool"]
    rad_path = files["rad"]
    afv_path = files["afv"]

    outdir = "outputs"  # or let user pick later if you want

    # ---------- LOAD ----------
    rbw = load_rbw(rbw_path)
    carpool = load_carpool(carpool_path)
    rad = load_rad(rad_path)
    afv = load_afv(afv_path)

    # ---------- STANDARDIZE + MERGE ----------
    master_raw = pd.concat([rbw, carpool, rad, afv], ignore_index=True)
    master = standardize(master_raw)

    run_log = {
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "input_files": {
            "rbw": rbw_path,
            "carpool": carpool_path,
            "rad": rad_path,
            "afv": afv_path,
        },
        "record_counts": master["program"].value_counts().to_dict(),
        "run_type": run_config["mode"].upper()
    }

    winners_all = []
    exclude_keys = set()

    # ---------- MONTHLY ----------
    if run_config["mode"] == "monthly":
        year, month = most_recent_month_from_rbw_carpool(master)

        run_log.update({
            "period_year": year,
            "period_month": month,
        })

        seed = build_run_seed("MONTHLY", year, month, None)
        run_log["random_seed"] = seed

        month_df = filter_month(master, year, month)

        lunch_report = calculate_lunch_report(month_df)

        monthly_winners, exclude_keys = run_monthly_drawing(month_df, exclude_keys, seed=seed)
        winners_all.append(monthly_winners)

    # ---------- QUARTERLY ----------
    else:
        q = run_config["quarter"]
        year = run_config["year"]

        run_log.update({
            "fiscal_quarter": q,
            "period_year": year,
        })

        # RBW/CARPOOL: use most recent month within quarter for lunches + monthly drawing
        my, mm = most_recent_month_in_quarter(master, q, year)
        run_log["quarter_recent_month_used_for_lunches"] = {"year": my, "month": mm}

        quarter_df = filter_quarter(master, q, year)

        seed = build_run_seed("QUARTERLY", year, mm, q)
        run_log["random_seed"] = seed

        month_df = filter_month(master, my, mm)
        lunch_report = calculate_lunch_report(month_df)

        # drawings (with winner exclusion across pools)
        monthly_winners, exclude_keys = run_monthly_drawing(month_df, exclude_keys, seed=seed + 1)
        winners_all.append(monthly_winners)

        rad_winners, exclude_keys = run_rad_drawing(quarter_df, exclude_keys, seed=seed + 2)
        winners_all.append(rad_winners)

        afv_winners, exclude_keys = run_afv_drawing(master, exclude_keys, seed=seed + 3)
        winners_all.append(afv_winners)

    # ---------- FINAL OUTPUTS ----------
    winners = pd.concat(winners_all, ignore_index=True) if winners_all else pd.DataFrame(
        columns=["program_award", "name", "badge_id", "email", "key"]
    )

    winners_report = winners.drop(columns=["key"], errors="ignore")

    write_outputs(outdir, master, lunch_report, winners_report, run_log)

    print(f"✅ Done. Outputs written to: {outdir}")
    print(" - cleaned_master.csv")
    print(" - lunch_report.csv")
    print(" - winner_report.csv")
    print(" - run_log.json")

def run_trp(
    rbw_path: str,
    carpool_path: str,
    rad_path: str,
    afv_path: str,
    outdir: str,
    mode: str,                 # "monthly" or "quarterly"
    quarter: str | None = None,
    year: int | None = None,
    status_cb=None,
) -> str:
    """
    Single entry point for BOTH GUI and CLI.
    GUI should call this.
    """

    def status(msg: str):
        print(msg)
        if callable(status_cb):
            status_cb(msg)

    status("Loading files...")
    rbw = load_rbw(rbw_path)
    carpool = load_carpool(carpool_path)
    rad = load_rad(rad_path)
    afv = load_afv(afv_path)

    status("Cleaning + merging datasets...")
    master_raw = pd.concat([rbw, carpool, rad, afv], ignore_index=True)
    master = standardize(master_raw)

    run_log = {
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "input_files": {
            "rbw": rbw_path,
            "carpool": carpool_path,
            "rad": rad_path,
            "afv": afv_path,
        },
        "record_counts": master["program"].value_counts().to_dict(),
        "run_type": mode.upper(),
    }

    winners_all = []
    exclude_keys = set()

    if mode == "monthly":
        status("Detecting most recent month...")
        y, m = most_recent_month_from_rbw_carpool(master)
        run_log.update({"period_year": y, "period_month": m})

        seed = build_run_seed("MONTHLY", y, m, None)
        run_log["random_seed"] = seed

        month_df = filter_month(master, y, m)

        status("Calculating lunches...")
        lunch_report = calculate_lunch_report(month_df)

        status("Drawing monthly winners...")
        monthly_winners, exclude_keys = run_monthly_drawing(month_df, exclude_keys, seed=seed)
        winners_all.append(monthly_winners)

    elif mode == "quarterly":
        if not quarter or not year:
            raise ValueError("Quarterly mode requires quarter and year.")
        q = quarter.upper()
        y = int(year)

        status(f"Running quarterly logic for {q} {y}...")
        run_log.update({"fiscal_quarter": q, "period_year": y})

        my, mm = most_recent_month_in_quarter(master, q, y)
        run_log["quarter_recent_month_used_for_lunches"] = {"year": my, "month": mm}

        quarter_df = filter_quarter(master, q, y)

        seed = build_run_seed("QUARTERLY", y, mm, q)
        run_log["random_seed"] = seed

        month_df = filter_month(master, my, mm)

        status("Calculating lunches...")
        lunch_report = calculate_lunch_report(month_df)

        status("Drawing winners (Monthly + RAD + AFV)...")
        monthly_winners, exclude_keys = run_monthly_drawing(month_df, exclude_keys, seed=seed + 1)
        winners_all.append(monthly_winners)

        rad_winners, exclude_keys = run_rad_drawing(quarter_df, exclude_keys, seed=seed + 2)
        winners_all.append(rad_winners)

        afv_winners, exclude_keys = run_afv_drawing(master, exclude_keys, seed=seed + 3)
        winners_all.append(afv_winners)

    else:
        raise ValueError("mode must be 'monthly' or 'quarterly'")

    winners = pd.concat(winners_all, ignore_index=True) if winners_all else pd.DataFrame(
        columns=["program_award", "name", "badge_id", "email", "key"]
    )
    winners_report = winners.drop(columns=["key"], errors="ignore")

    status("Writing outputs...")
    write_outputs(outdir, master, lunch_report, winners_report, run_log)

    status("Done.")
    return outdir

if __name__ == "__main__":
    main()
