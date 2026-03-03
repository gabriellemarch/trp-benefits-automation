# trp_app.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

import trp_core as core


def run_trp(
    rbw_path: str,
    carpool_path: str,
    rad_path: str,
    afv_path: str,
    outdir: str,
    mode: str,                 # "monthly" or "quarterly"
    quarter: str | None = None,
    year: int | None = None,
    create_email_drafts: bool = False,
    status_cb=None,
) -> str:
    """
    Single entry point for GUI (and optional future CLI).

    Monthly:
      - reporting_month_for_run(today, cutoff_day=15)
      - if month has no RBW/CARPOOL rows, fallback to most_recent_month_from_rbw_carpool(data)

    Quarterly:
      - user supplies quarter + year
      - lunches + monthly winners use most recent month WITHIN that quarter (based on RBW/CARPOOL dates)
      - RAD uses the full quarter date range
      - AFV uses roster only
      - no one can win twice in a single run (cross-pool exclusions)
    """

    def status(msg: str) -> None:
        print(msg)
        if callable(status_cb):
            status_cb(msg)

    mode = (mode or "").strip().lower()
    if mode not in ("monthly", "quarterly"):
        raise ValueError("mode must be 'monthly' or 'quarterly'")

    status("Loading files...")
    rbw = core.load_rbw(rbw_path)
    carpool = core.load_carpool(carpool_path)
    rad = core.load_rad(rad_path)
    afv = core.load_afv(afv_path)

    status("Cleaning + merging datasets...")
    master_raw = pd.concat([rbw, carpool, rad, afv], ignore_index=True)
    master = core.standardize(master_raw)

    run_dt = datetime.now()

    run_log = {
        "run_timestamp": run_dt.isoformat(timespec="seconds"),
        "mode": mode.upper(),
        "input_files": {
            "rbw": rbw_path,
            "carpool": carpool_path,
            "rad": rad_path,
            "afv": afv_path,
        },
        "record_counts": master["program"].value_counts().to_dict(),
    }

    winners_all = []
    exclude_keys = set()

    report_year: int
    report_month: int

    # -------------------------
    # MONTHLY
    # -------------------------
    if mode == "monthly":
        status("Selecting reporting month...")
        y, m = core.reporting_month_for_run(run_dt, cutoff_day=15)
        run_log["reporting_month_rule"] = "day<=15 => prev month; else current only on last business day; else prev month"
        run_log["selected_period"] = {"year": y, "month": m}
        run_log["selected_period_reason"] = f"reporting_month_for_run({run_dt.date().isoformat()})"

        month_df = core.filter_month(master, y, m)

        # If selected month has no RBW/CARPOOL activity, fallback to most recent month in data
        rbw_cp = month_df[month_df["program"].isin([core.PROGRAM_RBW, core.PROGRAM_CARPOOL])].dropna(subset=["created_date"])
        if rbw_cp.empty:
            status("No RBW/CARPOOL activity found in selected reporting month; falling back to most recent month in data.")
            y, m = core.most_recent_month_from_rbw_carpool(master)
            month_df = core.filter_month(master, y, m)
            run_log["selected_period"] = {"year": y, "month": m}
            run_log["selected_period_reason"] = "fallback_to_most_recent_month_in_data"

        seed = core.build_run_seed("MONTHLY", y, m, None)
        run_log["random_seed"] = seed

        status(f"Calculating lunches for {y}-{m:02d}...")
        lunch_report = core.calculate_lunch_report(month_df)
        report_year, report_month = y, m

        status("Drawing monthly winners (8)...")
        monthly_winners, exclude_keys = core.run_monthly_drawing(month_df, exclude_keys, seed=seed)
        winners_all.append(monthly_winners)

    # -------------------------
    # QUARTERLY
    # -------------------------
    else:
        if not quarter or year is None:
            raise ValueError("Quarterly mode requires quarter and year.")

        q = quarter.strip().upper()
        y = int(year)

        status(f"Running quarterly logic for {q} {y}...")
        run_log["fiscal_quarter"] = q
        run_log["period_year"] = y

        # RBW/CARPOOL month used for lunches + monthly drawing:
        my, mm = core.most_recent_month_in_quarter(master, q, y)
        run_log["quarter_recent_month_used_for_lunches"] = {"year": my, "month": mm}

        month_df = core.filter_month(master, my, mm)
        quarter_df = core.filter_quarter(master, q, y)

        seed = core.build_run_seed("QUARTERLY", y, mm, q)
        run_log["random_seed"] = seed

        status(f"Calculating lunches for {my}-{mm:02d} (most recent month in quarter)...")
        lunch_report = core.calculate_lunch_report(month_df)
        report_year, report_month = my, mm

        status("Drawing winners (Monthly 8 + RAD 4 + AFV 2) with cross-pool exclusions...")
        monthly_winners, exclude_keys = core.run_monthly_drawing(month_df, exclude_keys, seed=seed + 1)
        winners_all.append(monthly_winners)

        rad_winners, exclude_keys = core.run_rad_drawing(quarter_df, exclude_keys, seed=seed + 2)
        winners_all.append(rad_winners)

        afv_winners, exclude_keys = core.run_afv_drawing(master, exclude_keys, seed=seed + 3)
        winners_all.append(afv_winners)

    winners = pd.concat(winners_all, ignore_index=True) if winners_all else pd.DataFrame(
        columns=["program_award", "name", "badge_id", "email", "key"]
    )
    winners_report = winners.drop(columns=["key"], errors="ignore")

    status("Writing outputs...")
    core.write_outputs(outdir, master, lunch_report, winners_report, run_log)
    
    lunch_checklist_path = core.write_lunch_checkoff_xlsx(
        outdir=outdir,
        lunch_report=lunch_report,
        period_year=report_year,
        period_month=report_month,
        site_name="Chandler",
        filename="lunch_checkoff.xlsx",
    )

    if create_email_drafts:
        status("Creating Outlook drafts...")
        draft_counts = core.create_outlook_drafts(
            lunch_report=lunch_report,
            winners_report=winners_report,
            lunch_checklist_path=lunch_checklist_path,
        )
        run_log["outlook_drafts"] = {
            "enabled": True,
            **draft_counts,
        }
        core.write_outputs(outdir, master, lunch_report, winners_report, run_log)

    # sanity check
    core.assert_expected_outputs(outdir)

    status("Done.")
    return outdir