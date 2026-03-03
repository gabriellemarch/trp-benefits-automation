"""
TRP Benefits Automation — Modern GUI (Windows-friendly)

Copy/paste this entire file as: trp_gui.py

✅ Clean architecture (recommended):
- trp_gui.py  -> GUI only
- trp_app.py  -> single backend entrypoint: run_trp(...)
- trp_core.py -> pure functions (cleaning, filtering, drawings, outputs)

This GUI expects trp_app.py to define:

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
        ...
        return outdir

Notes:
- Uses ttkbootstrap if installed (nicer look). If not installed, falls back to ttk.
- Runs backend in a thread so the UI stays responsive.
- Verifies expected output files exist after run; otherwise shows an error with details.

Run:
    python trp_gui.py
"""

from __future__ import annotations

import os
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# ------------------ Optional modern theming ------------------
try:
    import ttkbootstrap as tb
    from ttkbootstrap.constants import PRIMARY, SUCCESS, WARNING
    USING_TTKBOOTSTRAP = True
except Exception:
    USING_TTKBOOTSTRAP = False
    tb = None  # type: ignore

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# ------------------ Backend import ------------------
# IMPORTANT: GUI imports trp_app (not trp_draw) so you have ONE entry point.
try:
    import trp_app  # type: ignore
    BACKEND_OK = True
except Exception as _imp_err:
    trp_app = None  # type: ignore
    BACKEND_OK = False
    IMPORT_ERROR_TEXT = str(_imp_err)

FISCAL_QUARTERS = ["Q1", "Q2", "Q3", "Q4"]  # Q1: Apr–Jun, Q2: Jul–Sep, Q3: Oct–Dec, Q4: Jan–Mar


@dataclass
class RunConfig:
    rbw_path: str
    carpool_path: str
    rad_path: str
    afv_path: str
    outdir: str
    mode: str  # "monthly" | "quarterly"
    quarter: Optional[str] = None
    year: Optional[int] = None
    create_email_drafts: bool = False

def _basename(p: str) -> str:
    return os.path.basename(p) if p else ""


def _open_folder(path: str) -> None:
    """Open a folder cross-platform."""
    if not os.path.isdir(path):
        messagebox.showinfo("Open Outputs", "Output folder does not exist yet. Run the program first.")
        return

    try:
        if os.name == "nt":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            os.system(f'open "{path}"')
        else:
            os.system(f'xdg-open "{path}"')
    except Exception:
        messagebox.showinfo("Open Outputs", f"Outputs are located at:\n{path}")


class TRPApp:
    def __init__(self) -> None:
        # Root window
        if USING_TTKBOOTSTRAP:
            self.root = tb.Window(themename="flatly")  # modern theme
        else:
            self.root = tk.Tk()
            style = ttk.Style(self.root)
            try:
                style.theme_use("clam")
            except Exception:
                pass

        self.root.title("TRP Benefits Automation")
        self.root.geometry("1050x740")
        self.root.minsize(980, 700)
        self.root.resizable(True, True)

        # State
        self.rbw_path = tk.StringVar(value="")
        self.carpool_path = tk.StringVar(value="")
        self.rad_path = tk.StringVar(value="")
        self.afv_path = tk.StringVar(value="")

        self.outdir = tk.StringVar(value=str(Path.cwd() / "outputs"))

        self.mode = tk.StringVar(value="monthly")
        self.quarter = tk.StringVar(value="Q4")
        self.year = tk.StringVar(value=str(datetime.now().year))

        self.status = tk.StringVar(value="Select files to begin.")
        self.create_email_drafts = tk.BooleanVar(value=False)
        self.is_running = tk.BooleanVar(value=False)

        # Build UI
        self._build_layout()
        self._apply_mode_visibility()

    # ---------------- UI BUILD ----------------

    def _build_layout(self) -> None:
        pad = 14

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        container = ttk.Frame(self.root, padding=pad)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)

        # Header
        ttk.Label(container, text="TRP Benefits Automation Program", font=("Segoe UI", 18, "bold")).grid(
            row=0, column=0, sticky="w", pady=(0, 8)
        )
        ttk.Label(
            container,
            text="Select program files, choose Monthly or Quarterly, then Run to generate audit-ready outputs.",
        ).grid(row=1, column=0, sticky="w", pady=(0, 14))

        # Cards
        self._build_files_card(container, row=2)
        self._build_options_card(container, row=3)
        self._build_output_card(container, row=4)
        self._build_actions_row(container, row=5)
        self._build_status_card(container, row=6)

        # allow vertical expansion
        container.rowconfigure(7, weight=1)

    def _card(self, parent, title: str) -> ttk.LabelFrame:
        return ttk.LabelFrame(parent, text=title, padding=12)

    def _build_files_card(self, parent, row: int) -> None:
        card = self._card(parent, "Input Files")
        card.grid(row=row, column=0, sticky="ew", pady=(0, 14))
        card.columnconfigure(1, weight=1)

        ttk.Label(card, text="Program", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w", pady=(0, 8))
        ttk.Label(card, text="Selected file", font=("Segoe UI", 10, "bold")).grid(row=0, column=1, sticky="w", pady=(0, 8))

        self._file_row(card, 1, "RBW", self.rbw_path)
        self._file_row(card, 2, "Carpool", self.carpool_path)
        self._file_row(card, 3, "RAD", self.rad_path)
        self._file_row(card, 4, "AFV", self.afv_path)

    def _file_row(self, parent, r: int, label: str, var: tk.StringVar) -> None:
        ttk.Label(parent, text=label).grid(row=r, column=0, sticky="w", padx=(0, 10), pady=6)

        entry = ttk.Entry(parent, textvariable=var)
        entry.grid(row=r, column=1, sticky="ew", pady=6)

        if USING_TTKBOOTSTRAP:
            btn = tb.Button(parent, text="Browse…", bootstyle=PRIMARY, command=lambda: self._choose_file(label, var))  # type: ignore
        else:
            btn = ttk.Button(parent, text="Browse…", command=lambda: self._choose_file(label, var))
        btn.grid(row=r, column=2, sticky="e", padx=(10, 0), pady=6)

    def _build_options_card(self, parent, row: int) -> None:
        card = self._card(parent, "Run Options")
        card.grid(row=row, column=0, sticky="ew", pady=(0, 14))
        card.columnconfigure(0, weight=1)

        ttk.Label(card, text="Run type:", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w", pady=(0, 8))

        ttk.Radiobutton(
            card,
            text="Monthly (uses reporting-month rule; prevents early next-month entries from hijacking lunches)",
            variable=self.mode,
            value="monthly",
            command=self._apply_mode_visibility,
        ).grid(row=1, column=0, sticky="w", pady=(0, 6))

        ttk.Radiobutton(
            card,
            text="Quarterly (select fiscal quarter + year; RAD uses full quarter; AFV uses roster)",
            variable=self.mode,
            value="quarterly",
            command=self._apply_mode_visibility,
        ).grid(row=2, column=0, sticky="w")

        self.quarter_frame = ttk.Frame(card)
        self.quarter_frame.grid(row=3, column=0, sticky="w", pady=(12, 0))

        ttk.Label(self.quarter_frame, text="Quarter:").grid(row=0, column=0, sticky="w")
        self.quarter_cb = ttk.Combobox(
            self.quarter_frame,
            textvariable=self.quarter,
            values=FISCAL_QUARTERS,
            width=6,
            state="readonly",
        )
        self.quarter_cb.grid(row=0, column=1, sticky="w", padx=(8, 18))

        ttk.Label(self.quarter_frame, text="Year:").grid(row=0, column=2, sticky="w")
        self.year_entry = ttk.Entry(self.quarter_frame, textvariable=self.year, width=10)

        self.email_drafts_cb = ttk.Checkbutton(
            card,
            text="Create Outlook draft emails (lunch recipients + gift card winners)",
            variable=self.create_email_drafts,
        )
        self.email_drafts_cb.grid(row=4, column=0, sticky="w", pady=(12, 0))

        self.year_entry.grid(row=0, column=3, sticky="w", padx=(8, 0))

    def _build_output_card(self, parent, row: int) -> None:
        card = self._card(parent, "Output")
        card.grid(row=row, column=0, sticky="ew", pady=(0, 14))
        card.columnconfigure(1, weight=1)

        ttk.Label(card, text="Output folder:").grid(row=0, column=0, sticky="w")

        out_entry = ttk.Entry(card, textvariable=self.outdir)
        out_entry.grid(row=0, column=1, sticky="ew", padx=(10, 10))

        if USING_TTKBOOTSTRAP:
            btn = tb.Button(card, text="Browse…", bootstyle=PRIMARY, command=self._choose_outdir)  # type: ignore
        else:
            btn = ttk.Button(card, text="Browse…", command=self._choose_outdir)
        btn.grid(row=0, column=2, sticky="e")

    def _build_actions_row(self, parent, row: int) -> None:
        rowf = ttk.Frame(parent)
        rowf.grid(row=row, column=0, sticky="ew", pady=(0, 14))
        rowf.columnconfigure(0, weight=1)

        left = ttk.Frame(rowf)
        left.grid(row=0, column=0, sticky="w")

        right = ttk.Frame(rowf)
        right.grid(row=0, column=1, sticky="e")

        if USING_TTKBOOTSTRAP:
            self.run_btn = tb.Button(left, text="Run", bootstyle=SUCCESS, command=self._on_run_clicked)  # type: ignore
            self.clear_btn = tb.Button(left, text="Clear", bootstyle=WARNING, command=self._clear)  # type: ignore
            self.open_btn = tb.Button(right, text="Open Outputs", bootstyle=PRIMARY, command=self._open_outputs)  # type: ignore
        else:
            self.run_btn = ttk.Button(left, text="Run", command=self._on_run_clicked)
            self.clear_btn = ttk.Button(left, text="Clear", command=self._clear)
            self.open_btn = ttk.Button(right, text="Open Outputs", command=self._open_outputs)

        self.run_btn.grid(row=0, column=0, padx=(0, 10))
        self.clear_btn.grid(row=0, column=1)
        self.open_btn.grid(row=0, column=0)

    def _build_status_card(self, parent, row: int) -> None:
        card = self._card(parent, "Status")
        card.grid(row=row, column=0, sticky="ew")
        card.columnconfigure(0, weight=1)

        self.status_label = ttk.Label(card, textvariable=self.status, wraplength=980)
        self.status_label.grid(row=0, column=0, sticky="w")

        self.pbar = ttk.Progressbar(card, mode="indeterminate")
        self.pbar.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        self.pbar.grid_remove()

    # ---------------- UI EVENTS ----------------

    def _choose_file(self, program: str, var: tk.StringVar) -> None:
        path = filedialog.askopenfilename(
            title=f"Select {program} file",
            filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx;*.xls"), ("All files", "*.*")],
        )
        if path:
            var.set(path)
            self._set_status(f"Selected {program}: {_basename(path)}")

    def _choose_outdir(self) -> None:
        path = filedialog.askdirectory(title="Select output folder")
        if path:
            self.outdir.set(path)
            self._set_status(f"Output folder set to: {path}")

    def _apply_mode_visibility(self) -> None:
        if self.mode.get() == "quarterly":
            self.quarter_frame.grid()
        else:
            self.quarter_frame.grid_remove()

    def _clear(self) -> None:
        self.rbw_path.set("")
        self.carpool_path.set("")
        self.rad_path.set("")
        self.afv_path.set("")
        self._set_status("Cleared selections.")

    def _open_outputs(self) -> None:
        _open_folder(self.outdir.get().strip() or str(Path.cwd() / "outputs"))

    def _set_status(self, msg: str) -> None:
        self.status.set(msg)
        self.root.update_idletasks()

    def _set_running(self, running: bool) -> None:
        self.is_running.set(running)
        if running:
            self.run_btn.configure(state="disabled")
            self.clear_btn.configure(state="disabled")
            self.open_btn.configure(state="disabled")
            self.pbar.grid()
            self.pbar.start(12)
        else:
            self.pbar.stop()
            self.pbar.grid_remove()
            self.run_btn.configure(state="normal")
            self.clear_btn.configure(state="normal")
            self.open_btn.configure(state="normal")

    def _validate(self) -> Optional[RunConfig]:
        rbw = self.rbw_path.get().strip()
        carpool = self.carpool_path.get().strip()
        rad = self.rad_path.get().strip()
        afv = self.afv_path.get().strip()
        outdir = self.outdir.get().strip() or str(Path.cwd() / "outputs")

        missing = []
        if not rbw: missing.append("RBW")
        if not carpool: missing.append("Carpool")
        if not rad: missing.append("RAD")
        if not afv: missing.append("AFV")
        if missing:
            messagebox.showerror("Missing files", f"Please select files for: {', '.join(missing)}")
            return None

        mode = self.mode.get()
        if mode not in ("monthly", "quarterly"):
            messagebox.showerror("Invalid mode", "Please choose Monthly or Quarterly.")
            return None

        quarter = None
        year = None
        if mode == "quarterly":
            quarter = self.quarter.get().strip().upper()
            if quarter not in FISCAL_QUARTERS:
                messagebox.showerror("Invalid quarter", "Quarter must be Q1, Q2, Q3, or Q4.")
                return None
            try:
                year = int(self.year.get().strip())
            except Exception:
                messagebox.showerror("Invalid year", "Year must be a number (e.g., 2026).")
                return None

        return RunConfig(
            rbw_path=rbw,
            carpool_path=carpool,
            rad_path=rad,
            afv_path=afv,
            outdir=outdir,
            mode=mode,
            quarter=quarter,
            year=year,
            create_email_drafts=bool(self.create_email_drafts.get()),
        )

    def _on_run_clicked(self) -> None:
        cfg = self._validate()
        if not cfg:
            return

        if not BACKEND_OK:
            messagebox.showerror(
                "Backend import failed",
                "Could not import trp_app.py.\n\n"
                "Make sure trp_gui.py is in the same folder as trp_app.py.\n\n"
                f"Import error:\n{IMPORT_ERROR_TEXT}",
            )
            return

        self._set_running(True)
        self._set_status("Starting run...")

        threading.Thread(target=self._run_backend_thread, args=(cfg,), daemon=True).start()

    def _run_backend_thread(self, cfg: RunConfig) -> None:
        try:
            def status_cb(msg: str) -> None:
                # bind msg to avoid closure issues
                self.root.after(0, lambda m=msg: self._set_status(m))

            out_used = trp_app.run_trp(  # type: ignore[attr-defined]
                cfg.rbw_path,
                cfg.carpool_path,
                cfg.rad_path,
                cfg.afv_path,
                outdir=cfg.outdir,
                mode=cfg.mode,
                quarter=cfg.quarter,
                year=cfg.year,
                status_cb=status_cb,
                create_email_drafts=cfg.create_email_drafts,
            )

            def finish() -> None:
                expected = [
                    os.path.join(out_used, "cleaned_master.csv"),
                    os.path.join(out_used, "lunch_report.csv"),
                    os.path.join(out_used, "winner_report.csv"),
                    os.path.join(out_used, "run_log.json"),
                ]
                missing = [p for p in expected if not os.path.exists(p)]
                if missing:
                    self._on_run_error(
                        "Run finished but expected output files were not found.\n\nMissing:\n"
                        + "\n".join(missing)
                        + "\n\nCheck your backend write_outputs() and confirm it writes these exact filenames."
                    )
                else:
                    self._on_run_success(out_used)

            self.root.after(0, finish)

        except Exception as e:
            err_msg = str(e)
            self.root.after(0, lambda m=err_msg: self._on_run_error(m))

    def _on_run_success(self, outdir: str) -> None:
        self._set_running(False)
        self._set_status(f"✅ Complete. Outputs saved to: {outdir}")
        messagebox.showinfo("Complete", f"Run complete!\n\nOutputs saved to:\n{outdir}")

    def _on_run_error(self, msg: str) -> None:
        self._set_running(False)
        self._set_status("❌ Error occurred.")
        messagebox.showerror("Error", msg)

    def run(self) -> None:
        self.root.mainloop()


if __name__ == "__main__":
    TRPApp().run()