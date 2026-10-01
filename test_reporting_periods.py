import unittest
from contextlib import ExitStack
from datetime import datetime
from unittest.mock import patch

import pandas as pd

import trp_app as app
import trp_core as core
import trp_gui as gui


class ReportingPeriodTests(unittest.TestCase):
    def run_report(self, mode, dates, quarter='Q2', year=2026):
        raw = pd.DataFrame({
            'Name': ['Test Person'] * len(dates),
            'Email': ['test@example.com'] * len(dates),
            'Badge Number': ['12345'] * len(dates),
            'Completion time': pd.to_datetime(dates),
            'What are you registering?': ['Run Bike Walk'] * len(dates),
        })
        with ExitStack() as stack:
            stack.enter_context(patch.object(core, 'read_table', return_value=raw))
            clock = stack.enter_context(patch.object(app, 'datetime'))
            clock.now.return_value = datetime(2026, 10, 1)
            output = stack.enter_context(patch.object(core, 'write_outputs'))
            stack.enter_context(patch.object(core, 'write_lunch_checkoff_pdf'))
            stack.enter_context(patch.object(core, 'assert_expected_outputs'))
            app.run_trp('daily.xlsx', 'daily.xlsx', 'daily.xlsx', 'afv.xlsx',
                        'unused', mode, quarter=quarter, year=year)
            return output.call_args.args

    def test_monthly_always_uses_completed_month(self):
        for day in (1, 15, 16, 30, 31):
            self.assertEqual(core.reporting_month_for_run(datetime(2026, 10, day)), (2026, 9))
        self.assertEqual(core.reporting_month_for_run(datetime(2027, 1, 1)), (2026, 12))

    def test_completed_quarter_boundaries(self):
        for date, expected in [('2026-10-01', ('Q2', 2026)),
                               ('2027-01-01', ('Q3', 2026)),
                               ('2027-04-01', ('Q4', 2027)),
                               ('2027-07-01', ('Q1', 2027))]:
            self.assertEqual(gui._completed_fiscal_quarter(datetime.fromisoformat(date)), expected)

    def test_both_modes_count_only_september_unique_days(self):
        dates = ['2026-08-31', '2026-09-01', '2026-09-02', '2026-09-03',
                 '2026-09-04', '2026-09-05', '2026-09-05', '2026-10-01']
        for mode in ('monthly', 'quarterly'):
            with self.subTest(mode=mode):
                output = self.run_report(mode, dates)
                report, log = output[2], output[4]
                self.assertEqual(log['selected_period'], {'year': 2026, 'month': 9})
                self.assertEqual(report['trips'].tolist(), [5])
                self.assertEqual(report['lunches'].tolist(), [1])

    def test_missing_september_never_uses_other_month_or_year(self):
        for mode in ('monthly', 'quarterly'):
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, '2026-09'):
                self.run_report(mode, ['2025-09-01', '2026-08-31', '2026-10-01'])


if __name__ == '__main__':
    unittest.main()
