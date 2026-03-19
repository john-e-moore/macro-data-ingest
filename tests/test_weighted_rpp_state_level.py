from __future__ import annotations

import pandas as pd
import pytest

from macro_data_ingest.analysis.weighted_rpp_state_level import (
    build_combined_output_frame,
    compute_scenario_results,
    compute_state_change_summary,
)


def test_compute_scenario_results_renormalizes_exclusion_cases() -> None:
    latest_year_rows = pd.DataFrame(
        [
            {
                "year": 2024,
                "state_abbrev": "CA",
                "category": "All items",
                "rpp": 110.0,
                "pce_share": 0.40,
                "weighted_rpp": 44.0,
            },
            {
                "year": 2024,
                "state_abbrev": "TX",
                "category": "All items",
                "rpp": 95.0,
                "pce_share": 0.60,
                "weighted_rpp": 57.0,
            },
            {
                "year": 2024,
                "state_abbrev": "CA",
                "category": "Housing rents",
                "rpp": 120.0,
                "pce_share": 0.25,
                "weighted_rpp": 30.0,
            },
            {
                "year": 2024,
                "state_abbrev": "TX",
                "category": "Housing rents",
                "rpp": 100.0,
                "pce_share": 0.75,
                "weighted_rpp": 75.0,
            },
            {
                "year": 2024,
                "state_abbrev": "CA",
                "category": "Goods",
                "rpp": 101.0,
                "pce_share": 0.50,
                "weighted_rpp": 50.5,
            },
            {
                "year": 2024,
                "state_abbrev": "TX",
                "category": "Goods",
                "rpp": 99.0,
                "pce_share": 0.50,
                "weighted_rpp": 49.5,
            },
            {
                "year": 2024,
                "state_abbrev": "CA",
                "category": "Utilities",
                "rpp": 104.0,
                "pce_share": 0.50,
                "weighted_rpp": 52.0,
            },
            {
                "year": 2024,
                "state_abbrev": "TX",
                "category": "Utilities",
                "rpp": 96.0,
                "pce_share": 0.50,
                "weighted_rpp": 48.0,
            },
            {
                "year": 2024,
                "state_abbrev": "CA",
                "category": "Other services",
                "rpp": 103.0,
                "pce_share": 0.50,
                "weighted_rpp": 51.5,
            },
            {
                "year": 2024,
                "state_abbrev": "TX",
                "category": "Other services",
                "rpp": 97.0,
                "pce_share": 0.50,
                "weighted_rpp": 48.5,
            },
        ]
    )

    results = compute_scenario_results(latest_year_rows)
    all_items = results.loc[results["category"] == "All items"].set_index("scenario")

    assert all_items.loc["National", "rpp_level"] == 101.0
    assert all_items.loc["Without CA", "rpp_level"] == 95.0
    assert all_items.loc["Without CA", "difference_from_national"] == -6.0
    assert all_items.loc["Without CA", "pct_change_from_national"] == pytest.approx(
        (-6.0 / 101.0) * 100.0
    )
    assert all_items.loc["Without CA", "share_sum"] == 0.6


def test_compute_state_change_summary_uses_first_and_last_year() -> None:
    trend_rows = pd.DataFrame(
        [
            {"state_abbrev": "CA", "category": "All items", "year": 2020, "rpp": 103.0},
            {"state_abbrev": "CA", "category": "All items", "year": 2021, "rpp": 104.0},
            {"state_abbrev": "CA", "category": "All items", "year": 2022, "rpp": 105.0},
            {"state_abbrev": "CA", "category": "All items", "year": 2023, "rpp": 106.0},
            {"state_abbrev": "CA", "category": "All items", "year": 2024, "rpp": 107.0},
        ]
    )

    summary = compute_state_change_summary(trend_rows)

    assert len(summary) == 1
    assert summary.loc[0, "start_year"] == 2020
    assert summary.loc[0, "end_year"] == 2024
    assert summary.loc[0, "rpp_change"] == 4.0
    assert summary.loc[0, "pct_change"] == pytest.approx(((107.0 / 103.0) - 1.0) * 100.0)


def test_build_combined_output_frame_includes_all_views() -> None:
    scenario_results = pd.DataFrame(
        [
            {
                "category": "All items",
                "scenario": "National",
                "rpp_level": 100.0,
                "difference_from_national": 0.0,
                "pct_change_from_national": 0.0,
                "share_sum": 1.0,
            }
        ]
    )
    trend_rows = pd.DataFrame(
        [{"state_abbrev": "CA", "category": "All items", "year": 2024, "rpp": 105.0}]
    )
    state_change_summary = pd.DataFrame(
        [
            {
                "state_abbrev": "CA",
                "category": "All items",
                "start_year": 2020,
                "end_year": 2024,
                "start_rpp": 101.0,
                "end_rpp": 105.0,
                "rpp_change": 4.0,
                "pct_change": 3.96,
            }
        ]
    )

    combined = build_combined_output_frame(
        latest_year=2024,
        scenario_results=scenario_results,
        trend_rows=trend_rows,
        state_change_summary=state_change_summary,
    )

    assert set(combined["view_type"]) == {
        "scenario_comparison",
        "state_trend",
        "state_change_summary",
    }
