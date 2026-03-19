from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable

import matplotlib
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from macro_data_ingest.config import load_config

matplotlib.use("Agg")

import matplotlib.pyplot as plt

TARGET_STATES = ("CA", "NY", "NJ", "IL", "CT")
STATE_NAMES = {
    "CA": "California",
    "NY": "New York",
    "NJ": "New Jersey",
    "IL": "Illinois",
    "CT": "Connecticut",
}
CATEGORY_ORDER = (
    "All items",
    "Housing rents",
    "Goods",
    "Utilities",
    "Other services",
)
SCENARIO_ORDER = (
    "National",
    "Without CA",
    "Without CA, NY, NJ, IL, CT",
)
CATEGORY_COLORS = {
    "All items": "#1f77b4",
    "Housing rents": "#d62728",
    "Goods": "#2ca02c",
    "Utilities": "#ff7f0e",
    "Other services": "#9467bd",
}
SCENARIO_EXCLUSIONS = {
    "National": set(),
    "Without CA": {"CA"},
    "Without CA, NY, NJ, IL, CT": {"CA", "NY", "NJ", "IL", "CT"},
}


@dataclass(frozen=True)
class ResearchArtifacts:
    latest_year: int
    output_dir: Path
    scenario_results: pd.DataFrame
    trend_rows: pd.DataFrame
    state_change_summary: pd.DataFrame
    combined_csv_path: Path
    summary_path: Path
    chart_paths: list[Path]


def build_engine() -> Engine:
    config = load_config()
    dsn = (
        f"postgresql+psycopg://{config.pg_user}:{config.pg_password}"
        f"@{config.pg_host}:{config.pg_port}/{config.pg_database}"
    )
    return create_engine(dsn, connect_args={"connect_timeout": 10})


def fetch_latest_year(engine: Engine) -> int:
    query = text("SELECT MAX(year) AS latest_year FROM serving.v_state_rpp_pce_weighted_annual")
    with engine.connect() as conn:
        latest_year = conn.execute(query).scalar_one()
    return int(latest_year)


def fetch_latest_year_rows(engine: Engine, latest_year: int) -> pd.DataFrame:
    query = text(
        """
        SELECT
            year,
            state_abbrev,
            category,
            rpp,
            pce_share,
            weighted_rpp
        FROM serving.v_state_rpp_pce_weighted_annual
        WHERE year = :latest_year
        """
    )
    with engine.connect() as conn:
        frame = pd.read_sql_query(query, conn, params={"latest_year": latest_year})
    return frame


def fetch_state_trend_rows(
    engine: Engine,
    latest_year: int,
    states: Iterable[str] = TARGET_STATES,
) -> pd.DataFrame:
    query = text(
        """
        SELECT
            state_abbrev,
            category,
            year,
            rpp
        FROM serving.v_state_rpp_pce_weighted_annual
        WHERE year BETWEEN :start_year AND :latest_year
          AND state_abbrev = ANY(:states)
        ORDER BY state_abbrev, category, year
        """
    )
    with engine.connect() as conn:
        frame = pd.read_sql_query(
            query,
            conn,
            params={
                "start_year": latest_year - 4,
                "latest_year": latest_year,
                "states": list(states),
            },
        )
    return frame


def fetch_share_validation(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT
            year,
            category,
            SUM(pce_share) AS share_sum
        FROM serving.v_state_rpp_pce_weighted_annual
        GROUP BY year, category
        ORDER BY year, category
        """
    )
    with engine.connect() as conn:
        frame = pd.read_sql_query(query, conn)
    return frame


def fetch_sql_scenario_results(engine: Engine, latest_year: int) -> pd.DataFrame:
    query = text(
        """
        WITH scenario_rows AS (
            SELECT
                category,
                'National'::text AS scenario,
                SUM(weighted_rpp) AS rpp_level,
                SUM(pce_share) AS share_sum
            FROM serving.v_state_rpp_pce_weighted_annual
            WHERE year = :latest_year
            GROUP BY category

            UNION ALL

            SELECT
                category,
                'Without CA'::text AS scenario,
                SUM(weighted_rpp) / NULLIF(SUM(pce_share), 0) AS rpp_level,
                SUM(pce_share) AS share_sum
            FROM serving.v_state_rpp_pce_weighted_annual
            WHERE year = :latest_year
              AND state_abbrev <> 'CA'
            GROUP BY category

            UNION ALL

            SELECT
                category,
                'Without CA, NY, NJ, IL, CT'::text AS scenario,
                SUM(weighted_rpp) / NULLIF(SUM(pce_share), 0) AS rpp_level,
                SUM(pce_share) AS share_sum
            FROM serving.v_state_rpp_pce_weighted_annual
            WHERE year = :latest_year
              AND state_abbrev NOT IN ('CA', 'NY', 'NJ', 'IL', 'CT')
            GROUP BY category
        ),
        national AS (
            SELECT category, rpp_level AS national_rpp
            FROM scenario_rows
            WHERE scenario = 'National'
        )
        SELECT
            s.category,
            s.scenario,
            s.rpp_level,
            s.share_sum,
            s.rpp_level - n.national_rpp AS difference_from_national,
            ((s.rpp_level / NULLIF(n.national_rpp, 0)) - 1) * 100.0 AS pct_change_from_national
        FROM scenario_rows s
        JOIN national n USING (category)
        ORDER BY
            CASE s.category
                WHEN 'All items' THEN 1
                WHEN 'Housing rents' THEN 2
                WHEN 'Goods' THEN 3
                WHEN 'Utilities' THEN 4
                ELSE 5
            END,
            CASE s.scenario
                WHEN 'National' THEN 1
                WHEN 'Without CA' THEN 2
                ELSE 3
            END
        """
    )
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn, params={"latest_year": latest_year})


def compute_scenario_results(latest_year_rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, object]] = []

    for category in CATEGORY_ORDER:
        category_rows = latest_year_rows.loc[latest_year_rows["category"] == category].copy()
        national_rpp = float(category_rows["weighted_rpp"].sum())

        for scenario in SCENARIO_ORDER:
            excluded_states = SCENARIO_EXCLUSIONS[scenario]
            included_rows = category_rows.loc[~category_rows["state_abbrev"].isin(excluded_states)]
            share_sum = float(included_rows["pce_share"].sum())
            weighted_sum = float(included_rows["weighted_rpp"].sum())
            if scenario == "National":
                rpp_level = weighted_sum
            else:
                rpp_level = weighted_sum / share_sum
            difference = rpp_level - national_rpp
            pct_change = (difference / national_rpp) * 100.0
            records.append(
                {
                    "category": category,
                    "scenario": scenario,
                    "rpp_level": rpp_level,
                    "difference_from_national": difference,
                    "pct_change_from_national": pct_change,
                    "share_sum": share_sum,
                }
            )

    return pd.DataFrame.from_records(records)


def compute_state_change_summary(trend_rows: pd.DataFrame) -> pd.DataFrame:
    ordered = trend_rows.sort_values(["state_abbrev", "category", "year"]).reset_index(drop=True)
    counts = ordered.groupby(["state_abbrev", "category"])["year"].nunique()
    if not (counts == 5).all():
        missing = counts[counts != 5]
        raise ValueError(f"Expected exactly 5 years per state/category, found {missing.to_dict()}")

    summary = (
        ordered.groupby(["state_abbrev", "category"], as_index=False)
        .agg(
            start_year=("year", "first"),
            end_year=("year", "last"),
            start_rpp=("rpp", "first"),
            end_rpp=("rpp", "last"),
        )
        .sort_values(["state_abbrev", "category"])
        .reset_index(drop=True)
    )
    summary["rpp_change"] = summary["end_rpp"] - summary["start_rpp"]
    summary["pct_change"] = ((summary["end_rpp"] / summary["start_rpp"]) - 1.0) * 100.0
    return summary


def validate_share_sums(validation_rows: pd.DataFrame, tolerance: float = 1e-8) -> None:
    invalid = validation_rows.loc[(validation_rows["share_sum"] - 1.0).abs() > tolerance]
    if not invalid.empty:
        raise ValueError(f"Expected pce_share sums to equal 1, found {invalid.to_dict('records')[:5]}")


def validate_scenario_math(
    computed_results: pd.DataFrame,
    sql_results: pd.DataFrame,
    tolerance: float = 1e-4,
) -> None:
    computed = computed_results.sort_values(["category", "scenario"]).reset_index(drop=True)
    expected = sql_results.sort_values(["category", "scenario"]).reset_index(drop=True)
    numeric_columns = [
        "rpp_level",
        "difference_from_national",
        "pct_change_from_national",
        "share_sum",
    ]
    for column in numeric_columns:
        deltas = (computed[column] - expected[column]).abs()
        if (deltas > tolerance).any():
            raise ValueError(
                f"Scenario validation failed for {column}: "
                f"max_delta={float(deltas.max())}"
            )


def build_combined_output_frame(
    latest_year: int,
    scenario_results: pd.DataFrame,
    trend_rows: pd.DataFrame,
    state_change_summary: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "view_type",
        "latest_year",
        "scenario",
        "state_abbrev",
        "category",
        "year",
        "rpp_level",
        "difference_from_national",
        "pct_change_from_national",
        "share_sum",
        "rpp",
        "start_year",
        "end_year",
        "start_rpp",
        "end_rpp",
        "rpp_change",
        "pct_change",
    ]

    scenario_frame = scenario_results.assign(
        view_type="scenario_comparison",
        latest_year=latest_year,
        state_abbrev=pd.NA,
        year=latest_year,
        rpp=pd.NA,
        start_year=pd.NA,
        end_year=pd.NA,
        start_rpp=pd.NA,
        end_rpp=pd.NA,
        rpp_change=pd.NA,
        pct_change=pd.NA,
    )

    trend_frame = trend_rows.assign(
        view_type="state_trend",
        latest_year=latest_year,
        scenario=pd.NA,
        rpp_level=pd.NA,
        difference_from_national=pd.NA,
        pct_change_from_national=pd.NA,
        share_sum=pd.NA,
        start_year=pd.NA,
        end_year=pd.NA,
        start_rpp=pd.NA,
        end_rpp=pd.NA,
        rpp_change=pd.NA,
        pct_change=pd.NA,
    )

    state_change_frame = state_change_summary.assign(
        view_type="state_change_summary",
        latest_year=latest_year,
        scenario=pd.NA,
        year=pd.NA,
        rpp_level=pd.NA,
        difference_from_national=pd.NA,
        pct_change_from_national=pd.NA,
        share_sum=pd.NA,
        rpp=pd.NA,
    )

    parts = [
        scenario_frame.reindex(columns=columns).astype("object"),
        trend_frame.reindex(columns=columns).astype("object"),
        state_change_frame.reindex(columns=columns).astype("object"),
    ]
    return pd.concat(parts, ignore_index=True)


def write_combined_csv(
    output_dir: Path,
    latest_year: int,
    scenario_results: pd.DataFrame,
    trend_rows: pd.DataFrame,
    state_change_summary: pd.DataFrame,
) -> Path:
    output_path = output_dir / "weighted_rpp_state_level_analysis.csv"
    combined = build_combined_output_frame(latest_year, scenario_results, trend_rows, state_change_summary)
    combined.to_csv(output_path, index=False)
    return output_path


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower())
    return slug.strip("-")


def render_scenario_charts(
    output_dir: Path,
    scenario_results: pd.DataFrame,
    latest_year: int,
) -> list[Path]:
    chart_paths: list[Path] = []
    plt.style.use("seaborn-v0_8-whitegrid")

    for category in CATEGORY_ORDER:
        subset = (
            scenario_results.loc[scenario_results["category"] == category]
            .set_index("scenario")
            .loc[list(SCENARIO_ORDER)]
            .reset_index()
        )
        fig, ax = plt.subplots(figsize=(9, 5))
        bars = ax.bar(
            subset["scenario"],
            subset["rpp_level"],
            color=["#4c78a8", "#f58518", "#54a24b"],
            width=0.65,
        )
        ax.set_title(f"{category}: weighted RPP comparison ({latest_year})")
        ax.set_ylabel("RPP")
        ymin = min(subset["rpp_level"].min() - 3.0, 90.0)
        ymax = max(subset["rpp_level"].max() + 3.0, 110.0)
        ax.set_ylim(ymin, ymax)
        ax.axhline(subset.loc[subset["scenario"] == "National", "rpp_level"].iloc[0], color="#444444", linewidth=1.0, linestyle="--")
        for bar, value, pct in zip(
            bars,
            subset["rpp_level"],
            subset["pct_change_from_national"],
        ):
            label = f"{value:.2f}\n{pct:+.2f}%"
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.25,
                label,
                ha="center",
                va="bottom",
                fontsize=10,
            )
        fig.tight_layout()
        output_path = output_dir / f"bar_{slugify(category)}.png"
        fig.savefig(output_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        chart_paths.append(output_path)

    return chart_paths


def render_state_trend_charts(output_dir: Path, trend_rows: pd.DataFrame) -> list[Path]:
    chart_paths: list[Path] = []
    plt.style.use("seaborn-v0_8-whitegrid")

    for state_abbrev in TARGET_STATES:
        subset = trend_rows.loc[trend_rows["state_abbrev"] == state_abbrev].copy()
        fig, ax = plt.subplots(figsize=(10, 5.5))
        for category in CATEGORY_ORDER:
            category_rows = subset.loc[subset["category"] == category]
            ax.plot(
                category_rows["year"],
                category_rows["rpp"],
                marker="o",
                linewidth=2.2,
                label=category,
                color=CATEGORY_COLORS[category],
            )
        ax.set_title(
            f"{STATE_NAMES[state_abbrev]} weighted RPP categories "
            f"({int(subset['year'].min())}-{int(subset['year'].max())})"
        )
        ax.set_xlabel("Year")
        ax.set_ylabel("RPP")
        ax.set_xticks(sorted(subset["year"].unique()))
        ax.legend(loc="best", ncols=2, frameon=True)
        fig.tight_layout()
        output_path = output_dir / f"line_{state_abbrev.lower()}.png"
        fig.savefig(output_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        chart_paths.append(output_path)

    return chart_paths


def to_markdown_table(frame: pd.DataFrame, columns: list[str], rename_map: dict[str, str]) -> str:
    headers = [rename_map.get(column, column) for column in columns]
    rows = [headers, ["---"] * len(headers)]
    for _, row in frame[columns].iterrows():
        rows.append([str(row[column]) for column in columns])
    return "\n".join("| " + " | ".join(values) + " |" for values in rows)


def write_summary_markdown(
    output_dir: Path,
    latest_year: int,
    scenario_results: pd.DataFrame,
    state_change_summary: pd.DataFrame,
) -> Path:
    summary_path = output_dir / "summary.md"
    without_ca = scenario_results.loc[scenario_results["scenario"] == "Without CA"]
    without_five = scenario_results.loc[
        scenario_results["scenario"] == "Without CA, NY, NJ, IL, CT"
    ]

    biggest_ca_drop = without_ca.sort_values("difference_from_national").iloc[0]
    biggest_five_drop = without_five.sort_values("difference_from_national").iloc[0]
    largest_state_moves = (
        state_change_summary.sort_values("rpp_change", ascending=False)
        .groupby("state_abbrev", sort=False)
        .head(1)
        .reset_index(drop=True)
    )

    lines = [
        "# Weighted RPP State-Level Research",
        "",
        f"Most recent year analyzed: **{latest_year}**.",
        "",
        "## Key Takeaways",
        "",
        (
            f"- Excluding California alone lowers `{biggest_ca_drop['category']}` the most: "
            f"{biggest_ca_drop['difference_from_national']:.4f} RPP points "
            f"({biggest_ca_drop['pct_change_from_national']:.4f}%)."
        ),
        (
            f"- Excluding California, New York, New Jersey, Illinois, and Connecticut lowers "
            f"`{biggest_five_drop['category']}` the most: "
            f"{biggest_five_drop['difference_from_national']:.4f} RPP points "
            f"({biggest_five_drop['pct_change_from_national']:.4f}%)."
        ),
        (
            "- The national baseline includes DC because the serving view contains 51 geographies "
            "(50 states plus DC)."
        ),
        (
            "- Prompt category `housing` is reported here as `Housing rents`, matching the "
            "serving view contract."
        ),
        "",
        f"## Question 1: Latest-Year National vs Exclusion Scenarios ({latest_year})",
        "",
        to_markdown_table(
            scenario_results.assign(
                rpp_level=lambda df: df["rpp_level"].map(lambda value: f"{value:.4f}"),
                share_sum=lambda df: df["share_sum"].map(lambda value: f"{value * 100.0:.4f}%"),
                difference_from_national=lambda df: df["difference_from_national"].map(
                    lambda value: f"{value:+.4f}"
                ),
                pct_change_from_national=lambda df: df["pct_change_from_national"].map(
                    lambda value: f"{value:+.4f}%"
                ),
            ),
            columns=[
                "category",
                "scenario",
                "rpp_level",
                "share_sum",
                "difference_from_national",
                "pct_change_from_national",
            ],
            rename_map={
                "category": "Category",
                "scenario": "Scenario",
                "rpp_level": "RPP",
                "share_sum": "Share of national PCE",
                "difference_from_national": "Difference vs national",
                "pct_change_from_national": "% change vs national",
            },
        ),
        "",
        "## Question 2: Five-Year Change by State and Category",
        "",
        "Largest five-year increase by state:",
        "",
    ]

    for _, row in largest_state_moves.iterrows():
        lines.append(
            f"- `{row['state_abbrev']}`: `{row['category']}` rose {row['rpp_change']:.3f} "
            f"RPP points ({row['pct_change']:.3f}%) from {int(row['start_year'])} to {int(row['end_year'])}."
        )

    lines.append("")

    for state_abbrev in TARGET_STATES:
        state_rows = state_change_summary.loc[state_change_summary["state_abbrev"] == state_abbrev].copy()
        state_rows["start_rpp"] = state_rows["start_rpp"].map(lambda value: f"{value:.3f}")
        state_rows["end_rpp"] = state_rows["end_rpp"].map(lambda value: f"{value:.3f}")
        state_rows["rpp_change"] = state_rows["rpp_change"].map(lambda value: f"{value:+.3f}")
        state_rows["pct_change"] = state_rows["pct_change"].map(lambda value: f"{value:+.3f}%")
        lines.extend(
            [
                f"### {STATE_NAMES[state_abbrev]}",
                "",
                to_markdown_table(
                    state_rows,
                    columns=["category", "start_rpp", "end_rpp", "rpp_change", "pct_change"],
                    rename_map={
                        "category": "Category",
                        "start_rpp": f"RPP {int(state_rows['start_year'].iloc[0])}",
                        "end_rpp": f"RPP {int(state_rows['end_year'].iloc[0])}",
                        "rpp_change": "5-year change",
                        "pct_change": "5-year % change",
                    },
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## Method Notes",
            "",
            "- Source table: `serving.v_state_rpp_pce_weighted_annual`.",
            (
                "- Subset scenarios are renormalized with "
                "`SUM(weighted_rpp) / SUM(pce_share)` over included states."
            ),
            "- `Other services` is derived in the serving view rather than mapped from a single raw BEA line.",
            "- `Housing rents` uses the view's documented housing proxy mapping.",
        ]
    )

    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary_path


def generate_research(output_dir: Path) -> ResearchArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    engine = build_engine()

    latest_year = fetch_latest_year(engine)
    latest_year_rows = fetch_latest_year_rows(engine, latest_year)
    trend_rows = fetch_state_trend_rows(engine, latest_year)
    share_validation = fetch_share_validation(engine)

    validate_share_sums(share_validation)

    scenario_results = compute_scenario_results(latest_year_rows)
    sql_results = fetch_sql_scenario_results(engine, latest_year)
    validate_scenario_math(scenario_results, sql_results)

    trend_rows = trend_rows.sort_values(["state_abbrev", "category", "year"]).reset_index(drop=True)
    state_change_summary = compute_state_change_summary(trend_rows)

    combined_csv_path = write_combined_csv(
        output_dir,
        latest_year,
        scenario_results,
        trend_rows,
        state_change_summary,
    )
    chart_paths = render_scenario_charts(output_dir, scenario_results, latest_year)
    chart_paths.extend(render_state_trend_charts(output_dir, trend_rows))
    summary_path = write_summary_markdown(output_dir, latest_year, scenario_results, state_change_summary)

    return ResearchArtifacts(
        latest_year=latest_year,
        output_dir=output_dir,
        scenario_results=scenario_results,
        trend_rows=trend_rows,
        state_change_summary=state_change_summary,
        combined_csv_path=combined_csv_path,
        summary_path=summary_path,
        chart_paths=chart_paths,
    )
