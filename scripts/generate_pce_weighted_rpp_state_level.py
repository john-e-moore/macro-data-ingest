from __future__ import annotations

import argparse
from pathlib import Path

from macro_data_ingest.analysis.weighted_rpp_state_level import generate_research


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate weighted RPP state-level research outputs."
    )
    parser.add_argument(
        "--output-dir",
        default="research/outputs/pce-weighted-rpp-state-level",
        help="Directory where PNG, CSV, and markdown outputs will be written.",
    )
    args = parser.parse_args()

    artifacts = generate_research(Path(args.output_dir))
    print(f"latest_year={artifacts.latest_year}")
    print(f"csv={artifacts.combined_csv_path}")
    print(f"summary={artifacts.summary_path}")
    for chart_path in artifacts.chart_paths:
        print(f"chart={chart_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
