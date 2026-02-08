from pathlib import Path

import pandas as pd

from births_pipeline import RATE_SCALE, main


YEAR = 2024
EXPECTED_OUTPUT_KEYS = {"totals", "by_sex", "by_region", "by_sex_region"}


def test_main_pipeline_produces_outputs(tmp_path: Path) -> None:
    outputs = main(YEAR, out_dir=tmp_path)
    assert set(outputs) == EXPECTED_OUTPUT_KEYS

    rate_column = f"birth_rate_per_{RATE_SCALE}"
    totals = outputs["totals"].iloc[0]
    assert totals["live_births"] > 0
    assert pd.notna(totals[rate_column])

    for key, df in outputs.items():
        assert not df.empty
        assert {"live_births", "still_births"}.issubset(df.columns)
        assert rate_column in df.columns

        csv_file = tmp_path / f"{YEAR}_{key}.csv"
        assert csv_file.exists()
        assert csv_file.stat().st_size > 0
