"""Births processing pipeline.

Analysts:
    - Import and call main(2023) from a notebook, OR
    - Run from the command line: python births_pipeline.py --year 2023

Developers:
    - Maintain and extend individual functions below (load/validate/calculate/aggregate/save).
"""

from pathlib import Path
from typing import Dict, Tuple

import pandas as pd


DATA_DIR = Path("data")
RATE_SCALE = 1000  # births per 1,000 population


def load_data(year: int | str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load births data for a year and population data.

    Looks first in data/ (preferred project structure), then falls back to
    CSVs in the project root so it still works with your current files.
    """

    year = str(year)

    def _find_births_file() -> Path:
        # Preferred: data/data_YYYY.csv
        preferred = DATA_DIR / f"data_{year}.csv"
        if preferred.exists():
            return preferred

        # Fallback: any CSV whose name contains the year
        for base in (DATA_DIR, Path(".")):
            if not base.exists():
                continue
            for p in base.iterdir():
                if p.suffix.lower() == ".csv" and year in p.name:
                    return p
        raise FileNotFoundError(f"No births CSV found for year {year} in data/ or project root")

    def _find_population_file() -> Path:
        for candidate in (DATA_DIR / "pop_data.csv", Path("pop_data.csv")):
            if candidate.exists():
                return candidate
        raise FileNotFoundError("pop_data.csv not found in data/ or project root")

    births_path = _find_births_file()
    pop_path = _find_population_file()

    births_df = pd.read_csv(births_path)
    pop_df = pd.read_csv(pop_path)

    # Basic column normalisation so we can treat all years consistently
    births_df = births_df.rename(
        columns={
            "btype": "birth_type",
            "birth_type": "birth_type",
            "dobyr": "year",
            "place_of_birth": "region",
        }
    )

    pop_df = pop_df.rename(columns={"geography": "region"})

    return births_df, pop_df


def validate_data(births_df: pd.DataFrame, pop_df: pd.DataFrame) -> None:
    """Raise a clear error if expected columns or values are missing.

    This keeps the rest of the pipeline code simple and assumes validated inputs.
    """

    required_birth_cols = {"year", "birth_type", "sex", "region"}
    missing_birth_cols = required_birth_cols - set(births_df.columns)
    if missing_birth_cols:
        raise ValueError(f"Births data missing columns: {sorted(missing_birth_cols)}")

    required_pop_cols = {"sex", "age", "region", "year", "population"}
    missing_pop_cols = required_pop_cols - set(pop_df.columns)
    if missing_pop_cols:
        raise ValueError(f"Population data missing columns: {sorted(missing_pop_cols)}")

    # Basic content checks
    if births_df.empty:
        raise ValueError("Births data is empty after loading")
    if pop_df.empty:
        raise ValueError("Population data is empty after loading")


def calculate_births(births_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Calculate live and still births at required aggregation levels.

    Returns a dict of DataFrames with keys:
        - 'totals'
        - 'by_sex'
        - 'by_region'
        - 'by_sex_region'
    """

    # Normalise sex and birth_type values
    births = births_df.copy()
    births["sex"] = births["sex"].astype(str).str.strip().replace(
        {"1": "Male", "2": "Female", "M": "Male", "F": "Female"}
    )
    births["birth_type"] = births["birth_type"].astype(str).str.strip().str.title()
    births["region"] = births["region"].fillna("Unknown").replace({"NA": "Unknown", "": "Unknown"})

    births["is_live"] = births["birth_type"].str.contains("Live", case=False, na=False)
    births["is_still"] = births["birth_type"].str.contains("Still", case=False, na=False)

    # Totals
    totals = pd.DataFrame(
        [
            {
                "live_births": int(births["is_live"].sum()),
                "still_births": int(births["is_still"].sum()),
            }
        ]
    )

    # By sex
    by_sex = (
        births.groupby("sex", dropna=False)
        .agg(live_births=("is_live", "sum"), still_births=("is_still", "sum"))
        .reset_index()
    )

    # By region
    by_region = (
        births.groupby("region", dropna=False)
        .agg(live_births=("is_live", "sum"), still_births=("is_still", "sum"))
        .reset_index()
    )

    # By sex & region
    by_sex_region = (
        births.groupby(["sex", "region"], dropna=False)
        .agg(live_births=("is_live", "sum"), still_births=("is_still", "sum"))
        .reset_index()
    )

    return {
        "totals": totals,
        "by_sex": by_sex,
        "by_region": by_region,
        "by_sex_region": by_sex_region,
    }


def calculate_birth_rate(
    births_aggs: Dict[str, pd.DataFrame], pop_df: pd.DataFrame, year: int | str
) -> Dict[str, pd.DataFrame]:
    """Attach birth-rate columns (births per RATE_SCALE population)."""

    year = str(year)
    pop_year = pop_df[pop_df["year"].astype(str) == year].copy()
    pop_year["sex"] = pop_year["sex"].astype(str).str.strip().replace(
        {"1": "Male", "2": "Female", "M": "Male", "F": "Female"}
    )
    pop_year["region"] = pop_year["region"].fillna("Unknown").replace(
        {"NA": "Unknown", "": "Unknown"}
    )
    if pop_year.empty:
        raise ValueError(f"Population data missing for year {year}")

    # Aggregate population at needed levels
    pop_country = float(pop_year["population"].astype(float).sum())
    pop_by_sex = (
        pop_year.groupby("sex", dropna=False)["population"].sum().astype(float)
    )
    pop_by_region = (
        pop_year.groupby("region", dropna=False)["population"].sum().astype(float)
    )
    pop_by_sex_region = (
        pop_year.groupby(["sex", "region"], dropna=False)["population"].sum().astype(float)
    )

    def _rate(count: float, population: float | int | None) -> float | None:
        if population is None or population == 0:
            return None
        return float(count) / float(population) * RATE_SCALE

    out: Dict[str, pd.DataFrame] = {}

    # Totals
    totals = births_aggs["totals"].copy()
    live_total = float(totals.loc[0, "live_births"])
    totals[f"birth_rate_per_{RATE_SCALE}"] = _rate(live_total, pop_country)
    out["totals"] = totals

    # By sex
    by_sex = births_aggs["by_sex"].copy()
    by_sex["population"] = by_sex["sex"].map(pop_by_sex).fillna(0)
    by_sex[f"birth_rate_per_{RATE_SCALE}"] = [
        _rate(live, pop) for live, pop in zip(by_sex["live_births"], by_sex["population"])
    ]
    out["by_sex"] = by_sex

    # By region
    by_region = births_aggs["by_region"].copy()
    by_region["population"] = by_region["region"].map(pop_by_region).fillna(0)
    by_region[f"birth_rate_per_{RATE_SCALE}"] = [
        _rate(live, pop) for live, pop in zip(by_region["live_births"], by_region["population"])
    ]
    out["by_region"] = by_region

    # By sex & region
    by_sex_region = births_aggs["by_sex_region"].copy()
    pop_indexed = pop_by_sex_region
    populations = []
    for _, row in by_sex_region.iterrows():
        key = (row["sex"], row["region"])
        populations.append(pop_indexed.get(key, 0))
    by_sex_region["population"] = populations
    by_sex_region[f"birth_rate_per_{RATE_SCALE}"] = [
        _rate(live, pop)
        for live, pop in zip(by_sex_region["live_births"], by_sex_region["population"])
    ]
    out["by_sex_region"] = by_sex_region

    return out


def aggregate_outputs(outputs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Final chance to adjust schemas or add metadata before saving.

    For now this is a pass-through, but having this layer makes it easy to
    add extra columns or reformat for downstream users later.
    """

    return outputs


def save_outputs(outputs: Dict[str, pd.DataFrame], year: int | str, out_dir: str = "outputs") -> None:
    """Write CSV outputs to disk for the given year."""

    year = str(year)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    mapping = {
        "totals": f"{year}_totals.csv",
        "by_sex": f"{year}_by_sex.csv",
        "by_region": f"{year}_by_region.csv",
        "by_sex_region": f"{year}_by_sex_region.csv",
    }

    for key, df in outputs.items():
        filename = mapping.get(key, f"{year}_{key}.csv")
        df.to_csv(out_path / filename, index=False)


def main(year: int | str, out_dir: str = "outputs") -> Dict[str, pd.DataFrame]:
    """Run the end-to-end pipeline for a single year.

    This is the main function analysts are expected to call.
    It returns the in-memory DataFrames *and* writes CSVs to disk.
    """

    births_df, pop_df = load_data(year)
    validate_data(births_df, pop_df)
    births_aggs = calculate_births(births_df)
    with_rates = calculate_birth_rate(births_aggs, pop_df, year)
    final_outputs = aggregate_outputs(with_rates)
    save_outputs(final_outputs, year, out_dir=out_dir)
    return final_outputs


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run births pipeline for a single year")
    parser.add_argument("--year", required=True, help="Year to process, e.g. 2024")
    parser.add_argument("--out-dir", default="outputs", help="Directory for CSV outputs")
    args = parser.parse_args()

    result = main(args.year, out_dir=args.out_dir)
    print(f"Wrote outputs to {args.out_dir}")
