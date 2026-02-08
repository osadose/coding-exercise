# Births processing pipeline

This project computes annual birth statistics (live births, still births and birth rate) and saves outputs in CSV format.

## Project layout

```text
project/
├── births_pipeline.py
├── README.md
├── requirements.txt
├── data/
	├── data_2023.csv
	├── data_2024.csv
	├── data_2025.csv
	└── pop_data.csv
├── outputs/
├── docs/
	└── data_dictionary.xlsx
└── tests/
	└── test_pipeline.py
```


# Quick start 

1. Ensure the `data/` folder contains `data_YYYY.csv` for the year(s) you want to process and `pop_data.csv`.
2. Review `docs/data_dictionary.xlsx` for field definitions and validation rules before touching the source data.
3. (Optional) Create a virtual environment and install requirements:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

4. Run the pipeline for a year from the command line (the example below uses 2024):

```bash
python births_pipeline.py --year 2024 --out-dir outputs
```

## Testing

Run the standard test suite to ensure the pipeline can execute end-to-end on the data and emit all of the expected CSVs:

```bash
python -m pytest
```

## Documentation

All data definitions live in `docs/data_dictionary.xlsx`; consult it to understand required columns, types, and acceptable value ranges.

## Outputs

The following CSV files are produced in the chosen output directory:

- `outputs/{year}_totals.csv`
- `outputs/{year}_by_sex.csv`
- `outputs/{year}_by_region.csv`
- `outputs/{year}_by_sex_region.csv`

Birth rate is calculated as:

$$
		\text{birth rate} = \frac{\text{live births}}{\text{population}} \times 1000
$$

