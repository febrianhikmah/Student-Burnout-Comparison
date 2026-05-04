from __future__ import annotations

import pandas as pd

from common import COLUMN_MAP, DATA_PATH, validate_columns


def main() -> None:
    sample = pd.read_csv(DATA_PATH, nrows=5)
    validate_columns(list(sample.columns))

    print(f"Dataset path: {DATA_PATH}")
    print("\nFirst 5 rows:")
    print(sample)
    print("\nColumns:")
    for column in sample.columns:
        print(f"- {column}")
    print("\nDtypes from sample:")
    print(sample.dtypes)
    print("\nAnalysis column mapping:")
    for standard_name, source_name in COLUMN_MAP.items():
        print(f"- {standard_name} <- {source_name}")


if __name__ == "__main__":
    main()
