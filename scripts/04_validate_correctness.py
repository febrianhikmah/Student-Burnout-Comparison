from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from common import OUTPUT_DIR, TOLERANCE, TOOL_DIRS, ensure_output_dirs


FILES_TO_VALIDATE = {
    "dataset_summary.csv": "summary_values",
    "missing_values.csv": "missing_count",
    "burnout_distribution.csv": "distribution_values",
    "avg_mental_health_by_burnout.csv": "average_values",
    "sleep_hours_vs_burnout.csv": "average_values",
    "academic_pressure_vs_burnout.csv": "average_values",
}


def read_output(tool: str, filename: str) -> pd.DataFrame:
    path = TOOL_DIRS[tool] / filename
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    sort_cols = [column for column in ["metric", "column", "burnout_level"] if column in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    return df


def compare_frames(reference: pd.DataFrame, candidate: pd.DataFrame) -> tuple[bool, str]:
    if list(reference.columns) != list(candidate.columns):
        return False, f"column mismatch: {list(candidate.columns)}"
    if reference.shape != candidate.shape:
        return False, f"shape mismatch: {candidate.shape} vs {reference.shape}"

    for column in reference.columns:
        left = reference[column]
        right = candidate[column]
        if pd.api.types.is_numeric_dtype(left) or pd.api.types.is_numeric_dtype(right):
            left_num = pd.to_numeric(left)
            right_num = pd.to_numeric(right)
            delta = (left_num - right_num).abs().max()
            if pd.notna(delta) and delta > TOLERANCE:
                return False, f"{column} max diff {delta}"
        else:
            mismatch = left.astype(str).reset_index(drop=True) != right.astype(str).reset_index(drop=True)
            if mismatch.any():
                return False, f"{column} values differ"
    return True, ""


def main() -> None:
    ensure_output_dirs()
    tools = [tool for tool, path in TOOL_DIRS.items() if path.exists()]
    available = [
        tool
        for tool in tools
        if all((TOOL_DIRS[tool] / filename).exists() for filename in FILES_TO_VALIDATE)
    ]

    rows = []
    if "pandas" not in available:
        rows.append(
            {
                "file": "all",
                "metric": "reference",
                "status": "FAIL",
                "notes": "pandas output is required as validation reference",
            }
        )
    else:
        for filename, metric in FILES_TO_VALIDATE.items():
            reference = read_output("pandas", filename)
            for tool in ["polars", "spark"]:
                if tool not in available:
                    rows.append(
                        {
                            "file": filename,
                            "metric": f"{metric}_{tool}",
                            "status": "SKIP",
                            "notes": f"{tool} output not found",
                        }
                    )
                    continue
                candidate = read_output(tool, filename)
                ok, notes = compare_frames(reference, candidate)
                rows.append(
                    {
                        "file": filename,
                        "metric": f"{metric}_{tool}",
                        "status": "PASS" if ok else "FAIL",
                        "notes": notes,
                    }
                )

    path = OUTPUT_DIR / "validation_results.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["file", "metric", "status", "notes"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Validation results written to {path}")
    for row in rows:
        print(f"{row['status']}: {row['file']} ({row['metric']}) {row['notes']}")


if __name__ == "__main__":
    main()
