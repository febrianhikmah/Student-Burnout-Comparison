from __future__ import annotations

from pathlib import Path

import pandas as pd

from common import OUTPUT_DIR


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"Missing: {path}")
        return pd.DataFrame()
    return pd.read_csv(path)


def read_benchmark_pair(primary_name: str, fallback_name: str) -> pd.DataFrame:
    frames = []
    for name in [primary_name, fallback_name]:
        path = OUTPUT_DIR / name
        if path.exists():
            frames.append(pd.read_csv(path))
    if not frames:
        print(f"Missing: {OUTPUT_DIR / primary_name}")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    speed = read_benchmark_pair("benchmark_speed.csv", "benchmark_speed_fallback.csv")
    resource = read_benchmark_pair("benchmark_resource.csv", "benchmark_resource_fallback.csv")
    validation = read_csv_if_exists(OUTPUT_DIR / "validation_results.csv")

    if not speed.empty:
        latest_speed = speed.drop_duplicates(["tool", "task"], keep="last")
        print("\nFastest tool by task:")
        for task, group in latest_speed.groupby("task"):
            winner = group.sort_values("runtime_seconds").iloc[0]
            print(f"- {task}: {winner['tool']} ({winner['runtime_seconds']:.4f}s)")

        total = latest_speed[latest_speed["task"] == "total_pipeline"].sort_values("runtime_seconds")
        if not total.empty:
            print("\nTotal runtime:")
            for _, row in total.iterrows():
                print(f"- {row['tool']}: {row['runtime_seconds']:.4f}s")

    if not resource.empty:
        print("\nResource usage:")
        for _, row in resource.iterrows():
            print(
                f"- {row['tool']}: peak_memory={row['peak_memory_mb']} MB, "
                f"output_size={row['output_size_mb']} MB"
            )

    if not validation.empty:
        print("\nCorrectness:")
        status_counts = validation["status"].value_counts().to_dict()
        print(status_counts)
        failures = validation[validation["status"] == "FAIL"]
        if not failures.empty:
            print("\nFailures:")
            print(failures.to_string(index=False))


if __name__ == "__main__":
    main()
