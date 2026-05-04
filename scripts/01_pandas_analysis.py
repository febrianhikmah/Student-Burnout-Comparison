from __future__ import annotations

import pandas as pd

from common import (
    COLUMN_MAP,
    DATA_PATH,
    TOOL_DIRS,
    ResourceMonitor,
    SpeedTracker,
    burnout_sort_key,
    ensure_output_dirs,
    folder_size_mb,
    upsert_resource_row,
    validate_columns,
)


TOOL = "pandas"
OUT_DIR = TOOL_DIRS[TOOL]


def sort_by_burnout(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.assign(_sort=df["burnout_level"].map(lambda value: burnout_sort_key(value)))
        .sort_values("_sort")
        .drop(columns="_sort")
        .reset_index(drop=True)
    )


def main() -> None:
    ensure_output_dirs()
    speed = SpeedTracker(TOOL)
    monitor = ResourceMonitor()
    monitor.start()

    with speed.track("total_pipeline"):
        with speed.track("read_csv"):
            raw = pd.read_csv(DATA_PATH)
        validate_columns(list(raw.columns))
        df = raw.rename(columns={source: target for target, source in COLUMN_MAP.items()})

        with speed.track("shape_check"):
            summary = pd.DataFrame(
                [
                    {"metric": "row_count", "value": len(df)},
                    {"metric": "column_count", "value": len(raw.columns)},
                ]
            )

        with speed.track("missing_values"):
            missing = pd.DataFrame(
                {
                    "column": list(COLUMN_MAP.keys()),
                    "missing_count": [int(df[column].isna().sum()) for column in COLUMN_MAP],
                }
            )

        with speed.track("burnout_distribution"):
            distribution = (
                df.groupby("burnout_level", dropna=False)
                .size()
                .reset_index(name="count")
            )
            distribution["percentage"] = distribution["count"] / len(df) * 100
            distribution = sort_by_burnout(distribution)

        with speed.track("avg_mental_health_by_burnout"):
            mental = (
                df.groupby("burnout_level", dropna=False)[
                    ["stress_level", "anxiety_level", "depression_level"]
                ]
                .mean()
                .reset_index()
                .rename(
                    columns={
                        "stress_level": "avg_stress_level",
                        "anxiety_level": "avg_anxiety_level",
                        "depression_level": "avg_depression_level",
                    }
                )
            )
            mental = sort_by_burnout(mental)

        with speed.track("sleep_hours_vs_burnout"):
            sleep = (
                df.groupby("burnout_level", dropna=False)
                .agg(avg_sleep_hours=("sleep_hours", "mean"), count=("sleep_hours", "size"))
                .reset_index()
            )
            sleep = sort_by_burnout(sleep)

        with speed.track("academic_pressure_vs_burnout"):
            academic = (
                df.groupby("burnout_level", dropna=False)
                .agg(
                    avg_academic_pressure=("academic_pressure", "mean"),
                    count=("academic_pressure", "size"),
                )
                .reset_index()
            )
            academic = sort_by_burnout(academic)

        with speed.track("write_output"):
            summary.to_csv(OUT_DIR / "dataset_summary.csv", index=False)
            missing.to_csv(OUT_DIR / "missing_values.csv", index=False)
            distribution.to_csv(OUT_DIR / "burnout_distribution.csv", index=False)
            mental.to_csv(OUT_DIR / "avg_mental_health_by_burnout.csv", index=False)
            sleep.to_csv(OUT_DIR / "sleep_hours_vs_burnout.csv", index=False)
            academic.to_csv(OUT_DIR / "academic_pressure_vs_burnout.csv", index=False)

    peak_memory_mb = monitor.stop()
    speed.write()
    upsert_resource_row(
        {
            "tool": TOOL,
            "peak_memory_mb": round(peak_memory_mb, 2),
            "output_size_mb": round(folder_size_mb(OUT_DIR), 2),
        }
    )
    print(f"{TOOL} analysis complete. Output: {OUT_DIR}")


if __name__ == "__main__":
    main()
