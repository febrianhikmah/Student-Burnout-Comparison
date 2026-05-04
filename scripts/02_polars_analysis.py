from __future__ import annotations

import polars as pl

from common import (
    BURNOUT_ORDER,
    COLUMN_MAP,
    DATA_PATH,
    TOOL_DIRS,
    ResourceMonitor,
    SpeedTracker,
    ensure_output_dirs,
    folder_size_mb,
    upsert_resource_row,
    validate_columns,
)


TOOL = "polars"
OUT_DIR = TOOL_DIRS[TOOL]


def sort_by_burnout(df: pl.DataFrame) -> pl.DataFrame:
    order_expr = (
        pl.when(pl.col("burnout_level") == "Low")
        .then(BURNOUT_ORDER["Low"])
        .when(pl.col("burnout_level") == "Medium")
        .then(BURNOUT_ORDER["Medium"])
        .when(pl.col("burnout_level") == "High")
        .then(BURNOUT_ORDER["High"])
        .otherwise(99)
        .alias("_sort")
    )
    return df.with_columns(order_expr).sort(["_sort", "burnout_level"]).drop("_sort")


def main() -> None:
    ensure_output_dirs()
    speed = SpeedTracker(TOOL)
    monitor = ResourceMonitor()
    monitor.start()

    with speed.track("total_pipeline"):
        with speed.track("read_csv"):
            raw = pl.read_csv(DATA_PATH)
        validate_columns(raw.columns)
        df = raw.rename({source: target for target, source in COLUMN_MAP.items()})

        with speed.track("shape_check"):
            summary = pl.DataFrame(
                {
                    "metric": ["row_count", "column_count"],
                    "value": [df.height, raw.width],
                }
            )

        with speed.track("missing_values"):
            missing = pl.DataFrame(
                {
                    "column": list(COLUMN_MAP.keys()),
                    "missing_count": [
                        int(df.select(pl.col(column).null_count()).item())
                        for column in COLUMN_MAP
                    ],
                }
            )

        with speed.track("burnout_distribution"):
            distribution = (
                df.group_by("burnout_level")
                .len(name="count")
                .with_columns((pl.col("count") / df.height * 100).alias("percentage"))
            )
            distribution = sort_by_burnout(distribution)

        with speed.track("avg_mental_health_by_burnout"):
            mental = (
                df.group_by("burnout_level")
                .agg(
                    pl.col("stress_level").mean().alias("avg_stress_level"),
                    pl.col("anxiety_level").mean().alias("avg_anxiety_level"),
                    pl.col("depression_level").mean().alias("avg_depression_level"),
                )
            )
            mental = sort_by_burnout(mental)

        with speed.track("sleep_hours_vs_burnout"):
            sleep = (
                df.group_by("burnout_level")
                .agg(
                    pl.col("sleep_hours").mean().alias("avg_sleep_hours"),
                    pl.len().alias("count"),
                )
            )
            sleep = sort_by_burnout(sleep)

        with speed.track("academic_pressure_vs_burnout"):
            academic = (
                df.group_by("burnout_level")
                .agg(
                    pl.col("academic_pressure").mean().alias("avg_academic_pressure"),
                    pl.len().alias("count"),
                )
            )
            academic = sort_by_burnout(academic)

        with speed.track("write_output"):
            summary.write_csv(OUT_DIR / "dataset_summary.csv")
            missing.write_csv(OUT_DIR / "missing_values.csv")
            distribution.write_csv(OUT_DIR / "burnout_distribution.csv")
            mental.write_csv(OUT_DIR / "avg_mental_health_by_burnout.csv")
            sleep.write_csv(OUT_DIR / "sleep_hours_vs_burnout.csv")
            academic.write_csv(OUT_DIR / "academic_pressure_vs_burnout.csv")

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
