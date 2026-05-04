from __future__ import annotations

import csv
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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


TOOL = "spark"
OUT_DIR = TOOL_DIRS[TOOL]


def write_dicts_csv(rows, columns, path) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_spark_csv(df, path) -> None:
    columns = df.columns
    rows = df._jdf.collect()
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row.get(index) for index in range(len(columns))])


def remove_success_markers() -> None:
    for marker in OUT_DIR.glob("._SUCCESS.crc"):
        marker.unlink(missing_ok=True)
    for marker in OUT_DIR.glob("_SUCCESS"):
        marker.unlink(missing_ok=True)


def with_burnout_order(df):
    order_expr = (
        F.when(F.col("burnout_level") == "Low", F.lit(BURNOUT_ORDER["Low"]))
        .when(F.col("burnout_level") == "Medium", F.lit(BURNOUT_ORDER["Medium"]))
        .when(F.col("burnout_level") == "High", F.lit(BURNOUT_ORDER["High"]))
        .otherwise(F.lit(99))
    )
    return df.withColumn("_sort", order_expr).orderBy("_sort", "burnout_level").drop("_sort")


def main() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    ensure_output_dirs()
    speed = SpeedTracker(TOOL)
    monitor = ResourceMonitor(interval_seconds=0.1)
    monitor.start()

    spark = (
        SparkSession.builder.appName("student-burnout-comparison")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        with speed.track("total_pipeline"):
            with speed.track("read_csv"):
                raw = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA_PATH))
                raw.cache()
                raw.count()
            validate_columns(raw.columns)
            df = raw
            for target, source in COLUMN_MAP.items():
                if target != source:
                    df = df.withColumnRenamed(source, target)

            with speed.track("shape_check"):
                row_count = df.count()
                column_count = len(raw.columns)
                summary = [
                    {"metric": "row_count", "value": row_count},
                    {"metric": "column_count", "value": column_count},
                ]

            with speed.track("missing_values"):
                missing_rows = []
                for column in COLUMN_MAP:
                    count = df.filter(F.col(column).isNull()).count()
                    missing_rows.append({"column": column, "missing_count": count})

            with speed.track("burnout_distribution"):
                distribution = (
                    df.groupBy("burnout_level")
                    .count()
                    .withColumn("percentage", F.col("count") / F.lit(row_count) * F.lit(100))
                )
                distribution = with_burnout_order(distribution)

            with speed.track("avg_mental_health_by_burnout"):
                mental = df.groupBy("burnout_level").agg(
                    F.avg("stress_level").alias("avg_stress_level"),
                    F.avg("anxiety_level").alias("avg_anxiety_level"),
                    F.avg("depression_level").alias("avg_depression_level"),
                )
                mental = with_burnout_order(mental)

            with speed.track("sleep_hours_vs_burnout"):
                sleep = df.groupBy("burnout_level").agg(
                    F.avg("sleep_hours").alias("avg_sleep_hours"),
                    F.count("*").alias("count"),
                )
                sleep = with_burnout_order(sleep)

            with speed.track("academic_pressure_vs_burnout"):
                academic = df.groupBy("burnout_level").agg(
                    F.avg("academic_pressure").alias("avg_academic_pressure"),
                    F.count("*").alias("count"),
                )
                academic = with_burnout_order(academic)

            with speed.track("write_output"):
                write_dicts_csv(summary, ["metric", "value"], OUT_DIR / "dataset_summary.csv")
                write_dicts_csv(
                    missing_rows,
                    ["column", "missing_count"],
                    OUT_DIR / "missing_values.csv",
                )
                write_spark_csv(distribution, OUT_DIR / "burnout_distribution.csv")
                write_spark_csv(mental, OUT_DIR / "avg_mental_health_by_burnout.csv")
                write_spark_csv(sleep, OUT_DIR / "sleep_hours_vs_burnout.csv")
                write_spark_csv(academic, OUT_DIR / "academic_pressure_vs_burnout.csv")
                remove_success_markers()
    finally:
        spark.stop()

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
