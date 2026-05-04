from __future__ import annotations

import csv
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import psutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "student_mental_health_burnout_1M.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"

TOOL_DIRS = {
    "pandas": OUTPUT_DIR / "pandas",
    "polars": OUTPUT_DIR / "polars",
    "spark": OUTPUT_DIR / "spark",
}

COLUMN_MAP = {
    "burnout_level": "risk_level",
    "stress_level": "stress_level",
    "anxiety_level": "anxiety_score",
    "depression_level": "depression_score",
    "sleep_hours": "sleep_hours",
    "academic_pressure": "exam_pressure",
}

BURNOUT_ORDER = {"Low": 0, "Medium": 1, "High": 2}
TOLERANCE = 0.0001


def ensure_output_dirs() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    for directory in TOOL_DIRS.values():
        directory.mkdir(parents=True, exist_ok=True)


def validate_columns(columns: list[str]) -> None:
    missing = sorted(set(COLUMN_MAP.values()) - set(columns))
    if missing:
        expected = ", ".join(missing)
        raise ValueError(f"Dataset is missing required source columns: {expected}")


def append_speed_rows(rows: list[dict[str, object]]) -> None:
    ensure_output_dirs()
    path = OUTPUT_DIR / "benchmark_speed.csv"
    fallback_path = OUTPUT_DIR / "benchmark_speed_fallback.csv"
    fieldnames = ["tool", "task", "runtime_seconds"]
    try:
        file_exists = path.exists()
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)
    except PermissionError:
        file_exists = fallback_path.exists()
        with fallback_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)
        print(f"Warning: {path.name} is locked; wrote rows to {fallback_path.name}")


def upsert_resource_row(row: dict[str, object]) -> None:
    ensure_output_dirs()
    path = OUTPUT_DIR / "benchmark_resource.csv"
    fallback_path = OUTPUT_DIR / "benchmark_resource_fallback.csv"
    fieldnames = ["tool", "peak_memory_mb", "output_size_mb"]
    rows: list[dict[str, object]] = []
    target_path = path
    try:
        if path.exists():
            with path.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
    except PermissionError:
        target_path = fallback_path
        if fallback_path.exists():
            with fallback_path.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
    rows = [existing for existing in rows if existing["tool"] != row["tool"]]
    rows.append(row)
    try:
        with target_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    except PermissionError:
        rows = [existing for existing in rows if existing["tool"] != row["tool"]]
        rows.append(row)
        with fallback_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Warning: {path.name} is locked; wrote rows to {fallback_path.name}")


class SpeedTracker:
    def __init__(self, tool: str) -> None:
        self.tool = tool
        self.rows: list[dict[str, object]] = []

    @contextmanager
    def track(self, task: str):
        start = time.perf_counter()
        yield
        elapsed = time.perf_counter() - start
        self.rows.append(
            {
                "tool": self.tool,
                "task": task,
                "runtime_seconds": round(elapsed, 6),
            }
        )

    def write(self) -> None:
        append_speed_rows(self.rows)


class ResourceMonitor:
    def __init__(self, interval_seconds: float = 0.05) -> None:
        self.interval_seconds = interval_seconds
        self.process = psutil.Process(os.getpid())
        self.peak_bytes = 0
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> float:
        self._stop_event.set()
        self._thread.join()
        return self.peak_bytes / (1024 * 1024)

    def _poll(self) -> None:
        while not self._stop_event.is_set():
            try:
                total = self.process.memory_info().rss
                for child in self.process.children(recursive=True):
                    try:
                        total += child.memory_info().rss
                    except psutil.Error:
                        pass
                self.peak_bytes = max(self.peak_bytes, total)
            except psutil.Error:
                pass
            time.sleep(self.interval_seconds)


def folder_size_mb(path: Path) -> float:
    total = 0
    if path.exists():
        for file_path in path.rglob("*"):
            if file_path.is_file():
                total += file_path.stat().st_size
    return total / (1024 * 1024)


def burnout_sort_key(value: object) -> tuple[int, str]:
    text = str(value)
    return (BURNOUT_ORDER.get(text, 99), text)
