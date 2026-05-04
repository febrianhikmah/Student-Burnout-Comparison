# Student Burnout Data Analysis: pandas vs Polars vs Spark

Project ini membandingkan pandas, Polars, dan PySpark untuk menjalankan analisis data yang sama pada dataset CSV lokal berukuran menengah.

Project ini tidak berfokus pada machine learning. Fokus utamanya adalah data processing, aggregation, benchmarking, dan correctness validation.

## Dataset

Dataset yang digunakan:

- File: `data/student_mental_health_burnout_1M.csv`
- Rows: 1,000,000
- Columns: 20
- Size: sekitar 281 MB
- Format: CSV
- Environment: local laptop / single machine

## Objective

Tujuan eksperimen:

- Membandingkan runtime pandas, Polars, dan Spark pada workload yang sama.
- Membandingkan estimasi peak memory setiap tool.
- Memastikan semua tool menghasilkan output analisis yang konsisten.
- Memahami kapan pandas cukup, kapan Polars lebih efisien, dan kapan Spark mulai relevan.

## Analysis Scope

Analisis yang dijalankan oleh setiap tool:

- Read CSV
- Dataset shape check
- Missing values
- Burnout level distribution
- Average stress, anxiety, and depression by burnout level
- Average sleep hours by burnout level
- Average academic pressure by burnout level
- Output writing
- Total pipeline benchmark


## Project Structure

```text
student-burnout-comparison/
|-- data/
|   `-- student_mental_health_burnout_1M.csv
|-- output/
|   |-- pandas/
|   |-- polars/
|   |-- spark/
|   |-- benchmark_speed.csv
|   |-- benchmark_resource.csv
|   `-- validation_results.csv
|-- scripts/
|   |-- common.py
|   |-- 00_check_dataset.py
|   |-- 01_pandas_analysis.py
|   |-- 02_polars_analysis.py
|   |-- 03_spark_analysis.py
|   |-- 04_validate_correctness.py
|   `-- 05_report_benchmark.py
|-- requirements.txt
`-- README.md
```

## How to Run

```powershell
python scripts/00_check_dataset.py
python scripts/01_pandas_analysis.py
python scripts/02_polars_analysis.py
python scripts/03_spark_analysis.py
python scripts/04_validate_correctness.py
python scripts/05_report_benchmark.py
```

## Output Files

Setiap tool menghasilkan file berikut di `output/{tool}/`:

- `dataset_summary.csv`
- `missing_values.csv`
- `burnout_distribution.csv`
- `avg_mental_health_by_burnout.csv`
- `sleep_hours_vs_burnout.csv`
- `academic_pressure_vs_burnout.csv`

Benchmark dan validasi global:

- `output/benchmark_speed.csv`
- `output/benchmark_resource.csv`
- `output/validation_results.csv`

## Benchmark Results

Hasil benchmark pada run lokal terakhir:

| Tool | Total runtime seconds | Peak memory MB |
| --- | ---: | ---: | ---: |
| pandas | 3.0299 | 553.68 |
| Polars | 0.8659 | 491.65 |
| Spark | 19.6320 | 956.46 |


## Speed Breakdown

| Task | pandas | Polars | Spark |
| --- | ---: | ---: | ---: |
| read_csv | 2.5689 | 0.6993 | 12.6685 |
| shape_check | 0.0004 | 0.0012 | 0.2645 |
| missing_values | 0.0490 | 0.0020 | 2.0778 |
| burnout_distribution | 0.0920 | 0.0524 | 0.1398 |
| avg_mental_health_by_burnout | 0.1006 | 0.0270 | 0.0951 |
| sleep_hours_vs_burnout | 0.0869 | 0.0353 | 0.0747 |
| academic_pressure_vs_burnout | 0.0736 | 0.0221 | 0.0781 |
| write_output | 0.0107 | 0.0243 | 4.1720 |
| total_pipeline | 3.0299 | 0.8659 | 19.6320 |

## Correctness Validation

Validation membandingkan output Polars dan Spark terhadap output pandas sebagai baseline reference.

| Validation file | Polars | Spark |
| --- | --- | --- |
| `dataset_summary.csv` | PASS | PASS |
| `missing_values.csv` | PASS | PASS |
| `burnout_distribution.csv` | PASS | PASS |
| `avg_mental_health_by_burnout.csv` | PASS | PASS |
| `sleep_hours_vs_burnout.csv` | PASS | PASS |
| `academic_pressure_vs_burnout.csv` | PASS | PASS |

Semua 12 validation checks menghasilkan status `PASS`.

## Discussion

Pada dataset CSV 1 juta rows di single machine, Polars menjadi tool paling efisien. Total runtime Polars adalah 0.8659 detik, sekitar 3.5x lebih cepat dari pandas dan sekitar 22.7x lebih cepat dari Spark lokal.

Keunggulan Polars paling terlihat pada `read_csv` dan operasi agregasi. Polars membaca CSV dalam 0.6993 detik, sedangkan pandas membutuhkan 2.5689 detik dan Spark membutuhkan 12.6685 detik. Untuk workload berukuran menengah seperti ini, overhead Spark seperti SparkSession startup, JVM runtime, schema inference, dan local execution cost lebih besar daripada manfaat distributed processing.

Dari sisi memory, Polars juga menggunakan estimasi peak memory paling rendah, yaitu 491.65 MB. Pandas berada di 553.68 MB, masih cukup efisien untuk ukuran data ini. Spark menggunakan 956.46 MB karena berjalan di atas JVM dan membawa runtime tambahan walaupun dijalankan dalam mode lokal.

Hasil ini menunjukkan bahwa Spark tidak selalu menjadi pilihan terbaik untuk semua ukuran data. Pada dataset yang masih nyaman diproses dalam satu mesin, pandas dan Polars lebih sederhana dan lebih cepat. Spark mulai lebih relevan ketika ukuran data, kebutuhan paralelisme, atau kebutuhan distributed processing sudah melebihi kapasitas single-machine workflow.

## Conclusion

Untuk workload lokal 1 juta rows:

- Polars adalah pilihan terbaik dari sisi speed dan memory.
- Pandas tetap layak untuk workflow sederhana dan eksplorasi cepat.
- Spark memiliki overhead paling besar pada mode lokal, sehingga belum optimal untuk dataset menengah di satu laptop.

Eksperimen ini menegaskan bahwa pemilihan tool data processing harus mempertimbangkan ukuran data, environment eksekusi, overhead runtime, dan kebutuhan scaling, bukan sekadar memilih tool yang paling besar atau paling populer.

