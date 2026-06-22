from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


KEY_COLUMNS = [
    "SepsisLabel",
    "sofa_24hours",
    "gcs_total",
    "pao2fio2ratio",
    "urine_output_24h",
    "ventilation_status",
    "curr_service",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate MIMIC-IV wide parquet snapshot.")
    parser.add_argument("--input", required=True, help="Input parquet path")
    parser.add_argument("--output", required=True, help="Output markdown path")
    parser.add_argument(
        "--label",
        default="snapshot",
        help="Short label for the validation run",
    )
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    summary = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT stay_id) AS stays,
            COUNT(DISTINCT subject_id) AS subjects,
            COUNT(DISTINCT hadm_id) AS hadm,
            COUNT(DISTINCT hr) AS distinct_hr_values,
            MIN(hr) AS min_hr,
            MAX(hr) AS max_hr,
            SUM(SepsisLabel) AS sepsis_rows,
            COUNT(DISTINCT CASE WHEN SepsisLabel = 1 THEN stay_id END) AS sepsis_stays
        FROM read_parquet('{input_path.as_posix()}')
        """
    ).fetchone()

    nonnull_rows = con.execute(
        f"""
        SELECT
            {", ".join(
                f"ROUND(100.0 * AVG(CASE WHEN {col} IS NOT NULL THEN 1 ELSE 0 END), 2) AS {col}_nonnull_pct"
                for col in KEY_COLUMNS
            )}
        FROM read_parquet('{input_path.as_posix()}')
        """
    ).fetchdf()

    lines = [
        f"# MIMIC-IV Wide Validation: {args.label}",
        "",
        f"- input: `{input_path.as_posix()}`",
        "",
        "## Summary",
        "",
        f"- rows: `{summary[0]}`",
        f"- stays: `{summary[1]}`",
        f"- subjects: `{summary[2]}`",
        f"- hadm: `{summary[3]}`",
        f"- distinct hr values: `{summary[4]}`",
        f"- hr range: `{summary[5]}` to `{summary[6]}`",
        f"- sepsis rows: `{int(summary[7])}`",
        f"- sepsis stays: `{summary[8]}`",
        "",
        "## Key Non-Null Rates",
        "",
        "| Column | Non-null % |",
        "|---|---:|",
    ]

    row = nonnull_rows.iloc[0].to_dict()
    for col in KEY_COLUMNS:
        lines.append(f"| {col} | {row[f'{col}_nonnull_pct']:.2f} |")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
