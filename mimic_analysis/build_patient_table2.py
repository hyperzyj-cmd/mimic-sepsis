from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd
from scipy import stats


ROOT = Path(r"D:\ESILV_S2\Intern")
OUT_DIR = ROOT / "build_mimic" / "output_alignment_analysis"
M3_PATH = ROOT / "build_mimic" / "mimiciii" / "output" / "mimic3_wide.parquet"
M4_PATH = ROOT / "build_mimic" / "mimiciv" / "output" / "mimic4_wide.parquet"


@dataclass
class DatasetSpec:
    name: str
    parquet_path: Path
    subject_col: str
    stay_col: str
    intime_col: str
    age_col: str
    gender_col: str
    race_col: str
    race_grouped_expr: str
    charlson_col: str
    los_icu_col: str
    hospital_expire_col: str
    death_time_expr: str
    sepsis_col: str
    sofa_col: str
    vent_expr: str
    height_col: str
    weight_col: str
    sirs_expr: str


M3_SPEC = DatasetSpec(
    name="mimic3",
    parquet_path=M3_PATH,
    subject_col="SUBJECT_ID",
    stay_col="ICUSTAY_ID",
    intime_col="INTIME",
    age_col="age",
    gender_col="GENDER",
    race_col="ETHNICITY",
    race_grouped_expr="LOWER(COALESCE(ethnicity_grouped, 'unknown'))",
    charlson_col="elixhauser_vanwalraven",
    los_icu_col="los_icu",
    hospital_expire_col="hospital_expire_flag",
    death_time_expr="COALESCE(DEATHTIME, DOD_HOSP, DOD)",
    sepsis_col="SepsisLabel",
    sofa_col="sofa_total",
    vent_expr="CASE WHEN COALESCE(vent_flag, 0) = 1 THEN 1 ELSE 0 END",
    height_col="height_cm",
    weight_col="weight_kg",
    sirs_expr="sirs",
)

M4_SPEC = DatasetSpec(
    name="mimic4",
    parquet_path=M4_PATH,
    subject_col="subject_id",
    stay_col="stay_id",
    intime_col="intime",
    age_col="age",
    gender_col="gender",
    race_col="race",
    race_grouped_expr="""
        CASE
            WHEN race IS NULL THEN 'unknown'
            WHEN UPPER(race) IN ('UNKNOWN', 'UNABLE TO OBTAIN', 'PATIENT DECLINED TO ANSWER') THEN 'unknown'
            WHEN UPPER(race) LIKE 'WHITE%' OR UPPER(race) = 'PORTUGUESE' THEN 'white'
            WHEN UPPER(race) LIKE 'BLACK%' THEN 'black'
            WHEN UPPER(race) LIKE 'HISPANIC%' OR UPPER(race) LIKE 'SOUTH AMERICAN%' THEN 'hispanic'
            WHEN UPPER(race) LIKE 'ASIAN%' THEN 'asian'
            WHEN UPPER(race) LIKE 'AMERICAN INDIAN%' OR UPPER(race) LIKE '%ALASKA NATIVE%' THEN 'native'
            WHEN UPPER(race) LIKE 'PATIENT DECLINED%' OR UPPER(race) LIKE 'UNABLE TO OBTAIN%' THEN 'unknown'
            ELSE 'other'
        END
    """,
    charlson_col="charlson_score",
    los_icu_col="los_icu",
    hospital_expire_col="hospital_expire_flag",
    death_time_expr="COALESCE(deathtime, dod)",
    sepsis_col="SepsisLabel",
    sofa_col="sofa_24hours",
    vent_expr="""
        CASE
            WHEN ventilation_status IN ('InvasiveVent', 'NonInvasiveVent', 'Tracheostomy') THEN 1
            ELSE 0
        END
    """,
    height_col="height",
    weight_col="weight",
    sirs_expr="""
        COALESCE(CASE WHEN temperature < 36 OR temperature > 38 THEN 1 ELSE 0 END, 0)
      + COALESCE(CASE WHEN heart_rate > 90 THEN 1 ELSE 0 END, 0)
      + COALESCE(CASE WHEN resp_rate > 20 THEN 1 ELSE 0 END, 0)
      + COALESCE(CASE WHEN wbc < 4 OR wbc > 12 THEN 1 ELSE 0 END, 0)
    """,
)


def quantile_fmt(series: pd.Series) -> str:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return "NA"
    q1, med, q3 = s.quantile([0.25, 0.5, 0.75]).tolist()
    return f"{med:.1f} [{q1:.1f}, {q3:.1f}]"


def mean_sd_fmt(series: pd.Series) -> str:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return "NA"
    return f"{s.mean():.1f} +/- {s.std(ddof=1):.1f}"


def count_pct_fmt(mask: pd.Series) -> str:
    valid = mask.fillna(False)
    n = int(valid.sum())
    total = int(len(valid))
    pct = (n / total * 100.0) if total else 0.0
    return f"{n} ({pct:.1f}%)"


def pvalue_fmt(p: float | None) -> str:
    if p is None or pd.isna(p):
        return ""
    if p < 0.001:
        return "<0.001"
    return f"{p:.3f}"


def mask_count_pct_fmt(mask: pd.Series, within: pd.Series | None = None) -> str:
    if within is None:
        within = pd.Series(True, index=mask.index)
    valid = mask.fillna(False) & within.fillna(False)
    total = int(within.fillna(False).sum())
    n = int(valid.sum())
    pct = (n / total * 100.0) if total else 0.0
    return f"{n} ({pct:.1f}%)"


def build_patient_cohort(con: duckdb.DuckDBPyConnection, spec: DatasetSpec) -> pd.DataFrame:
    query = f"""
    WITH base AS (
        SELECT *
        FROM read_parquet('{spec.parquet_path.as_posix()}')
    ),
    stay_level AS (
        SELECT
            {spec.subject_col} AS subject_id,
            {spec.stay_col} AS stay_id,
            MIN({spec.intime_col}) AS intime,
            MAX({spec.age_col}) AS age,
            MAX({spec.gender_col}) AS gender,
            MAX({spec.race_col}) AS race_raw,
            MAX({spec.race_grouped_expr}) AS race_grouped,
            MAX({spec.charlson_col}) AS charlson_score,
            MAX({spec.los_icu_col}) AS los_icu,
            MAX({spec.hospital_expire_col}) AS hospital_expire_flag,
            MAX({spec.death_time_expr}) AS death_time,
            MAX(COALESCE({spec.sepsis_col}, 0)) AS sepsis_any,
            MAX(CASE WHEN hr BETWEEN 0 AND 23 THEN {spec.sofa_col} END) AS sofa_day1,
            MAX(CASE WHEN hr BETWEEN 0 AND 23 THEN {spec.sirs_expr} END) AS sirs_day1_max,
            MAX(CASE WHEN {spec.vent_expr} = 1 THEN 1 ELSE 0 END) AS vent_any,
            ARG_MIN(
                CASE
                    WHEN {spec.height_col} IS NOT NULL AND {spec.weight_col} IS NOT NULL
                         AND {spec.height_col} > 0 AND {spec.weight_col} > 0
                    THEN {spec.weight_col} / POWER({spec.height_col} / 100.0, 2)
                    ELSE NULL
                END,
                CASE WHEN hr >= 0 THEN hr ELSE NULL END
            ) AS bmi
        FROM base
        GROUP BY 1, 2
    ),
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime, stay_id) AS stay_rank
        FROM stay_level
    )
    SELECT
        subject_id,
        stay_id,
        intime,
        age,
        gender,
        race_raw,
        race_grouped,
        charlson_score,
        los_icu,
        hospital_expire_flag,
        death_time,
        sepsis_any,
        sofa_day1,
        sirs_day1_max,
        vent_any,
        bmi
    FROM ranked
    WHERE stay_rank = 1
    ORDER BY subject_id
    """
    df = con.execute(query).df()
    df["intime"] = pd.to_datetime(df["intime"], errors="coerce")
    df["death_time"] = pd.to_datetime(df["death_time"], errors="coerce")
    df["mortality_30d"] = (
        df["death_time"].notna()
        & df["intime"].notna()
        & ((df["death_time"] - df["intime"]).dt.total_seconds() <= 30 * 24 * 3600)
        & ((df["death_time"] - df["intime"]).dt.total_seconds() >= 0)
    )
    df["hospital_expire_flag"] = df["hospital_expire_flag"].fillna(0).astype(int)
    df["sepsis_any"] = df["sepsis_any"].fillna(0).astype(int)
    df["vent_any"] = df["vent_any"].fillna(0).astype(int)
    df["male"] = df["gender"].astype(str).str.upper().str.startswith("M")
    return df


def summarize_groups(df: pd.DataFrame) -> pd.DataFrame:
    groups: list[tuple[str, pd.Series]] = [
        ("All patients", pd.Series(True, index=df.index)),
        ("Survivors", df["hospital_expire_flag"] == 0),
        ("Non-survivors", df["hospital_expire_flag"] == 1),
        ("Sepsis", df["sepsis_any"] == 1),
        ("Non-sepsis", df["sepsis_any"] == 0),
    ]

    race_order = ["white", "black", "hispanic", "asian", "native", "other", "unknown"]
    rows: list[dict[str, str]] = []

    def add_row(variable: str, formatter: Callable[[pd.DataFrame], str]) -> None:
        row = {"Variable": variable}
        for name, mask in groups:
            row[name] = formatter(df.loc[mask])
        rows.append(row)

    add_row("N", lambda x: f"{len(x):,}")
    add_row("Age (y) [Q1-Q3]", lambda x: quantile_fmt(x["age"]))
    add_row("Male, n (%)", lambda x: count_pct_fmt(x["male"]))
    add_row("BMI (kg/m^2), mean +/- SD", lambda x: mean_sd_fmt(x["bmi"]))

    for race_value in race_order:
        add_row(
            f"Race: {race_value}",
            lambda x, race_value=race_value: count_pct_fmt(x["race_grouped"].fillna("unknown") == race_value),
        )

    add_row("Comorbidity index [Q1-Q3]", lambda x: quantile_fmt(x["charlson_score"]))
    add_row("SIRS first-day max [Q1-Q3]", lambda x: quantile_fmt(x["sirs_day1_max"]))
    add_row("SOFA first-day [Q1-Q3]", lambda x: quantile_fmt(x["sofa_day1"]))
    add_row("Mechanical ventilation, n (%)", lambda x: count_pct_fmt(x["vent_any"] == 1))
    add_row("ICU length-of-stay (d) [Q1-Q3]", lambda x: quantile_fmt(x["los_icu"]))
    add_row("30-day mortality, n (%)", lambda x: count_pct_fmt(x["mortality_30d"]))
    add_row("Hospital mortality, n (%)", lambda x: count_pct_fmt(x["hospital_expire_flag"] == 1))

    return pd.DataFrame(rows)


def pct_header(n_str: str, total: int) -> str:
    n = int(str(n_str).replace(",", ""))
    pct = (n / total * 100.0) if total else 0.0
    return f"{n:,} ({pct:.1f}%)"


def continuous_pvalue(
    df: pd.DataFrame,
    group_col: str,
    positive_value: int | bool,
    value_col: str,
    method: str = "mw",
) -> str:
    x1 = pd.to_numeric(df.loc[df[group_col] == positive_value, value_col], errors="coerce").dropna()
    x0 = pd.to_numeric(df.loc[df[group_col] != positive_value, value_col], errors="coerce").dropna()
    if len(x1) == 0 or len(x0) == 0:
        return ""
    try:
        if method == "ttest":
            p = stats.ttest_ind(x1, x0, equal_var=False, nan_policy="omit").pvalue
        else:
            p = stats.mannwhitneyu(x1, x0, alternative="two-sided").pvalue
    except Exception:
        return ""
    return pvalue_fmt(float(p))


def binary_pvalue(df: pd.DataFrame, group_col: str, positive_value: int | bool, value_mask: pd.Series) -> str:
    g1 = df[group_col] == positive_value
    g0 = df[group_col] != positive_value
    a = int((g1 & value_mask).sum())
    b = int((g1 & ~value_mask).sum())
    c = int((g0 & value_mask).sum())
    d = int((g0 & ~value_mask).sum())
    try:
        p = stats.chi2_contingency([[a, b], [c, d]], correction=False).pvalue
    except Exception:
        return ""
    return pvalue_fmt(float(p))


def race_pvalue(df: pd.DataFrame, group_col: str, positive_value: int | bool) -> str:
    race_order = ["white", "black", "hispanic", "asian", "native", "other", "unknown"]
    g1 = df[group_col] == positive_value
    g0 = df[group_col] != positive_value
    table = []
    race_series = df["race_grouped"].fillna("unknown")
    for race_value in race_order:
        table.append([
            int((g1 & (race_series == race_value)).sum()),
            int((g0 & (race_series == race_value)).sum()),
        ])
    try:
        p = stats.chi2_contingency(table, correction=False).pvalue
    except Exception:
        return ""
    return pvalue_fmt(float(p))


def write_markdown_table(df: pd.DataFrame, path: Path, title: str) -> None:
    lines = [f"# {title}", ""]
    lines.extend(markdown_lines(df))
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def markdown_lines(df: pd.DataFrame) -> list[str]:
    header = "| " + " | ".join(df.columns) + " |"
    sep = "| " + " | ".join(["---"] * len(df.columns)) + " |"
    lines = [header, sep]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in df.columns) + " |")
    return lines


def table_with_n_in_headers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    n_row = out[out["Variable"] == "N"].iloc[0]
    total = int(str(n_row["All patients"]).replace(",", ""))
    rename_map = {
        "All patients": f"All patients (N = {pct_header(n_row['All patients'], total)})",
        "Survivors": f"Survivors (N = {pct_header(n_row['Survivors'], total)})",
        "Non-survivors": f"Non-survivors (N = {pct_header(n_row['Non-survivors'], total)})",
        "Sepsis": f"Sepsis (N = {pct_header(n_row['Sepsis'], total)})",
        "Non-sepsis": f"Non-sepsis (N = {pct_header(n_row['Non-sepsis'], total)})",
    }
    out = out.rename(columns=rename_map)
    out = out[out["Variable"] != "N"].reset_index(drop=True)
    return out


def build_pretty_table(df: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    n_row = summary_df[summary_df["Variable"] == "N"].iloc[0]
    total = int(str(n_row["All patients"]).replace(",", ""))
    cols = {
        "Variable": [],
        f"All patients (N = {pct_header(n_row['All patients'], total)})": [],
        f"Survivors (N = {pct_header(n_row['Survivors'], total)})": [],
        f"Non-survivors (N = {pct_header(n_row['Non-survivors'], total)})": [],
        "P-value (Survival)": [],
        f"Sepsis (N = {pct_header(n_row['Sepsis'], total)})": [],
        f"Non-sepsis (N = {pct_header(n_row['Non-sepsis'], total)})": [],
        "P-value (Sepsis)": [],
    }
    surv0 = df["hospital_expire_flag"] == 0
    surv1 = df["hospital_expire_flag"] == 1
    sep1 = df["sepsis_any"] == 1
    sep0 = df["sepsis_any"] == 0

    def add_row(
        variable: str,
        all_val: str,
        surv_val: str,
        nonsurv_val: str,
        p_surv: str,
        sepsis_val: str,
        nonsepsis_val: str,
        p_sepsis: str,
    ) -> None:
        cols["Variable"].append(variable)
        cols[next(k for k in cols if k.startswith("All patients"))].append(all_val)
        cols[next(k for k in cols if k.startswith("Survivors"))].append(surv_val)
        cols[next(k for k in cols if k.startswith("Non-survivors"))].append(nonsurv_val)
        cols["P-value (Survival)"].append(p_surv)
        cols[next(k for k in cols if k.startswith("Sepsis"))].append(sepsis_val)
        cols[next(k for k in cols if k.startswith("Non-sepsis"))].append(nonsepsis_val)
        cols["P-value (Sepsis)"].append(p_sepsis)

    add_row(
        "Age (y) [Q1-Q3]",
        quantile_fmt(df["age"]),
        quantile_fmt(df.loc[df["hospital_expire_flag"] == 0, "age"]),
        quantile_fmt(df.loc[df["hospital_expire_flag"] == 1, "age"]),
        continuous_pvalue(df, "hospital_expire_flag", 1, "age"),
        quantile_fmt(df.loc[df["sepsis_any"] == 1, "age"]),
        quantile_fmt(df.loc[df["sepsis_any"] == 0, "age"]),
        continuous_pvalue(df, "sepsis_any", 1, "age"),
    )
    add_row(
        "Male, n (%)",
        mask_count_pct_fmt(df["male"]),
        mask_count_pct_fmt(df["male"], surv0),
        mask_count_pct_fmt(df["male"], surv1),
        binary_pvalue(df, "hospital_expire_flag", 1, df["male"]),
        mask_count_pct_fmt(df["male"], sep1),
        mask_count_pct_fmt(df["male"], sep0),
        binary_pvalue(df, "sepsis_any", 1, df["male"]),
    )
    add_row(
        "BMI (kg/m^2), mean +/- SD",
        mean_sd_fmt(df["bmi"]),
        mean_sd_fmt(df.loc[df["hospital_expire_flag"] == 0, "bmi"]),
        mean_sd_fmt(df.loc[df["hospital_expire_flag"] == 1, "bmi"]),
        continuous_pvalue(df, "hospital_expire_flag", 1, "bmi", method="ttest"),
        mean_sd_fmt(df.loc[df["sepsis_any"] == 1, "bmi"]),
        mean_sd_fmt(df.loc[df["sepsis_any"] == 0, "bmi"]),
        continuous_pvalue(df, "sepsis_any", 1, "bmi", method="ttest"),
    )
    add_row("Race, n (%)", "", "", "", race_pvalue(df, "hospital_expire_flag", 1), "", "", race_pvalue(df, "sepsis_any", 1))
    race_order = ["white", "black", "hispanic", "asian", "native", "other", "unknown"]
    for race_value in race_order:
        mask = df["race_grouped"].fillna("unknown") == race_value
        add_row(
            f"  {race_value.capitalize()}",
            mask_count_pct_fmt(mask),
            mask_count_pct_fmt(mask, surv0),
            mask_count_pct_fmt(mask, surv1),
            "",
            mask_count_pct_fmt(mask, sep1),
            mask_count_pct_fmt(mask, sep0),
            "",
        )
    for variable, col, method in [
        ("Comorbidity index [Q1-Q3]", "charlson_score", "mw"),
        ("SIRS first-day max [Q1-Q3]", "sirs_day1_max", "mw"),
        ("SOFA first-day [Q1-Q3]", "sofa_day1", "mw"),
        ("ICU length-of-stay (d) [Q1-Q3]", "los_icu", "mw"),
    ]:
        add_row(
            variable,
            quantile_fmt(df[col]),
            quantile_fmt(df.loc[df["hospital_expire_flag"] == 0, col]),
            quantile_fmt(df.loc[df["hospital_expire_flag"] == 1, col]),
            continuous_pvalue(df, "hospital_expire_flag", 1, col, method=method),
            quantile_fmt(df.loc[df["sepsis_any"] == 1, col]),
            quantile_fmt(df.loc[df["sepsis_any"] == 0, col]),
            continuous_pvalue(df, "sepsis_any", 1, col, method=method),
        )
    for variable, mask in [
        ("Mechanical ventilation, n (%)", df["vent_any"] == 1),
        ("30-day mortality, n (%)", df["mortality_30d"] == 1),
        ("Hospital mortality, n (%)", df["hospital_expire_flag"] == 1),
    ]:
        add_row(
            variable,
            mask_count_pct_fmt(mask),
            mask_count_pct_fmt(mask, surv0),
            mask_count_pct_fmt(mask, surv1),
            binary_pvalue(df, "hospital_expire_flag", 1, mask),
            mask_count_pct_fmt(mask, sep1),
            mask_count_pct_fmt(mask, sep0),
            binary_pvalue(df, "sepsis_any", 1, mask),
        )
    return pd.DataFrame(cols)


SUMMARY_MD = ROOT / "mimic_analysis" / "summary_output" / "Mimic_analysis_summary.md"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    outputs: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []
    for spec in (M3_SPEC, M4_SPEC):
        patient_df = build_patient_cohort(con, spec)
        summary_df = summarize_groups(patient_df)
        outputs.append((spec.name, patient_df, summary_df))

    # Build Part 2 content
    part2_lines = [
        "## Part 2: Patient-Level Cohort Summary",
        "",
        "**Inclusion:** One row per patient, first ICU stay only.",
        "",
        "**Definitions:**",
        "- **SOFA:** first-day maximum SOFA score",
        "- **SIRS:** first-day maximum SIRS score",
        "- **Comorbidity index:** Elixhauser van Walraven score (MIMIC-III, official mimic-code ICD-9 implementation) / Charlson Comorbidity Index with age adjustment (MIMIC-IV, official mimic-code ICD-9 + ICD-10 implementation). *Scores are not directly comparable across datasets.*",
        "- **Mechanical ventilation:** any invasive/non-invasive ventilation or tracheostomy during the first ICU stay",
        "- **30-day mortality:** measured from first ICU admission time",
        "- **Race/ethnicity:** grouped into 7 categories (White, Black, Hispanic, Asian, Native, Other, Unknown)",
        "",
    ]
    table_num = 4
    table_labels = {"mimic3": "MIMIC-III Patient Characteristics", "mimic4": "MIMIC-IV Patient Characteristics"}
    for name, patient_df, summary_df in outputs:
        part2_lines.append(f"### Table {table_num}. {table_labels[name]}")
        part2_lines.append("")
        part2_lines.extend(markdown_lines(build_pretty_table(patient_df, summary_df)))
        part2_lines.append("")
        table_num += 1
    part2_lines += [
        "---",
        "",
        "*† Elixhauser van Walraven score (MIMIC-III): 29-condition weighted index based on secondary ICD-9 diagnoses (Quan et al. 2005; van Walraven et al. 2009). Scores range from approximately −19 to +89; higher scores indicate greater predicted in-hospital mortality risk. Negative weights apply to certain conditions (e.g., drug abuse −7, obesity −4).*",
        "",
        "*‡ Charlson Comorbidity Index with age adjustment (MIMIC-IV): 17-condition index based on ICD-9 and ICD-10 diagnoses (Charlson et al. 1987; Quan et al. 2011). Age contributes 0–4 points (1 point per decade above age 50). Scores range from 0 to approximately 30; higher scores indicate greater comorbidity burden. The two comorbidity indices are not directly comparable.*",
    ]
    part2_content = "\n".join(part2_lines)

    # Write Part 2 into Mimic_analysis_summary.md, replacing everything from "## Part 2:" onwards
    if SUMMARY_MD.exists():
        existing = SUMMARY_MD.read_text(encoding="utf-8")
        idx = existing.find("## Part 2:")
        part1_content = existing[:idx].rstrip() if idx != -1 else existing.rstrip()
        SUMMARY_MD.write_text(part1_content + "\n\n" + part2_content, encoding="utf-8")
        print("Updated Part 2 →", SUMMARY_MD)
    else:
        print("Warning: summary file not found, writing Part 2 only →", SUMMARY_MD)
        SUMMARY_MD.write_text(part2_content, encoding="utf-8")


if __name__ == "__main__":
    main()
