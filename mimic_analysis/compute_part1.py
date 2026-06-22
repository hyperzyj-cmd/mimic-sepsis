"""
Compute Part 1 statistics for Mimic_analysis_summary.md
All stats from hr >= 0 rows only.
"""
import duckdb
from pathlib import Path

III_PARQUET = "D:/ESILV_S2/Intern/build_mimic/mimiciii/output/mimic3_wide.parquet"
IV_PARQUET  = "D:/ESILV_S2/Intern/build_mimic/mimiciv/output/mimic4_wide.parquet"
OUT_MD      = "D:/ESILV_S2/Intern/mimic_analysis/summary_output/Mimic_analysis_summary.md"

con = duckdb.connect()
con.execute(f"CREATE VIEW w3 AS SELECT * FROM read_parquet('{III_PARQUET}')")
con.execute(f"CREATE VIEW w4 AS SELECT * FROM read_parquet('{IV_PARQUET}')")

def q(sql): return con.execute(sql).fetchone()
def pct(n, total): return f"{int(n):,} ({100*n/total:.1f}%)"
def iqr(med, p25, p75, d=1): return f"{med:.{d}f} [{p25:.{d}f}, {p75:.{d}f}]"

# ---------- 1. Basic scale --------------------------------------------------
def basic_scale(view, id_col, sub_col):
    stays   = q(f"SELECT COUNT(DISTINCT {id_col}), COUNT(DISTINCT {sub_col}) FROM {view} WHERE hr>=0")
    hours   = q(f"SELECT COUNT(*) FROM {view} WHERE hr>=0")
    avg_los = q(f"""
        SELECT AVG(max_hr) FROM
        (SELECT {id_col}, MAX(hr) AS max_hr FROM {view} WHERE hr>=0 GROUP BY {id_col})
    """)
    return (stays[0], stays[1], hours[0], avg_los[0])

s3 = basic_scale("w3", "icustay_id", "subject_id")
s4 = basic_scale("w4", "stay_id",    "subject_id")

# ---------- 2. Sepsis labels ------------------------------------------------
def sepsis_stats(view, id_col, sub_col):
    return q(f"""
        WITH ps AS (
            SELECT {id_col}, {sub_col},
                   MAX(CASE WHEN SepsisLabel=1 THEN 1 ELSE 0 END) AS is_sep
            FROM {view} WHERE hr >= 0 GROUP BY {id_col}, {sub_col}
        )
        SELECT COUNT(*), SUM(is_sep),
               COUNT(DISTINCT CASE WHEN is_sep=1 THEN {sub_col} END),
               COUNT(DISTINCT {sub_col})
        FROM ps
    """)

# onset_hr: III has no onset_hr column — derive as MIN(hr) WHERE SepsisLabel=1
def onset_stats_iii():
    return q("""
        WITH ps AS (
            SELECT icustay_id, MIN(hr) AS onset_hr
            FROM w3 WHERE hr >= 0 AND SepsisLabel = 1
            GROUP BY icustay_id
        )
        SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY onset_hr),
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY onset_hr),
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY onset_hr)
        FROM ps
    """)

def onset_stats_iv():
    return q("""
        WITH ps AS (
            SELECT stay_id, MIN(onset_hr) AS onset_hr
            FROM w4 WHERE hr >= 0 AND onset_hr IS NOT NULL
            GROUP BY stay_id
        )
        SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY onset_hr),
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY onset_hr),
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY onset_hr)
        FROM ps
    """)

sep3 = sepsis_stats("w3", "icustay_id", "subject_id")
sep4 = sepsis_stats("w4", "stay_id",    "subject_id")
ons3 = onset_stats_iii()
ons4 = onset_stats_iv()

# ---------- 3. ICU LOS ------------------------------------------------------
def los_stats(view, id_col):
    return q(f"""
        WITH los AS (SELECT {id_col}, MAX(hr) AS h FROM {view} WHERE hr>=0 GROUP BY {id_col})
        SELECT ROUND(AVG(h),1), ROUND(STDDEV(h),1),
               PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY h),
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY h),
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY h),
               ROUND(100.0*SUM(CASE WHEN h<24 THEN 1 ELSE 0 END)/COUNT(*),1)
        FROM los
    """)

los3 = los_stats("w3", "icustay_id")
los4 = los_stats("w4", "stay_id")

# ---------- 4. ICU unit type ------------------------------------------------
def unit_dist(view, id_col):
    return con.execute(f"""
        SELECT first_careunit, COUNT(DISTINCT {id_col}) AS stays
        FROM {view} WHERE hr=0
        GROUP BY first_careunit ORDER BY stays DESC LIMIT 8
    """).fetchdf()

unit3 = unit_dist("w3", "icustay_id")
unit4 = unit_dist("w4", "stay_id")
tot3u = q("SELECT COUNT(DISTINCT icustay_id) FROM w3 WHERE hr=0")[0]
tot4u = q("SELECT COUNT(DISTINCT stay_id)    FROM w4 WHERE hr=0")[0]

# ---------- 5. Hospital mortality -------------------------------------------
def hosp_mort(view, id_col):
    return q(f"""
        SELECT COUNT(DISTINCT {id_col}),
               COUNT(DISTINCT CASE WHEN hospital_expire_flag=1 THEN {id_col} END)
        FROM {view} WHERE hr=0
    """)

mort3 = hosp_mort("w3", "icustay_id")
mort4 = hosp_mort("w4", "stay_id")

# ---------- 6. Coverage at hr=0 ---------------------------------------------
def cov(view, id_col, col):
    try:
        r = q(f"""
            SELECT COUNT(DISTINCT {id_col}),
                   COUNT(DISTINCT CASE WHEN {col} IS NOT NULL THEN {id_col} END)
            FROM {view} WHERE hr=0
        """)
        return f"{100*r[1]/r[0]:.1f}%"
    except Exception:
        return "N/A"

cov3 = {
    "Heart rate":        cov("w3", "icustay_id", "heartrate"),
    "Systolic BP":       cov("w3", "icustay_id", "sysbp"),
    "Temperature":       "N/A (not in wide table)",
    "SpO₂":             cov("w3", "icustay_id", "spo2"),
    "Creatinine":        cov("w3", "icustay_id", "creatinine"),
    "Platelet":          cov("w3", "icustay_id", "platelet"),
    "Bilirubin (total)": cov("w3", "icustay_id", "bilirubin"),
    "GCS total":         cov("w3", "icustay_id", "gcs_total"),
    "PaO₂":             cov("w3", "icustay_id", "po2"),
    "FiO₂":             cov("w3", "icustay_id", "fio2"),
}
cov4 = {
    "Heart rate":       cov("w4", "stay_id", "heart_rate"),
    "Systolic BP":      cov("w4", "stay_id", "sbp"),
    "Temperature":      cov("w4", "stay_id", "temperature"),
    "SpO₂":            cov("w4", "stay_id", "spo2"),
    "Creatinine":       cov("w4", "stay_id", "creatinine"),
    "Platelet":         cov("w4", "stay_id", "platelet"),
    "Bilirubin (total)":cov("w4", "stay_id", "bilirubin_total"),
    "GCS total":        cov("w4", "stay_id", "gcs_total"),
    "PaO₂":            cov("w4", "stay_id", "po2"),
    "FiO₂":            cov("w4", "stay_id", "fio2"),
}

# ---------- 7. SOFA distribution --------------------------------------------
def sofa_dist(view, id_col, sofa_col):
    return q(f"""
        WITH ps AS (
            SELECT {id_col}, MAX({sofa_col}) AS ms
            FROM {view} WHERE hr>=0 AND {sofa_col} IS NOT NULL GROUP BY {id_col}
        )
        SELECT ROUND(AVG(ms),1),
               PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ms),
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ms),
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ms),
               ROUND(100.0*SUM(CASE WHEN ms>=2 THEN 1 ELSE 0 END)/COUNT(*),1),
               ROUND(100.0*SUM(CASE WHEN ms>=6 THEN 1 ELSE 0 END)/COUNT(*),1)
        FROM ps
    """)

# SOFA at onset: III → sofa_total at MIN(hr) where SepsisLabel=1
def sofa_at_onset_iii():
    return q("""
        WITH onset AS (
            SELECT icustay_id, MIN(hr) AS onset_hr
            FROM w3 WHERE hr>=0 AND SepsisLabel=1 GROUP BY icustay_id
        ),
        vals AS (
            SELECT w3.icustay_id, w3.sofa_total
            FROM w3 JOIN onset ON w3.icustay_id=onset.icustay_id AND w3.hr=onset.onset_hr
            WHERE w3.sofa_total IS NOT NULL
        )
        SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY sofa_total),
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sofa_total),
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sofa_total)
        FROM vals
    """)

# IV → sofa_score column (SOFA at time of sepsis onset, official)
def sofa_at_onset_iv():
    return q("""
        WITH ps AS (
            SELECT stay_id, MAX(sofa_score) AS sc
            FROM w4 WHERE hr>=0 AND sofa_score IS NOT NULL AND SepsisLabel=1
            GROUP BY stay_id
        )
        SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY sc),
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sc),
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sc)
        FROM ps
    """)

# III delta at onset
def delta_at_onset_iii():
    return q("""
        WITH onset AS (
            SELECT icustay_id, MIN(hr) AS onset_hr
            FROM w3 WHERE hr>=0 AND SepsisLabel=1 GROUP BY icustay_id
        ),
        vals AS (
            SELECT w3.icustay_id, w3.sofa_delta_24h AS dv
            FROM w3 JOIN onset ON w3.icustay_id=onset.icustay_id AND w3.hr=onset.onset_hr
            WHERE w3.sofa_delta_24h IS NOT NULL
        )
        SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dv),
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY dv),
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dv)
        FROM vals
    """)

sd3 = sofa_dist("w3", "icustay_id", "sofa_total")
sd4 = sofa_dist("w4", "stay_id",    "sofa_24hours")
so3 = sofa_at_onset_iii()
so4 = sofa_at_onset_iv()
dd3 = delta_at_onset_iii()

print("All queries done. Building markdown...")

# ---------- Build markdown --------------------------------------------------
lines = []
lines.append("# MIMIC-III vs MIMIC-IV: Dataset Comparison Summary\n")
lines.append("""\
> **Generated from:** `mimic_analysis/compute_part1.py` (Part 1) · `mimic_analysis/build_patient_table2.py` (Part 2)
> **Pipeline:** `build_mimic3_wide.py` · `build_mimic4_wide.py`

---

## Part 1: Wide-Table Overview (All ICU Stays)

**Inclusion:** All ICU stays; one row per stay. All statistics computed over `hr ≥ 0` rows only.

### Definitions

**Time axis.** Both datasets extend to pre-ICU hours (negative `hr`):

| | MIMIC-III | MIMIC-IV |
|---|---|---|
| Pre-ICU window | `hr = −12` to discharge | `hr = −24` to discharge |
| Reason for difference | Official `pivoted_lab.sql` uses a ±12 h fuzzy window; labs up to 12 h before admission map to `hr = 0`, so extending beyond −12 produces empty rows | `labevents` is queried by raw `charttime` with no restriction, enabling capture of labs up to 24 h before ICU admission |

For `hr < 0`, ICU-sourced variables (vitals, GCS, vasopressors, urine output, ventilation, SOFA, SepsisLabel) are NULL by construction; only hospital laboratory values may be non-null.

**SOFA score.** Computed hourly for `hr ≥ 0`. Each of the six components (respiration, coagulation, liver, cardiovascular, CNS, renal) is scored from raw values at the current hour (no carry-forward), then the maximum over the preceding 24 hours is taken (`MAX(score) OVER ROWS BETWEEN 24 PRECEDING AND CURRENT ROW`). `sofa_24hours` = sum of the six 24 h-max component scores.

**Sepsis label.**

| | MIMIC-III | MIMIC-IV |
|---|---|---|
| Suspected infection | Antibiotic + culture within [−24 h, +12 h] | Antibiotic + culture within [−48 h, +24 h] |
| Sepsis criterion | SOFA rise ≥ 2 from 24 h rolling minimum (`sofa_delta_24h ≥ 2`) | Absolute SOFA ≥ 2 (`sofa_24hours ≥ 2`) |
| Reference | Seymour et al. 2016 / MIMIC-III Sepsis Challenge | Singer et al. 2016 / mimic-code `sepsis3.sql` |

`SepsisLabel = 1` from `onset_hr` onward within the stay; `= 0` before onset; `= NULL` for `hr < 0`. A stay is classified as a **sepsis stay** if `SepsisLabel = 1` at any `hr ≥ 0`.

**ICU LOS:** `MAX(hr)` per stay (hours from admission to discharge). **Coverage rate:** proportion of stays with ≥ 1 non-null value for the variable at `hr = 0`.

---

### Table 1. Dataset Characteristics
""")

lines.append("| | MIMIC-III | MIMIC-IV |")
lines.append("|---|---:|---:|")

# Scale
lines.append("| **Scale** | | |")
lines.append(f"| Total ICU stays | {int(s3[0]):,} | {int(s4[0]):,} |")
lines.append(f"| Unique subjects | {int(s3[1]):,} | {int(s4[1]):,} |")
lines.append(f"| Total patient-hours (hr ≥ 0) | {int(s3[2]):,} | {int(s4[2]):,} |")
lines.append(f"| ICU LOS, mean (h) | {s3[3]:.1f} | {s4[3]:.1f} |")
lines.append(f"| ICU LOS, median [IQR] (h) | {iqr(los3[3], los3[2], los3[4])} | {iqr(los4[3], los4[2], los4[4])} |")
lines.append(f"| LOS < 24 h, % | {los3[5]}% | {los4[5]}% |")
lines.append(f"| Hospital mortality, n (%) | {pct(mort3[1], mort3[0])} | {pct(mort4[1], mort4[0])} |")

# Sepsis
lines.append("| **Sepsis** | | |")
lines.append(f"| Sepsis stays, n (%) | {pct(sep3[1], sep3[0])} | {pct(sep4[1], sep4[0])} |")
lines.append(f"| Non-sepsis stays, n (%) | {pct(sep3[0]-sep3[1], sep3[0])} | {pct(sep4[0]-sep4[1], sep4[0])} |")
lines.append(f"| Sepsis subjects, n (%) | {pct(sep3[2], sep3[3])} | {pct(sep4[2], sep4[3])} |")
lines.append(f"| Sepsis onset, median [IQR] (h) | {iqr(ons3[1], ons3[0], ons3[2], 0)} | {iqr(ons4[1], ons4[0], ons4[2], 0)} |")

# SOFA
lines.append("| **SOFA** | | |")
lines.append(f"| Max SOFA per stay, mean | {sd3[0]} | {sd4[0]} |")
lines.append(f"| Max SOFA per stay, median [IQR] | {iqr(sd3[2], sd3[1], sd3[3], 0)} | {iqr(sd4[2], sd4[1], sd4[3], 0)} |")
lines.append(f"| Max SOFA ≥ 2, % of stays | {sd3[4]}% | {sd4[4]}% |")
lines.append(f"| Max SOFA ≥ 6, % of stays | {sd3[5]}% | {sd4[5]}% |")
lines.append(f"| SOFA at sepsis onset, median [IQR] | {iqr(so3[1], so3[0], so3[2], 0)} | {iqr(so4[1], so4[0], so4[2], 0)} |")
lines.append(f"| SOFA Δ at sepsis onset (III only), median [IQR] | {iqr(dd3[1], dd3[0], dd3[2], 0)} | — |")

lines.append("")

# Table 2: ICU units
unit3.columns = [c.lower() for c in unit3.columns]
unit4.columns = [c.lower() for c in unit4.columns]
u3m = dict(zip(unit3["first_careunit"], unit3["stays"]))
u4m = dict(zip(unit4["first_careunit"], unit4["stays"]))
all_units = sorted(set(list(unit3["first_careunit"]) + list(unit4["first_careunit"])),
                   key=lambda x: -(u3m.get(x,0)+u4m.get(x,0)))
lines.append("### Table 2. First ICU Unit by Volume\n")
lines.append("| ICU Unit | MIMIC-III | MIMIC-IV |")
lines.append("|---|---:|---:|")
for u in all_units:
    v3, v4 = u3m.get(u,0), u4m.get(u,0)
    c3 = f"{v3:,} ({100*v3/tot3u:.1f}%)" if v3 else "—"
    c4 = f"{v4:,} ({100*v4/tot4u:.1f}%)" if v4 else "—"
    lines.append(f"| {u} | {c3} | {c4} |")
lines.append("")

# Table 3: Coverage
lines.append("### Table 3. Clinical Data Coverage at hr = 0 (% of stays with ≥ 1 non-null value)\n")
lines.append("| Variable | MIMIC-III | MIMIC-IV |")
lines.append("|---|---:|---:|")
for label in cov3:
    lines.append(f"| {label} | {cov3[label]} | {cov4[label]} |")
lines.append("")
lines.append("† Temperature is not available as a standalone vital in the MIMIC-III wide table (arterial blood gas temperature only).")
lines.append("")

# ---------- Assemble final file ---------------------------------------------
# Preserve everything from "### Key Observations" onward (manually curated analysis + Part 2)
preserve_marker = "### Key Observations"
existing_full = Path(OUT_MD).read_text(encoding="utf-8") if Path(OUT_MD).exists() else ""
idx = existing_full.find(preserve_marker)
preserved = existing_full[idx:] if idx != -1 else ""

Path(OUT_MD).write_text("\n".join(lines) + "\n---\n\n" + preserved, encoding="utf-8")
print("Done →", OUT_MD)
