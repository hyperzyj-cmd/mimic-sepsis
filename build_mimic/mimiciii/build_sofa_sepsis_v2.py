"""
MIMIC-III: recompute SOFA / Sepsis-3 using the MIMIC-IV-style logic on top of
the current repaired MIMIC-III intermediate tables, then generate:

1. A refreshed standalone hourly table with IV-style columns only.
2. A new full wide-table variant by appending those IV-style columns onto the
   current `mimic3_wide.parquet`.

The original `mimic3_wide.parquet` and earlier standalone parquet outputs are
left untouched.

Outputs:
  - standalone slim table:
      `mimic3_sofa_sepsis_ivmethod_on_iii_20260619.parquet`
  - full wide-table variant:
      `mimic3_wide_ivlogic_on_iii_20260619.parquet`

Usage:
    python build_sofa_sepsis_v2.py --sample 2000
    python build_sofa_sepsis_v2.py --full
"""

import argparse
import logging
import os
import time
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

RAW_DIR = r"D:\ESILV_S2\mimic\raw\physionet.org\files\mimiciii\1.4\mimiciii csv"
REPO_ROOT = Path(__file__).resolve().parents[2]
INTER_DIR = str(REPO_ROOT / "build_mimic" / "mimiciii" / "intermediate" / "mimiciii")
DB_PATH = str(REPO_ROOT / "build_mimic" / "mimiciii" / "output" / "mimic3_build.duckdb")
OUT_DIR = str(REPO_ROOT / "build_mimic" / "mimiciii" / "output")
WIDE_IN_PATH = os.path.join(OUT_DIR, "mimic3_wide.parquet").replace("\\", "/")
REFRESH_TAG = "20260619"


def inter(name: str) -> str:
    return os.path.join(INTER_DIR, f"{name}.parquet").replace("\\", "/")


def slim_out_path(sample_limit):
    stem = f"mimic3_sofa_sepsis_ivmethod_on_iii_{REFRESH_TAG}"
    if sample_limit is None:
        return os.path.join(OUT_DIR, f"{stem}.parquet").replace("\\", "/")
    return os.path.join(OUT_DIR, f"{stem}_sample.parquet").replace("\\", "/")


def wide_out_path(sample_limit):
    stem = f"mimic3_wide_ivlogic_on_iii_{REFRESH_TAG}"
    if sample_limit is None:
        return os.path.join(OUT_DIR, f"{stem}.parquet").replace("\\", "/")
    return os.path.join(OUT_DIR, f"{stem}_sample.parquet").replace("\\", "/")


ABX_LIKE = """(
    LOWER(p.drug) LIKE '%amikacin%' OR LOWER(p.drug) LIKE '%amoxicillin%'
    OR LOWER(p.drug) LIKE '%ampicillin%' OR LOWER(p.drug) LIKE '%azithromycin%'
    OR LOWER(p.drug) LIKE '%aztreonam%' OR LOWER(p.drug) LIKE '%cefazolin%'
    OR LOWER(p.drug) LIKE '%cefepime%' OR LOWER(p.drug) LIKE '%cefotaxime%'
    OR LOWER(p.drug) LIKE '%cefoxitin%' OR LOWER(p.drug) LIKE '%ceftazidime%'
    OR LOWER(p.drug) LIKE '%ceftriaxone%' OR LOWER(p.drug) LIKE '%cefuroxime%'
    OR LOWER(p.drug) LIKE '%ciprofloxacin%' OR LOWER(p.drug) LIKE '%clarithromycin%'
    OR LOWER(p.drug) LIKE '%clindamycin%' OR LOWER(p.drug) LIKE '%colistin%'
    OR LOWER(p.drug) LIKE '%daptomycin%' OR LOWER(p.drug) LIKE '%doripenem%'
    OR LOWER(p.drug) LIKE '%doxycycline%' OR LOWER(p.drug) LIKE '%ertapenem%'
    OR LOWER(p.drug) LIKE '%erythromycin%' OR LOWER(p.drug) LIKE '%fluconazole%'
    OR LOWER(p.drug) LIKE '%gentamicin%' OR LOWER(p.drug) LIKE '%imipenem%'
    OR LOWER(p.drug) LIKE '%levofloxacin%' OR LOWER(p.drug) LIKE '%linezolid%'
    OR LOWER(p.drug) LIKE '%meropenem%' OR LOWER(p.drug) LIKE '%metronidazole%'
    OR LOWER(p.drug) LIKE '%micafungin%' OR LOWER(p.drug) LIKE '%minocycline%'
    OR LOWER(p.drug) LIKE '%moxifloxacin%' OR LOWER(p.drug) LIKE '%nafcillin%'
    OR LOWER(p.drug) LIKE '%nitrofurantoin%' OR LOWER(p.drug) LIKE '%oxacillin%'
    OR LOWER(p.drug) LIKE '%penicillin%' OR LOWER(p.drug) LIKE '%piperacillin%'
    OR LOWER(p.drug) LIKE '%polymyxin%' OR LOWER(p.drug) LIKE '%rifampin%'
    OR LOWER(p.drug) LIKE '%sulfamethoxazole%' OR LOWER(p.drug) LIKE '%tetracycline%'
    OR LOWER(p.drug) LIKE '%tigecycline%' OR LOWER(p.drug) LIKE '%tobramycin%'
    OR LOWER(p.drug) LIKE '%trimethoprim%' OR LOWER(p.drug) LIKE '%vancomycin%'
    OR LOWER(p.drug) LIKE '%voriconazole%'
)"""


def register_raw_views(con):
    for table in ["PRESCRIPTIONS", "MICROBIOLOGYEVENTS"]:
        path = os.path.join(RAW_DIR, f"{table}.csv").replace("\\", "/")
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM read_csv_auto('{path}', header=True, ignore_errors=True)"
        )


def create_input_views(con, sample_limit):
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW cohort_base AS
        SELECT
            SUBJECT_ID AS subject_id,
            HADM_ID AS hadm_id,
            ICUSTAY_ID AS icustay_id,
            INTIME AS intime,
            OUTTIME AS outtime,
            los_hours,
            GENDER AS gender,
            age
        FROM read_parquet('{inter("01_cohort")}')
        """
    )
    if sample_limit is None:
        con.execute("CREATE OR REPLACE TEMP VIEW cohort AS SELECT * FROM cohort_base")
    else:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW cohort AS
            SELECT * FROM cohort_base
            ORDER BY icustay_id
            LIMIT {sample_limit}
            """
        )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW time_axis AS
        SELECT
            t.SUBJECT_ID AS subject_id,
            t.HADM_ID AS hadm_id,
            t.ICUSTAY_ID AS icustay_id,
            t.hr,
            t.charttime_floor
        FROM read_parquet('{inter("02_time_axis")}') t
        INNER JOIN cohort c ON t.ICUSTAY_ID = c.icustay_id
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW labs AS
        SELECT
            ICUSTAY_ID AS icustay_id,
            charttime_floor,
            platelet,
            bilirubin,
            creatinine
        FROM read_parquet('{inter("04_labs")}')
        WHERE ICUSTAY_ID IN (SELECT icustay_id FROM cohort)
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW bg AS
        SELECT
            ICUSTAY_ID AS icustay_id,
            charttime_floor,
            po2,
            fio2_bg
        FROM read_parquet('{inter("05_bg")}')
        WHERE ICUSTAY_ID IN (SELECT icustay_id FROM cohort)
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW gcs_raw AS
        SELECT
            ICUSTAY_ID AS icustay_id,
            charttime_floor,
            gcs_motor,
            gcs_verbal,
            gcs_eyes
        FROM read_parquet('{inter("06_gcs_raw")}')
        WHERE ICUSTAY_ID IN (SELECT icustay_id FROM cohort)
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW uo AS
        SELECT
            ICUSTAY_ID AS icustay_id,
            charttime_floor,
            urineoutput
        FROM read_parquet('{inter("07_uo")}')
        WHERE ICUSTAY_ID IN (SELECT icustay_id FROM cohort)
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW vaso AS
        SELECT
            icustay_id,
            charttime_floor,
            rate_norepinephrine,
            rate_epinephrine,
            rate_dopamine,
            rate_phenylephrine,
            rate_vasopressin
        FROM read_parquet('{inter("08_vaso")}')
        WHERE icustay_id IN (SELECT icustay_id FROM cohort)
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW vent AS
        SELECT
            icustay_id,
            charttime_floor,
            vent_status
        FROM read_parquet('{inter("10_vent_raw")}')
        WHERE icustay_id IN (SELECT icustay_id FROM cohort)
        """
    )


def build_slim_table(con, sample_limit, out_path):
    t0 = time.time()
    create_input_views(con, sample_limit)
    sql = f"""
    COPY (
        WITH base AS (
            SELECT
                ta.icustay_id,
                ta.hr,
                ta.charttime_floor,
                CASE
                    WHEN bg.po2 IS NOT NULL AND bg.fio2_bg IS NOT NULL AND bg.fio2_bg > 0
                    THEN bg.po2 / (bg.fio2_bg / 100.0)
                END AS pao2fio2ratio_v2,
                labs.platelet,
                labs.bilirubin,
                labs.creatinine,
                CASE
                    WHEN gcs_raw.icustay_id IS NOT NULL
                    THEN COALESCE(gcs_raw.gcs_motor, 6)
                       + COALESCE(gcs_raw.gcs_verbal, 5)
                       + COALESCE(gcs_raw.gcs_eyes, 4)
                END AS gcs_total_v2,
                COALESCE(uo.urineoutput, 0) AS urineoutput,
                COALESCE(vaso.rate_norepinephrine, 0) AS norepi_rate,
                COALESCE(vaso.rate_epinephrine, 0) AS epi_rate,
                COALESCE(vaso.rate_dopamine, 0) AS dopa_rate,
                COALESCE(vaso.rate_phenylephrine, 0) AS phenyl_rate,
                COALESCE(vaso.rate_vasopressin, 0) AS vaso_rate,
                vent.vent_status
            FROM time_axis ta
            LEFT JOIN bg
              ON ta.icustay_id = bg.icustay_id
             AND ta.charttime_floor = bg.charttime_floor
            LEFT JOIN labs
              ON ta.icustay_id = labs.icustay_id
             AND ta.charttime_floor = labs.charttime_floor
            LEFT JOIN gcs_raw
              ON ta.icustay_id = gcs_raw.icustay_id
             AND ta.charttime_floor = gcs_raw.charttime_floor
            LEFT JOIN uo
              ON ta.icustay_id = uo.icustay_id
             AND ta.charttime_floor = uo.charttime_floor
            LEFT JOIN vaso
              ON ta.icustay_id = vaso.icustay_id
             AND ta.charttime_floor = vaso.charttime_floor
            LEFT JOIN vent
              ON ta.icustay_id = vent.icustay_id
             AND ta.charttime_floor = vent.charttime_floor
        ),
        rolling AS (
            SELECT
                icustay_id,
                hr,
                charttime_floor,
                MIN(pao2fio2ratio_v2) OVER w24 AS pf_min_24h,
                MAX(CASE WHEN vent_status = 'InvasiveVent' THEN 1 ELSE 0 END) OVER w24 AS on_vent_24h,
                MIN(platelet) OVER w24 AS platelet_min_24h,
                MAX(bilirubin) OVER w24 AS bili_max_24h,
                MIN(gcs_total_v2) OVER w24 AS gcs_min_24h,
                MAX(creatinine) OVER w24 AS creat_max_24h,
                SUM(urineoutput) OVER w24 AS urine_output_24h_v2,
                MAX(norepi_rate) OVER w24 AS norepi_max_24h,
                MAX(epi_rate) OVER w24 AS epi_max_24h,
                MAX(dopa_rate) OVER w24 AS dopa_max_24h,
                MAX(phenyl_rate) OVER w24 AS phenyl_max_24h,
                MAX(vaso_rate) OVER w24 AS vaso_max_24h
            FROM base
            WINDOW w24 AS (
                PARTITION BY icustay_id
                ORDER BY hr
                ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
            )
        ),
        sofa_v2 AS (
            SELECT
                r.icustay_id,
                r.hr,
                r.charttime_floor,
                CASE
                    WHEN pf_min_24h IS NULL THEN NULL
                    WHEN pf_min_24h >= 400 THEN 0
                    WHEN pf_min_24h >= 300 THEN 1
                    WHEN pf_min_24h >= 200 THEN 2
                    WHEN pf_min_24h >= 100 AND on_vent_24h = 1 THEN 3
                    WHEN on_vent_24h = 1 THEN 4
                    ELSE 2
                END AS sofa_respiration_v2,
                CASE
                    WHEN platelet_min_24h IS NULL THEN NULL
                    WHEN platelet_min_24h >= 150 THEN 0
                    WHEN platelet_min_24h >= 100 THEN 1
                    WHEN platelet_min_24h >= 50 THEN 2
                    WHEN platelet_min_24h >= 20 THEN 3
                    ELSE 4
                END AS sofa_coagulation_v2,
                CASE
                    WHEN bili_max_24h IS NULL THEN NULL
                    WHEN bili_max_24h < 1.2 THEN 0
                    WHEN bili_max_24h < 2.0 THEN 1
                    WHEN bili_max_24h < 6.0 THEN 2
                    WHEN bili_max_24h < 12.0 THEN 3
                    ELSE 4
                END AS sofa_liver_v2,
                CASE
                    WHEN norepi_max_24h > 0.1 OR epi_max_24h > 0.1 OR dopa_max_24h > 15 THEN 4
                    WHEN norepi_max_24h > 0 OR epi_max_24h > 0 OR dopa_max_24h > 5 OR phenyl_max_24h > 0 THEN 3
                    WHEN dopa_max_24h > 0 OR vaso_max_24h > 0 THEN 2
                    WHEN norepi_max_24h = 0 AND epi_max_24h = 0 AND dopa_max_24h = 0
                     AND phenyl_max_24h = 0 AND vaso_max_24h = 0 THEN 0
                    ELSE 0
                END AS sofa_cardiovascular_v2,
                CASE
                    WHEN gcs_min_24h IS NULL THEN NULL
                    WHEN gcs_min_24h >= 15 THEN 0
                    WHEN gcs_min_24h >= 13 THEN 1
                    WHEN gcs_min_24h >= 10 THEN 2
                    WHEN gcs_min_24h >= 6 THEN 3
                    ELSE 4
                END AS sofa_cns_v2,
                CASE
                    WHEN creat_max_24h IS NULL AND (urine_output_24h_v2 IS NULL OR urine_output_24h_v2 = 0) THEN NULL
                    WHEN creat_max_24h >= 5.0 OR (urine_output_24h_v2 > 0 AND urine_output_24h_v2 < 200) THEN 4
                    WHEN creat_max_24h >= 3.5 OR (urine_output_24h_v2 > 0 AND urine_output_24h_v2 < 500) THEN 3
                    WHEN creat_max_24h >= 2.0 THEN 2
                    WHEN creat_max_24h >= 1.2 THEN 1
                    ELSE 0
                END AS sofa_renal_v2,
                COALESCE(
                    CASE
                        WHEN pf_min_24h IS NULL THEN NULL
                        WHEN pf_min_24h >= 400 THEN 0
                        WHEN pf_min_24h >= 300 THEN 1
                        WHEN pf_min_24h >= 200 THEN 2
                        WHEN pf_min_24h >= 100 AND on_vent_24h = 1 THEN 3
                        WHEN on_vent_24h = 1 THEN 4
                        ELSE 2
                    END,
                    0
                )
                + COALESCE(
                    CASE
                        WHEN platelet_min_24h IS NULL THEN NULL
                        WHEN platelet_min_24h >= 150 THEN 0
                        WHEN platelet_min_24h >= 100 THEN 1
                        WHEN platelet_min_24h >= 50 THEN 2
                        WHEN platelet_min_24h >= 20 THEN 3
                        ELSE 4
                    END,
                    0
                )
                + COALESCE(
                    CASE
                        WHEN bili_max_24h IS NULL THEN NULL
                        WHEN bili_max_24h < 1.2 THEN 0
                        WHEN bili_max_24h < 2.0 THEN 1
                        WHEN bili_max_24h < 6.0 THEN 2
                        WHEN bili_max_24h < 12.0 THEN 3
                        ELSE 4
                    END,
                    0
                )
                + CASE
                    WHEN norepi_max_24h > 0.1 OR epi_max_24h > 0.1 OR dopa_max_24h > 15 THEN 4
                    WHEN norepi_max_24h > 0 OR epi_max_24h > 0 OR dopa_max_24h > 5 OR phenyl_max_24h > 0 THEN 3
                    WHEN dopa_max_24h > 0 OR vaso_max_24h > 0 THEN 2
                    ELSE 0
                END
                + COALESCE(
                    CASE
                        WHEN gcs_min_24h IS NULL THEN NULL
                        WHEN gcs_min_24h >= 15 THEN 0
                        WHEN gcs_min_24h >= 13 THEN 1
                        WHEN gcs_min_24h >= 10 THEN 2
                        WHEN gcs_min_24h >= 6 THEN 3
                        ELSE 4
                    END,
                    0
                )
                + COALESCE(
                    CASE
                        WHEN creat_max_24h IS NULL AND (urine_output_24h_v2 IS NULL OR urine_output_24h_v2 = 0) THEN NULL
                        WHEN creat_max_24h >= 5.0 OR (urine_output_24h_v2 > 0 AND urine_output_24h_v2 < 200) THEN 4
                        WHEN creat_max_24h >= 3.5 OR (urine_output_24h_v2 > 0 AND urine_output_24h_v2 < 500) THEN 3
                        WHEN creat_max_24h >= 2.0 THEN 2
                        WHEN creat_max_24h >= 1.2 THEN 1
                        ELSE 0
                    END,
                    0
                ) AS sofa_24hours_v2
            FROM rolling r
        ),
        abx AS (
            SELECT
                c.icustay_id,
                CAST(p.startdate AS TIMESTAMP) AS antibiotic_time,
                DATE_TRUNC('day', CAST(p.startdate AS TIMESTAMP)) AS antibiotic_date,
                ROW_NUMBER() OVER (
                    PARTITION BY c.icustay_id
                    ORDER BY p.startdate NULLS LAST, p.drug
                ) AS ab_id
            FROM cohort c
            INNER JOIN PRESCRIPTIONS p ON c.hadm_id = p.hadm_id
            WHERE p.startdate IS NOT NULL
              AND {ABX_LIKE}
        ),
        me AS (
            SELECT
                hadm_id,
                spec_itemid,
                chartdate,
                charttime,
                spec_type_desc
            FROM MICROBIOLOGYEVENTS
            WHERE hadm_id IS NOT NULL
            GROUP BY hadm_id, spec_itemid, chartdate, charttime, spec_type_desc
        ),
        me_then_ab AS (
            SELECT
                ab.icustay_id,
                ab.ab_id,
                COALESCE(me.charttime, CAST(me.chartdate AS TIMESTAMP)) AS last72_charttime,
                ROW_NUMBER() OVER (
                    PARTITION BY ab.icustay_id, ab.ab_id
                    ORDER BY me.chartdate NULLS FIRST, me.charttime
                ) AS micro_seq
            FROM abx ab
            INNER JOIN cohort c ON ab.icustay_id = c.icustay_id
            LEFT JOIN me
              ON me.hadm_id = c.hadm_id
             AND (
                  (me.charttime IS NOT NULL
                   AND ab.antibiotic_time > me.charttime
                   AND ab.antibiotic_time <= me.charttime + INTERVAL 72 HOUR)
               OR (me.charttime IS NULL
                   AND ab.antibiotic_date >= me.chartdate
                   AND ab.antibiotic_date <= me.chartdate + INTERVAL 3 DAY)
             )
        ),
        ab_then_me AS (
            SELECT
                ab.icustay_id,
                ab.ab_id,
                COALESCE(me.charttime, CAST(me.chartdate AS TIMESTAMP)) AS next24_charttime,
                ROW_NUMBER() OVER (
                    PARTITION BY ab.icustay_id, ab.ab_id
                    ORDER BY me.chartdate NULLS FIRST, me.charttime
                ) AS micro_seq
            FROM abx ab
            INNER JOIN cohort c ON ab.icustay_id = c.icustay_id
            LEFT JOIN me
              ON me.hadm_id = c.hadm_id
             AND (
                  (me.charttime IS NOT NULL
                   AND ab.antibiotic_time >= me.charttime - INTERVAL 24 HOUR
                   AND ab.antibiotic_time < me.charttime)
               OR (me.charttime IS NULL
                   AND ab.antibiotic_date >= me.chartdate - INTERVAL 1 DAY
                   AND ab.antibiotic_date <= me.chartdate)
             )
        ),
        suspicion AS (
            SELECT
                ab.icustay_id,
                ab.ab_id,
                CASE
                    WHEN m2a.last72_charttime IS NULL AND a2m.next24_charttime IS NULL THEN NULL
                    ELSE COALESCE(m2a.last72_charttime, ab.antibiotic_time)
                END AS suspected_infection_time
            FROM abx ab
            LEFT JOIN ab_then_me a2m
              ON ab.icustay_id = a2m.icustay_id
             AND ab.ab_id = a2m.ab_id
             AND a2m.micro_seq = 1
            LEFT JOIN me_then_ab m2a
              ON ab.icustay_id = m2a.icustay_id
             AND ab.ab_id = m2a.ab_id
             AND m2a.micro_seq = 1
        ),
        susp_per_stay AS (
            SELECT icustay_id, MIN(suspected_infection_time) AS t_suspicion_v2
            FROM suspicion
            WHERE suspected_infection_time IS NOT NULL
            GROUP BY icustay_id
        ),
        onset AS (
            SELECT sf.icustay_id, MIN(sf.hr) AS onset_hr_v2
            FROM sofa_v2 sf
            INNER JOIN susp_per_stay sp ON sf.icustay_id = sp.icustay_id
            WHERE sf.sofa_24hours_v2 >= 2
              AND sf.charttime_floor >= sp.t_suspicion_v2 - INTERVAL 48 HOUR
              AND sf.charttime_floor <= sp.t_suspicion_v2 + INTERVAL 24 HOUR
            GROUP BY sf.icustay_id
        )
        SELECT
            sf.icustay_id,
            sf.hr,
            sf.charttime_floor,
            sf.sofa_respiration_v2,
            sf.sofa_coagulation_v2,
            sf.sofa_liver_v2,
            sf.sofa_cardiovascular_v2,
            sf.sofa_cns_v2,
            sf.sofa_renal_v2,
            sf.sofa_24hours_v2,
            sp.t_suspicion_v2,
            on_.onset_hr_v2,
            CASE WHEN on_.onset_hr_v2 IS NOT NULL AND sf.hr >= on_.onset_hr_v2 THEN 1 ELSE 0 END AS SepsisLabel_v2
        FROM sofa_v2 sf
        LEFT JOIN susp_per_stay sp ON sf.icustay_id = sp.icustay_id
        LEFT JOIN onset on_ ON sf.icustay_id = on_.icustay_id
    ) TO '{out_path}' (FORMAT PARQUET)
    """
    con.execute(sql)
    log.info("slim table built in %.1fs -> %s", time.time() - t0, out_path)


def build_full_wide_variant(con, sample_limit, slim_path, out_path):
    t0 = time.time()
    if sample_limit is None:
        wide_source = f"read_parquet('{WIDE_IN_PATH}')"
    else:
        wide_source = (
            f"(SELECT w.* "
            f"FROM read_parquet('{WIDE_IN_PATH}') w "
            f"INNER JOIN (SELECT DISTINCT icustay_id FROM read_parquet('{slim_path}')) s "
            f"ON w.icustay_id = s.icustay_id)"
        )

    sql = f"""
    COPY (
        SELECT
            w.*,
            iv.sofa_respiration_v2 AS sofa_respiration_iv,
            iv.sofa_coagulation_v2 AS sofa_coagulation_iv,
            iv.sofa_liver_v2 AS sofa_liver_iv,
            iv.sofa_cardiovascular_v2 AS sofa_cardiovascular_iv,
            iv.sofa_cns_v2 AS sofa_cns_iv,
            iv.sofa_renal_v2 AS sofa_renal_iv,
            iv.sofa_24hours_v2 AS sofa_24hours_iv,
            iv.t_suspicion_v2 AS t_suspicion_iv,
            iv.onset_hr_v2 AS onset_hr_iv,
            iv.SepsisLabel_v2 AS SepsisLabel_iv
        FROM {wide_source} w
        LEFT JOIN read_parquet('{slim_path}') iv
          ON w.icustay_id = iv.icustay_id
         AND w.hr = iv.hr
         AND w.charttime_floor = iv.charttime_floor
    ) TO '{out_path}' (FORMAT PARQUET)
    """
    con.execute(sql)
    log.info("wide variant built in %.1fs -> %s", time.time() - t0, out_path)


def report_slim(con, path, label):
    rows, stays, avg_sofa, sepsis_rows, sepsis_stays = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT icustay_id) AS stays,
            AVG(sofa_24hours_v2) AS avg_sofa,
            SUM(SepsisLabel_v2) AS sepsis_rows,
            COUNT(DISTINCT CASE WHEN onset_hr_v2 IS NOT NULL THEN icustay_id END) AS sepsis_stays
        FROM read_parquet('{path}')
        """
    ).fetchone()
    log.info(
        "%s | rows=%d stays=%d avg_sofa=%.2f sepsis_rows=%d sepsis_stays=%d",
        label,
        rows,
        stays,
        avg_sofa or 0,
        sepsis_rows or 0,
        sepsis_stays or 0,
    )


def report_wide(con, path, label):
    rows, stays, sepsis_rows, sepsis_stays = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT icustay_id) AS stays,
            SUM(SepsisLabel_iv) AS sepsis_rows,
            COUNT(DISTINCT CASE WHEN SepsisLabel_iv = 1 THEN icustay_id END) AS sepsis_stays
        FROM read_parquet('{path}')
        """
    ).fetchone()
    cols = con.execute(
        f"SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM read_parquet('{path}'))"
    ).fetchone()[0]
    log.info(
        "%s | rows=%d stays=%d cols=%d sepsis_rows=%d sepsis_stays=%d",
        label,
        rows,
        stays,
        cols,
        sepsis_rows or 0,
        sepsis_stays or 0,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=None, help="limit to first N icustay_ids by id order")
    parser.add_argument("--full", action="store_true", help="run on all ICU stays")
    args = parser.parse_args()

    if not args.full and args.sample is None:
        args.sample = 2000

    sample_limit = None if args.full else args.sample
    os.makedirs(OUT_DIR, exist_ok=True)

    con = duckdb.connect(DB_PATH)
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA memory_limit='12GB'")
    register_raw_views(con)

    slim_path = slim_out_path(sample_limit)
    wide_path = wide_out_path(sample_limit)

    build_slim_table(con, sample_limit, slim_path)
    build_full_wide_variant(con, sample_limit, slim_path, wide_path)
    report_slim(con, slim_path, "IV-logic slim output")
    report_wide(con, wide_path, "IV-logic full-wide output")

    con.close()


if __name__ == "__main__":
    main()
