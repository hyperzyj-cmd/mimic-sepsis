"""
Build MIMIC-IV wide table: rows = stay_id x HOUR, columns = clinical variables.
SepsisLabel follows mimic-code official Sepsis-3 definition (SOFA absolute >= 2).

References:
  - MIT-LCP/mimic-code mimic-iv/concepts/
  - MIMIC-IV v3.1 official documentation (physionet.org/content/mimiciv/3.1)

Output: <repo>/build_mimic/mimiciv/output/mimic4_wide.parquet
"""

import os
import time
import duckdb
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HOSP_DIR  = r"D:\ESILV_S2\mimic\raw\physionet.org\files\mimiciv\3.1\hosp"
ICU_DIR   = r"D:\ESILV_S2\mimic\raw\physionet.org\files\mimiciv\3.1\icu"
REPO_ROOT = Path(__file__).resolve().parents[2]
INTER_DIR = str(REPO_ROOT / "build_mimic" / "mimiciv" / "intermediate" / "mimiciv")
DB_PATH   = str(REPO_ROOT / "build_mimic" / "mimiciv" / "output" / "mimic4_build.duckdb")
OUT_PATH  = str(REPO_ROOT / "build_mimic" / "mimiciv" / "output" / "mimic4_wide.parquet")
DUCKDB_TEMP_DIR = str(REPO_ROOT / "build_mimic" / "mimiciv" / "output" / "duckdb_tmp")

ANTIBIOTIC_DRUG_FILTER = """
(
    LOWER(pr.drug) LIKE '%adoxa%' OR LOWER(pr.drug) LIKE '%ala-tet%'
    OR LOWER(pr.drug) LIKE '%alodox%' OR LOWER(pr.drug) LIKE '%amikacin%'
    OR LOWER(pr.drug) LIKE '%amikin%' OR LOWER(pr.drug) LIKE '%amoxicill%'
    OR LOWER(pr.drug) LIKE '%amphotericin%' OR LOWER(pr.drug) LIKE '%anidulafungin%'
    OR LOWER(pr.drug) LIKE '%ancef%' OR LOWER(pr.drug) LIKE '%clavulanate%'
    OR LOWER(pr.drug) LIKE '%ampicillin%' OR LOWER(pr.drug) LIKE '%augmentin%'
    OR LOWER(pr.drug) LIKE '%avelox%' OR LOWER(pr.drug) LIKE '%avidoxy%'
    OR LOWER(pr.drug) LIKE '%azactam%' OR LOWER(pr.drug) LIKE '%azithromycin%'
    OR LOWER(pr.drug) LIKE '%aztreonam%' OR LOWER(pr.drug) LIKE '%axetil%'
    OR LOWER(pr.drug) LIKE '%bactocill%' OR LOWER(pr.drug) LIKE '%bactrim%'
    OR LOWER(pr.drug) LIKE '%bactroban%' OR LOWER(pr.drug) LIKE '%bethkis%'
    OR LOWER(pr.drug) LIKE '%biaxin%' OR LOWER(pr.drug) LIKE '%bicillin l-a%'
    OR LOWER(pr.drug) LIKE '%cayston%' OR LOWER(pr.drug) LIKE '%cefazolin%'
    OR LOWER(pr.drug) LIKE '%cedax%' OR LOWER(pr.drug) LIKE '%cefoxitin%'
    OR LOWER(pr.drug) LIKE '%ceftazidime%' OR LOWER(pr.drug) LIKE '%cefaclor%'
    OR LOWER(pr.drug) LIKE '%cefadroxil%' OR LOWER(pr.drug) LIKE '%cefdinir%'
    OR LOWER(pr.drug) LIKE '%cefditoren%' OR LOWER(pr.drug) LIKE '%cefepime%'
    OR LOWER(pr.drug) LIKE '%cefotan%' OR LOWER(pr.drug) LIKE '%cefotetan%'
    OR LOWER(pr.drug) LIKE '%cefotaxime%' OR LOWER(pr.drug) LIKE '%ceftaroline%'
    OR LOWER(pr.drug) LIKE '%cefpodoxime%' OR LOWER(pr.drug) LIKE '%cefpirome%'
    OR LOWER(pr.drug) LIKE '%cefprozil%' OR LOWER(pr.drug) LIKE '%ceftibuten%'
    OR LOWER(pr.drug) LIKE '%ceftin%' OR LOWER(pr.drug) LIKE '%ceftriaxone%'
    OR LOWER(pr.drug) LIKE '%cefuroxime%' OR LOWER(pr.drug) LIKE '%cephalexin%'
    OR LOWER(pr.drug) LIKE '%cephalothin%' OR LOWER(pr.drug) LIKE '%cephapririn%'
    OR LOWER(pr.drug) LIKE '%chloramphenicol%' OR LOWER(pr.drug) LIKE '%cipro%'
    OR LOWER(pr.drug) LIKE '%ciprofloxacin%' OR LOWER(pr.drug) LIKE '%claforan%'
    OR LOWER(pr.drug) LIKE '%clarithromycin%' OR LOWER(pr.drug) LIKE '%cleocin%'
    OR LOWER(pr.drug) LIKE '%clindamycin%' OR LOWER(pr.drug) LIKE '%cubicin%'
    OR LOWER(pr.drug) LIKE '%dicloxacillin%' OR LOWER(pr.drug) LIKE '%dirithromycin%'
    OR LOWER(pr.drug) LIKE '%doryx%' OR LOWER(pr.drug) LIKE '%doxycy%'
    OR LOWER(pr.drug) LIKE '%duricef%' OR LOWER(pr.drug) LIKE '%dynacin%'
    OR LOWER(pr.drug) LIKE '%ery-tab%' OR LOWER(pr.drug) LIKE '%eryped%'
    OR LOWER(pr.drug) LIKE '%eryc%' OR LOWER(pr.drug) LIKE '%erythrocin%'
    OR LOWER(pr.drug) LIKE '%erythromycin%' OR LOWER(pr.drug) LIKE '%factive%'
    OR LOWER(pr.drug) LIKE '%flagyl%' OR LOWER(pr.drug) LIKE '%fortaz%'
    OR LOWER(pr.drug) LIKE '%furadantin%' OR LOWER(pr.drug) LIKE '%garamycin%'
    OR LOWER(pr.drug) LIKE '%gentamicin%' OR LOWER(pr.drug) LIKE '%kanamycin%'
    OR LOWER(pr.drug) LIKE '%keflex%' OR LOWER(pr.drug) LIKE '%kefzol%'
    OR LOWER(pr.drug) LIKE '%ketek%' OR LOWER(pr.drug) LIKE '%levaquin%'
    OR LOWER(pr.drug) LIKE '%levofloxacin%' OR LOWER(pr.drug) LIKE '%lincocin%'
    OR LOWER(pr.drug) LIKE '%linezolid%' OR LOWER(pr.drug) LIKE '%macrobid%'
    OR LOWER(pr.drug) LIKE '%macrodantin%' OR LOWER(pr.drug) LIKE '%maxipime%'
    OR LOWER(pr.drug) LIKE '%mefoxin%' OR LOWER(pr.drug) LIKE '%metronidazole%'
    OR LOWER(pr.drug) LIKE '%meropenem%' OR LOWER(pr.drug) LIKE '%methicillin%'
    OR LOWER(pr.drug) LIKE '%minocin%' OR LOWER(pr.drug) LIKE '%minocycline%'
    OR LOWER(pr.drug) LIKE '%monodox%' OR LOWER(pr.drug) LIKE '%monurol%'
    OR LOWER(pr.drug) LIKE '%morgidox%' OR LOWER(pr.drug) LIKE '%moxatag%'
    OR LOWER(pr.drug) LIKE '%moxifloxacin%' OR LOWER(pr.drug) LIKE '%mupirocin%'
    OR LOWER(pr.drug) LIKE '%myrac%' OR LOWER(pr.drug) LIKE '%nafcillin%'
    OR LOWER(pr.drug) LIKE '%neomycin%' OR LOWER(pr.drug) LIKE '%nicazel doxy 30%'
    OR LOWER(pr.drug) LIKE '%nitrofurantoin%' OR LOWER(pr.drug) LIKE '%norfloxacin%'
    OR LOWER(pr.drug) LIKE '%noroxin%' OR LOWER(pr.drug) LIKE '%ocudox%'
    OR LOWER(pr.drug) LIKE '%ofloxacin%' OR LOWER(pr.drug) LIKE '%omnicef%'
    OR LOWER(pr.drug) LIKE '%oracea%' OR LOWER(pr.drug) LIKE '%oraxyl%'
    OR LOWER(pr.drug) LIKE '%oxacillin%' OR LOWER(pr.drug) LIKE '%pc pen vk%'
    OR LOWER(pr.drug) LIKE '%pce dispertab%' OR LOWER(pr.drug) LIKE '%panixine%'
    OR LOWER(pr.drug) LIKE '%pediazole%' OR LOWER(pr.drug) LIKE '%penicillin%'
    OR LOWER(pr.drug) LIKE '%periostat%' OR LOWER(pr.drug) LIKE '%pfizerpen%'
    OR LOWER(pr.drug) LIKE '%piperacillin%' OR LOWER(pr.drug) LIKE '%tazobactam%'
    OR LOWER(pr.drug) LIKE '%primsol%' OR LOWER(pr.drug) LIKE '%proquin%'
    OR LOWER(pr.drug) LIKE '%raniclor%' OR LOWER(pr.drug) LIKE '%rifadin%'
    OR LOWER(pr.drug) LIKE '%rifampin%' OR LOWER(pr.drug) LIKE '%rocephin%'
    OR LOWER(pr.drug) LIKE '%smz-tmp%' OR LOWER(pr.drug) LIKE '%septra%'
    OR LOWER(pr.drug) LIKE '%septra ds%' OR LOWER(pr.drug) LIKE '%septra%'
    OR LOWER(pr.drug) LIKE '%solodyn%' OR LOWER(pr.drug) LIKE '%spectracef%'
    OR LOWER(pr.drug) LIKE '%streptomycin%' OR LOWER(pr.drug) LIKE '%sulfadiazine%'
    OR LOWER(pr.drug) LIKE '%sulfamethoxazole%' OR LOWER(pr.drug) LIKE '%trimethoprim%'
    OR LOWER(pr.drug) LIKE '%sulfatrim%' OR LOWER(pr.drug) LIKE '%sulfisoxazole%'
    OR LOWER(pr.drug) LIKE '%suprax%' OR LOWER(pr.drug) LIKE '%synercid%'
    OR LOWER(pr.drug) LIKE '%tazicef%' OR LOWER(pr.drug) LIKE '%tetracycline%'
    OR LOWER(pr.drug) LIKE '%timentin%' OR LOWER(pr.drug) LIKE '%tobramycin%'
    OR LOWER(pr.drug) LIKE '%trimethoprim%' OR LOWER(pr.drug) LIKE '%unasyn%'
    OR LOWER(pr.drug) LIKE '%vancocin%' OR LOWER(pr.drug) LIKE '%vancomycin%'
    OR LOWER(pr.drug) LIKE '%vantin%' OR LOWER(pr.drug) LIKE '%vibativ%'
    OR LOWER(pr.drug) LIKE '%vibra-tabs%' OR LOWER(pr.drug) LIKE '%vibramycin%'
    OR LOWER(pr.drug) LIKE '%zinacef%' OR LOWER(pr.drug) LIKE '%zithromax%'
    OR LOWER(pr.drug) LIKE '%zosyn%' OR LOWER(pr.drug) LIKE '%zyvox%'
)
"""

ANTIBIOTIC_ROUTE_EXCLUSION = """
(
    COALESCE(pr.route, '') IN ('OU', 'OS', 'OD', 'AU', 'AS', 'AD', 'TP')
    OR LOWER(COALESCE(pr.route, '')) LIKE '%ear%'
    OR LOWER(COALESCE(pr.route, '')) LIKE '%eye%'
    OR LOWER(COALESCE(pr.drug, '')) LIKE '%cream%'
    OR LOWER(COALESCE(pr.drug, '')) LIKE '%desensitization%'
    OR LOWER(COALESCE(pr.drug, '')) LIKE '%ophth oint%'
    OR LOWER(COALESCE(pr.drug, '')) LIKE '%gel%'
)
"""


def inter(name):
    return os.path.join(INTER_DIR, name + ".parquet").replace("\\", "/")


def exists(name):
    return os.path.exists(os.path.join(INTER_DIR, name + ".parquet"))


def register_views(con):
    hosp = HOSP_DIR.replace("\\", "/")
    icu  = ICU_DIR.replace("\\", "/")

    for tbl in ["admissions", "patients", "labevents", "microbiologyevents",
                "prescriptions", "diagnoses_icd", "services", "omr", "poe", "poe_detail"]:
        con.execute(
            f"CREATE OR REPLACE VIEW {tbl} AS "
            f"SELECT * FROM read_csv_auto('{hosp}/{tbl}.csv.gz', header=True)"
        )

    for tbl in ["icustays", "chartevents", "inputevents",
                "outputevents", "procedureevents", "d_items"]:
        con.execute(
            f"CREATE OR REPLACE VIEW {tbl} AS "
            f"SELECT * FROM read_csv_auto('{icu}/{tbl}.csv.gz', header=True)"
        )

    log.info("registered hosp + icu views")


def step01_cohort(con):
    name = "01_cohort"
    if exists(name):
        log.info("step01 cached")
        return
    t0 = time.time()
    # Reference: demographics/icustay_detail.sql + demographics/age.sql
    # This block keeps the official icustay_detail core fields and sequence flags,
    # with age computed from the official age.sql formula.
    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT
                    ie.subject_id,
                    ie.hadm_id,
                    ie.stay_id,
                    ie.intime,
                    ie.outtime,
                    p.gender,
                    p.dod,
                    a.admittime,
                    a.dischtime,
                    a.race,
                    a.hospital_expire_flag,
                    p.anchor_age + (YEAR(a.admittime) - p.anchor_year) AS age,
                    date_diff('day', a.admittime, a.dischtime) AS los_hospital,
                    ROUND(date_diff('hour', ie.intime, ie.outtime) / 24.0, 2) AS los_icu
                FROM icustays ie
                JOIN patients p ON ie.subject_id = p.subject_id
                JOIN admissions a ON ie.hadm_id = a.hadm_id
            )
            SELECT
                base.subject_id,
                base.hadm_id,
                base.stay_id,
                base.intime,
                base.outtime,
                base.age,
                base.gender,
                base.dod,
                base.admittime,
                base.dischtime,
                base.race,
                base.hospital_expire_flag,
                base.los_hospital,
                base.los_icu,
                DENSE_RANK() OVER (
                    PARTITION BY base.subject_id
                    ORDER BY base.admittime
                ) AS hospstay_seq,
                CASE
                    WHEN DENSE_RANK() OVER (
                        PARTITION BY base.subject_id
                        ORDER BY base.admittime
                    ) = 1 THEN TRUE
                    ELSE FALSE
                END AS first_hosp_stay,
                DENSE_RANK() OVER (
                    PARTITION BY base.hadm_id
                    ORDER BY base.intime
                ) AS icustay_seq,
                CASE
                    WHEN DENSE_RANK() OVER (
                        PARTITION BY base.hadm_id
                        ORDER BY base.intime
                    ) = 1 THEN TRUE
                    ELSE FALSE
                END AS first_icu_stay
            FROM base
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT subject_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step01 done %.1fs  stays=%d  patients=%d", time.time() - t0, r[0], r[1])


def step01b_icustay_times(con):
    name = "01b_icustay_times"
    if exists(name):
        log.info("step01b cached")
        return
    t0 = time.time()
    # Reference: demographics/icustay_times.sql
    # Build the official heart-rate-bounded ICU time range before generating
    # any hourly axis for downstream wide-table joins.
    con.execute(f"""
        COPY (
            WITH t1 AS (
                SELECT
                    ce.stay_id,
                    MIN(ce.charttime) AS intime_hr,
                    MAX(ce.charttime) AS outtime_hr
                FROM chartevents ce
                WHERE ce.itemid = 220045
                GROUP BY ce.stay_id
            )
            SELECT
                ie.subject_id,
                ie.hadm_id,
                ie.stay_id,
                t1.intime_hr,
                t1.outtime_hr
            FROM icustays ie
            LEFT JOIN t1
                ON ie.stay_id = t1.stay_id
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT stay_id), COUNT(intime_hr)
        FROM read_parquet('{inter(name)}')
    """).fetchone()
    log.info(
        "step01b done %.1fs  rows=%d  stays=%d  stays_with_hr=%d",
        time.time() - t0, r[0], r[1], r[2]
    )


def step02_time_axis(con):
    name = "02_time_axis"
    if exists(name):
        log.info("step02 cached")
        return
    t0 = time.time()
    # Reference: demographics/icustay_times.sql + demographics/icustay_hourly.sql
    # Follow the official hourly axis anchored on the first heart-rate time,
    # then keep local start/end helpers for the later hour-window joins.
    con.execute(f"CREATE OR REPLACE VIEW icustay_times_p AS SELECT * FROM read_parquet('{inter('01b_icustay_times')}')")
    con.execute(f"""
        COPY (
            WITH all_hours AS (
                SELECT
                    it.stay_id,
                    CASE
                        WHEN date_trunc('hour', it.intime_hr) = it.intime_hr
                            THEN it.intime_hr
                        ELSE date_trunc('hour', it.intime_hr) + INTERVAL 1 HOUR
                    END AS base_endtime,
                    generate_series(
                        -24,
                        CAST(CEIL(date_diff('minute', it.intime_hr, it.outtime_hr) / 60.0) AS INTEGER)
                    ) AS hrs
                FROM icustay_times_p it
                WHERE it.intime_hr IS NOT NULL
                  AND it.outtime_hr IS NOT NULL
            )
            SELECT
                ah.stay_id,
                CAST(hr.generate_series AS INTEGER) AS hr,
                ah.base_endtime + INTERVAL (CAST(hr.generate_series AS INTEGER) - 1) HOUR AS starttime,
                ah.base_endtime + INTERVAL (CAST(hr.generate_series AS INTEGER)) HOUR AS endtime,
                ah.base_endtime + INTERVAL (CAST(hr.generate_series AS INTEGER) - 1) HOUR AS charttime_floor
            FROM all_hours ah,
                 UNNEST(ah.hrs) AS hr(generate_series)
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id), MAX(hr) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step02 done %.1fs  rows=%d  stays=%d  max_hr=%d", time.time() - t0, r[0], r[1], r[2])


def step03_vitals(con):
    name = "03_vitals"
    if exists(name):
        log.info("step03 cached")
        return
    t0 = time.time()
    # Reference: measurement/vitalsign.sql
    # Build the official charttime-level vital-sign concept first,
    # then collapse it to ICU-hour rows for the wide table.
    # Itemids:
    #   heart_rate   220045
    #   sbp_ni       220179  dbp_ni  220180  mbp_ni  220181
    #   sbp_art      220050  dbp_art 220051  mbp_art 220052
    #   sbp_ni2      225309  dbp_ni2 225310  mbp_ni2 225312
    #   resp_rate    220210 224690
    #   temp_f       223761  temp_c  223762
    #   spo2         220277
    #   glucose      225664 220621 226537
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"""
        COPY (
            WITH vitalsign AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    AVG(
                        CASE
                            WHEN ce.itemid = 220045
                             AND ce.valuenum > 0
                             AND ce.valuenum < 300
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS heart_rate,
                    AVG(
                        CASE
                            WHEN ce.itemid IN (220179, 220050, 225309)
                             AND ce.valuenum > 0
                             AND ce.valuenum < 400
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS sbp,
                    AVG(
                        CASE
                            WHEN ce.itemid IN (220180, 220051, 225310)
                             AND ce.valuenum > 0
                             AND ce.valuenum < 300
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS dbp,
                    AVG(
                        CASE
                            WHEN ce.itemid IN (220052, 220181, 225312)
                             AND ce.valuenum > 0
                             AND ce.valuenum < 300
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS mbp,
                    AVG(
                        CASE
                            WHEN ce.itemid = 220179
                             AND ce.valuenum > 0
                             AND ce.valuenum < 400
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS sbp_ni,
                    AVG(
                        CASE
                            WHEN ce.itemid = 220180
                             AND ce.valuenum > 0
                             AND ce.valuenum < 300
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS dbp_ni,
                    AVG(
                        CASE
                            WHEN ce.itemid = 220181
                             AND ce.valuenum > 0
                             AND ce.valuenum < 300
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS mbp_ni,
                    AVG(
                        CASE
                            WHEN ce.itemid IN (220210, 224690)
                             AND ce.valuenum > 0
                             AND ce.valuenum < 70
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS resp_rate,
                    ROUND(
                        AVG(
                            CASE
                                WHEN ce.itemid = 223761
                                 AND ce.valuenum > 70
                                 AND ce.valuenum < 120
                                    THEN (ce.valuenum - 32) / 1.8
                                WHEN ce.itemid = 223762
                                 AND ce.valuenum > 10
                                 AND ce.valuenum < 50
                                    THEN ce.valuenum
                                ELSE NULL
                            END
                        ),
                        2
                    ) AS temperature,
                    MAX(CASE WHEN ce.itemid = 224642 THEN ce.value END) AS temperature_site,
                    AVG(
                        CASE
                            WHEN ce.itemid = 220277
                             AND ce.valuenum > 0
                             AND ce.valuenum <= 100
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS spo2,
                    AVG(
                        CASE
                            WHEN ce.itemid IN (225664, 220621, 226537)
                             AND ce.valuenum > 0
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS glucose_vital
                FROM chartevents ce
                INNER JOIN cohort co ON ce.stay_id = co.stay_id
                WHERE ce.itemid IN (
                    220045,
                    220179, 220180, 220181,
                    220050, 220051, 220052,
                    225309, 225310, 225312,
                    220210, 224690,
                    223761, 223762, 224642,
                    220277,
                    225664, 220621, 226537
                )
                GROUP BY ce.subject_id, ce.stay_id, ce.charttime
            ),
            vitalsign_hourly AS (
                WITH ranked AS (
                    SELECT
                        vs.stay_id,
                        date_trunc('hour', vs.charttime) AS charttime_floor,
                        vs.charttime,
                        vs.heart_rate,
                        vs.sbp,
                        vs.dbp,
                        vs.mbp,
                        vs.sbp_ni,
                        vs.dbp_ni,
                        vs.mbp_ni,
                        vs.resp_rate,
                        vs.temperature,
                        vs.temperature_site,
                        vs.spo2,
                        vs.glucose_vital,
                        ROW_NUMBER() OVER (
                            PARTITION BY vs.stay_id, date_trunc('hour', vs.charttime)
                            ORDER BY vs.charttime DESC
                        ) AS hour_seq
                    FROM vitalsign vs
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    AVG(heart_rate) AS heart_rate,
                    AVG(sbp) AS sbp,
                    AVG(dbp) AS dbp,
                    AVG(mbp) AS mbp,
                    AVG(sbp_ni) AS sbp_ni,
                    AVG(dbp_ni) AS dbp_ni,
                    AVG(mbp_ni) AS mbp_ni,
                    AVG(resp_rate) AS resp_rate,
                    AVG(temperature) AS temperature,
                    MAX(CASE WHEN hour_seq = 1 THEN temperature_site END) AS temperature_site,
                    AVG(spo2) AS spo2,
                    AVG(glucose_vital) AS glucose_vital
                FROM ranked
                GROUP BY stay_id, charttime_floor
            )
            SELECT
                stay_id,
                charttime_floor,
                heart_rate,
                sbp,
                dbp,
                mbp,
                sbp_ni,
                dbp_ni,
                mbp_ni,
                resp_rate,
                temperature,
                temperature_site,
                spo2,
                glucose_vital
            FROM vitalsign_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step03 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step03b_icp(con):
    name = "03b_icp"
    if exists(name):
        log.info("step03b cached")
        return
    t0 = time.time()
    # Reference: measurement/icp.sql
    # Build the official charttime-level ICP concept first, then project it
    # onto ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH icp_event AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    MAX(
                        CASE
                            WHEN ce.valuenum > 0 AND ce.valuenum < 100 THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS icp
                FROM chartevents ce
                WHERE ce.itemid IN (220765, 227989)
                GROUP BY ce.subject_id, ce.stay_id, ce.charttime
            ),
            icp_hourly AS (
                WITH ranked AS (
                    SELECT
                        ie.stay_id,
                        date_trunc('hour', ie.charttime) AS charttime_floor,
                        ie.charttime,
                        ie.icp,
                        ROW_NUMBER() OVER (
                            PARTITION BY ie.stay_id, date_trunc('hour', ie.charttime)
                            ORDER BY ie.charttime DESC
                        ) AS hour_seq
                    FROM icp_event ie
                    INNER JOIN cohort co
                        ON ie.stay_id = co.stay_id
                       AND ie.charttime >= co.intime - INTERVAL '24' HOUR
                       AND ie.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    icp
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                icp
            FROM icp_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step03b done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step03c_oxygen_delivery(con):
    name = "03c_oxygen_delivery"
    if exists(name):
        log.info("step03c cached")
        return
    t0 = time.time()
    # Reference: measurement/oxygen_delivery.sql
    # Build the official charttime-level oxygen-delivery concept first, then
    # project it onto ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH ce_stg1 AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    CASE
                        WHEN ce.itemid IN (223834, 227582) THEN 223834
                        ELSE ce.itemid
                    END AS itemid,
                    ce.value,
                    ce.valuenum,
                    ce.valueuom,
                    ce.storetime
                FROM chartevents ce
                WHERE ce.value IS NOT NULL
                  AND ce.itemid IN (223834, 227582, 227287)
            ),
            ce_stg2 AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    ce.itemid,
                    ce.value,
                    ce.valuenum,
                    ce.valueuom,
                    ROW_NUMBER() OVER (
                        PARTITION BY ce.subject_id, ce.charttime, ce.itemid
                        ORDER BY ce.storetime DESC
                    ) AS rn
                FROM ce_stg1 ce
            ),
            o2 AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    ce.itemid,
                    ce.value AS o2_device,
                    ROW_NUMBER() OVER (
                        PARTITION BY ce.subject_id, ce.charttime, ce.itemid
                        ORDER BY ce.value
                    ) AS rn
                FROM chartevents ce
                WHERE ce.itemid = 226732
            ),
            stg AS (
                SELECT
                    COALESCE(ce.subject_id, o2.subject_id) AS subject_id,
                    COALESCE(ce.stay_id, o2.stay_id) AS stay_id,
                    COALESCE(ce.charttime, o2.charttime) AS charttime,
                    COALESCE(ce.itemid, o2.itemid) AS itemid,
                    ce.value,
                    ce.valuenum,
                    o2.o2_device,
                    o2.rn
                FROM ce_stg2 ce
                FULL OUTER JOIN o2
                    ON ce.subject_id = o2.subject_id
                   AND ce.charttime = o2.charttime
                WHERE ce.rn = 1
            ),
            oxygen_delivery AS (
                SELECT
                    subject_id,
                    MAX(stay_id) AS stay_id,
                    charttime,
                    MAX(CASE WHEN itemid = 223834 THEN valuenum END) AS o2_flow,
                    MAX(CASE WHEN itemid = 227287 THEN valuenum END) AS o2_flow_additional,
                    MAX(CASE WHEN rn = 1 THEN o2_device END) AS o2_delivery_device_1,
                    MAX(CASE WHEN rn = 2 THEN o2_device END) AS o2_delivery_device_2,
                    MAX(CASE WHEN rn = 3 THEN o2_device END) AS o2_delivery_device_3,
                    MAX(CASE WHEN rn = 4 THEN o2_device END) AS o2_delivery_device_4
                FROM stg
                GROUP BY subject_id, charttime
            ),
            oxygen_delivery_hourly AS (
                WITH ranked AS (
                    SELECT
                        od.stay_id,
                        date_trunc('hour', od.charttime) AS charttime_floor,
                        od.charttime,
                        od.o2_flow,
                        od.o2_flow_additional,
                        od.o2_delivery_device_1,
                        od.o2_delivery_device_2,
                        od.o2_delivery_device_3,
                        od.o2_delivery_device_4,
                        ROW_NUMBER() OVER (
                            PARTITION BY od.stay_id, date_trunc('hour', od.charttime)
                            ORDER BY od.charttime DESC
                        ) AS hour_seq
                    FROM oxygen_delivery od
                    INNER JOIN cohort co
                        ON od.stay_id = co.stay_id
                       AND od.charttime >= co.intime - INTERVAL '24' HOUR
                       AND od.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    o2_flow,
                    o2_flow_additional,
                    o2_delivery_device_1,
                    o2_delivery_device_2,
                    o2_delivery_device_3,
                    o2_delivery_device_4
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                o2_flow,
                o2_flow_additional,
                o2_delivery_device_1,
                o2_delivery_device_2,
                o2_delivery_device_3,
                o2_delivery_device_4
            FROM oxygen_delivery_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step03c done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step03d_rhythm(con):
    name = "03d_rhythm"
    if exists(name):
        log.info("step03d cached")
        return
    t0 = time.time()
    # Reference: measurement/rhythm.sql
    # Build the official charttime-level rhythm concept first, then project it
    # onto ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH rhythm AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    MAX(CASE WHEN ce.itemid = 220048 THEN ce.value END) AS heart_rhythm,
                    MAX(CASE WHEN ce.itemid = 224650 THEN ce.value END) AS ectopy_type,
                    MAX(CASE WHEN ce.itemid = 224651 THEN ce.value END) AS ectopy_frequency,
                    MAX(CASE WHEN ce.itemid = 226479 THEN ce.value END) AS ectopy_type_secondary,
                    MAX(CASE WHEN ce.itemid = 226480 THEN ce.value END) AS ectopy_frequency_secondary
                FROM chartevents ce
                WHERE ce.stay_id IS NOT NULL
                  AND ce.itemid IN (220048, 224650, 224651, 226479, 226480)
                GROUP BY ce.subject_id, ce.stay_id, ce.charttime
            ),
            rhythm_hourly AS (
                WITH ranked AS (
                    SELECT
                        rh.stay_id,
                        date_trunc('hour', rh.charttime) AS charttime_floor,
                        rh.charttime,
                        rh.heart_rhythm,
                        rh.ectopy_type,
                        rh.ectopy_frequency,
                        rh.ectopy_type_secondary,
                        rh.ectopy_frequency_secondary,
                        ROW_NUMBER() OVER (
                            PARTITION BY rh.stay_id, date_trunc('hour', rh.charttime)
                            ORDER BY rh.charttime DESC
                        ) AS hour_seq
                    FROM rhythm rh
                    INNER JOIN cohort co
                        ON rh.stay_id = co.stay_id
                       AND rh.charttime >= co.intime - INTERVAL '24' HOUR
                       AND rh.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    heart_rhythm,
                    ectopy_type,
                    ectopy_frequency,
                    ectopy_type_secondary,
                    ectopy_frequency_secondary
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                heart_rhythm,
                ectopy_type,
                ectopy_frequency,
                ectopy_type_secondary,
                ectopy_frequency_secondary
            FROM rhythm_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step03d done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step03e_ventilator_setting(con):
    name = "03e_ventilator_setting"
    if exists(name):
        log.info("step03e cached")
        return
    t0 = time.time()
    # Reference: measurement/ventilator_setting.sql
    # Build the official charttime-level ventilator-setting concept first, then
    # project it onto ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH ce AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    ce.itemid,
                    ce.value,
                    CASE
                        WHEN ce.itemid = 223835 THEN
                            CASE
                                WHEN ce.valuenum >= 0.20 AND ce.valuenum <= 1 THEN ce.valuenum * 100
                                WHEN ce.valuenum > 1 AND ce.valuenum < 20 THEN NULL
                                WHEN ce.valuenum >= 20 AND ce.valuenum <= 100 THEN ce.valuenum
                                ELSE NULL
                            END
                        WHEN ce.itemid IN (220339, 224700) THEN
                            CASE
                                WHEN ce.valuenum > 100 THEN NULL
                                WHEN ce.valuenum < 0 THEN NULL
                                ELSE ce.valuenum
                            END
                        ELSE ce.valuenum
                    END AS valuenum,
                    ce.valueuom,
                    ce.storetime
                FROM chartevents ce
                WHERE ce.value IS NOT NULL
                  AND ce.stay_id IS NOT NULL
                  AND ce.itemid IN (
                      224688, 224689, 224690, 224687, 224685, 224684, 224686,
                      224696, 220339, 224700, 223835, 223849, 229314, 223848, 224691
                  )
            ),
            ventilator_setting AS (
                SELECT
                    subject_id,
                    MAX(stay_id) AS stay_id,
                    charttime,
                    MAX(CASE WHEN itemid = 224688 THEN valuenum END) AS respiratory_rate_set,
                    MAX(CASE WHEN itemid = 224690 THEN valuenum END) AS respiratory_rate_total,
                    MAX(CASE WHEN itemid = 224689 THEN valuenum END) AS respiratory_rate_spontaneous,
                    MAX(CASE WHEN itemid = 224687 THEN valuenum END) AS minute_volume,
                    MAX(CASE WHEN itemid = 224684 THEN valuenum END) AS tidal_volume_set,
                    MAX(CASE WHEN itemid = 224685 THEN valuenum END) AS tidal_volume_observed,
                    MAX(CASE WHEN itemid = 224686 THEN valuenum END) AS tidal_volume_spontaneous,
                    MAX(CASE WHEN itemid = 224696 THEN valuenum END) AS plateau_pressure,
                    MAX(CASE WHEN itemid IN (220339, 224700) THEN valuenum END) AS peep_vent,
                    MAX(CASE WHEN itemid = 223835 THEN valuenum END) AS fio2_vent,
                    MAX(CASE WHEN itemid = 224691 THEN valuenum END) AS flow_rate,
                    MAX(CASE WHEN itemid = 223849 THEN value END) AS ventilator_mode,
                    MAX(CASE WHEN itemid = 229314 THEN value END) AS ventilator_mode_hamilton,
                    MAX(CASE WHEN itemid = 223848 THEN value END) AS ventilator_type
                FROM ce
                GROUP BY subject_id, charttime
            ),
            ventilator_setting_hourly AS (
                WITH ranked AS (
                    SELECT
                        vs.stay_id,
                        date_trunc('hour', vs.charttime) AS charttime_floor,
                        vs.charttime,
                        vs.respiratory_rate_set,
                        vs.respiratory_rate_total,
                        vs.respiratory_rate_spontaneous,
                        vs.minute_volume,
                        vs.tidal_volume_set,
                        vs.tidal_volume_observed,
                        vs.tidal_volume_spontaneous,
                        vs.plateau_pressure,
                        vs.peep_vent,
                        vs.fio2_vent,
                        vs.flow_rate,
                        vs.ventilator_mode,
                        vs.ventilator_mode_hamilton,
                        vs.ventilator_type,
                        ROW_NUMBER() OVER (
                            PARTITION BY vs.stay_id, date_trunc('hour', vs.charttime)
                            ORDER BY vs.charttime DESC
                        ) AS hour_seq
                    FROM ventilator_setting vs
                    INNER JOIN cohort co
                        ON vs.stay_id = co.stay_id
                       AND vs.charttime >= co.intime - INTERVAL '24' HOUR
                       AND vs.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    respiratory_rate_set,
                    respiratory_rate_total,
                    respiratory_rate_spontaneous,
                    minute_volume,
                    tidal_volume_set,
                    tidal_volume_observed,
                    tidal_volume_spontaneous,
                    plateau_pressure,
                    peep_vent,
                    fio2_vent,
                    flow_rate,
                    ventilator_mode,
                    ventilator_mode_hamilton,
                    ventilator_type
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                respiratory_rate_set,
                respiratory_rate_total,
                respiratory_rate_spontaneous,
                minute_volume,
                tidal_volume_set,
                tidal_volume_observed,
                tidal_volume_spontaneous,
                plateau_pressure,
                peep_vent,
                fio2_vent,
                flow_rate,
                ventilator_mode,
                ventilator_mode_hamilton,
                ventilator_type
            FROM ventilator_setting_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step03e done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step03f_code_status(con):
    name = "03f_code_status"
    if exists(name):
        log.info("step03f cached")
        return
    t0 = time.time()
    # Reference: treatment/code_status.sql
    # Build the official charttime-level code-status concept first, then project
    # it onto ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH t1 AS (
                SELECT
                    subject_id,
                    hadm_id,
                    stay_id,
                    charttime,
                    CASE WHEN value IN ('Full code') THEN 1 ELSE 0 END AS fullcode,
                    CASE WHEN value IN ('Comfort measures only') THEN 1 ELSE 0 END AS cmo,
                    CASE WHEN value IN ('DNI (do not intubate)', 'DNR / DNI') THEN 1 ELSE 0 END AS dni,
                    CASE WHEN value IN ('DNR (do not resuscitate)', 'DNR / DNI') THEN 1 ELSE 0 END AS dnr
                FROM chartevents
                WHERE itemid IN (223758)
            ),
            poe_cs AS (
                SELECT
                    p.subject_id,
                    p.hadm_id,
                    ie.stay_id,
                    p.ordertime AS charttime,
                    CASE
                        WHEN pd.field_value = 'Resuscitate (Full code)' THEN 1
                        WHEN pd.field_value = 'Full code  (attempt resuscitation)' THEN 1
                        ELSE 0
                    END AS fullcode,
                    0 AS cmo,
                    CASE
                        WHEN pd.field_value = 'Do not resuscitate (DNR/DNI)' THEN 1
                        ELSE 0
                    END AS dni,
                    CASE
                        WHEN pd.field_value = 'DNAR (DO NOT attempt resuscitation for cardiac arrest) ' THEN 1
                        WHEN pd.field_value = 'Do not resuscitate (DNR/DNI)' THEN 1
                        ELSE 0
                    END AS dnr
                FROM poe p
                INNER JOIN poe_detail pd
                    ON p.poe_id = pd.poe_id
                LEFT JOIN icustays ie
                    ON p.hadm_id = ie.hadm_id
                   AND p.ordertime >= ie.intime
                   AND p.ordertime <= ie.outtime
                WHERE p.order_type = 'General Care'
                  AND p.order_subtype = 'Code status'
            ),
            code_status AS (
                SELECT
                    subject_id,
                    hadm_id,
                    stay_id,
                    charttime,
                    fullcode,
                    cmo,
                    dni,
                    dnr
                FROM t1
                UNION ALL
                SELECT
                    subject_id,
                    hadm_id,
                    stay_id,
                    charttime,
                    fullcode,
                    cmo,
                    dni,
                    dnr
                FROM poe_cs
            ),
            code_status_hourly AS (
                WITH ranked AS (
                    SELECT
                        co.stay_id,
                        date_trunc('hour', cs.charttime) AS charttime_floor,
                        cs.charttime,
                        cs.fullcode,
                        cs.cmo,
                        cs.dni,
                        cs.dnr,
                        ROW_NUMBER() OVER (
                            PARTITION BY co.stay_id, date_trunc('hour', cs.charttime)
                            ORDER BY cs.charttime DESC
                        ) AS hour_seq
                    FROM code_status cs
                    INNER JOIN cohort co
                        ON cs.hadm_id = co.hadm_id
                       AND cs.charttime <= co.outtime
                    WHERE cs.stay_id IS NOT NULL
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    fullcode,
                    cmo,
                    dni,
                    dnr
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                fullcode,
                cmo,
                dni,
                dnr
            FROM code_status_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step03f done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step03g_invasive_line(con):
    name = "03g_invasive_line"
    if exists(name):
        log.info("step03g cached")
        return
    t0 = time.time()
    # Reference: treatment/invasive_line.sql
    # Build the official interval-level invasive-line concept first, then
    # project active lines onto ICU-hour rows for the wide table.
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
    con.execute(f"""
        COPY (
            WITH mv AS (
                SELECT
                    mv.stay_id,
                    mv.itemid AS line_number,
                    di.label AS line_type,
                    mv.location AS line_site,
                    mv.starttime,
                    mv.endtime
                FROM procedureevents mv
                INNER JOIN d_items di
                    ON mv.itemid = di.itemid
                WHERE mv.itemid IN (
                    227719, 225752, 224269, 224267, 224270, 224272, 226124,
                    228169, 225202, 228286, 225204, 224263, 224560, 224264,
                    225203, 224273, 225789, 225761, 228201, 228202, 224268,
                    225199, 225315, 225205
                )
            ),
            invasive_line AS (
                SELECT
                    stay_id,
                    CASE
                        WHEN line_type IN ('Arterial Line', 'A-Line') THEN 'Arterial'
                        WHEN line_type IN ('CCO PA Line', 'CCO PAC') THEN 'Continuous Cardiac Output PA'
                        WHEN line_type IN ('Dialysis Catheter', 'Dialysis Line') THEN 'Dialysis'
                        WHEN line_type IN ('Hickman', 'Tunneled (Hickman) Line') THEN 'Hickman'
                        WHEN line_type IN ('IABP', 'IABP line') THEN 'IABP'
                        WHEN line_type IN ('Multi Lumen', 'Multi-lumen') THEN 'Multi Lumen'
                        WHEN line_type IN ('PA Catheter', 'PA line') THEN 'PA'
                        WHEN line_type IN ('PICC Line', 'PICC line') THEN 'PICC'
                        WHEN line_type IN ('Pre-Sep Catheter', 'Presep Catheter') THEN 'Pre-Sep'
                        WHEN line_type IN ('Trauma Line', 'Trauma line') THEN 'Trauma'
                        WHEN line_type IN ('Triple Introducer', 'TripleIntroducer') THEN 'Triple Introducer'
                        WHEN line_type IN ('Portacath', 'Indwelling Port (PortaCath)') THEN 'Portacath'
                        ELSE line_type
                    END AS line_type,
                    CASE
                        WHEN line_site IN ('Left Antecub', 'Left Antecube') THEN 'Left Antecube'
                        WHEN line_site IN ('Left Axilla', 'Left Axilla.') THEN 'Left Axilla'
                        WHEN line_site IN ('Left Brachial', 'Left Brachial.') THEN 'Left Brachial'
                        WHEN line_site IN ('Left Femoral', 'Left Femoral.') THEN 'Left Femoral'
                        WHEN line_site IN ('Right Antecub', 'Right Antecube') THEN 'Right Antecube'
                        WHEN line_site IN ('Right Axilla', 'Right Axilla.') THEN 'Right Axilla'
                        WHEN line_site IN ('Right Brachial', 'Right Brachial.') THEN 'Right Brachial'
                        WHEN line_site IN ('Right Femoral', 'Right Femoral.') THEN 'Right Femoral'
                        ELSE line_site
                    END AS line_site,
                    starttime,
                    endtime
                FROM mv
            ),
            invasive_line_hourly AS (
                SELECT
                    ta.stay_id,
                    ta.charttime_floor,
                    COUNT(*) AS invasive_line_count,
                    string_agg(DISTINCT il.line_type, ' | ') AS invasive_line_types,
                    string_agg(DISTINCT il.line_site, ' | ') AS invasive_line_sites
                FROM time_axis ta
                INNER JOIN invasive_line il
                    ON ta.stay_id = il.stay_id
                   AND ta.starttime < il.endtime
                   AND ta.endtime > il.starttime
                GROUP BY ta.stay_id, ta.charttime_floor
            )
            SELECT
                stay_id,
                charttime_floor,
                invasive_line_count,
                invasive_line_types,
                invasive_line_sites
            FROM invasive_line_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step03g done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step03h_rrt(con):
    name = "03h_rrt"
    if exists(name):
        log.info("step03h cached")
        return
    t0 = time.time()
    # Reference: treatment/rrt.sql
    # Build the official charttime/interval-level RRT concept first, then
    # project it onto ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH ce AS (
                SELECT
                    ce.stay_id,
                    ce.charttime,
                    CASE
                        WHEN ce.itemid IN (226118, 227357, 225725) THEN 1
                        WHEN ce.itemid IN (
                            226499, 224154, 225810, 225959, 227639, 225183, 227438,
                            224191, 225806, 225807, 228004, 228005, 228006, 224144,
                            224145, 224149, 224150, 224151, 224152, 224153, 224404,
                            224406, 226457
                        ) THEN 1
                        WHEN ce.itemid IN (
                            224135, 224139, 224146, 225323, 225740, 225776, 225951,
                            225952, 225953, 225954, 225956, 225958, 225961, 225963,
                            225965, 225976, 225977, 227124, 227290, 227638, 227640,
                            227753
                        ) THEN 1
                        ELSE 0
                    END AS dialysis_present,
                    CASE
                        WHEN ce.itemid = 225965 AND ce.value = 'In use' THEN 1
                        WHEN ce.itemid IN (
                            226499, 224154, 225183, 227438, 224191, 225806, 225807,
                            228004, 228005, 228006, 224144, 224145, 224153, 226457
                        ) THEN 1
                        ELSE 0
                    END AS dialysis_active,
                    CASE
                        WHEN ce.itemid = 227290 THEN ce.value
                        WHEN ce.itemid IN (
                            225810, 225806, 225807, 227639, 225959, 225951, 225952,
                            225961, 225953, 225963, 225965, 227638, 227640
                        ) THEN 'Peritoneal'
                        WHEN ce.itemid = 226499 THEN 'IHD'
                        ELSE NULL
                    END AS dialysis_type
                FROM chartevents ce
                WHERE ce.itemid IN (
                    226118, 227357, 225725, 226499, 224154, 225810, 227639, 225183,
                    227438, 224191, 225806, 225807, 228004, 228005, 228006, 224144,
                    224145, 224149, 224150, 224151, 224152, 224153, 224404, 224406,
                    226457, 225959, 224135, 224139, 224146, 225323, 225740, 225776,
                    225951, 225952, 225953, 225954, 225956, 225958, 225961, 225963,
                    225965, 225976, 225977, 227124, 227290, 227638, 227640, 227753
                )
                  AND ce.value IS NOT NULL
            ),
            mv_ranges AS (
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    1 AS dialysis_present,
                    1 AS dialysis_active,
                    'CRRT' AS dialysis_type
                FROM inputevents
                WHERE itemid IN (227536, 227525)
                  AND amount > 0
                UNION DISTINCT
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    1 AS dialysis_present,
                    CASE WHEN itemid NOT IN (224270, 225436) THEN 1 ELSE 0 END AS dialysis_active,
                    CASE
                        WHEN itemid = 225441 THEN 'IHD'
                        WHEN itemid = 225802 THEN 'CRRT'
                        WHEN itemid = 225803 THEN 'CVVHD'
                        WHEN itemid = 225805 THEN 'Peritoneal'
                        WHEN itemid = 225809 THEN 'CVVHDF'
                        WHEN itemid = 225955 THEN 'SCUF'
                        ELSE NULL
                    END AS dialysis_type
                FROM procedureevents
                WHERE itemid IN (225441, 225802, 225803, 225805, 224270, 225809, 225955, 225436)
                  AND value IS NOT NULL
            ),
            stg0 AS (
                SELECT
                    stay_id,
                    charttime,
                    dialysis_present,
                    dialysis_active,
                    dialysis_type
                FROM ce
                WHERE dialysis_present = 1
                UNION DISTINCT
                SELECT
                    stay_id,
                    starttime AS charttime,
                    dialysis_present,
                    dialysis_active,
                    dialysis_type
                FROM mv_ranges
            ),
            rrt_event AS (
                SELECT
                    stg0.stay_id,
                    stg0.charttime,
                    COALESCE(mv.dialysis_present, stg0.dialysis_present) AS dialysis_present,
                    COALESCE(mv.dialysis_active, stg0.dialysis_active) AS dialysis_active,
                    COALESCE(mv.dialysis_type, stg0.dialysis_type) AS dialysis_type
                FROM stg0
                LEFT JOIN mv_ranges mv
                    ON stg0.stay_id = mv.stay_id
                   AND stg0.charttime >= mv.starttime
                   AND stg0.charttime <= mv.endtime
            ),
            rrt_hourly AS (
                WITH ranked AS (
                    SELECT
                        re.stay_id,
                        date_trunc('hour', re.charttime) AS charttime_floor,
                        re.charttime,
                        re.dialysis_present,
                        re.dialysis_active,
                        re.dialysis_type,
                        ROW_NUMBER() OVER (
                            PARTITION BY re.stay_id, date_trunc('hour', re.charttime)
                            ORDER BY re.charttime DESC
                        ) AS hour_seq
                    FROM rrt_event re
                    INNER JOIN cohort co
                        ON re.stay_id = co.stay_id
                       AND re.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    dialysis_present,
                    dialysis_active,
                    dialysis_type
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                dialysis_present,
                dialysis_active,
                dialysis_type
            FROM rrt_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step03h done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step12_uo(con):
    name = "12_uo"
    if exists(name):
        log.info("step12 cached")
        return
    t0 = time.time()
    # Reference: measurement/urine_output.sql + measurement/urine_output_rate.sql
    # Build the official urine-output-rate concept first, then collapse it
    # to ICU-hour rows for the wide table.
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"CREATE OR REPLACE VIEW icustay_times_p AS SELECT * FROM read_parquet('{inter('01b_icustay_times')}')")
    con.execute(f"""
        COPY (
            WITH uo_raw AS (
                SELECT
                    oe.stay_id,
                    oe.charttime,
                    CASE
                        WHEN oe.itemid = 227488 AND oe.value > 0 THEN -oe.value
                        ELSE oe.value
                    END AS urineoutput
                FROM outputevents oe
                WHERE oe.itemid IN (
                    226559, 226560, 226561, 226584, 226563, 226564,
                    226565, 226567, 226557, 226558, 227488, 227489
                )
                  AND oe.value IS NOT NULL
            ),
            urine_output AS (
                SELECT
                    stay_id,
                    charttime,
                    SUM(urineoutput) AS urineoutput
                FROM uo_raw
                GROUP BY stay_id, charttime
            ),
            tm AS (
                SELECT
                    stay_id,
                    intime_hr,
                    outtime_hr
                FROM icustay_times_p
                
                WHERE intime_hr IS NOT NULL
                  AND outtime_hr IS NOT NULL
            ),
            uo_tm AS (
                SELECT
                    tm.stay_id,
                    CASE
                        WHEN LAG(uo.charttime) OVER w IS NULL
                            THEN date_diff('minute', tm.intime_hr, uo.charttime)
                        ELSE date_diff('minute', LAG(uo.charttime) OVER w, uo.charttime)
                    END AS tm_since_last_uo,
                    uo.charttime,
                    uo.urineoutput
                FROM tm
                INNER JOIN urine_output uo
                    ON tm.stay_id = uo.stay_id
                WINDOW w AS (
                    PARTITION BY tm.stay_id
                    ORDER BY uo.charttime
                )
            ),
            ur_stg AS (
                SELECT
                    io.stay_id,
                    io.charttime,
                    SUM(DISTINCT io.urineoutput) AS uo,
                    SUM(
                        CASE
                            WHEN date_diff('hour', iosum.charttime, io.charttime) <= 5
                                THEN iosum.urineoutput
                            ELSE NULL
                        END
                    ) AS urineoutput_6hr,
                    SUM(
                        CASE
                            WHEN date_diff('hour', iosum.charttime, io.charttime) <= 5
                                THEN iosum.tm_since_last_uo
                            ELSE NULL
                        END
                    ) / 60.0 AS uo_tm_6hr,
                    SUM(
                        CASE
                            WHEN date_diff('hour', iosum.charttime, io.charttime) <= 11
                                THEN iosum.urineoutput
                            ELSE NULL
                        END
                    ) AS urineoutput_12hr,
                    SUM(
                        CASE
                            WHEN date_diff('hour', iosum.charttime, io.charttime) <= 11
                                THEN iosum.tm_since_last_uo
                            ELSE NULL
                        END
                    ) / 60.0 AS uo_tm_12hr,
                    SUM(iosum.urineoutput) AS urineoutput_24hr,
                    SUM(iosum.tm_since_last_uo) / 60.0 AS uo_tm_24hr
                FROM uo_tm io
                LEFT JOIN uo_tm iosum
                    ON io.stay_id = iosum.stay_id
                    AND io.charttime >= iosum.charttime
                    AND io.charttime <= iosum.charttime + INTERVAL 23 HOUR
                GROUP BY io.stay_id, io.charttime
            ),
            wt_raw AS (
                SELECT
                    ce.stay_id,
                    ce.charttime,
                    CASE WHEN ce.itemid = 226512 THEN 'admit' ELSE 'daily' END AS wt_type,
                    ce.valuenum AS weight
                FROM chartevents ce
                WHERE ce.itemid IN (224639, 226512)
                  AND ce.valuenum IS NOT NULL
                  AND ce.valuenum > 0
            ),
            wt_stg1 AS (
                SELECT
                    stay_id,
                    wt_type,
                    charttime,
                    weight,
                    ROW_NUMBER() OVER (
                        PARTITION BY stay_id, wt_type
                        ORDER BY charttime
                    ) AS rn
                FROM wt_raw
            ),
            wt_stg2 AS (
                SELECT
                    w1.stay_id,
                    co.intime,
                    co.outtime,
                    w1.wt_type,
                    CASE
                        WHEN w1.wt_type = 'admit' AND w1.rn = 1
                            THEN co.intime - INTERVAL 2 HOUR
                        ELSE w1.charttime
                    END AS starttime,
                    w1.weight
                FROM wt_stg1 w1
                INNER JOIN cohort co
                    ON co.stay_id = w1.stay_id
            ),
            wt_stg3 AS (
                SELECT
                    stay_id,
                    intime,
                    outtime,
                    starttime,
                    COALESCE(
                        LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime),
                        outtime + INTERVAL 2 HOUR
                    ) AS endtime,
                    weight,
                    wt_type
                FROM wt_stg2
            ),
            wt1 AS (
                SELECT
                    stay_id,
                    starttime,
                    COALESCE(
                        endtime,
                        LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime),
                        outtime + INTERVAL 2 HOUR
                    ) AS endtime,
                    weight,
                    wt_type
                FROM wt_stg3
            ),
            wt_fix AS (
                SELECT
                    co.stay_id,
                    co.intime - INTERVAL 2 HOUR AS starttime,
                    MIN(wt.starttime) AS endtime,
                    wt.weight,
                    wt.wt_type
                FROM cohort co
                INNER JOIN wt1 wt
                    ON co.stay_id = wt.stay_id
                WHERE wt.wt_type = 'daily'
                GROUP BY co.stay_id, co.intime, wt.weight, wt.wt_type
                HAVING MIN(wt.starttime) > co.intime - INTERVAL 2 HOUR
            ),
            weight_durations AS (
                SELECT stay_id, starttime, endtime, weight
                FROM wt1
                UNION ALL
                SELECT stay_id, starttime, endtime, weight
                FROM wt_fix
            ),
            uo_event AS (
                SELECT
                    ur.stay_id,
                    ur.charttime,
                    wd.weight,
                    ur.uo,
                    ur.urineoutput_6hr,
                    ur.urineoutput_12hr,
                    ur.urineoutput_24hr,
                    CASE
                        WHEN ur.uo_tm_6hr >= 6 AND wd.weight > 0
                            THEN ROUND(ur.urineoutput_6hr / wd.weight / ur.uo_tm_6hr, 4)
                    END AS uo_mlkghr_6hr,
                    CASE
                        WHEN ur.uo_tm_12hr >= 12 AND wd.weight > 0
                            THEN ROUND(ur.urineoutput_12hr / wd.weight / ur.uo_tm_12hr, 4)
                    END AS uo_mlkghr_12hr,
                    CASE
                        WHEN ur.uo_tm_24hr >= 24 AND wd.weight > 0
                            THEN ROUND(ur.urineoutput_24hr / wd.weight / ur.uo_tm_24hr, 4)
                    END AS uo_mlkghr_24hr,
                    ROUND(ur.uo_tm_6hr, 2) AS uo_tm_6hr,
                    ROUND(ur.uo_tm_12hr, 2) AS uo_tm_12hr,
                    ROUND(ur.uo_tm_24hr, 2) AS uo_tm_24hr,
                    CASE
                        WHEN ur.uo_tm_24hr >= 22 AND ur.uo_tm_24hr <= 30
                            THEN ur.urineoutput_24hr / ur.uo_tm_24hr * 24.0
                    END AS uo_24hr
                FROM ur_stg ur
                LEFT JOIN weight_durations wd
                    ON ur.stay_id = wd.stay_id
                    AND ur.charttime > wd.starttime
                    AND ur.charttime <= wd.endtime
                    AND wd.weight > 0
            ),
            uo_hourly AS (
                WITH hour_last AS (
                    SELECT
                        ue.*,
                        date_trunc('hour', ue.charttime) AS charttime_floor,
                        ROW_NUMBER() OVER (
                            PARTITION BY ue.stay_id, date_trunc('hour', ue.charttime)
                            ORDER BY ue.charttime DESC
                        ) AS hour_seq
                    FROM uo_event ue
                )
                SELECT
                    co.stay_id,
                    hl.charttime_floor,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.weight END) AS weight,
                    SUM(hl.uo) AS urine_output,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.urineoutput_6hr END) AS urineoutput_6hr,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.urineoutput_12hr END) AS urineoutput_12hr,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.urineoutput_24hr END) AS urine_output_24h,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.uo_mlkghr_6hr END) AS uo_mlkghr_6hr,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.uo_mlkghr_12hr END) AS uo_mlkghr_12hr,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.uo_mlkghr_24hr END) AS uo_mlkghr_24hr,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.uo_tm_6hr END) AS uo_tm_6hr,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.uo_tm_12hr END) AS uo_tm_12hr,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.uo_tm_24hr END) AS uo_tm_24hr,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.uo_24hr END) AS uo_24hr,
                    MAX(CASE WHEN hl.hour_seq = 1 THEN hl.uo_24hr END) AS urine_output_24h_est
                FROM hour_last hl
                INNER JOIN cohort co
                    ON hl.stay_id = co.stay_id
                   AND hl.charttime <= co.outtime
                INNER JOIN icustay_times_p it
                    ON hl.stay_id = it.stay_id
                   AND hl.charttime >= it.intime_hr - INTERVAL 24 HOUR
                GROUP BY co.stay_id, hl.charttime_floor
            )
            SELECT
                stay_id,
                charttime_floor,
                weight,
                urine_output,
                urineoutput_6hr,
                urineoutput_12hr,
                urine_output_24h,
                uo_mlkghr_6hr,
                uo_mlkghr_12hr,
                uo_mlkghr_24hr,
                uo_tm_6hr,
                uo_tm_12hr,
                uo_tm_24hr,
                uo_24hr,
                urine_output_24h_est
            FROM uo_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step12 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step13_vaso(con):
    name = "13_vaso"
    if exists(name):
        log.info("step13 cached")
        return
    t0 = time.time()
    # Reference: medication/norepinephrine.sql, epinephrine.sql, dopamine.sql,
    #            dobutamine.sql, phenylephrine.sql, vasopressin.sql
    # Port the official medication-specific normalization rules first, then project
    # onto ICU-hour windows for the wide table.
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
    con.execute(f"""
        COPY (
            WITH norepinephrine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    ie.amount AS norepi_amount,
                    CASE
                        WHEN ie.rateuom = 'mg/kg/min' AND ie.patientweight = 1 THEN ie.rate
                        WHEN ie.rateuom = 'mg/kg/min' THEN ie.rate * 1000.0
                        ELSE ie.rate
                    END AS norepi_rate
                FROM inputevents ie
                WHERE ie.itemid = 221906
            ),
            epinephrine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    ie.amount AS epi_amount,
                    ie.rate AS epi_rate
                FROM inputevents ie
                WHERE ie.itemid = 221289
            ),
            dopamine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    ie.amount AS dopa_amount,
                    ie.rate AS dopa_rate
                FROM inputevents ie
                WHERE ie.itemid = 221662
            ),
            dobutamine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    ie.amount AS dobu_amount,
                    ie.rate AS dobu_rate
                FROM inputevents ie
                WHERE ie.itemid = 221653
            ),
            phenylephrine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    ie.amount AS phenyl_amount,
                    CASE
                        WHEN ie.rateuom = 'mcg/min' AND ie.patientweight > 0
                            THEN ie.rate / ie.patientweight
                        ELSE ie.rate
                    END AS phenyl_rate
                FROM inputevents ie
                WHERE ie.itemid = 221749
            ),
            vasopressin AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    ie.amount AS vaso_amount,
                    CASE
                        WHEN ie.rateuom = 'units/min' THEN ie.rate * 60.0
                        ELSE ie.rate
                    END AS vaso_rate
                FROM inputevents ie
                WHERE ie.itemid = 222315
            ),
            vaso_raw AS (
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    norepi_amount,
                    norepi_rate,
                    NULL::DOUBLE AS epi_amount,
                    NULL::DOUBLE AS epi_rate,
                    NULL::DOUBLE AS dopa_amount,
                    NULL::DOUBLE AS dopa_rate,
                    NULL::DOUBLE AS dobu_amount,
                    NULL::DOUBLE AS dobu_rate,
                    NULL::DOUBLE AS phenyl_amount,
                    NULL::DOUBLE AS phenyl_rate,
                    NULL::DOUBLE AS vaso_amount,
                    NULL::DOUBLE AS vaso_rate
                FROM norepinephrine
                UNION ALL
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    epi_amount,
                    epi_rate,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE
                FROM epinephrine
                UNION ALL
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    dopa_amount,
                    dopa_rate,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE
                FROM dopamine
                UNION ALL
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    dobu_amount,
                    dobu_rate,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE
                FROM dobutamine
                UNION ALL
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    phenyl_amount,
                    phenyl_rate,
                    NULL::DOUBLE,
                    NULL::DOUBLE
                FROM phenylephrine
                UNION ALL
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    NULL::DOUBLE,
                    vaso_amount,
                    vaso_rate
                FROM vasopressin
            )
            SELECT
                ta.stay_id,
                ta.charttime_floor,
                MAX(vr.norepi_amount) AS norepi_amount,
                MAX(vr.norepi_rate) AS norepi_rate,
                MAX(vr.epi_amount)    AS epi_amount,
                MAX(vr.epi_rate)    AS epi_rate,
                MAX(vr.dopa_amount)   AS dopa_amount,
                MAX(vr.dopa_rate)   AS dopa_rate,
                MAX(vr.dobu_amount)   AS dobu_amount,
                MAX(vr.dobu_rate)   AS dobu_rate,
                MAX(vr.phenyl_amount) AS phenyl_amount,
                MAX(vr.phenyl_rate) AS phenyl_rate,
                MAX(vr.vaso_amount)   AS vaso_amount,
                MAX(vr.vaso_rate)   AS vaso_rate
            FROM time_axis ta
            LEFT JOIN vaso_raw vr
                ON ta.stay_id = vr.stay_id
               AND ta.endtime > vr.starttime
               AND ta.endtime <= vr.endtime
            GROUP BY ta.stay_id, ta.charttime_floor
            HAVING MAX(vr.norepi_rate) IS NOT NULL
                OR MAX(vr.epi_rate)    IS NOT NULL
                OR MAX(vr.dopa_rate)   IS NOT NULL
                OR MAX(vr.dobu_rate)   IS NOT NULL
                OR MAX(vr.phenyl_rate) IS NOT NULL
                OR MAX(vr.vaso_rate)   IS NOT NULL
                OR MAX(vr.norepi_amount) IS NOT NULL
                OR MAX(vr.epi_amount)    IS NOT NULL
                OR MAX(vr.dopa_amount)   IS NOT NULL
                OR MAX(vr.dobu_amount)   IS NOT NULL
                OR MAX(vr.phenyl_amount) IS NOT NULL
                OR MAX(vr.vaso_amount)   IS NOT NULL
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step13 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step14_vent(con):
    name = "14_vent"
    if exists(name):
        log.info("step14 cached")
        return
    t0 = time.time()
    # Reference: treatment/ventilation.sql
    # Emulate official ventilation stitching from oxygen-delivery and ventilator-mode
    # charting. Priority: Tracheostomy > InvasiveVent > NonInvasiveVent >
    # HFNC > SupplementalOxygen > None.
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
    con.execute(f"""
        COPY (
            WITH oxygen_delivery AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    ce.itemid,
                    ce.value AS o2_device,
                    ROW_NUMBER() OVER (
                        PARTITION BY ce.subject_id, ce.charttime, ce.itemid
                        ORDER BY ce.value
                    ) AS rn
                FROM chartevents ce
                WHERE ce.itemid = 226732
                  AND ce.stay_id IS NOT NULL
                  AND ce.value IS NOT NULL
            ),
            oxygen_delivery_p AS (
                SELECT
                    subject_id,
                    stay_id,
                    charttime,
                    MAX(CASE WHEN rn = 1 THEN o2_device END) AS o2_delivery_device_1
                FROM oxygen_delivery
                GROUP BY subject_id, stay_id, charttime
            ),
            ventilator_setting AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    ventilator_mode,
                    ventilator_mode_hamilton
                FROM (
                    SELECT
                        ce.subject_id,
                        ce.stay_id,
                        ce.charttime,
                        MAX(CASE WHEN ce.itemid = 223849 THEN ce.value END) AS ventilator_mode,
                        MAX(CASE WHEN ce.itemid = 229314 THEN ce.value END) AS ventilator_mode_hamilton
                    FROM chartevents ce
                    WHERE ce.itemid IN (223849, 229314)
                      AND ce.stay_id IS NOT NULL
                      AND ce.value IS NOT NULL
                    GROUP BY ce.subject_id, ce.stay_id, ce.charttime
                ) ce
            ),
            tm AS (
                SELECT stay_id, charttime FROM ventilator_setting
                UNION DISTINCT
                SELECT stay_id, charttime FROM oxygen_delivery_p
            ),
            vs AS (
                SELECT
                    tm.stay_id,
                    tm.charttime,
                    od.o2_delivery_device_1,
                    CASE
                        WHEN od.o2_delivery_device_1 IN ('Tracheostomy tube', 'Trach mask ') THEN 'Tracheostomy'
                        WHEN od.o2_delivery_device_1 IN ('Endotracheal tube')
                             OR vs2.ventilator_mode IN (
                                '(S) CMV','APRV','APRV/Biphasic+ApnPress',
                                'APRV/Biphasic+ApnVol','APV (cmv)','Ambient',
                                'Apnea Ventilation','CMV','CMV/ASSIST',
                                'CMV/ASSIST/AutoFlow','CMV/AutoFlow','CPAP/PPS',
                                'CPAP/PSV','CPAP/PSV+Apn TCPL','CPAP/PSV+ApnPres',
                                'CPAP/PSV+ApnVol','MMV','MMV/AutoFlow','MMV/PSV',
                                'MMV/PSV/AutoFlow','P-CMV','PCV+','PCV+/PSV',
                                'PCV+Assist','PRES/AC','PRVC/AC','PRVC/SIMV',
                                'PSV/SBT','SIMV','SIMV/AutoFlow','SIMV/PRES',
                                'SIMV/PSV','SIMV/PSV/AutoFlow','SIMV/VOL',
                                'SYNCHRON MASTER','SYNCHRON SLAVE','VOL/AC'
                             )
                             OR vs2.ventilator_mode_hamilton IN (
                                'APRV','APV (cmv)','Ambient','(S) CMV',
                                'P-CMV','SIMV','APV (simv)','P-SIMV','VS','ASV'
                             ) THEN 'InvasiveVent'
                        WHEN od.o2_delivery_device_1 IN ('Bipap mask ', 'CPAP mask ')
                             OR vs2.ventilator_mode_hamilton IN ('DuoPaP', 'NIV', 'NIV-ST')
                            THEN 'NonInvasiveVent'
                        WHEN od.o2_delivery_device_1 = 'High flow nasal cannula' THEN 'HFNC'
                        WHEN od.o2_delivery_device_1 IN (
                            'Non-rebreather','Face tent','Aerosol-cool',
                            'Venti mask ','Medium conc mask ','Ultrasonic neb',
                            'Vapomist','Oxymizer','High flow neb','Nasal cannula'
                        ) THEN 'SupplementalOxygen'
                        WHEN od.o2_delivery_device_1 = 'None' THEN 'None'
                        ELSE NULL
                    END AS ventilation_status
                FROM tm
                LEFT JOIN ventilator_setting vs2
                    ON tm.stay_id = vs2.stay_id
                   AND tm.charttime = vs2.charttime
                LEFT JOIN oxygen_delivery_p od
                    ON tm.stay_id = od.stay_id
                   AND tm.charttime = od.charttime
            ),
            vd0 AS (
                SELECT
                    stay_id,
                    ventilation_status,
                    charttime,
                    LAG(charttime, 1) OVER (
                        PARTITION BY stay_id, ventilation_status
                        ORDER BY charttime
                    ) AS charttime_lag,
                    LEAD(charttime, 1) OVER (
                        PARTITION BY stay_id
                        ORDER BY charttime
                    ) AS charttime_lead,
                    LAG(ventilation_status, 1) OVER (
                        PARTITION BY stay_id
                        ORDER BY charttime
                    ) AS ventilation_status_lag
                FROM vs
                WHERE ventilation_status IS NOT NULL
            ),
            vd1 AS (
                SELECT
                    stay_id,
                    charttime_lag,
                    ventilation_status,
                    charttime,
                    charttime_lead,
                    CASE
                        WHEN ventilation_status_lag IS NULL THEN 1
                        WHEN charttime_lag IS NULL THEN 1
                        WHEN date_diff('hour', charttime_lag, charttime) >= 14 THEN 1
                        WHEN ventilation_status_lag != ventilation_status THEN 1
                        ELSE 0
                    END AS new_ventilation_event
                FROM vd0
            ),
            vd2 AS (
                SELECT
                    stay_id,
                    ventilation_status,
                    charttime,
                    charttime_lead,
                    SUM(new_ventilation_event) OVER (
                        PARTITION BY stay_id
                        ORDER BY charttime
                    ) AS vent_seq
                FROM vd1
            ),
            vent_intervals AS (
                SELECT
                    stay_id,
                    MIN(charttime) AS starttime,
                    MAX(
                        CASE
                            WHEN charttime_lead IS NULL
                                 OR date_diff('hour', charttime, charttime_lead) >= 14
                                THEN charttime
                            ELSE charttime_lead
                        END
                    ) AS endtime,
                    MAX(ventilation_status) AS ventilation_status
                FROM vd2
                GROUP BY stay_id, vent_seq
                HAVING MIN(charttime) != MAX(charttime)
            ),
            active_vent AS (
                SELECT
                    ta.stay_id,
                    ta.charttime_floor,
                    vi.ventilation_status,
                    CASE vi.ventilation_status
                        WHEN 'Tracheostomy' THEN 6
                        WHEN 'InvasiveVent' THEN 5
                        WHEN 'NonInvasiveVent' THEN 4
                        WHEN 'HFNC' THEN 3
                        WHEN 'SupplementalOxygen' THEN 2
                        WHEN 'None' THEN 1
                        ELSE 0
                    END AS priority
                FROM time_axis ta
                INNER JOIN vent_intervals vi
                    ON ta.stay_id = vi.stay_id
                   AND ta.starttime < vi.endtime
                   AND ta.endtime > vi.starttime
            ),
            active_priority AS (
                SELECT
                    stay_id,
                    charttime_floor,
                    MAX(priority) AS priority
                FROM active_vent
                GROUP BY stay_id, charttime_floor
            )
            SELECT
                ap.stay_id,
                ap.charttime_floor,
                CASE ap.priority
                    WHEN 6 THEN 'Tracheostomy'
                    WHEN 5 THEN 'InvasiveVent'
                    WHEN 4 THEN 'NonInvasiveVent'
                    WHEN 3 THEN 'HFNC'
                    WHEN 2 THEN 'SupplementalOxygen'
                    WHEN 1 THEN 'None'
                END AS ventilation_status
            FROM active_priority ap
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step14 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step15_crrt(con):
    name = "15_crrt"
    if exists(name):
        log.info("step15 cached")
        return
    t0 = time.time()
    # Reference: treatment/crrt.sql
    # Port the official CRRT settings concept, then collapse to an hourly wide-table view.
    con.execute(f"""
        COPY (
            WITH crrt_settings AS (
                SELECT
                    ce.stay_id,
                    ce.charttime,
                    CASE WHEN ce.itemid = 227290 THEN ce.value END AS crrt_mode,
                    CASE WHEN ce.itemid = 224149 THEN ce.valuenum END AS access_pressure,
                    CASE WHEN ce.itemid = 224144 THEN ce.valuenum END AS blood_flow,
                    CASE WHEN ce.itemid = 228004 THEN ce.valuenum END AS citrate,
                    CASE WHEN ce.itemid = 225183 THEN ce.valuenum END AS current_goal,
                    CASE WHEN ce.itemid = 225977 THEN ce.value END AS dialysate_fluid,
                    CASE WHEN ce.itemid = 224154 THEN ce.valuenum END AS dialysate_rate,
                    CASE WHEN ce.itemid = 224151 THEN ce.valuenum END AS effluent_pressure,
                    CASE WHEN ce.itemid = 224150 THEN ce.valuenum END AS filter_pressure,
                    CASE WHEN ce.itemid = 225958 THEN ce.value END AS heparin_concentration,
                    CASE WHEN ce.itemid = 224145 THEN ce.valuenum END AS heparin_dose,
                    CASE WHEN ce.itemid = 224191 THEN ce.valuenum END AS hourly_patient_fluid_removal,
                    CASE WHEN ce.itemid = 228005 THEN ce.valuenum END AS prefilter_replacement_rate,
                    CASE WHEN ce.itemid = 228006 THEN ce.valuenum END AS postfilter_replacement_rate,
                    CASE WHEN ce.itemid = 225976 THEN ce.value END AS replacement_fluid,
                    CASE WHEN ce.itemid = 224153 THEN ce.valuenum END AS replacement_rate,
                    CASE WHEN ce.itemid = 224152 THEN ce.valuenum END AS return_pressure,
                    CASE WHEN ce.itemid = 226457 THEN ce.valuenum END AS ultrafiltrate_output,
                    CASE
                        WHEN ce.itemid = 224146 AND ce.value IN ('Active', 'Initiated', 'Reinitiated', 'New Filter') THEN 1
                        WHEN ce.itemid = 224146 AND ce.value IN ('Recirculating', 'Discontinued') THEN 0
                        ELSE NULL
                    END AS system_active,
                    CASE
                        WHEN ce.itemid = 224146 AND ce.value IN ('Clots Present', 'Clots Present') THEN 1
                        WHEN ce.itemid = 224146 AND ce.value IN ('No Clot Present', 'No Clot Present') THEN 0
                        ELSE NULL
                    END AS clots,
                    CASE WHEN ce.itemid = 224146 AND ce.value IN ('Clots Increasing', 'Clot Increasing') THEN 1 END AS clots_increasing,
                    CASE WHEN ce.itemid = 224146 AND ce.value = 'Clotted' THEN 1 END AS clotted
                FROM chartevents ce
                INNER JOIN cohort co ON ce.stay_id = co.stay_id
                WHERE ce.itemid IN (
                    227290, 224146, 224149, 224144, 228004, 225183, 225977,
                    224154, 224151, 224150, 225958, 224145, 224191, 228005,
                    228006, 225976, 224153, 224152, 226457
                )
                  AND ce.value IS NOT NULL
            )
            SELECT
                stay_id,
                date_trunc('hour', charttime) AS charttime_floor,
                MAX(crrt_mode) AS crrt_mode,
                MAX(access_pressure) AS access_pressure,
                MAX(citrate) AS citrate,
                MAX(current_goal) AS current_goal,
                MAX(dialysate_fluid) AS dialysate_fluid,
                MAX(blood_flow) AS blood_flow,
                MAX(dialysate_rate) AS dialysate_rate,
                MAX(effluent_pressure) AS effluent_pressure,
                MAX(filter_pressure) AS filter_pressure,
                MAX(heparin_concentration) AS heparin_concentration,
                MAX(heparin_dose) AS heparin_dose,
                MAX(hourly_patient_fluid_removal) AS hourly_patient_fluid_removal,
                MAX(prefilter_replacement_rate) AS prefilter_replacement_rate,
                MAX(postfilter_replacement_rate) AS postfilter_replacement_rate,
                MAX(replacement_fluid) AS replacement_fluid,
                MAX(replacement_rate) AS replacement_rate,
                MAX(return_pressure) AS return_pressure,
                MAX(ultrafiltrate_output) AS ultrafiltrate_output,
                MAX(system_active) AS system_active,
                MAX(clots) AS clots,
                MAX(clots_increasing) AS clots_increasing,
                MAX(clotted) AS clotted,
                MAX(
                    CASE
                        WHEN COALESCE(system_active, 0) = 1
                             OR crrt_mode IS NOT NULL
                             OR current_goal IS NOT NULL
                             OR blood_flow IS NOT NULL
                             OR dialysate_rate IS NOT NULL
                             OR replacement_rate IS NOT NULL
                             OR ultrafiltrate_output IS NOT NULL
                            THEN 1
                        ELSE 0
                    END
                ) AS crrt_flag
            FROM crrt_settings
            GROUP BY stay_id, date_trunc('hour', charttime)
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step15 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step16_antibiotic(con):
    name = "16_antibiotic"
    if exists(name):
        log.info("step16 cached")
        return
    t0 = time.time()
    # Reference: medication/antibiotic.sql
    # Build an official-style antibiotic concept first (drug+route lookup, then
    # exact join back to prescriptions and stay_id mapping by starttime), then
    # project it onto ICU-hour windows for the wide table.
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
    con.execute(f"""
        COPY (
            WITH abx AS (
                SELECT DISTINCT
                    pr.drug,
                    pr.route,
                    CASE
                        WHEN {ANTIBIOTIC_DRUG_FILTER} THEN 1
                        ELSE 0
                    END AS antibiotic
                FROM prescriptions pr
                WHERE pr.drug_type NOT IN ('BASE')
                  AND NOT ({ANTIBIOTIC_ROUTE_EXCLUSION})
            ),
            antibiotic AS (
                SELECT
                    pr.subject_id,
                    pr.hadm_id,
                    ie.stay_id,
                    pr.drug AS antibiotic,
                    pr.route,
                    pr.starttime,
                    pr.stoptime AS stoptime
                FROM prescriptions pr
                INNER JOIN abx
                    ON pr.drug = abx.drug
                   AND pr.route = abx.route
                LEFT JOIN icustays ie
                    ON pr.hadm_id = ie.hadm_id
                   AND pr.starttime >= ie.intime
                   AND pr.starttime < ie.outtime
                WHERE abx.antibiotic = 1
                  AND pr.starttime IS NOT NULL
            )
            SELECT DISTINCT
                ta.stay_id,
                ta.charttime_floor,
                1 AS antibiotic_flag
            FROM time_axis ta
            INNER JOIN antibiotic abx
                ON ta.stay_id = abx.stay_id
               AND ta.starttime < abx.stoptime
               AND ta.endtime > abx.starttime
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step16 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step17_height_weight(con):
    name = "17_height_weight"
    if exists(name):
        log.info("step17 cached")
        return
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
    # Reference: measurement/height.sql + demographics/weight_durations.sql
    # Height follows the official chartevents concept, then is projected to ICU hours.
    # Weight follows the official admit/daily split and duration construction,
    # then is forward-filled onto the hourly stay grid.
    con.execute(f"""
        COPY (
            WITH ht_in AS (
                SELECT
                    c.subject_id,
                    c.stay_id,
                    c.charttime,
                    ROUND(c.valuenum * 2.54, 2) AS height
                FROM chartevents c
                WHERE c.valuenum IS NOT NULL
                  AND c.itemid = 226707
            ),
            ht_cm AS (
                SELECT
                    c.subject_id,
                    c.stay_id,
                    c.charttime,
                    ROUND(c.valuenum, 2) AS height
                FROM chartevents c
                WHERE c.valuenum IS NOT NULL
                  AND c.itemid = 226730
            ),
            ht_stg0 AS (
                SELECT
                    COALESCE(h1.subject_id, h2.subject_id) AS subject_id,
                    COALESCE(h1.stay_id, h2.stay_id) AS stay_id,
                    COALESCE(h1.charttime, h2.charttime) AS charttime,
                    COALESCE(h1.height, h2.height) AS height
                FROM ht_cm h1
                FULL OUTER JOIN ht_in h2
                    ON h1.subject_id = h2.subject_id
                   AND h1.charttime = h2.charttime
            ),
            height_rows AS (
                SELECT
                    subject_id,
                    stay_id,
                    charttime,
                    height
                FROM ht_stg0
                WHERE height IS NOT NULL
                  AND height > 120
                  AND height < 230
            ),
            height AS (
                SELECT
                    stay_id,
                    date_trunc('hour', charttime) AS charttime_floor,
                    AVG(height) AS height
                FROM height_rows
                GROUP BY stay_id, charttime_floor
            ),
            wt_raw AS (
                SELECT
                    ce.stay_id,
                    ce.charttime,
                    CASE WHEN ce.itemid = 226512 THEN 'admit' ELSE 'daily' END AS wt_type,
                    ce.valuenum AS weight
                FROM chartevents ce
                WHERE ce.itemid IN (224639, 226512)
                  AND ce.valuenum IS NOT NULL
                  AND ce.valuenum > 0
            ),
            wt_stg1 AS (
                SELECT
                    stay_id,
                    wt_type,
                    charttime,
                    weight,
                    ROW_NUMBER() OVER (
                        PARTITION BY stay_id, wt_type
                        ORDER BY charttime
                    ) AS rn
                FROM wt_raw
            ),
            wt_stg2 AS (
                SELECT
                    w1.stay_id,
                    co.intime,
                    co.outtime,
                    w1.wt_type,
                    CASE
                        WHEN w1.wt_type = 'admit' AND w1.rn = 1
                            THEN co.intime - INTERVAL 2 HOUR
                        ELSE w1.charttime
                    END AS starttime,
                    w1.weight
                FROM wt_stg1 w1
                INNER JOIN cohort co
                    ON co.stay_id = w1.stay_id
            ),
            wt_stg3 AS (
                SELECT
                    stay_id,
                    intime,
                    outtime,
                    starttime,
                    COALESCE(
                        LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime),
                        outtime + INTERVAL 2 HOUR
                    ) AS endtime,
                    weight,
                    wt_type
                FROM wt_stg2
            ),
            wt1 AS (
                SELECT
                    stay_id,
                    starttime,
                    COALESCE(
                        endtime,
                        LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime),
                        outtime + INTERVAL 2 HOUR
                    ) AS endtime,
                    weight,
                    wt_type
                FROM wt_stg3
            ),
            wt_fix AS (
                SELECT
                    co.stay_id,
                    co.intime - INTERVAL 2 HOUR AS starttime,
                    wt.starttime AS endtime,
                    wt.weight,
                    wt.wt_type
                FROM cohort co
                INNER JOIN (
                    SELECT
                        wt1.stay_id,
                        wt1.starttime,
                        wt1.weight,
                        wt1.wt_type,
                        ROW_NUMBER() OVER (
                            PARTITION BY wt1.stay_id
                            ORDER BY wt1.starttime
                        ) AS rn
                    FROM wt1
                ) wt
                    ON co.stay_id = wt.stay_id
                   AND wt.rn = 1
                   AND co.intime < wt.starttime
            ),
            wt_intervals AS (
                SELECT stay_id, starttime, endtime, weight, wt_type
                FROM wt1
                UNION ALL
                SELECT stay_id, starttime, endtime, weight, wt_type
                FROM wt_fix
            )
            SELECT
                ta.stay_id,
                ta.charttime_floor,
                ht.height,
                wi.wt_type AS weight_type,
                CASE WHEN wi.wt_type = 'admit' THEN wi.weight END AS admit_weight,
                CASE WHEN wi.wt_type = 'daily' THEN wi.weight END AS daily_weight,
                wi.weight
            FROM time_axis ta
            LEFT JOIN height ht
                ON ta.stay_id = ht.stay_id
               AND ta.charttime_floor = ht.charttime_floor
            LEFT JOIN wt_intervals wi
                ON ta.stay_id = wi.stay_id
               AND ta.endtime > wi.starttime
               AND ta.endtime <= wi.endtime
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step17 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step18_charlson(con):
    name = "18_charlson"
    if exists(name):
        log.info("step18 cached")
        return
    t0 = time.time()
    # Reference: comorbidity/charlson.sql (official mimic-code)
    # Follow the official structure more closely:
    # admission universe -> LEFT JOIN diagnoses -> admission-level 0/1 flags
    # -> age score -> final admission score -> broadcast to ICU stays
    con.execute(f"""
        COPY (
            WITH cohort_adm AS (
                SELECT DISTINCT hadm_id
                FROM cohort
                WHERE hadm_id IS NOT NULL
            ),
            diag AS (
                SELECT
                    hadm_id,
                    CASE WHEN icd_version = 9 THEN icd_code ELSE NULL END AS icd9,
                    CASE WHEN icd_version = 10 THEN icd_code ELSE NULL END AS icd10
                FROM diagnoses_icd
            ),
            ag AS (
                SELECT
                    hadm_id,
                    MAX(age) AS age,
                    CASE
                        WHEN MAX(age) <= 50 THEN 0
                        WHEN MAX(age) <= 60 THEN 1
                        WHEN MAX(age) <= 70 THEN 2
                        WHEN MAX(age) <= 80 THEN 3
                        ELSE 4
                    END AS age_score
                FROM cohort
                GROUP BY hadm_id
            ),
            com AS (
                SELECT
                    ad.hadm_id,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) IN ('410', '412')
                        OR SUBSTR(icd10, 1, 3) IN ('I21', 'I22')
                        OR SUBSTR(icd10, 1, 4) = 'I252'
                        THEN 1 ELSE 0 END) AS myocardial_infarct,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) = '428'
                        OR SUBSTR(icd9, 1, 5) IN ('39891', '40201', '40211', '40291', '40401', '40403',
                                                   '40411', '40413', '40491', '40493')
                        OR SUBSTR(icd9, 1, 4) BETWEEN '4254' AND '4259'
                        OR SUBSTR(icd10, 1, 3) IN ('I43', 'I50')
                        OR SUBSTR(icd10, 1, 4) IN ('I099', 'I110', 'I130', 'I132', 'I255', 'I420',
                                                    'I425', 'I426', 'I427', 'I428', 'I429', 'P290')
                        THEN 1 ELSE 0 END) AS congestive_heart_failure,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) IN ('440', '441')
                        OR SUBSTR(icd9, 1, 4) IN ('0930', '4373', '4471', '5571', '5579', 'V434')
                        OR SUBSTR(icd9, 1, 4) BETWEEN '4431' AND '4439'
                        OR SUBSTR(icd10, 1, 3) IN ('I70', 'I71')
                        OR SUBSTR(icd10, 1, 4) IN ('I731', 'I738', 'I739', 'I771', 'I790', 'I792',
                                                    'K551', 'K558', 'K559', 'Z958', 'Z959')
                        THEN 1 ELSE 0 END) AS peripheral_vascular_disease,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) BETWEEN '430' AND '438'
                        OR SUBSTR(icd9, 1, 5) = '36234'
                        OR SUBSTR(icd10, 1, 3) IN ('G45', 'G46')
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'I60' AND 'I69'
                        OR SUBSTR(icd10, 1, 4) = 'H340'
                        THEN 1 ELSE 0 END) AS cerebrovascular_disease,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) = '290'
                        OR SUBSTR(icd9, 1, 4) IN ('2941', '3312')
                        OR SUBSTR(icd10, 1, 3) IN ('F00', 'F01', 'F02', 'F03', 'G30')
                        OR SUBSTR(icd10, 1, 4) IN ('F051', 'G311')
                        THEN 1 ELSE 0 END) AS dementia,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) BETWEEN '490' AND '505'
                        OR SUBSTR(icd9, 1, 4) IN ('4168', '4169', '5064', '5081', '5088')
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'J40' AND 'J47'
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'J60' AND 'J67'
                        OR SUBSTR(icd10, 1, 4) IN ('I278', 'I279', 'J684', 'J701', 'J703')
                        THEN 1 ELSE 0 END) AS chronic_pulmonary_disease,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) = '725'
                        OR SUBSTR(icd9, 1, 4) IN ('4465', '7100', '7101', '7102', '7103',
                                                   '7104', '7140', '7141', '7142', '7148')
                        OR SUBSTR(icd10, 1, 3) IN ('M05', 'M06', 'M32', 'M33', 'M34')
                        OR SUBSTR(icd10, 1, 4) IN ('M315', 'M351', 'M353', 'M360')
                        THEN 1 ELSE 0 END) AS rheumatic_disease,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) IN ('531', '532', '533', '534')
                        OR SUBSTR(icd10, 1, 3) IN ('K25', 'K26', 'K27', 'K28')
                        THEN 1 ELSE 0 END) AS peptic_ulcer_disease,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) IN ('570', '571')
                        OR SUBSTR(icd9, 1, 4) IN ('0706', '0709', '5733', '5734', '5738', '5739', 'V427')
                        OR SUBSTR(icd9, 1, 5) IN ('07022', '07023', '07032', '07033', '07044', '07054')
                        OR SUBSTR(icd10, 1, 3) IN ('B18', 'K73', 'K74')
                        OR SUBSTR(icd10, 1, 4) IN ('K700', 'K701', 'K702', 'K703', 'K709', 'K713',
                                                    'K714', 'K715', 'K717', 'K760', 'K762', 'K763',
                                                    'K764', 'K768', 'K769', 'Z944')
                        THEN 1 ELSE 0 END) AS mild_liver_disease,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 4) IN ('2500', '2501', '2502', '2503', '2508', '2509')
                        OR SUBSTR(icd10, 1, 4) IN ('E100', 'E101', 'E106', 'E108', 'E109', 'E110', 'E111',
                                                    'E116', 'E118', 'E119', 'E120', 'E121', 'E126',
                                                    'E128', 'E129', 'E130', 'E131', 'E136', 'E138',
                                                    'E139', 'E140', 'E141', 'E146', 'E148', 'E149')
                        THEN 1 ELSE 0 END) AS diabetes_without_cc,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 4) IN ('2504', '2505', '2506', '2507')
                        OR SUBSTR(icd10, 1, 4) IN ('E102', 'E103', 'E104', 'E105', 'E107', 'E112', 'E113',
                                                    'E114', 'E115', 'E117', 'E122', 'E123', 'E124',
                                                    'E125', 'E127', 'E132', 'E133', 'E134', 'E135',
                                                    'E137', 'E142', 'E143', 'E144', 'E145', 'E147')
                        THEN 1 ELSE 0 END) AS diabetes_with_cc,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) IN ('342', '343')
                        OR SUBSTR(icd9, 1, 4) IN ('3341', '3440', '3441', '3442', '3443',
                                                   '3444', '3445', '3446', '3449')
                        OR SUBSTR(icd10, 1, 3) IN ('G81', 'G82')
                        OR SUBSTR(icd10, 1, 4) IN ('G041', 'G114', 'G801', 'G802', 'G830',
                                                    'G831', 'G832', 'G833', 'G834', 'G839')
                        THEN 1 ELSE 0 END) AS paraplegia,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) IN ('582', '585', '586', 'V56')
                        OR SUBSTR(icd9, 1, 4) IN ('5880', 'V420', 'V451')
                        OR SUBSTR(icd9, 1, 4) BETWEEN '5830' AND '5837'
                        OR SUBSTR(icd9, 1, 5) IN ('40301', '40311', '40391', '40402', '40403',
                                                   '40412', '40413', '40492', '40493')
                        OR SUBSTR(icd10, 1, 3) IN ('N18', 'N19')
                        OR SUBSTR(icd10, 1, 4) IN ('I120', 'I131', 'N032', 'N033', 'N034', 'N035',
                                                    'N036', 'N037', 'N052', 'N053', 'N054', 'N055',
                                                    'N056', 'N057', 'N250', 'Z490', 'Z491', 'Z492',
                                                    'Z940', 'Z992')
                        THEN 1 ELSE 0 END) AS renal_disease,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) BETWEEN '140' AND '172'
                        OR SUBSTR(icd9, 1, 4) BETWEEN '1740' AND '1958'
                        OR SUBSTR(icd9, 1, 3) BETWEEN '200' AND '208'
                        OR SUBSTR(icd9, 1, 4) = '2386'
                        OR SUBSTR(icd10, 1, 3) IN ('C43', 'C88')
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'C00' AND 'C26'
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'C30' AND 'C34'
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'C37' AND 'C41'
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'C45' AND 'C58'
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'C60' AND 'C76'
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'C81' AND 'C85'
                        OR SUBSTR(icd10, 1, 3) BETWEEN 'C90' AND 'C97'
                        THEN 1 ELSE 0 END) AS malignant_cancer,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 4) IN ('4560', '4561', '4562')
                        OR SUBSTR(icd9, 1, 4) BETWEEN '5722' AND '5728'
                        OR SUBSTR(icd10, 1, 4) IN ('I850', 'I859', 'I864', 'I982', 'K704',
                                                    'K711', 'K721', 'K729', 'K765', 'K766', 'K767')
                        THEN 1 ELSE 0 END) AS severe_liver_disease,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) IN ('196', '197', '198', '199')
                        OR SUBSTR(icd10, 1, 3) IN ('C77', 'C78', 'C79', 'C80')
                        THEN 1 ELSE 0 END) AS metastatic_solid_tumor,
                    MAX(CASE WHEN
                        SUBSTR(icd9, 1, 3) IN ('042', '043', '044')
                        OR SUBSTR(icd10, 1, 3) IN ('B20', 'B21', 'B22', 'B24')
                        THEN 1 ELSE 0 END) AS aids
                FROM cohort_adm ad
                LEFT JOIN diag
                    ON ad.hadm_id = diag.hadm_id
                GROUP BY ad.hadm_id
            )
            SELECT
                co.stay_id,
                (
                 ag.age_score
                 + com.myocardial_infarct
                 + com.congestive_heart_failure
                 + com.peripheral_vascular_disease
                 + com.cerebrovascular_disease
                 + com.dementia
                 + com.chronic_pulmonary_disease
                 + com.rheumatic_disease
                 + com.peptic_ulcer_disease
                 + GREATEST(com.mild_liver_disease, 3 * com.severe_liver_disease)
                 + GREATEST(2 * com.diabetes_with_cc, com.diabetes_without_cc)
                 + 2 * com.paraplegia
                 + 2 * com.renal_disease
                 + GREATEST(2 * com.malignant_cancer, 6 * com.metastatic_solid_tumor)
                 + com.aids * 6
                ) AS charlson_score
            FROM cohort co
            LEFT JOIN com ON co.hadm_id = com.hadm_id
            LEFT JOIN ag  ON co.hadm_id = ag.hadm_id
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step18 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step19_service(con):
    name = "19_service"
    if exists(name):
        log.info("step19 cached")
        return
    t0 = time.time()
    # Hospital service per ICU hour, forward-filled from services.transfertime
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
    con.execute(f"""
        COPY (
            WITH svc AS (
                SELECT
                    s.hadm_id,
                    s.transfertime,
                    s.curr_service
                FROM services s
                WHERE s.transfertime IS NOT NULL
            )
            SELECT
                ta.stay_id,
                ta.charttime_floor,
                svc.curr_service
            FROM time_axis ta
            INNER JOIN cohort co ON ta.stay_id = co.stay_id
            ASOF LEFT JOIN svc
                ON co.hadm_id = svc.hadm_id
               AND ta.starttime >= svc.transfertime
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step19 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step20_sofa(con):
    name = "20_sofa"
    if exists(name):
        log.info("step20 cached")
        return
    t0 = time.time()
    # Reference: score/sofa.sql
    # Rebuild the official SOFA structure inside this step so each ICU hour first
    # looks back at event-level concepts to extract the worst value in that hour,
    # and only then applies the rolling 24-hour SOFA window.
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"CREATE OR REPLACE VIEW uo_p   AS SELECT * FROM read_parquet('{inter('12_uo')}')")
    con.execute(f"CREATE OR REPLACE VIEW vaso_p AS SELECT * FROM read_parquet('{inter('13_vaso')}')")
    con.execute(f"""
        COPY (
            WITH co AS (
                SELECT
                    ta.stay_id,
                    c.subject_id,
                    c.hadm_id,
                    ta.hr,
                    ta.charttime_floor,
                    ta.starttime,
                    ta.endtime
                FROM time_axis ta
                INNER JOIN cohort c
                    ON ta.stay_id = c.stay_id
            ),
            oxygen_delivery AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    ce.itemid,
                    ce.value AS o2_device,
                    ROW_NUMBER() OVER (
                        PARTITION BY ce.subject_id, ce.charttime, ce.itemid
                        ORDER BY ce.value
                    ) AS rn
                FROM chartevents ce
                WHERE ce.itemid = 226732
                  AND ce.stay_id IS NOT NULL
                  AND ce.value IS NOT NULL
            ),
            oxygen_delivery_p AS (
                SELECT
                    subject_id,
                    stay_id,
                    charttime,
                    MAX(CASE WHEN rn = 1 THEN o2_device END) AS o2_delivery_device_1
                FROM oxygen_delivery
                GROUP BY subject_id, stay_id, charttime
            ),
            ventilator_setting AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    MAX(CASE WHEN ce.itemid = 223849 THEN ce.value END) AS ventilator_mode,
                    MAX(CASE WHEN ce.itemid = 229314 THEN ce.value END) AS ventilator_mode_hamilton
                FROM chartevents ce
                WHERE ce.itemid IN (223849, 229314)
                  AND ce.stay_id IS NOT NULL
                  AND ce.value IS NOT NULL
                GROUP BY ce.subject_id, ce.stay_id, ce.charttime
            ),
            tm AS (
                SELECT stay_id, charttime FROM ventilator_setting
                UNION DISTINCT
                SELECT stay_id, charttime FROM oxygen_delivery_p
            ),
            vent_vs AS (
                SELECT
                    tm.stay_id,
                    tm.charttime,
                    od.o2_delivery_device_1,
                    CASE
                        WHEN od.o2_delivery_device_1 IN ('Tracheostomy tube', 'Trach mask ') THEN 'Tracheostomy'
                        WHEN od.o2_delivery_device_1 IN ('Endotracheal tube')
                             OR vs2.ventilator_mode IN (
                                '(S) CMV','APRV','APRV/Biphasic+ApnPress',
                                'APRV/Biphasic+ApnVol','APV (cmv)','Ambient',
                                'Apnea Ventilation','CMV','CMV/ASSIST',
                                'CMV/ASSIST/AutoFlow','CMV/AutoFlow','CPAP/PPS',
                                'CPAP/PSV','CPAP/PSV+Apn TCPL','CPAP/PSV+ApnPres',
                                'CPAP/PSV+ApnVol','MMV','MMV/AutoFlow','MMV/PSV',
                                'MMV/PSV/AutoFlow','P-CMV','PCV+','PCV+/PSV',
                                'PCV+Assist','PRES/AC','PRVC/AC','PRVC/SIMV',
                                'PSV/SBT','SIMV','SIMV/AutoFlow','SIMV/PRES',
                                'SIMV/PSV','SIMV/PSV/AutoFlow','SIMV/VOL',
                                'SYNCHRON MASTER','SYNCHRON SLAVE','VOL/AC'
                             )
                             OR vs2.ventilator_mode_hamilton IN (
                                'APRV','APV (cmv)','Ambient','(S) CMV',
                                'P-CMV','SIMV','APV (simv)','P-SIMV','VS','ASV'
                             ) THEN 'InvasiveVent'
                        WHEN od.o2_delivery_device_1 IN ('Bipap mask ', 'CPAP mask ')
                             OR vs2.ventilator_mode_hamilton IN ('DuoPaP', 'NIV', 'NIV-ST')
                            THEN 'NonInvasiveVent'
                        WHEN od.o2_delivery_device_1 = 'High flow nasal cannula' THEN 'HFNC'
                        WHEN od.o2_delivery_device_1 IN (
                            'Non-rebreather','Face tent','Aerosol-cool',
                            'Venti mask ','Medium conc mask ','Ultrasonic neb',
                            'Vapomist','Oxymizer','High flow neb','Nasal cannula'
                        ) THEN 'SupplementalOxygen'
                        WHEN od.o2_delivery_device_1 = 'None' THEN 'None'
                        ELSE NULL
                    END AS ventilation_status
                FROM tm
                LEFT JOIN ventilator_setting vs2
                    ON tm.stay_id = vs2.stay_id
                   AND tm.charttime = vs2.charttime
                LEFT JOIN oxygen_delivery_p od
                    ON tm.stay_id = od.stay_id
                   AND tm.charttime = od.charttime
            ),
            vd0 AS (
                SELECT
                    stay_id,
                    charttime,
                    LAG(charttime, 1) OVER (
                        PARTITION BY stay_id, ventilation_status
                        ORDER BY charttime
                    ) AS charttime_lag,
                    LEAD(charttime, 1) OVER (
                        PARTITION BY stay_id
                        ORDER BY charttime
                    ) AS charttime_lead,
                    ventilation_status,
                    LAG(ventilation_status, 1) OVER (
                        PARTITION BY stay_id
                        ORDER BY charttime
                    ) AS ventilation_status_lag
                FROM vent_vs
                WHERE ventilation_status IS NOT NULL
            ),
            vd1 AS (
                SELECT
                    stay_id,
                    charttime,
                    charttime_lag,
                    charttime_lead,
                    ventilation_status,
                    CASE
                        WHEN ventilation_status_lag IS NULL THEN 1
                        WHEN date_diff('hour', charttime_lag, charttime) >= 14 THEN 1
                        WHEN ventilation_status_lag != ventilation_status THEN 1
                        ELSE 0
                    END AS new_ventilation_event
                FROM vd0
            ),
            vd2 AS (
                SELECT
                    stay_id,
                    charttime,
                    charttime_lead,
                    ventilation_status,
                    SUM(new_ventilation_event) OVER (
                        PARTITION BY stay_id
                        ORDER BY charttime
                    ) AS vent_seq
                FROM vd1
            ),
            vent_intervals AS (
                SELECT
                    stay_id,
                    MIN(charttime) AS starttime,
                    MAX(
                        CASE
                            WHEN charttime_lead IS NULL
                              OR date_diff('hour', charttime, charttime_lead) >= 14
                                THEN charttime
                            ELSE charttime_lead
                        END
                    ) AS endtime,
                    MAX(ventilation_status) AS ventilation_status
                FROM vd2
                GROUP BY stay_id, vent_seq
                HAVING MIN(charttime) != MAX(charttime)
            ),
            pafi_bg AS (
                SELECT
                    MAX(le.subject_id) AS subject_id,
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 52033 THEN le.value END) AS specimen,
                    MAX(CASE WHEN le.itemid = 50818 THEN le.valuenum END) AS pco2,
                    MAX(CASE WHEN le.itemid = 50821 THEN le.valuenum END) AS po2,
                    MAX(
                        CASE
                            WHEN le.itemid = 50816 AND le.valuenum > 20 AND le.valuenum <= 100 THEN le.valuenum
                            WHEN le.itemid = 50816 AND le.valuenum > 0.2 AND le.valuenum <= 1.0 THEN le.valuenum * 100.0
                            ELSE NULL
                        END
                    ) AS fio2_bg
                FROM labevents le
                WHERE le.itemid IN (52033, 50816, 50818, 50821)
                GROUP BY le.specimen_id
            ),
            stg_spo2 AS (
                SELECT
                    ce.subject_id,
                    ce.charttime,
                    AVG(ce.valuenum) AS spo2_bg
                FROM chartevents ce
                WHERE ce.itemid = 220277
                  AND ce.valuenum > 0
                  AND ce.valuenum <= 100
                GROUP BY ce.subject_id, ce.charttime
            ),
            fio2_ce AS (
                SELECT
                    ce.subject_id,
                    ce.charttime,
                    MAX(
                        CASE
                            WHEN ce.valuenum > 0.2 AND ce.valuenum <= 1 THEN ce.valuenum * 100
                            WHEN ce.valuenum > 1 AND ce.valuenum < 20 THEN NULL
                            WHEN ce.valuenum >= 20 AND ce.valuenum <= 100 THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS fio2_chartevents
                FROM chartevents ce
                WHERE ce.itemid = 223835
                  AND ce.valuenum > 0
                  AND ce.valuenum <= 100
                GROUP BY ce.subject_id, ce.charttime
            ),
            pafi_stg2 AS (
                SELECT
                    bg.*,
                    spo2.spo2_bg,
                    ROW_NUMBER() OVER (
                        PARTITION BY bg.subject_id, bg.charttime
                        ORDER BY spo2.charttime DESC NULLS LAST
                    ) AS lastrowspo2
                FROM pafi_bg bg
                LEFT JOIN stg_spo2 spo2
                    ON bg.subject_id = spo2.subject_id
                   AND spo2.charttime BETWEEN bg.charttime - INTERVAL 2 HOUR AND bg.charttime
                WHERE bg.po2 IS NOT NULL
            ),
            pafi_stg3 AS (
                SELECT
                    bg.*,
                    fio2.fio2_chartevents,
                    ROW_NUMBER() OVER (
                        PARTITION BY bg.subject_id, bg.charttime
                        ORDER BY fio2.charttime DESC NULLS LAST
                    ) AS lastrowfio2
                FROM pafi_stg2 bg
                LEFT JOIN fio2_ce fio2
                    ON bg.subject_id = fio2.subject_id
                   AND fio2.charttime >= bg.charttime - INTERVAL 4 HOUR
                   AND fio2.charttime <= bg.charttime
                   AND fio2.fio2_chartevents > 0
                WHERE bg.lastrowspo2 = 1
            ),
            bg_event AS (
                SELECT
                    subject_id,
                    hadm_id,
                    charttime,
                    specimen,
                    CASE
                        WHEN po2 IS NULL THEN NULL
                        WHEN fio2_bg IS NOT NULL THEN 100.0 * po2 / fio2_bg
                        WHEN fio2_chartevents IS NOT NULL THEN 100.0 * po2 / fio2_chartevents
                        ELSE NULL
                    END AS pao2fio2ratio
                FROM pafi_stg3
                WHERE lastrowfio2 = 1
            ),
            pafi AS (
                SELECT
                    co.stay_id,
                    bg.charttime,
                    CASE
                        WHEN vd.stay_id IS NULL THEN bg.pao2fio2ratio
                        ELSE NULL
                    END AS pao2fio2ratio_novent,
                    CASE
                        WHEN vd.stay_id IS NOT NULL THEN bg.pao2fio2ratio
                        ELSE NULL
                    END AS pao2fio2ratio_vent
                FROM cohort co
                INNER JOIN bg_event bg
                    ON co.subject_id = bg.subject_id
                   AND bg.charttime >= co.intime - INTERVAL 24 HOUR
                   AND bg.charttime <= co.outtime
                LEFT JOIN vent_intervals vd
                    ON co.stay_id = vd.stay_id
                   AND bg.charttime >= vd.starttime
                   AND bg.charttime <= vd.endtime
                   AND vd.ventilation_status = 'InvasiveVent'
                WHERE bg.specimen = 'ART.'
            ),
            vitalsign_event AS (
                SELECT
                    ce.stay_id,
                    ce.charttime,
                    AVG(
                        CASE
                            WHEN ce.itemid IN (220052, 220181, 225312)
                             AND ce.valuenum > 0
                             AND ce.valuenum < 300
                                THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS mbp
                FROM chartevents ce
                INNER JOIN cohort c
                    ON ce.stay_id = c.stay_id
                WHERE ce.itemid IN (220052, 220181, 225312)
                GROUP BY ce.stay_id, ce.charttime
            ),
            vs AS (
                SELECT
                    co.stay_id,
                    co.hr,
                    MIN(vs.mbp) AS meanbp_min
                FROM co
                LEFT JOIN vitalsign_event vs
                    ON co.stay_id = vs.stay_id
                   AND co.starttime < vs.charttime
                   AND co.endtime >= vs.charttime
                GROUP BY co.stay_id, co.hr
            ),
            gcs_base AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    MAX(CASE WHEN ce.itemid = 223901 THEN ce.valuenum END) AS gcs_motor,
                    MAX(
                        CASE
                            WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 0
                            WHEN ce.itemid = 223900 THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS gcs_verbal,
                    MAX(CASE WHEN ce.itemid = 220739 THEN ce.valuenum END) AS gcs_eyes,
                    ROW_NUMBER() OVER (
                        PARTITION BY ce.stay_id
                        ORDER BY ce.charttime ASC
                    ) AS rn
                FROM chartevents ce
                INNER JOIN cohort c
                    ON ce.stay_id = c.stay_id
                WHERE ce.itemid IN (223901, 223900, 220739)
                GROUP BY ce.subject_id, ce.stay_id, ce.charttime
            ),
            gcs_prev AS (
                SELECT
                    b.stay_id,
                    b.charttime,
                    CASE
                        WHEN b.gcs_verbal = 0 THEN 15
                        WHEN b.gcs_verbal IS NULL AND b2.gcs_verbal = 0 THEN 15
                        WHEN b2.gcs_verbal = 0 THEN
                            COALESCE(b.gcs_motor, 6) + COALESCE(b.gcs_verbal, 5) + COALESCE(b.gcs_eyes, 4)
                        ELSE
                            COALESCE(b.gcs_motor, COALESCE(b2.gcs_motor, 6))
                            + COALESCE(b.gcs_verbal, COALESCE(b2.gcs_verbal, 5))
                            + COALESCE(b.gcs_eyes, COALESCE(b2.gcs_eyes, 4))
                    END AS gcs_total
                FROM gcs_base b
                LEFT JOIN gcs_base b2
                    ON b.stay_id = b2.stay_id
                   AND b.rn = b2.rn + 1
                   AND b2.charttime > b.charttime - INTERVAL 6 HOUR
            ),
            gcs AS (
                SELECT
                    co.stay_id,
                    co.hr,
                    MIN(g.gcs_total) AS gcs_min
                FROM co
                LEFT JOIN gcs_prev g
                    ON co.stay_id = g.stay_id
                   AND co.starttime < g.charttime
                   AND co.endtime >= g.charttime
                GROUP BY co.stay_id, co.hr
            ),
            enzyme_event AS (
                SELECT
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 50885 THEN le.valuenum END) AS bilirubin_total
                FROM labevents le
                WHERE le.itemid = 50885
                  AND le.valuenum IS NOT NULL
                  AND le.valuenum > 0
                GROUP BY le.specimen_id
            ),
            bili AS (
                SELECT
                    co.stay_id,
                    co.hr,
                    MAX(enz.bilirubin_total) AS bilirubin_max
                FROM co
                LEFT JOIN enzyme_event enz
                    ON co.hadm_id = enz.hadm_id
                   AND co.starttime < enz.charttime
                   AND co.endtime >= enz.charttime
                GROUP BY co.stay_id, co.hr
            ),
            chemistry_event AS (
                SELECT
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 50912 AND le.valuenum <= 150 THEN le.valuenum END) AS creatinine
                FROM labevents le
                WHERE le.itemid = 50912
                  AND le.valuenum IS NOT NULL
                  AND le.valuenum > 0
                GROUP BY le.specimen_id
            ),
            cr AS (
                SELECT
                    co.stay_id,
                    co.hr,
                    MAX(ch.creatinine) AS creatinine_max
                FROM co
                LEFT JOIN chemistry_event ch
                    ON co.hadm_id = ch.hadm_id
                   AND co.starttime < ch.charttime
                   AND co.endtime >= ch.charttime
                GROUP BY co.stay_id, co.hr
            ),
            cbc_event AS (
                SELECT
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 51265 THEN le.valuenum END) AS platelet
                FROM labevents le
                WHERE le.itemid = 51265
                  AND le.valuenum IS NOT NULL
                  AND le.valuenum > 0
                GROUP BY le.specimen_id
            ),
            plt AS (
                SELECT
                    co.stay_id,
                    co.hr,
                    MIN(cbc.platelet) AS platelet_min
                FROM co
                LEFT JOIN cbc_event cbc
                    ON co.hadm_id = cbc.hadm_id
                   AND co.starttime < cbc.charttime
                   AND co.endtime >= cbc.charttime
                GROUP BY co.stay_id, co.hr
            ),
            pf AS (
                SELECT
                    co.stay_id,
                    co.hr,
                    MIN(pafi.pao2fio2ratio_novent) AS pao2fio2ratio_novent,
                    MIN(pafi.pao2fio2ratio_vent) AS pao2fio2ratio_vent
                FROM co
                LEFT JOIN pafi
                    ON co.stay_id = pafi.stay_id
                   AND co.starttime < pafi.charttime
                   AND co.endtime >= pafi.charttime
                GROUP BY co.stay_id, co.hr
            ),
            uo AS (
                SELECT
                    co.stay_id,
                    co.hr,
                    MAX(
                        CASE
                            WHEN uo.uo_tm_24hr >= 22 AND uo.uo_tm_24hr <= 30
                                THEN uo.urine_output_24h / uo.uo_tm_24hr * 24
                            ELSE NULL
                        END
                    ) AS uo_24hr
                FROM co
                LEFT JOIN uo_p uo
                    ON co.stay_id = uo.stay_id
                   AND co.charttime_floor = uo.charttime_floor
                GROUP BY co.stay_id, co.hr
            ),
            vaso AS (
                SELECT
                    co.stay_id,
                    co.hr,
                    MAX(va.epi_rate) AS rate_epinephrine,
                    MAX(va.norepi_rate) AS rate_norepinephrine,
                    MAX(va.dopa_rate) AS rate_dopamine,
                    MAX(va.dobu_rate) AS rate_dobutamine
                FROM co
                LEFT JOIN vaso_p va
                    ON co.stay_id = va.stay_id
                   AND co.charttime_floor = va.charttime_floor
                GROUP BY co.stay_id, co.hr
            ),
            scorecomp AS (
                SELECT
                    co.stay_id,
                    co.hr,
                    co.charttime_floor,
                    co.endtime,
                    pf.pao2fio2ratio_novent,
                    pf.pao2fio2ratio_vent,
                    vaso.rate_epinephrine,
                    vaso.rate_norepinephrine,
                    vaso.rate_dopamine,
                    vaso.rate_dobutamine,
                    vs.meanbp_min,
                    gcs.gcs_min,
                    uo.uo_24hr,
                    bili.bilirubin_max,
                    cr.creatinine_max,
                    plt.platelet_min
                FROM co
                LEFT JOIN vs
                    ON co.stay_id = vs.stay_id
                   AND co.hr = vs.hr
                LEFT JOIN gcs
                    ON co.stay_id = gcs.stay_id
                   AND co.hr = gcs.hr
                LEFT JOIN bili
                    ON co.stay_id = bili.stay_id
                   AND co.hr = bili.hr
                LEFT JOIN cr
                    ON co.stay_id = cr.stay_id
                   AND co.hr = cr.hr
                LEFT JOIN plt
                    ON co.stay_id = plt.stay_id
                   AND co.hr = plt.hr
                LEFT JOIN pf
                    ON co.stay_id = pf.stay_id
                   AND co.hr = pf.hr
                LEFT JOIN uo
                    ON co.stay_id = uo.stay_id
                   AND co.hr = uo.hr
                LEFT JOIN vaso
                    ON co.stay_id = vaso.stay_id
                   AND co.hr = vaso.hr
            ),
            scorecalc AS (
                SELECT
                    scorecomp.*,
                    CASE
                        WHEN pao2fio2ratio_vent < 100 THEN 4
                        WHEN pao2fio2ratio_vent < 200 THEN 3
                        WHEN pao2fio2ratio_novent < 300 THEN 2
                        WHEN pao2fio2ratio_vent < 300 THEN 2
                        WHEN pao2fio2ratio_novent < 400 THEN 1
                        WHEN pao2fio2ratio_vent < 400 THEN 1
                        WHEN COALESCE(pao2fio2ratio_vent, pao2fio2ratio_novent) IS NULL THEN NULL
                        ELSE 0
                    END AS respiration,
                    CASE
                        WHEN platelet_min < 20 THEN 4
                        WHEN platelet_min < 50 THEN 3
                        WHEN platelet_min < 100 THEN 2
                        WHEN platelet_min < 150 THEN 1
                        WHEN platelet_min IS NULL THEN NULL
                        ELSE 0
                    END AS coagulation,
                    CASE
                        WHEN bilirubin_max >= 12.0 THEN 4
                        WHEN bilirubin_max >= 6.0 THEN 3
                        WHEN bilirubin_max >= 2.0 THEN 2
                        WHEN bilirubin_max >= 1.2 THEN 1
                        WHEN bilirubin_max IS NULL THEN NULL
                        ELSE 0
                    END AS liver,
                    CASE
                        WHEN rate_dopamine > 15
                            OR rate_epinephrine > 0.1
                            OR rate_norepinephrine > 0.1
                            THEN 4
                        WHEN rate_dopamine > 5
                            OR rate_epinephrine <= 0.1
                            OR rate_norepinephrine <= 0.1
                            THEN 3
                        WHEN rate_dopamine > 0
                            OR rate_dobutamine > 0
                            THEN 2
                        WHEN meanbp_min < 70 THEN 1
                        WHEN COALESCE(
                            meanbp_min,
                            rate_dopamine,
                            rate_dobutamine,
                            rate_epinephrine,
                            rate_norepinephrine
                        ) IS NULL THEN NULL
                        ELSE 0
                    END AS cardiovascular,
                    CASE
                        WHEN gcs_min >= 13 AND gcs_min <= 14 THEN 1
                        WHEN gcs_min >= 10 AND gcs_min <= 12 THEN 2
                        WHEN gcs_min >= 6 AND gcs_min <= 9 THEN 3
                        WHEN gcs_min < 6 THEN 4
                        WHEN gcs_min IS NULL THEN NULL
                        ELSE 0
                    END AS cns,
                    CASE
                        WHEN creatinine_max >= 5.0 THEN 4
                        WHEN uo_24hr < 200 THEN 4
                        WHEN creatinine_max >= 3.5 THEN 3
                        WHEN uo_24hr < 500 THEN 3
                        WHEN creatinine_max >= 2.0 THEN 2
                        WHEN creatinine_max >= 1.2 THEN 1
                        WHEN COALESCE(uo_24hr, creatinine_max) IS NULL THEN NULL
                        ELSE 0
                    END AS renal
                FROM scorecomp
            ),
            score_final AS (
                SELECT
                    stay_id,
                    hr,
                    charttime_floor,
                    endtime,
                    COALESCE(MAX(respiration) OVER w24, 0) AS sofa_respiration,
                    COALESCE(MAX(coagulation) OVER w24, 0) AS sofa_coagulation,
                    COALESCE(MAX(liver) OVER w24, 0) AS sofa_liver,
                    COALESCE(MAX(cardiovascular) OVER w24, 0) AS sofa_cardiovascular,
                    COALESCE(MAX(cns) OVER w24, 0) AS sofa_cns,
                    COALESCE(MAX(renal) OVER w24, 0) AS sofa_renal
                FROM scorecalc
                WINDOW w24 AS (
                    PARTITION BY stay_id ORDER BY hr
                    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                )
            )
            SELECT
                stay_id,
                hr,
                charttime_floor,
                endtime,
                sofa_respiration,
                sofa_coagulation,
                sofa_liver,
                sofa_cardiovascular,
                sofa_cns,
                sofa_renal,
                sofa_respiration
                + sofa_coagulation
                + sofa_liver
                + sofa_cardiovascular
                + sofa_cns
                + sofa_renal AS sofa_24hours
            FROM score_final
            WHERE hr >= 0
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id), AVG(sofa_24hours) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step20 done %.1fs  rows=%d  stays=%d  avg_sofa=%.2f", time.time() - t0, r[0], r[1], r[2] or 0)


def step21_sepsis(con):
    name = "21_sepsis"
    if exists(name):
        log.info("step21 cached")
        return
    t0 = time.time()
    # Reference: sepsis/suspicion_of_infection.sql + sepsis/sepsis3.sql
    # Follow the official MIMIC-IV pattern more closely, but materialize the
    # major stages to keep the full-wide build tractable.
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
    con.execute(f"CREATE OR REPLACE VIEW sofa_p AS SELECT * FROM read_parquet('{inter('20_sofa')}')")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ab_tbl_tmp AS
        WITH abx AS (
            SELECT
                pr.drug,
                pr.route,
                CASE
                    WHEN {ANTIBIOTIC_DRUG_FILTER} THEN 1
                    ELSE 0
                END AS antibiotic
            FROM prescriptions pr
            WHERE pr.drug_type NOT IN ('BASE')
              AND NOT ({ANTIBIOTIC_ROUTE_EXCLUSION})
            GROUP BY pr.drug, pr.route
        ),
        antibiotic AS (
            SELECT
                pr.subject_id,
                pr.hadm_id,
                ie.stay_id,
                pr.drug AS antibiotic,
                pr.starttime AS antibiotic_time,
                pr.stoptime AS antibiotic_stoptime
            FROM prescriptions pr
            INNER JOIN abx
                ON pr.drug = abx.drug
               AND pr.route = abx.route
            INNER JOIN icustays ie
                ON pr.hadm_id = ie.hadm_id
               AND pr.starttime >= ie.intime
               AND pr.starttime < ie.outtime
            WHERE abx.antibiotic = 1
              AND pr.starttime IS NOT NULL
        )
        SELECT
            abx.subject_id,
            abx.hadm_id,
            abx.stay_id,
            abx.antibiotic,
            abx.antibiotic_time,
            DATE_TRUNC('day', abx.antibiotic_time) AS antibiotic_date,
            abx.antibiotic_stoptime,
            ROW_NUMBER() OVER (
                PARTITION BY abx.subject_id
                ORDER BY
                    abx.antibiotic_time NULLS FIRST,
                    abx.antibiotic_stoptime NULLS FIRST,
                    abx.antibiotic NULLS FIRST
            ) AS ab_id
        FROM antibiotic abx
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE me_tmp AS
        SELECT
            micro_specimen_id,
            MAX(subject_id) AS subject_id,
            MAX(hadm_id) AS hadm_id,
            CAST(MAX(chartdate) AS DATE) AS chartdate,
            MAX(charttime) AS charttime,
            MAX(spec_type_desc) AS spec_type_desc,
            MAX(
                CASE
                    WHEN org_name IS NOT NULL AND org_itemid <> 90856 AND org_name <> ''
                    THEN 1 ELSE 0
                END
            ) AS positiveculture
        FROM microbiologyevents
        WHERE subject_id IN (SELECT DISTINCT subject_id FROM ab_tbl_tmp)
        GROUP BY micro_specimen_id
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE soi_tmp AS
        WITH me_then_ab AS (
            SELECT
                ab_tbl.subject_id,
                ab_tbl.hadm_id,
                ab_tbl.stay_id,
                ab_tbl.ab_id,
                me72.micro_specimen_id,
                COALESCE(me72.charttime, CAST(me72.chartdate AS TIMESTAMP)) AS last72_charttime,
                me72.positiveculture AS last72_positiveculture,
                me72.spec_type_desc AS last72_specimen,
                ROW_NUMBER() OVER (
                    PARTITION BY ab_tbl.subject_id, ab_tbl.ab_id
                    ORDER BY me72.chartdate, me72.charttime NULLS LAST
                ) AS micro_seq
            FROM ab_tbl_tmp ab_tbl
            LEFT JOIN me_tmp AS me72
                ON ab_tbl.subject_id = me72.subject_id
               AND (
                    (
                        me72.charttime IS NOT NULL
                        AND ab_tbl.antibiotic_time > me72.charttime
                        AND ab_tbl.antibiotic_time <= me72.charttime + INTERVAL 72 HOUR
                    )
                    OR (
                        me72.charttime IS NULL
                        AND ab_tbl.antibiotic_date >= me72.chartdate
                        AND ab_tbl.antibiotic_date <= me72.chartdate + INTERVAL 3 DAY
                    )
               )
        ),
        ab_then_me AS (
            SELECT
                ab_tbl.subject_id,
                ab_tbl.hadm_id,
                ab_tbl.stay_id,
                ab_tbl.ab_id,
                me24.micro_specimen_id,
                COALESCE(me24.charttime, CAST(me24.chartdate AS TIMESTAMP)) AS next24_charttime,
                me24.positiveculture AS next24_positiveculture,
                me24.spec_type_desc AS next24_specimen,
                ROW_NUMBER() OVER (
                    PARTITION BY ab_tbl.subject_id, ab_tbl.ab_id
                    ORDER BY me24.chartdate, me24.charttime NULLS LAST
                ) AS micro_seq
            FROM ab_tbl_tmp ab_tbl
            LEFT JOIN me_tmp AS me24
                ON ab_tbl.subject_id = me24.subject_id
               AND (
                    (
                        me24.charttime IS NOT NULL
                        AND ab_tbl.antibiotic_time >= me24.charttime - INTERVAL 24 HOUR
                        AND ab_tbl.antibiotic_time < me24.charttime
                    )
                    OR (
                        me24.charttime IS NULL
                        AND ab_tbl.antibiotic_date >= me24.chartdate - INTERVAL 1 DAY
                        AND ab_tbl.antibiotic_date <= me24.chartdate
                    )
               )
        )
        SELECT
            ab_tbl.stay_id,
            ab_tbl.hadm_id,
            ab_tbl.subject_id,
            ab_tbl.ab_id,
            ab_tbl.antibiotic,
            ab_tbl.antibiotic_time,
            CASE
                WHEN me2ab.last72_specimen IS NULL AND ab2me.next24_specimen IS NULL THEN 0
                ELSE 1
            END AS suspected_infection,
            CASE
                WHEN me2ab.last72_specimen IS NULL AND ab2me.next24_specimen IS NULL THEN NULL
                ELSE COALESCE(me2ab.last72_charttime, ab_tbl.antibiotic_time)
            END AS suspected_infection_time,
            COALESCE(me2ab.last72_charttime, ab2me.next24_charttime) AS culture_time,
            COALESCE(me2ab.last72_specimen, ab2me.next24_specimen) AS specimen,
            COALESCE(me2ab.last72_positiveculture, ab2me.next24_positiveculture) AS positive_culture
        FROM ab_tbl_tmp ab_tbl
        LEFT JOIN ab_then_me AS ab2me
            ON ab_tbl.subject_id = ab2me.subject_id
           AND ab_tbl.ab_id = ab2me.ab_id
           AND ab2me.micro_seq = 1
        LEFT JOIN me_then_ab AS me2ab
            ON ab_tbl.subject_id = me2ab.subject_id
           AND ab_tbl.ab_id = me2ab.ab_id
           AND me2ab.micro_seq = 1
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE onset_tmp AS
        WITH sofa_pos AS (
            SELECT stay_id, hr, endtime, sofa_24hours
            FROM sofa_p
            WHERE sofa_24hours >= 2
        ),
        sepsis3_candidates AS (
            SELECT
                soi.subject_id,
                sf.stay_id,
                soi.antibiotic_time,
                soi.culture_time,
                soi.suspected_infection_time AS t_suspicion,
                soi.specimen,
                soi.positive_culture,
                sf.hr AS onset_hr,
                sf.endtime AS sofa_time,
                sf.sofa_24hours AS sofa_score,
                ROW_NUMBER() OVER (
                    PARTITION BY soi.stay_id
                    ORDER BY
                        soi.suspected_infection_time,
                        soi.antibiotic_time,
                        soi.culture_time,
                        sf.endtime
                ) AS rn
            FROM soi_tmp soi
            INNER JOIN sofa_pos sf
                ON soi.stay_id = sf.stay_id
            WHERE soi.suspected_infection = 1
              AND soi.suspected_infection_time IS NOT NULL
              AND sf.endtime >= soi.suspected_infection_time - INTERVAL 48 HOUR
              AND sf.endtime <= soi.suspected_infection_time + INTERVAL 24 HOUR
        )
        SELECT
            subject_id,
            stay_id,
            antibiotic_time,
            culture_time,
            t_suspicion,
            specimen,
            positive_culture,
            onset_hr,
            sofa_time,
            sofa_score,
            1 AS sepsis3
        FROM sepsis3_candidates
        WHERE rn = 1
    """)
    con.execute(f"""
        COPY (
            SELECT
                ta.stay_id,
                ta.hr,
                on_.antibiotic_time,
                on_.culture_time,
                on_.t_suspicion,
                on_.specimen,
                on_.positive_culture,
                on_.onset_hr,
                on_.sofa_time,
                on_.sofa_score,
                COALESCE(on_.sepsis3, 0) AS sepsis3,
                CASE WHEN on_.onset_hr IS NOT NULL AND ta.hr >= on_.onset_hr THEN 1 ELSE 0 END AS SepsisLabel
            FROM time_axis ta
            LEFT JOIN onset_tmp on_
                ON ta.stay_id = on_.stay_id
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT stay_id),
               SUM(SepsisLabel), COUNT(DISTINCT CASE WHEN SepsisLabel=1 THEN stay_id END)
        FROM read_parquet('{inter(name)}')
    """).fetchone()
    log.info("step21 done %.1fs  rows=%d  stays=%d  sepsis_rows=%d  sepsis_stays=%d",
             time.time() - t0, r[0], r[1], r[2], r[3])


def step22_final(con):
    name_check = "22_done"
    if exists(name_check):
        log.info("step22 cached")
        return
    t0 = time.time()
    # Join the official-style concept outputs onto the local ICU hourly axis,
    # then append the extra context columns still needed by the final wide table.
    for step_name in [
        "03_vitals","03b_icp","03c_oxygen_delivery","03d_rhythm","03e_ventilator_setting","03f_code_status","03g_invasive_line","03h_rrt","04_gcs","05_bg","06_chemistry","07_cbc","08_enzyme",
        "09_coagulation","10_blood_diff","11_cardiac_crp","12_uo","13_vaso",
        "14_vent","15_crrt","16_antibiotic","17_height_weight","18_charlson",
        "19_service","20_sofa","21_sepsis",
    ]:
        view = step_name.replace("_", "v_")
        con.execute(f"CREATE OR REPLACE VIEW v_{step_name} AS SELECT * FROM read_parquet('{inter(step_name)}')")
    con.execute(f"""
        COPY (
            WITH adm_context AS (
                SELECT
                    hadm_id,
                    deathtime,
                    admission_type,
                    admission_location,
                    discharge_location,
                    insurance,
                    language,
                    marital_status,
                    edregtime,
                    edouttime
                FROM admissions
            ),
            ext_context AS (
                SELECT
                    ie.stay_id,
                    ie.first_careunit,
                    ie.last_careunit,
                    p.anchor_year_group,
                    ROUND(date_diff('minute', a.admittime, ie.intime) / 60.0, 2) AS hospadmtime
                FROM icustays ie
                INNER JOIN admissions a
                    ON ie.hadm_id = a.hadm_id
                INNER JOIN patients p
                    ON ie.subject_id = p.subject_id
            )
            SELECT
                -- identifiers
                co.subject_id,
                co.hadm_id,
                ta.stay_id,
                ta.hr,
                ta.starttime,
                ta.endtime,
                ta.charttime_floor,
                -- static
                co.intime,
                co.outtime,
                ext.first_careunit,
                ext.last_careunit,
                co.age,
                co.gender,
                co.dod,
                ext.anchor_year_group,
                co.admittime,
                co.dischtime,
                adm.deathtime,
                adm.admission_type,
                adm.admission_location,
                adm.discharge_location,
                adm.insurance,
                adm.language,
                adm.marital_status,
                co.race,
                co.hospital_expire_flag,
                ext.hospadmtime,
                adm.edregtime,
                adm.edouttime,
                co.los_hospital,
                co.los_icu,
                co.hospstay_seq,
                co.first_hosp_stay,
                co.icustay_seq,
                co.first_icu_stay,
                ch18.charlson_score,
                -- hospital service (time-varying; pre-ICU rows are retained)
                sv.curr_service AS curr_service,
                -- vitals and respiratory support from ICU charting; pre-ICU rows are retained
                vt.heart_rate AS heart_rate,
                vt.sbp AS sbp,
                vt.dbp AS dbp,
                vt.mbp AS mbp,
                vt.sbp_ni AS sbp_ni,
                vt.dbp_ni AS dbp_ni,
                vt.mbp_ni AS mbp_ni,
                vt.resp_rate AS resp_rate,
                vt.temperature AS temperature,
                vt.temperature_site AS temperature_site,
                vt.spo2 AS spo2,
                vt.glucose_vital AS glucose_vital,
                icp.icp AS icp,
                od.o2_flow AS o2_flow,
                od.o2_flow_additional AS o2_flow_additional,
                od.o2_delivery_device_1 AS o2_delivery_device_1,
                od.o2_delivery_device_2 AS o2_delivery_device_2,
                od.o2_delivery_device_3 AS o2_delivery_device_3,
                od.o2_delivery_device_4 AS o2_delivery_device_4,
                rh.heart_rhythm AS heart_rhythm,
                rh.ectopy_type AS ectopy_type,
                rh.ectopy_frequency AS ectopy_frequency,
                rh.ectopy_type_secondary AS ectopy_type_secondary,
                rh.ectopy_frequency_secondary AS ectopy_frequency_secondary,
                vs.respiratory_rate_set AS respiratory_rate_set,
                vs.respiratory_rate_total AS respiratory_rate_total,
                vs.respiratory_rate_spontaneous AS respiratory_rate_spontaneous,
                vs.minute_volume AS minute_volume,
                vs.tidal_volume_set AS tidal_volume_set,
                vs.tidal_volume_observed AS tidal_volume_observed,
                vs.tidal_volume_spontaneous AS tidal_volume_spontaneous,
                vs.plateau_pressure AS plateau_pressure,
                vs.peep_vent AS peep_vent,
                vs.fio2_vent AS fio2_vent,
                vs.flow_rate AS flow_rate,
                vs.ventilator_mode AS ventilator_mode,
                vs.ventilator_mode_hamilton AS ventilator_mode_hamilton,
                vs.ventilator_type AS ventilator_type,
                cs.fullcode AS fullcode,
                cs.cmo AS cmo,
                cs.dni AS dni,
                cs.dnr AS dnr,
                il.invasive_line_count AS invasive_line_count,
                il.invasive_line_types AS invasive_line_types,
                il.invasive_line_sites AS invasive_line_sites,
                rrt.dialysis_present AS dialysis_present,
                rrt.dialysis_active AS dialysis_active,
                rrt.dialysis_type AS dialysis_type,
                -- GCS from ICU charting; pre-ICU rows are retained
                gc.gcs_motor AS gcs_motor,
                gc.gcs_verbal AS gcs_verbal,
                gc.gcs_eyes AS gcs_eyes,
                gc.gcs_unable AS gcs_unable,
                gc.gcs_total AS gcs_total,
                -- blood gas and FiO2 pairing; pre-ICU rows are retained when source data exist
                bg.ph, bg.pco2, bg.po2, bg.so2,
                bg.aado2, bg.aado2_calc, bg.pao2fio2ratio, bg.pao2fio2ratio_art, bg.fio2,
                bg.fio2_chartevents AS fio2_chartevents,
                bg.bg_specimen,
                bg.arterial_bg_flag,
                bg.baseexcess, bg.lactate,
                bg.bicarbonate_bg, bg.totalco2_bg,
                bg.hematocrit_bg, bg.hemoglobin_bg,
                bg.carboxyhemoglobin, bg.methemoglobin,
                bg.o2flow, bg.peep, bg.requiredo2, bg.calcium_ionized,
                bg.chloride_bg, bg.temperature_bg,
                bg.potassium_bg, bg.sodium_bg, bg.glucose_bg,
                -- chemistry
                ch.creatinine, ch.sodium, ch.potassium,
                ch.bicarbonate, ch.bun, ch.calcium_total,
                ch.chloride, ch.glucose_lab, ch.anion_gap, ch.albumin,
                ch.globulin, ch.total_protein,
                -- CBC
                cb.platelet, cb.hemoglobin, cb.hematocrit,
                cb.wbc, cb.mch, cb.mchc, cb.mcv,
                cb.rbc, cb.rdw, cb.rdwsd,
                -- enzyme
                en.alt, en.alp, en.ast, en.amylase,
                en.bilirubin_total, en.bilirubin_direct, en.bilirubin_indirect,
                en.ck_cpk, en.ck_mb, en.ggt, en.ldh,
                -- coagulation
                co2.d_dimer, co2.fibrinogen, co2.thrombin,
                co2.inr, co2.pt, co2.ptt,
                -- blood differential
                bd.wbc_diff,
                bd.neutrophils_pct, bd.neutrophils_abs,
                bd.lymphocytes_pct, bd.lymphocytes_abs,
                bd.monocytes_pct, bd.monocytes_abs,
                bd.eosinophils_pct, bd.eosinophils_abs,
                bd.basophils_pct, bd.basophils_abs,
                bd.bands, bd.immature_granulocytes,
                bd.atypical_lymphocytes, bd.metamyelocytes, bd.nrbc,
                -- cardiac markers + CRP
                cm.troponin_t, cm.ntprobnp, cm.crp,
                -- urine output
                uo.weight AS uo_weight,
                uo.urine_output, uo.urineoutput_6hr, uo.urineoutput_12hr,
                uo.urine_output_24h, uo.uo_mlkghr_6hr, uo.uo_mlkghr_12hr, uo.uo_mlkghr_24hr,
                uo.uo_tm_6hr, uo.uo_tm_12hr, uo.uo_tm_24hr, uo.uo_24hr, uo.urine_output_24h_est,
                -- vasopressors; pre-ICU rows are retained
                va.norepi_amount AS norepi_amount,
                va.norepi_rate AS norepi_rate,
                va.epi_amount AS epi_amount,
                va.epi_rate AS epi_rate,
                va.dopa_amount AS dopa_amount,
                va.dopa_rate AS dopa_rate,
                va.dobu_amount AS dobu_amount,
                va.dobu_rate AS dobu_rate,
                va.phenyl_amount AS phenyl_amount,
                va.phenyl_rate AS phenyl_rate,
                va.vaso_amount AS vaso_amount,
                va.vaso_rate AS vaso_rate,
                -- ventilation status; pre-ICU rows are retained
                ve.ventilation_status AS ventilation_status,
                -- CRRT; pre-ICU rows are retained
                cr.crrt_mode AS crrt_mode,
                cr.access_pressure AS crrt_access_pressure,
                cr.citrate AS crrt_citrate,
                cr.current_goal AS crrt_current_goal,
                cr.dialysate_fluid AS crrt_dialysate_fluid,
                cr.blood_flow AS crrt_blood_flow,
                cr.dialysate_rate AS crrt_dialysate_rate,
                cr.effluent_pressure AS crrt_effluent_pressure,
                cr.filter_pressure AS crrt_filter_pressure,
                cr.heparin_concentration AS crrt_heparin_concentration,
                cr.heparin_dose AS crrt_heparin_dose,
                cr.hourly_patient_fluid_removal AS crrt_hourly_patient_fluid_removal,
                cr.prefilter_replacement_rate AS crrt_prefilter_replacement_rate,
                cr.postfilter_replacement_rate AS crrt_postfilter_replacement_rate,
                cr.replacement_fluid AS crrt_replacement_fluid,
                cr.replacement_rate AS crrt_replacement_rate,
                cr.return_pressure AS crrt_return_pressure,
                cr.ultrafiltrate_output AS crrt_ultrafiltrate_output,
                COALESCE(cr.system_active, 0) AS crrt_system_active,
                COALESCE(cr.clots, 0) AS crrt_clots,
                COALESCE(cr.clots_increasing, 0) AS crrt_clots_increasing,
                COALESCE(cr.clotted, 0) AS crrt_clotted,
                COALESCE(cr.crrt_flag, 0) AS crrt_flag,
                -- antibiotic
                COALESCE(ab.antibiotic_flag, 0) AS antibiotic_flag,
                -- height / weight
                hw.height,
                hw.weight_type,
                hw.admit_weight,
                hw.daily_weight,
                hw.weight,
                -- SOFA is only output for hr >= 0, but the rolling 24h calculation can use pre-ICU data
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_respiration    END AS sofa_respiration,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_coagulation    END AS sofa_coagulation,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_liver          END AS sofa_liver,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_cardiovascular END AS sofa_cardiovascular,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_cns            END AS sofa_cns,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_renal          END AS sofa_renal,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_24hours        END AS sofa_24hours,
                -- sepsis metadata/labels are only output for hr >= 0
                CASE WHEN ta.hr < 0 THEN NULL ELSE sp.antibiotic_time   END AS antibiotic_time,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sp.culture_time       END AS culture_time,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sp.t_suspicion        END AS t_suspicion,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sp.specimen           END AS specimen,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sp.positive_culture   END AS positive_culture,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sp.onset_hr           END AS onset_hr,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sp.sofa_time          END AS sofa_time,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sp.sofa_score         END AS sofa_score,
                CASE WHEN ta.hr < 0 THEN NULL ELSE COALESCE(sp.sepsis3, 0) END AS sepsis3,
                CASE WHEN ta.hr < 0 THEN NULL ELSE COALESCE(sp.SepsisLabel, 0) END AS SepsisLabel
            FROM time_axis ta
            INNER JOIN cohort co ON ta.stay_id = co.stay_id
            LEFT JOIN adm_context adm ON co.hadm_id = adm.hadm_id
            LEFT JOIN ext_context ext ON ta.stay_id = ext.stay_id
            -- vitals
            LEFT JOIN v_03_vitals         vt   ON ta.stay_id = vt.stay_id  AND ta.charttime_floor = vt.charttime_floor
            LEFT JOIN v_03b_icp           icp  ON ta.stay_id = icp.stay_id AND ta.charttime_floor = icp.charttime_floor
            LEFT JOIN v_03c_oxygen_delivery od  ON ta.stay_id = od.stay_id  AND ta.charttime_floor = od.charttime_floor
            LEFT JOIN v_03d_rhythm        rh   ON ta.stay_id = rh.stay_id  AND ta.charttime_floor = rh.charttime_floor
            LEFT JOIN v_03e_ventilator_setting vs ON ta.stay_id = vs.stay_id AND ta.charttime_floor = vs.charttime_floor
            LEFT JOIN v_03f_code_status  cs   ON ta.stay_id = cs.stay_id  AND ta.charttime_floor = cs.charttime_floor
            LEFT JOIN v_03g_invasive_line il   ON ta.stay_id = il.stay_id  AND ta.charttime_floor = il.charttime_floor
            LEFT JOIN v_03h_rrt          rrt  ON ta.stay_id = rrt.stay_id AND ta.charttime_floor = rrt.charttime_floor
            LEFT JOIN v_04_gcs            gc   ON ta.stay_id = gc.stay_id  AND ta.charttime_floor = gc.charttime_floor
            LEFT JOIN v_05_bg             bg   ON ta.stay_id = bg.stay_id  AND ta.charttime_floor = bg.charttime_floor
            LEFT JOIN v_06_chemistry      ch   ON ta.stay_id = ch.stay_id  AND ta.charttime_floor = ch.charttime_floor
            LEFT JOIN v_07_cbc            cb   ON ta.stay_id = cb.stay_id  AND ta.charttime_floor = cb.charttime_floor
            LEFT JOIN v_08_enzyme         en   ON ta.stay_id = en.stay_id  AND ta.charttime_floor = en.charttime_floor
            LEFT JOIN v_09_coagulation    co2  ON ta.stay_id = co2.stay_id AND ta.charttime_floor = co2.charttime_floor
            LEFT JOIN v_10_blood_diff     bd   ON ta.stay_id = bd.stay_id  AND ta.charttime_floor = bd.charttime_floor
            LEFT JOIN v_11_cardiac_crp    cm   ON ta.stay_id = cm.stay_id  AND ta.charttime_floor = cm.charttime_floor
            LEFT JOIN v_12_uo             uo   ON ta.stay_id = uo.stay_id  AND ta.charttime_floor = uo.charttime_floor
            LEFT JOIN v_13_vaso           va   ON ta.stay_id = va.stay_id  AND ta.charttime_floor = va.charttime_floor
            LEFT JOIN v_14_vent           ve   ON ta.stay_id = ve.stay_id  AND ta.charttime_floor = ve.charttime_floor
            LEFT JOIN v_15_crrt           cr   ON ta.stay_id = cr.stay_id  AND ta.charttime_floor = cr.charttime_floor
            LEFT JOIN v_16_antibiotic     ab   ON ta.stay_id = ab.stay_id  AND ta.charttime_floor = ab.charttime_floor
            LEFT JOIN v_17_height_weight  hw   ON ta.stay_id = hw.stay_id  AND ta.charttime_floor = hw.charttime_floor
            LEFT JOIN v_18_charlson       ch18 ON ta.stay_id = ch18.stay_id
            LEFT JOIN v_19_service        sv   ON ta.stay_id = sv.stay_id  AND ta.charttime_floor = sv.charttime_floor
            LEFT JOIN v_20_sofa           sf   ON ta.stay_id = sf.stay_id  AND ta.hr = sf.hr
            LEFT JOIN v_21_sepsis         sp   ON ta.stay_id = sp.stay_id  AND ta.hr = sp.hr
        ) TO '{OUT_PATH.replace(chr(92), '/')}' (FORMAT PARQUET)
    """)
    # write sentinel
    import pathlib
    pathlib.Path(os.path.join(INTER_DIR, name_check + ".parquet")).touch()
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id), SUM(SepsisLabel) FROM read_parquet('{OUT_PATH.replace(chr(92), '/')}')").fetchone()
    log.info("step22 done %.1fs  rows=%d  stays=%d  sepsis_rows=%d",
             time.time() - t0, r[0], r[1], r[2])


def _lab_step(con, name, label, items_sql, itemids):
    """Generic labevents hourly aggregation helper."""
    if exists(name):
        log.info("%s cached", name)
        return
    t0 = time.time()
    ids_str = ",".join(str(i) for i in itemids)
    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT
                    le.subject_id,
                    le.charttime,
                    {items_sql}
                FROM labevents le
                INNER JOIN cohort co ON le.subject_id = co.subject_id
                WHERE le.itemid IN ({ids_str})
            ),
            hourly AS (
                SELECT
                    co.stay_id,
                    date_trunc('hour', raw.charttime) AS charttime_floor,
                    {", ".join(f"AVG({col}) AS {col}" for col in _col_names(items_sql))}
                FROM raw
                INNER JOIN cohort co
                    ON raw.subject_id = co.subject_id
                   AND raw.charttime >= co.intime - INTERVAL '24' HOUR
                   AND raw.charttime <= co.outtime
                GROUP BY co.stay_id, date_trunc('hour', raw.charttime)
            )
            SELECT * FROM hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("%s done %.1fs  rows=%d  stays=%d", name, time.time() - t0, r[0], r[1])


def _col_names(items_sql):
    """Extract column alias names from a CASE WHEN block."""
    import re
    return re.findall(r"END AS (\w+)", items_sql)


def step06_chemistry(con):
    name = "06_chemistry"
    if exists(name):
        log.info("step06 cached")
        return
    t0 = time.time()
    # Reference: measurement/chemistry.sql
    # Build the official specimen-level chemistry concept first, then project it
    # onto ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH chemistry AS (
                SELECT
                    MAX(le.subject_id) AS subject_id,
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 50862 AND le.valuenum <= 10 THEN le.valuenum END) AS albumin,
                    MAX(CASE WHEN le.itemid = 50930 AND le.valuenum <= 10 THEN le.valuenum END) AS globulin,
                    MAX(CASE WHEN le.itemid = 50976 AND le.valuenum <= 20 THEN le.valuenum END) AS total_protein,
                    MAX(CASE WHEN le.itemid = 50868 AND le.valuenum <= 10000 THEN le.valuenum END) AS anion_gap,
                    MAX(CASE WHEN le.itemid = 50882 AND le.valuenum <= 10000 THEN le.valuenum END) AS bicarbonate,
                    MAX(CASE WHEN le.itemid = 51006 AND le.valuenum <= 300 THEN le.valuenum END) AS bun,
                    MAX(CASE WHEN le.itemid = 50893 AND le.valuenum <= 10000 THEN le.valuenum END) AS calcium_total,
                    MAX(CASE WHEN le.itemid = 50902 AND le.valuenum <= 10000 THEN le.valuenum END) AS chloride,
                    MAX(CASE WHEN le.itemid = 50912 AND le.valuenum <= 150 THEN le.valuenum END) AS creatinine,
                    MAX(CASE WHEN le.itemid = 50931 AND le.valuenum <= 10000 THEN le.valuenum END) AS glucose_lab,
                    MAX(CASE WHEN le.itemid = 50983 AND le.valuenum <= 200 THEN le.valuenum END) AS sodium,
                    MAX(CASE WHEN le.itemid = 50971 AND le.valuenum <= 30 THEN le.valuenum END) AS potassium
                FROM labevents le
                WHERE le.itemid IN (50862, 50930, 50976, 50868, 50882, 51006, 50893, 50902, 50912, 50931, 50983, 50971)
                  AND le.valuenum IS NOT NULL
                  AND (le.valuenum > 0 OR le.itemid = 50868)
                GROUP BY le.specimen_id
            ),
            chemistry_hourly AS (
                WITH ranked AS (
                    SELECT
                        co.stay_id,
                        date_trunc('hour', ch.charttime) AS charttime_floor,
                        ch.charttime,
                        ch.creatinine,
                        ch.sodium,
                        ch.potassium,
                        ch.bicarbonate,
                        ch.bun,
                        ch.calcium_total,
                        ch.chloride,
                        ch.glucose_lab,
                        ch.anion_gap,
                        ch.albumin,
                        ch.globulin,
                        ch.total_protein,
                        ROW_NUMBER() OVER (
                            PARTITION BY co.stay_id, date_trunc('hour', ch.charttime)
                            ORDER BY ch.charttime DESC, ch.specimen_id DESC
                        ) AS hour_seq
                    FROM chemistry ch
                    INNER JOIN cohort co
                        ON ch.hadm_id = co.hadm_id
                       AND ch.charttime >= co.intime - INTERVAL '24' HOUR
                       AND ch.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    creatinine,
                    sodium,
                    potassium,
                    bicarbonate,
                    bun,
                    calcium_total,
                    chloride,
                    glucose_lab,
                    anion_gap,
                    albumin,
                    globulin,
                    total_protein
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                creatinine,
                sodium,
                potassium,
                bicarbonate,
                bun,
                calcium_total,
                chloride,
                glucose_lab,
                anion_gap,
                albumin,
                globulin,
                total_protein
            FROM chemistry_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step06 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step07_cbc(con):
    name = "07_cbc"
    if exists(name):
        log.info("step07 cached")
        return
    t0 = time.time()
    # Reference: measurement/complete_blood_count.sql
    # Build the official specimen-level CBC concept first, then project it onto
    # ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH cbc AS (
                SELECT
                    MAX(le.subject_id) AS subject_id,
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 51221 THEN le.valuenum END) AS hematocrit,
                    MAX(CASE WHEN le.itemid = 51222 THEN le.valuenum END) AS hemoglobin,
                    MAX(CASE WHEN le.itemid = 51248 THEN le.valuenum END) AS mch,
                    MAX(CASE WHEN le.itemid = 51249 THEN le.valuenum END) AS mchc,
                    MAX(CASE WHEN le.itemid = 51250 THEN le.valuenum END) AS mcv,
                    MAX(CASE WHEN le.itemid = 51265 THEN le.valuenum END) AS platelet,
                    MAX(CASE WHEN le.itemid = 51279 THEN le.valuenum END) AS rbc,
                    MAX(CASE WHEN le.itemid = 51277 THEN le.valuenum END) AS rdw,
                    MAX(CASE WHEN le.itemid = 52159 THEN le.valuenum END) AS rdwsd,
                    MAX(CASE WHEN le.itemid = 51301 THEN le.valuenum END) AS wbc
                FROM labevents le
                WHERE le.itemid IN (51265,51222,51221,51301,51248,51249,51250,51279,51277,52159)
                  AND le.valuenum IS NOT NULL
                  AND le.valuenum > 0
                GROUP BY le.specimen_id
            ),
            cbc_hourly AS (
                WITH ranked AS (
                    SELECT
                        co.stay_id,
                        date_trunc('hour', cb.charttime) AS charttime_floor,
                        cb.charttime,
                        cb.platelet,
                        cb.hemoglobin,
                        cb.hematocrit,
                        cb.wbc,
                        cb.mch,
                        cb.mchc,
                        cb.mcv,
                        cb.rbc,
                        cb.rdw,
                        cb.rdwsd,
                        ROW_NUMBER() OVER (
                            PARTITION BY co.stay_id, date_trunc('hour', cb.charttime)
                            ORDER BY cb.charttime DESC, cb.specimen_id DESC
                        ) AS hour_seq
                    FROM cbc cb
                    INNER JOIN cohort co
                        ON cb.hadm_id = co.hadm_id
                       AND cb.charttime >= co.intime - INTERVAL '24' HOUR
                       AND cb.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    platelet,
                    hemoglobin,
                    hematocrit,
                    wbc,
                    mch,
                    mchc,
                    mcv,
                    rbc,
                    rdw,
                    rdwsd
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                platelet,
                hemoglobin,
                hematocrit,
                wbc,
                mch,
                mchc,
                mcv,
                rbc,
                rdw,
                rdwsd
            FROM cbc_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step07 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step08_enzyme(con):
    name = "08_enzyme"
    if exists(name):
        log.info("step08 cached")
        return
    t0 = time.time()
    # Reference: measurement/enzyme.sql
    # Build the official specimen-level enzyme concept first, then project it
    # onto ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH enzyme AS (
                SELECT
                    MAX(le.subject_id) AS subject_id,
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 50861 THEN le.valuenum END) AS alt,
                    MAX(CASE WHEN le.itemid = 50863 THEN le.valuenum END) AS alp,
                    MAX(CASE WHEN le.itemid = 50878 THEN le.valuenum END) AS ast,
                    MAX(CASE WHEN le.itemid = 50867 THEN le.valuenum END) AS amylase,
                    MAX(CASE WHEN le.itemid = 50885 THEN le.valuenum END) AS bilirubin_total,
                    MAX(CASE WHEN le.itemid = 50883 THEN le.valuenum END) AS bilirubin_direct,
                    MAX(CASE WHEN le.itemid = 50884 THEN le.valuenum END) AS bilirubin_indirect,
                    MAX(CASE WHEN le.itemid = 50910 THEN le.valuenum END) AS ck_cpk,
                    MAX(CASE WHEN le.itemid = 50911 THEN le.valuenum END) AS ck_mb,
                    MAX(CASE WHEN le.itemid = 50927 THEN le.valuenum END) AS ggt,
                    MAX(CASE WHEN le.itemid = 50954 THEN le.valuenum END) AS ldh
                FROM labevents le
                WHERE le.itemid IN (50861,50863,50878,50867,50885,50883,50884,50910,50911,50927,50954)
                  AND le.valuenum IS NOT NULL
                  AND le.valuenum > 0
                GROUP BY le.specimen_id
            ),
            enzyme_hourly AS (
                WITH ranked AS (
                    SELECT
                        co.stay_id,
                        date_trunc('hour', en.charttime) AS charttime_floor,
                        en.charttime,
                        en.alt,
                        en.alp,
                        en.ast,
                        en.amylase,
                        en.bilirubin_total,
                        en.bilirubin_direct,
                        en.bilirubin_indirect,
                        en.ck_cpk,
                        en.ck_mb,
                        en.ggt,
                        en.ldh,
                        ROW_NUMBER() OVER (
                            PARTITION BY co.stay_id, date_trunc('hour', en.charttime)
                            ORDER BY en.charttime DESC, en.specimen_id DESC
                        ) AS hour_seq
                    FROM enzyme en
                    INNER JOIN cohort co
                        ON en.hadm_id = co.hadm_id
                       AND en.charttime >= co.intime - INTERVAL '24' HOUR
                       AND en.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    alt,
                    alp,
                    ast,
                    amylase,
                    bilirubin_total,
                    bilirubin_direct,
                    bilirubin_indirect,
                    ck_cpk,
                    ck_mb,
                    ggt,
                    ldh
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                alt,
                alp,
                ast,
                amylase,
                bilirubin_total,
                bilirubin_direct,
                bilirubin_indirect,
                ck_cpk,
                ck_mb,
                ggt,
                ldh
            FROM enzyme_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step08 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step09_coagulation(con):
    name = "09_coagulation"
    if exists(name):
        log.info("step09 cached")
        return
    t0 = time.time()
    # Reference: measurement/coagulation.sql
    # Build the official specimen-level coagulation concept first, then project
    # it onto ICU-hour rows for the wide table.
    con.execute(f"""
        COPY (
            WITH coagulation AS (
                SELECT
                    MAX(le.subject_id) AS subject_id,
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 51196 THEN le.valuenum END) AS d_dimer,
                    MAX(CASE WHEN le.itemid = 51214 THEN le.valuenum END) AS fibrinogen,
                    MAX(CASE WHEN le.itemid = 51297 THEN le.valuenum END) AS thrombin,
                    MAX(CASE WHEN le.itemid = 51237 THEN le.valuenum END) AS inr,
                    MAX(CASE WHEN le.itemid = 51274 THEN le.valuenum END) AS pt,
                    MAX(CASE WHEN le.itemid = 51275 THEN le.valuenum END) AS ptt
                FROM labevents le
                WHERE le.itemid IN (51196,51214,51297,51237,51274,51275)
                  AND le.valuenum IS NOT NULL
                GROUP BY le.specimen_id
            ),
            coagulation_hourly AS (
                WITH ranked AS (
                    SELECT
                        co.stay_id,
                        date_trunc('hour', cg.charttime) AS charttime_floor,
                        cg.charttime,
                        cg.d_dimer,
                        cg.fibrinogen,
                        cg.thrombin,
                        cg.inr,
                        cg.pt,
                        cg.ptt,
                        ROW_NUMBER() OVER (
                            PARTITION BY co.stay_id, date_trunc('hour', cg.charttime)
                            ORDER BY cg.charttime DESC, cg.specimen_id DESC
                        ) AS hour_seq
                    FROM coagulation cg
                    INNER JOIN cohort co
                        ON cg.hadm_id = co.hadm_id
                       AND cg.charttime >= co.intime - INTERVAL '24' HOUR
                       AND cg.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    d_dimer,
                    fibrinogen,
                    thrombin,
                    inr,
                    pt,
                    ptt
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                d_dimer,
                fibrinogen,
                thrombin,
                inr,
                pt,
                ptt
            FROM coagulation_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step09 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step10_blood_diff(con):
    name = "10_blood_diff"
    if exists(name):
        log.info("step10 cached")
        return
    t0 = time.time()
    # Reference: measurement/blood_differential.sql
    # Build the official specimen-level blood differential concept first,
    # then project it onto ICU-hour windows for the wide table.
    con.execute(f"""
        COPY (
            WITH blood_diff AS (
                SELECT
                    MAX(le.subject_id) AS subject_id,
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid IN (51300, 51301, 51755) THEN le.valuenum END) AS wbc,
                    MAX(CASE WHEN le.itemid = 52069 THEN le.valuenum END) AS basophils_abs,
                    MAX(
                        CASE
                            WHEN le.itemid = 52073 THEN le.valuenum
                            WHEN le.itemid = 51199 THEN le.valuenum / 1000.0
                            ELSE NULL
                        END
                    ) AS eosinophils_abs,
                    MAX(
                        CASE
                            WHEN le.itemid = 51133 THEN le.valuenum
                            WHEN le.itemid = 52769 THEN le.valuenum / 1000.0
                            ELSE NULL
                        END
                    ) AS lymphocytes_abs,
                    MAX(
                        CASE
                            WHEN le.itemid = 52074 THEN le.valuenum
                            WHEN le.itemid = 51253 THEN le.valuenum / 1000.0
                            ELSE NULL
                        END
                    ) AS monocytes_abs,
                    MAX(CASE WHEN le.itemid = 52075 THEN le.valuenum END) AS neutrophils_abs,
                    MAX(CASE WHEN le.itemid = 51218 THEN le.valuenum / 1000.0 END) AS granulocytes_abs,
                    MAX(CASE WHEN le.itemid = 51146 THEN le.valuenum END) AS basophils,
                    MAX(CASE WHEN le.itemid = 51200 THEN le.valuenum END) AS eosinophils,
                    MAX(CASE WHEN le.itemid IN (51244, 51245) THEN le.valuenum END) AS lymphocytes,
                    MAX(CASE WHEN le.itemid = 51254 THEN le.valuenum END) AS monocytes,
                    MAX(CASE WHEN le.itemid = 51256 THEN le.valuenum END) AS neutrophils,
                    MAX(CASE WHEN le.itemid = 51143 THEN le.valuenum END) AS atypical_lymphocytes,
                    MAX(CASE WHEN le.itemid = 51144 THEN le.valuenum END) AS bands,
                    MAX(CASE WHEN le.itemid = 52135 THEN le.valuenum END) AS immature_granulocytes,
                    MAX(CASE WHEN le.itemid = 51251 THEN le.valuenum END) AS metamyelocytes,
                    MAX(CASE WHEN le.itemid = 51257 THEN le.valuenum END) AS nrbc,
                    CASE
                        WHEN MAX(CASE WHEN le.itemid IN (51300, 51301, 51755) THEN le.valuenum END) > 0
                         AND SUM(
                                CASE
                                    WHEN le.itemid IN (51146, 51200, 51244, 51245, 51254, 51256)
                                        THEN le.valuenum
                                    ELSE NULL
                                END
                             ) > 0
                            THEN 1
                        ELSE 0
                    END AS impute_abs
                FROM labevents le
                WHERE le.itemid IN (
                    51146, 52069, 51199, 51200, 52073,
                    51244, 51245, 51133, 52769,
                    51253, 51254, 52074,
                    51256, 52075,
                    51143, 51144, 51218, 52135, 51251, 51257,
                    51300, 51301, 51755
                )
                  AND le.valuenum IS NOT NULL
                  AND le.valuenum >= 0
                GROUP BY le.specimen_id
            ),
            blood_diff_final AS (
                SELECT
                    subject_id,
                    hadm_id,
                    charttime,
                    specimen_id,
                    wbc,
                    ROUND(
                        CASE
                            WHEN basophils_abs IS NULL AND basophils IS NOT NULL AND impute_abs = 1
                                THEN basophils * wbc / 100.0
                            ELSE basophils_abs
                        END,
                        4
                    ) AS basophils_abs,
                    ROUND(
                        CASE
                            WHEN eosinophils_abs IS NULL AND eosinophils IS NOT NULL AND impute_abs = 1
                                THEN eosinophils * wbc / 100.0
                            ELSE eosinophils_abs
                        END,
                        4
                    ) AS eosinophils_abs,
                    ROUND(
                        CASE
                            WHEN lymphocytes_abs IS NULL AND lymphocytes IS NOT NULL AND impute_abs = 1
                                THEN lymphocytes * wbc / 100.0
                            ELSE lymphocytes_abs
                        END,
                        4
                    ) AS lymphocytes_abs,
                    ROUND(
                        CASE
                            WHEN monocytes_abs IS NULL AND monocytes IS NOT NULL AND impute_abs = 1
                                THEN monocytes * wbc / 100.0
                            ELSE monocytes_abs
                        END,
                        4
                    ) AS monocytes_abs,
                    ROUND(
                        CASE
                            WHEN neutrophils_abs IS NULL AND neutrophils IS NOT NULL AND impute_abs = 1
                                THEN neutrophils * wbc / 100.0
                            ELSE neutrophils_abs
                        END,
                        4
                    ) AS neutrophils_abs,
                    basophils,
                    eosinophils,
                    lymphocytes,
                    monocytes,
                    neutrophils,
                    atypical_lymphocytes,
                    bands,
                    immature_granulocytes,
                    metamyelocytes,
                    nrbc
                FROM blood_diff
            ),
            blood_diff_hourly AS (
                WITH ranked AS (
                    SELECT
                        co.stay_id,
                        date_trunc('hour', bd.charttime) AS charttime_floor,
                        bd.charttime,
                        bd.wbc AS wbc_diff,
                        bd.basophils AS basophils_pct,
                        bd.basophils_abs,
                        bd.eosinophils AS eosinophils_pct,
                        bd.eosinophils_abs,
                        bd.lymphocytes AS lymphocytes_pct,
                        bd.lymphocytes_abs,
                        bd.monocytes AS monocytes_pct,
                        bd.monocytes_abs,
                        bd.neutrophils AS neutrophils_pct,
                        bd.neutrophils_abs,
                        bd.atypical_lymphocytes,
                        bd.bands,
                        bd.immature_granulocytes,
                        bd.metamyelocytes,
                        bd.nrbc,
                        ROW_NUMBER() OVER (
                            PARTITION BY co.stay_id, date_trunc('hour', bd.charttime)
                            ORDER BY bd.charttime DESC
                        ) AS hour_seq
                    FROM blood_diff_final bd
                    INNER JOIN cohort co
                        ON bd.subject_id = co.subject_id
                       AND bd.charttime >= co.intime - INTERVAL '24' HOUR
                       AND bd.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    wbc_diff,
                    neutrophils_pct,
                    neutrophils_abs,
                    lymphocytes_pct,
                    lymphocytes_abs,
                    monocytes_pct,
                    monocytes_abs,
                    eosinophils_pct,
                    eosinophils_abs,
                    basophils_pct,
                    basophils_abs,
                    bands,
                    immature_granulocytes,
                    atypical_lymphocytes,
                    metamyelocytes,
                    nrbc
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                wbc_diff,
                neutrophils_pct,
                neutrophils_abs,
                lymphocytes_pct,
                lymphocytes_abs,
                monocytes_pct,
                monocytes_abs,
                eosinophils_pct,
                eosinophils_abs,
                basophils_pct,
                basophils_abs,
                bands,
                immature_granulocytes,
                atypical_lymphocytes,
                metamyelocytes,
                nrbc
            FROM blood_diff_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step10 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step11_cardiac_crp(con):
    name = "11_cardiac_crp"
    if exists(name):
        log.info("step11 cached")
        return
    t0 = time.time()
    # Reference: measurement/cardiac_marker.sql + measurement/inflammation.sql
    # Build the official specimen-level marker concepts first, then project the
    # needed outputs onto ICU-hour rows for the wide table. troponin_i is absent
    # in MIMIC-IV and ck_mb remains populated from the separate enzyme block.
    con.execute(f"""
        COPY (
            WITH cardiac_marker AS (
                SELECT
                    MAX(le.subject_id) AS subject_id,
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 51003 THEN le.valuenum END) AS troponin_t,
                    MAX(CASE WHEN le.itemid = 50911 THEN le.valuenum END) AS ck_mb,
                    MAX(CASE WHEN le.itemid = 50963 THEN le.valuenum END) AS ntprobnp
                FROM labevents le
                WHERE le.itemid IN (51003, 50911, 50963)
                  AND le.valuenum IS NOT NULL
                GROUP BY le.specimen_id
            ),
            inflammation AS (
                SELECT
                    MAX(le.subject_id) AS subject_id,
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 50889 THEN le.valuenum END) AS crp
                FROM labevents le
                WHERE le.itemid IN (50889)
                  AND le.valuenum IS NOT NULL
                  AND le.valuenum > 0
                GROUP BY le.specimen_id
            ),
            cardiac_hourly AS (
                WITH ranked AS (
                    SELECT
                        co.stay_id,
                        date_trunc('hour', cm.charttime) AS charttime_floor,
                        cm.charttime,
                        cm.troponin_t,
                        cm.ntprobnp,
                        ROW_NUMBER() OVER (
                            PARTITION BY co.stay_id, date_trunc('hour', cm.charttime)
                            ORDER BY cm.charttime DESC, cm.specimen_id DESC
                        ) AS hour_seq
                    FROM cardiac_marker cm
                    INNER JOIN cohort co
                        ON cm.hadm_id = co.hadm_id
                       AND cm.charttime >= co.intime - INTERVAL '24' HOUR
                       AND cm.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    troponin_t,
                    ntprobnp
                FROM ranked
                WHERE hour_seq = 1
            ),
            inflammation_hourly AS (
                WITH ranked AS (
                    SELECT
                        co.stay_id,
                        date_trunc('hour', inf.charttime) AS charttime_floor,
                        inf.charttime,
                        inf.crp,
                        ROW_NUMBER() OVER (
                            PARTITION BY co.stay_id, date_trunc('hour', inf.charttime)
                            ORDER BY inf.charttime DESC, inf.specimen_id DESC
                        ) AS hour_seq
                    FROM inflammation inf
                    INNER JOIN cohort co
                        ON inf.hadm_id = co.hadm_id
                       AND inf.charttime >= co.intime - INTERVAL '24' HOUR
                       AND inf.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    crp
                FROM ranked
                WHERE hour_seq = 1
            )
            SELECT
                COALESCE(ch.stay_id, ih.stay_id) AS stay_id,
                COALESCE(ch.charttime_floor, ih.charttime_floor) AS charttime_floor,
                ch.troponin_t,
                ch.ntprobnp,
                ih.crp
            FROM cardiac_hourly ch
            FULL OUTER JOIN inflammation_hourly ih
                ON ch.stay_id = ih.stay_id
               AND ch.charttime_floor = ih.charttime_floor
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step11 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step05_bg(con):
    name = "05_bg"
    if exists(name):
        log.info("step05 cached")
        return
    t0 = time.time()
    # Reference: measurement/bg.sql
    # Port the official event-level blood-gas pairing chain first, then collapse to
    # the ICU hour for the final wide table.
    con.execute(f"""
        COPY (
            WITH bg AS (
                SELECT
                    MAX(le.subject_id) AS subject_id,
                    MAX(le.hadm_id) AS hadm_id,
                    MAX(le.charttime) AS charttime,
                    le.specimen_id,
                    MAX(CASE WHEN le.itemid = 52033 THEN le.value END) AS specimen,
                    MAX(CASE WHEN le.itemid = 50801 THEN le.valuenum END) AS aado2,
                    MAX(CASE WHEN le.itemid = 50802 THEN le.valuenum END) AS baseexcess,
                    MAX(CASE WHEN le.itemid = 50803 THEN le.valuenum END) AS bicarbonate_bg,
                    MAX(CASE WHEN le.itemid = 50804 THEN le.valuenum END) AS totalco2_bg,
                    MAX(CASE WHEN le.itemid = 50805 THEN le.valuenum END) AS carboxyhemoglobin,
                    MAX(CASE WHEN le.itemid = 50806 THEN le.valuenum END) AS chloride_bg,
                    MAX(CASE WHEN le.itemid = 50808 THEN le.valuenum END) AS calcium_ionized,
                    MAX(CASE WHEN le.itemid = 50809 AND le.valuenum <= 10000 THEN le.valuenum END) AS glucose_bg,
                    MAX(CASE WHEN le.itemid = 50810 AND le.valuenum <= 100 THEN le.valuenum END) AS hematocrit_bg,
                    MAX(CASE WHEN le.itemid = 50811 THEN le.valuenum END) AS hemoglobin_bg,
                    MAX(CASE WHEN le.itemid = 50813 AND le.valuenum <= 10000 THEN le.valuenum END) AS lactate,
                    MAX(CASE WHEN le.itemid = 50814 THEN le.valuenum END) AS methemoglobin,
                    MAX(CASE WHEN le.itemid = 50815 THEN le.valuenum END) AS o2flow,
                    MAX(
                        CASE
                            WHEN le.itemid = 50816 AND le.valuenum > 20 AND le.valuenum <= 100 THEN le.valuenum
                            WHEN le.itemid = 50816 AND le.valuenum > 0.2 AND le.valuenum <= 1.0 THEN le.valuenum * 100.0
                            ELSE NULL
                        END
                    ) AS fio2_bg,
                    MAX(CASE WHEN le.itemid = 50817 AND le.valuenum <= 100 THEN le.valuenum END) AS so2,
                    MAX(CASE WHEN le.itemid = 50818 THEN le.valuenum END) AS pco2,
                    MAX(CASE WHEN le.itemid = 50819 THEN le.valuenum END) AS peep,
                    MAX(CASE WHEN le.itemid = 50820 THEN le.valuenum END) AS ph,
                    MAX(CASE WHEN le.itemid = 50821 THEN le.valuenum END) AS po2,
                    MAX(CASE WHEN le.itemid = 50822 THEN le.valuenum END) AS potassium_bg,
                    MAX(CASE WHEN le.itemid = 50823 THEN le.valuenum END) AS requiredo2,
                    MAX(CASE WHEN le.itemid = 50824 THEN le.valuenum END) AS sodium_bg,
                    MAX(CASE WHEN le.itemid = 50825 THEN le.valuenum END) AS temperature_bg
                FROM labevents le
                WHERE le.itemid IN (
                    52033, 50801, 50802, 50803, 50804, 50805, 50806, 50808,
                    50809, 50810, 50811, 50813, 50814, 50815, 50816, 50817,
                    50818, 50819, 50820, 50821, 50822, 50823, 50824, 50825
                )
                GROUP BY le.specimen_id
            ),
            stg_spo2 AS (
                SELECT
                    ce.subject_id,
                    ce.charttime,
                    AVG(ce.valuenum) AS spo2_bg
                FROM chartevents ce
                WHERE ce.itemid = 220277
                  AND ce.valuenum > 0
                  AND ce.valuenum <= 100
                GROUP BY ce.subject_id, ce.charttime
            ),
            fio2_ce AS (
                SELECT
                    ce.subject_id,
                    ce.charttime,
                    MAX(
                        CASE
                            WHEN ce.valuenum > 0.2 AND ce.valuenum <= 1 THEN ce.valuenum * 100
                            WHEN ce.valuenum > 1 AND ce.valuenum < 20 THEN NULL
                            WHEN ce.valuenum >= 20 AND ce.valuenum <= 100 THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS fio2_chartevents
                FROM chartevents ce
                WHERE ce.itemid = 223835
                  AND ce.valuenum > 0
                  AND ce.valuenum <= 100
                GROUP BY ce.subject_id, ce.charttime
            ),
            stg2 AS (
                SELECT
                    bg.*,
                    spo2.spo2_bg,
                    ROW_NUMBER() OVER (
                        PARTITION BY bg.subject_id, bg.charttime
                        ORDER BY spo2.charttime DESC NULLS LAST
                    ) AS lastrowspo2
                FROM bg
                LEFT JOIN stg_spo2 spo2
                    ON bg.subject_id = spo2.subject_id
                   AND spo2.charttime BETWEEN bg.charttime - INTERVAL 2 HOUR AND bg.charttime
                WHERE bg.po2 IS NOT NULL
            ),
            stg3 AS (
                SELECT
                    bg.*,
                    fio2.fio2_chartevents,
                    ROW_NUMBER() OVER (
                        PARTITION BY bg.subject_id, bg.charttime
                        ORDER BY fio2.charttime DESC NULLS LAST
                    ) AS lastrowfio2
                FROM stg2 bg
                LEFT JOIN fio2_ce fio2
                    ON bg.subject_id = fio2.subject_id
                   AND fio2.charttime >= bg.charttime - INTERVAL 4 HOUR
                   AND fio2.charttime <= bg.charttime
                   AND fio2.fio2_chartevents > 0
                WHERE bg.lastrowspo2 = 1
            ),
            bg_event AS (
                SELECT
                    subject_id,
                    hadm_id,
                    charttime,
                    specimen,
                    so2,
                    spo2_bg,
                    po2,
                    pco2,
                    fio2_chartevents,
                    fio2_bg,
                    aado2,
                    CASE
                        WHEN po2 IS NULL OR pco2 IS NULL THEN NULL
                        WHEN fio2_bg IS NOT NULL THEN (fio2_bg / 100.0) * (760 - 47) - (pco2 / 0.8) - po2
                        WHEN fio2_chartevents IS NOT NULL THEN (fio2_chartevents / 100.0) * (760 - 47) - (pco2 / 0.8) - po2
                        ELSE NULL
                    END AS aado2_calc,
                    CASE
                        WHEN po2 IS NULL THEN NULL
                        WHEN fio2_bg IS NOT NULL THEN 100.0 * po2 / fio2_bg
                        WHEN fio2_chartevents IS NOT NULL THEN 100.0 * po2 / fio2_chartevents
                        ELSE NULL
                    END AS pao2fio2ratio,
                    ph,
                    baseexcess,
                    bicarbonate_bg,
                    totalco2_bg,
                    hematocrit_bg,
                    hemoglobin_bg,
                    carboxyhemoglobin,
                    methemoglobin,
                    chloride_bg,
                    calcium_ionized,
                    temperature_bg,
                    potassium_bg,
                    sodium_bg,
                    lactate,
                    glucose_bg,
                    peep,
                    o2flow,
                    requiredo2
                FROM stg3
                WHERE lastrowfio2 = 1
            ),
            bg_hourly AS (
                WITH bg_ranked AS (
                    SELECT
                        co.stay_id,
                        date_trunc('hour', bg.charttime) AS charttime_floor,
                        bg.charttime,
                        CASE WHEN bg.specimen = 'ART.' THEN 1 ELSE 0 END AS arterial_bg_flag,
                    bg.aado2,
                    bg.aado2_calc,
                    bg.pao2fio2ratio,
                    CASE WHEN bg.specimen = 'ART.' THEN bg.pao2fio2ratio END AS pao2fio2ratio_art,
                    COALESCE(bg.fio2_chartevents, bg.fio2_bg) AS fio2,
                    bg.fio2_chartevents,
                    bg.specimen AS bg_specimen,
                    bg.baseexcess,
                    bg.bicarbonate_bg,
                    bg.totalco2_bg,
                    bg.hematocrit_bg,
                    bg.hemoglobin_bg,
                    bg.carboxyhemoglobin,
                    bg.calcium_ionized,
                    bg.chloride_bg,
                    bg.lactate,
                    bg.methemoglobin,
                    bg.o2flow,
                    bg.peep,
                    bg.requiredo2,
                    bg.temperature_bg,
                    bg.potassium_bg,
                    bg.sodium_bg,
                    bg.so2,
                    bg.pco2,
                    bg.ph,
                    bg.po2,
                    bg.glucose_bg,
                    ROW_NUMBER() OVER (
                        PARTITION BY co.stay_id, date_trunc('hour', bg.charttime)
                        ORDER BY
                                CASE WHEN bg.specimen = 'ART.' THEN 1 ELSE 0 END DESC,
                                bg.charttime DESC
                        ) AS hour_seq
                    FROM bg_event bg
                    INNER JOIN cohort co
                        ON bg.subject_id = co.subject_id
                       AND bg.charttime >= co.intime - INTERVAL '24' HOUR
                       AND bg.charttime <= co.outtime
                )
                SELECT
                    stay_id,
                    charttime_floor,
                    arterial_bg_flag,
                    aado2,
                    aado2_calc,
                    pao2fio2ratio,
                    pao2fio2ratio_art,
                    fio2,
                    fio2_chartevents,
                    bg_specimen,
                    baseexcess,
                    bicarbonate_bg,
                    totalco2_bg,
                    hematocrit_bg,
                    hemoglobin_bg,
                    carboxyhemoglobin,
                    calcium_ionized,
                    chloride_bg,
                    lactate,
                    methemoglobin,
                    o2flow,
                    peep,
                    requiredo2,
                    temperature_bg,
                    potassium_bg,
                    sodium_bg,
                    so2,
                    pco2,
                    ph,
                    po2,
                    glucose_bg
                FROM bg_ranked
                WHERE hour_seq = 1
            )
            SELECT
                stay_id,
                charttime_floor,
                arterial_bg_flag,
                aado2,
                aado2_calc,
                pao2fio2ratio,
                pao2fio2ratio_art,
                fio2,
                fio2_chartevents,
                bg_specimen,
                baseexcess,
                bicarbonate_bg,
                totalco2_bg,
                hematocrit_bg,
                hemoglobin_bg,
                carboxyhemoglobin,
                calcium_ionized,
                chloride_bg,
                lactate,
                methemoglobin,
                o2flow,
                peep,
                requiredo2,
                temperature_bg,
                potassium_bg,
                sodium_bg,
                so2,
                pco2,
                ph,
                po2,
                glucose_bg
            FROM bg_hourly
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step05 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step04_gcs(con):
    name = "04_gcs"
    if exists(name):
        log.info("step04 cached")
        return
    t0 = time.time()
    # Reference: measurement/gcs.sql
    # Port the official 6-hour carry-forward behavior and intubated verbal rule,
    # then collapse to one representative row per ICU hour.
    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT
                    ce.subject_id,
                    ce.stay_id,
                    ce.charttime,
                    MAX(CASE WHEN ce.itemid = 223901 THEN ce.valuenum END) AS gcs_motor,
                    MAX(
                        CASE
                            WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 0
                            WHEN ce.itemid = 223900 THEN ce.valuenum
                            ELSE NULL
                        END
                    ) AS gcs_verbal,
                    MAX(CASE WHEN ce.itemid = 220739 THEN ce.valuenum END) AS gcs_eyes,
                    MAX(CASE WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 1 ELSE 0 END) AS gcs_unable,
                    ROW_NUMBER() OVER (
                        PARTITION BY ce.stay_id
                        ORDER BY ce.charttime ASC
                    ) AS rn
                FROM chartevents ce
                INNER JOIN cohort co ON ce.stay_id = co.stay_id
                WHERE ce.itemid IN (223901, 223900, 220739)
                GROUP BY ce.subject_id, ce.stay_id, ce.charttime
            ),
            gcs_prev AS (
                SELECT
                    b.*,
                    b2.gcs_verbal AS gcs_verbal_prev,
                    b2.gcs_motor AS gcs_motor_prev,
                    b2.gcs_eyes AS gcs_eyes_prev,
                    CASE
                        WHEN b.gcs_verbal = 0 THEN 15
                        WHEN b.gcs_verbal IS NULL AND b2.gcs_verbal = 0 THEN 15
                        WHEN b2.gcs_verbal = 0 THEN
                            COALESCE(b.gcs_motor, 6) + COALESCE(b.gcs_verbal, 5) + COALESCE(b.gcs_eyes, 4)
                        ELSE
                            COALESCE(b.gcs_motor, COALESCE(b2.gcs_motor, 6))
                            + COALESCE(b.gcs_verbal, COALESCE(b2.gcs_verbal, 5))
                            + COALESCE(b.gcs_eyes, COALESCE(b2.gcs_eyes, 4))
                    END AS gcs_total
                FROM base b
                LEFT JOIN base b2
                    ON b.stay_id = b2.stay_id
                   AND b.rn = b2.rn + 1
                   AND b2.charttime > b.charttime - INTERVAL 6 HOUR
            ),
            gcs_stg AS (
                SELECT
                    subject_id,
                    stay_id,
                    charttime,
                    COALESCE(gcs_motor, gcs_motor_prev) AS gcs_motor,
                    COALESCE(gcs_verbal, gcs_verbal_prev) AS gcs_verbal,
                    COALESCE(gcs_eyes, gcs_eyes_prev) AS gcs_eyes,
                    gcs_unable,
                    gcs_total
                FROM gcs_prev
            ),
            hourly_ranked AS (
                SELECT
                    stay_id,
                    date_trunc('hour', charttime) AS charttime_floor,
                    gcs_motor,
                    gcs_verbal,
                    gcs_eyes,
                    gcs_unable,
                    gcs_total,
                    ROW_NUMBER() OVER (
                        PARTITION BY stay_id, date_trunc('hour', charttime)
                        ORDER BY gcs_total ASC NULLS LAST, charttime DESC
                    ) AS rn
                FROM gcs_stg
            )
            SELECT
                stay_id,
                charttime_floor,
                gcs_motor,
                gcs_verbal,
                gcs_eyes,
                gcs_unable,
                gcs_total
            FROM hourly_ranked
            WHERE rn = 1
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step04 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def main():
    os.makedirs(INTER_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    os.makedirs(DUCKDB_TEMP_DIR, exist_ok=True)

    con = duckdb.connect(DB_PATH)
    con.execute(f"PRAGMA temp_directory='{DUCKDB_TEMP_DIR.replace(chr(92), '/')}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET threads = 1")
    con.execute("SET memory_limit = '8GB'")
    register_views(con)

    step01_cohort(con)
    step01b_icustay_times(con)
    step02_time_axis(con)
    step03_vitals(con)
    step03b_icp(con)
    step03c_oxygen_delivery(con)
    step03d_rhythm(con)
    step03e_ventilator_setting(con)
    step03f_code_status(con)
    step03g_invasive_line(con)
    step03h_rrt(con)
    step04_gcs(con)
    step05_bg(con)
    step06_chemistry(con)
    step07_cbc(con)
    step08_enzyme(con)
    step09_coagulation(con)
    step10_blood_diff(con)
    step11_cardiac_crp(con)
    step12_uo(con)
    step13_vaso(con)
    step14_vent(con)
    step15_crrt(con)
    step16_antibiotic(con)
    step17_height_weight(con)
    step18_charlson(con)
    step19_service(con)
    step20_sofa(con)
    step21_sepsis(con)
    step22_final(con)

    con.close()
    log.info("done")


if __name__ == "__main__":
    main()
