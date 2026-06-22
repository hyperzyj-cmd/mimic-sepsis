"""
Build MIMIC-IV wide table: rows = stay_id x HOUR, columns = clinical variables.
SepsisLabel follows mimic-code official Sepsis-3 definition (SOFA absolute >= 2).

References:
  - MIT-LCP/mimic-code mimic-iv/concepts/
  - MIMIC-IV v3.1 official documentation (physionet.org/content/mimiciv/3.1)

Output: D:/ESILV_S2/Intern/build_mimic/mimiciv/output/mimic4_wide.parquet
"""

import os
import time
import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HOSP_DIR  = r"D:\ESILV_S2\mimic\raw\physionet.org\files\mimiciv\3.1\hosp"
ICU_DIR   = r"D:\ESILV_S2\mimic\raw\physionet.org\files\mimiciv\3.1\icu"
INTER_DIR = r"D:\ESILV_S2\Intern\build_mimic\mimiciv\intermediate\mimiciv"
DB_PATH   = r"D:\ESILV_S2\Intern\build_mimic\mimiciv\output\mimic4_build.duckdb"
OUT_PATH  = r"D:\ESILV_S2\Intern\build_mimic\mimiciv\output\mimic4_wide.parquet"

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
                "prescriptions", "diagnoses_icd", "services", "omr"]:
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
    # Reference: demographics/icustay_detail.sql
    # Add key official icustay_detail-style context fields so downstream wide rows
    # retain admission/stay sequencing metadata rather than only core identifiers.
    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT
                    ie.subject_id,
                    ie.hadm_id,
                    ie.stay_id,
                    ie.intime,
                    ie.outtime,
                    ie.first_careunit,
                    ie.last_careunit,
                    p.gender,
                    p.dod,
                    p.anchor_year_group,
                    a.admittime,
                    a.dischtime,
                    a.race,
                    a.hospital_expire_flag,
                    p.anchor_age + (YEAR(a.admittime) - p.anchor_year) AS age,
                    ROUND(date_diff('minute', a.admittime, ie.intime) / 60.0, 2) AS hospadmtime,
                    ROUND(date_diff('minute', a.admittime, a.dischtime) / 60.0 / 24.0, 2) AS los_hospital,
                    ROUND(date_diff('minute', ie.intime, ie.outtime) / 60.0 / 24.0, 2) AS los_icu
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
                base.first_careunit,
                base.last_careunit,
                base.age,
                base.gender,
                base.dod,
                base.anchor_year_group,
                base.admittime,
                base.dischtime,
                base.race,
                base.hospital_expire_flag,
                base.hospadmtime,
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


def step02_time_axis(con):
    name = "02_time_axis"
    if exists(name):
        log.info("step02 cached")
        return
    t0 = time.time()
    # Reference: demographics/icustay_hourly.sql
    # Preserve true ICU-relative hourly windows for downstream concept alignment.
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"""
        COPY (
            SELECT
                c.stay_id,
                CAST(hr.generate_series AS INTEGER) AS hr,
                c.intime + INTERVAL (CAST(hr.generate_series AS INTEGER)) HOUR AS starttime,
                c.intime + INTERVAL (CAST(hr.generate_series AS INTEGER) + 1) HOUR AS endtime,
                date_trunc('hour', c.intime + INTERVAL (CAST(hr.generate_series AS INTEGER)) HOUR) AS charttime_floor
            FROM cohort c,
                 generate_series(-24, date_diff('hour', c.intime, c.outtime)) AS hr
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
            WITH ce AS (
                SELECT
                    ce.stay_id,
                    date_trunc('hour', ce.charttime) AS charttime_floor,
                    CASE WHEN ce.itemid = 220045
                         AND ce.valuenum BETWEEN 0 AND 300
                         THEN ce.valuenum END AS heart_rate,
                    CASE WHEN ce.itemid = 220179
                         AND ce.valuenum BETWEEN 0 AND 400
                         THEN ce.valuenum END AS sbp_ni,
                    CASE WHEN ce.itemid = 220180
                         AND ce.valuenum BETWEEN 0 AND 300
                         THEN ce.valuenum END AS dbp_ni,
                    CASE WHEN ce.itemid = 220181
                         AND ce.valuenum BETWEEN 0 AND 300
                         THEN ce.valuenum END AS mbp_ni,
                    CASE WHEN ce.itemid = 220050
                         AND ce.valuenum BETWEEN 0 AND 400
                         THEN ce.valuenum END AS sbp_art,
                    CASE WHEN ce.itemid = 220051
                         AND ce.valuenum BETWEEN 0 AND 300
                         THEN ce.valuenum END AS dbp_art,
                    CASE WHEN ce.itemid = 220052
                         AND ce.valuenum BETWEEN 0 AND 300
                         THEN ce.valuenum END AS mbp_art,
                    CASE WHEN ce.itemid = 225309
                         AND ce.valuenum BETWEEN 0 AND 400
                         THEN ce.valuenum END AS sbp_ni2,
                    CASE WHEN ce.itemid = 225310
                         AND ce.valuenum BETWEEN 0 AND 300
                         THEN ce.valuenum END AS dbp_ni2,
                    CASE WHEN ce.itemid = 225312
                         AND ce.valuenum BETWEEN 0 AND 300
                         THEN ce.valuenum END AS mbp_ni2,
                    CASE WHEN ce.itemid IN (220210, 224690)
                         AND ce.valuenum BETWEEN 0 AND 70
                         THEN ce.valuenum END AS resp_rate,
                    CASE WHEN ce.itemid = 223761
                         AND ce.valuenum BETWEEN 70 AND 120
                         THEN (ce.valuenum - 32) / 1.8 END AS temperature_f,
                    CASE WHEN ce.itemid = 223762
                         AND ce.valuenum BETWEEN 10 AND 50
                         THEN ce.valuenum END AS temperature_c,
                    CASE WHEN ce.itemid = 220277
                         AND ce.valuenum BETWEEN 0 AND 100
                         THEN ce.valuenum END AS spo2,
                    CASE WHEN ce.itemid IN (225664, 220621, 226537)
                         AND ce.valuenum BETWEEN 0 AND 10000
                         THEN ce.valuenum END AS glucose_vital
                FROM chartevents ce
                INNER JOIN cohort co ON ce.stay_id = co.stay_id
                WHERE ce.itemid IN (
                    220045,
                    220179, 220180, 220181,
                    220050, 220051, 220052,
                    225309, 225310, 225312,
                    220210, 224690,
                    223761, 223762,
                    220277,
                    225664, 220621, 226537
                )
            )
            SELECT
                stay_id,
                charttime_floor,
                AVG(heart_rate)                                       AS heart_rate,
                AVG(COALESCE(sbp_art, sbp_ni, sbp_ni2))              AS sbp,
                AVG(COALESCE(dbp_art, dbp_ni, dbp_ni2))              AS dbp,
                AVG(COALESCE(mbp_art, mbp_ni, mbp_ni2))              AS mbp,
                AVG(COALESCE(sbp_ni, sbp_ni2))                       AS sbp_ni,
                AVG(COALESCE(dbp_ni, dbp_ni2))                       AS dbp_ni,
                AVG(COALESCE(mbp_ni, mbp_ni2))                       AS mbp_ni,
                AVG(resp_rate)                                        AS resp_rate,
                AVG(COALESCE(temperature_c, temperature_f))           AS temperature,
                AVG(spo2)                                             AS spo2,
                AVG(glucose_vital)                                    AS glucose_vital
            FROM ce
            GROUP BY stay_id, charttime_floor
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM read_parquet('{inter(name)}')").fetchone()
    log.info("step03 done %.1fs  rows=%d  stays=%d", time.time() - t0, r[0], r[1])


def step12_uo(con):
    name = "12_uo"
    if exists(name):
        log.info("step12 cached")
        return
    t0 = time.time()
    # Reference: measurement/urine_output_rate.sql
    # Port the official event-level urine-output-rate concept first, then collapse
    # to the ICU hour for the wide table.
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"""
        COPY (
            WITH uo_raw AS (
                SELECT
                    oe.stay_id,
                    oe.charttime,
                    CASE
                        WHEN oe.itemid = 227488 THEN -oe.value
                        ELSE oe.value
                    END AS urineoutput
                FROM outputevents oe
                INNER JOIN cohort co ON oe.stay_id = co.stay_id
                WHERE oe.itemid IN (
                    226559, 226560, 226561, 226584, 226563, 226564,
                    226565, 226567, 226557, 226558, 226571, 227488, 227489
                )
                  AND oe.value IS NOT NULL
            ),
            tm AS (
                SELECT
                    co.stay_id,
                    co.intime AS intime_hr,
                    co.outtime AS outtime_hr
                FROM cohort co
            ),
            uo_tm AS (
                SELECT
                    tm.stay_id,
                    CASE
                        WHEN LAG(ur.charttime) OVER w IS NULL
                            THEN date_diff('minute', tm.intime_hr, ur.charttime)
                        ELSE date_diff('minute', LAG(ur.charttime) OVER w, ur.charttime)
                    END AS tm_since_last_uo,
                    ur.charttime,
                    ur.urineoutput
                FROM tm
                INNER JOIN uo_raw ur
                    ON tm.stay_id = ur.stay_id
                WINDOW w AS (
                    PARTITION BY tm.stay_id
                    ORDER BY ur.charttime
                )
            ),
            ur_stg AS (
                SELECT
                    io.stay_id,
                    io.charttime,
                    SUM(io.urineoutput) AS uo,
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
                    ce.valuenum AS weight
                FROM chartevents ce
                INNER JOIN cohort co
                    ON ce.stay_id = co.stay_id
                WHERE ce.itemid IN (224639, 226512)
                  AND ce.valuenum > 0
            ),
            wt_intervals AS (
                SELECT
                    stay_id,
                    charttime AS starttime,
                    COALESCE(
                        LEAD(charttime) OVER (PARTITION BY stay_id ORDER BY charttime),
                        (SELECT outtime FROM cohort c WHERE c.stay_id = wt_raw.stay_id) + INTERVAL 2 HOUR
                    ) AS endtime,
                    weight
                FROM wt_raw
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
                LEFT JOIN wt_intervals wd
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
                   AND hl.charttime >= co.intime
                   AND hl.charttime <= co.outtime
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
    con.execute(f"""
        COPY (
            WITH norepinephrine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    CASE
                        WHEN ie.rateuom = 'mg/kg/min' AND ie.patientweight = 1 THEN ie.rate
                        WHEN ie.rateuom = 'mg/kg/min' THEN ie.rate * 1000.0
                        ELSE ie.rate
                    END AS norepi_rate
                FROM inputevents ie
                INNER JOIN cohort co ON ie.stay_id = co.stay_id
                WHERE ie.itemid = 221906
                  AND ie.rate IS NOT NULL
                  AND ie.rate > 0
            ),
            epinephrine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    ie.rate AS epi_rate
                FROM inputevents ie
                INNER JOIN cohort co ON ie.stay_id = co.stay_id
                WHERE ie.itemid = 221289
                  AND ie.rate IS NOT NULL
                  AND ie.rate > 0
            ),
            dopamine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    ie.rate AS dopa_rate
                FROM inputevents ie
                INNER JOIN cohort co ON ie.stay_id = co.stay_id
                WHERE ie.itemid = 221662
                  AND ie.rate IS NOT NULL
                  AND ie.rate > 0
            ),
            dobutamine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    ie.rate AS dobu_rate
                FROM inputevents ie
                INNER JOIN cohort co ON ie.stay_id = co.stay_id
                WHERE ie.itemid = 221653
                  AND ie.rate IS NOT NULL
                  AND ie.rate > 0
            ),
            phenylephrine AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    CASE
                        WHEN ie.rateuom = 'mcg/min' AND ie.patientweight > 0
                            THEN ie.rate / ie.patientweight
                        ELSE ie.rate
                    END AS phenyl_rate
                FROM inputevents ie
                INNER JOIN cohort co ON ie.stay_id = co.stay_id
                WHERE ie.itemid = 221749
                  AND ie.rate IS NOT NULL
                  AND ie.rate > 0
            ),
            vasopressin AS (
                SELECT
                    ie.stay_id,
                    ie.linkorderid,
                    ie.starttime,
                    ie.endtime,
                    CASE
                        WHEN ie.rateuom = 'units/min' THEN ie.rate * 60.0
                        ELSE ie.rate
                    END AS vaso_rate
                FROM inputevents ie
                INNER JOIN cohort co ON ie.stay_id = co.stay_id
                WHERE ie.itemid = 222315
                  AND ie.rate IS NOT NULL
                  AND ie.rate > 0
            ),
            vaso_raw AS (
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    norepi_rate,
                    NULL::DOUBLE AS epi_rate,
                    NULL::DOUBLE AS dopa_rate,
                    NULL::DOUBLE AS dobu_rate,
                    NULL::DOUBLE AS phenyl_rate,
                    NULL::DOUBLE AS vaso_rate
                FROM norepinephrine
                UNION ALL
                SELECT
                    stay_id,
                    starttime,
                    endtime,
                    NULL::DOUBLE,
                    epi_rate,
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
                    dopa_rate,
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
                    dobu_rate,
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
                    phenyl_rate,
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
                    vaso_rate
                FROM vasopressin
            )
            SELECT
                ta.stay_id,
                ta.charttime_floor,
                MAX(vr.norepi_rate) AS norepi_rate,
                MAX(vr.epi_rate)    AS epi_rate,
                MAX(vr.dopa_rate)   AS dopa_rate,
                MAX(vr.dobu_rate)   AS dobu_rate,
                MAX(vr.phenyl_rate) AS phenyl_rate,
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
    con.execute(f"""
        COPY (
            WITH oxygen_delivery AS (
                SELECT
                    ce.stay_id,
                    ce.charttime,
                    MAX(CASE WHEN ce.itemid = 226732 THEN ce.value END) AS o2_delivery_device_1
                FROM chartevents ce
                INNER JOIN cohort co ON ce.stay_id = co.stay_id
                WHERE ce.itemid = 226732
                  AND ce.value IS NOT NULL
                GROUP BY ce.stay_id, ce.charttime
            ),
            ventilator_setting AS (
                SELECT
                    ce.stay_id,
                    ce.charttime,
                    MAX(CASE WHEN ce.itemid = 223849 THEN ce.value END) AS ventilator_mode,
                    MAX(CASE WHEN ce.itemid = 229314 THEN ce.value END) AS ventilator_mode_hamilton
                FROM chartevents ce
                INNER JOIN cohort co ON ce.stay_id = co.stay_id
                WHERE ce.itemid IN (223849, 229314)
                  AND ce.value IS NOT NULL
                GROUP BY ce.stay_id, ce.charttime
            ),
            tm AS (
                SELECT stay_id, charttime FROM ventilator_setting
                UNION DISTINCT
                SELECT stay_id, charttime FROM oxygen_delivery
            ),
            vs AS (
                SELECT
                    tm.stay_id,
                    tm.charttime,
                    od.o2_delivery_device_1,
                    COALESCE(vs.ventilator_mode, vs.ventilator_mode_hamilton) AS vent_mode,
                    CASE
                        WHEN od.o2_delivery_device_1 IN ('Tracheostomy tube', 'Trach mask ') THEN 'Tracheostomy'
                        WHEN od.o2_delivery_device_1 IN ('Endotracheal tube')
                             OR vs.ventilator_mode IN (
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
                             OR vs.ventilator_mode_hamilton IN (
                                'APRV','APV (cmv)','Ambient','(S) CMV',
                                'P-CMV','SIMV','APV (simv)','P-SIMV','VS','ASV'
                             ) THEN 'InvasiveVent'
                        WHEN od.o2_delivery_device_1 IN ('Bipap mask ', 'CPAP mask ')
                             OR vs.ventilator_mode_hamilton IN ('DuoPaP', 'NIV', 'NIV-ST')
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
                LEFT JOIN ventilator_setting vs
                    ON tm.stay_id = vs.stay_id
                   AND tm.charttime = vs.charttime
                LEFT JOIN oxygen_delivery od
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
                    ventilation_status,
                    charttime,
                    charttime_lead,
                    CASE ventilation_status
                        WHEN 'Tracheostomy'       THEN 6
                        WHEN 'InvasiveVent'       THEN 5
                        WHEN 'NonInvasiveVent'    THEN 4
                        WHEN 'HFNC'               THEN 3
                        WHEN 'SupplementalOxygen' THEN 2
                        WHEN 'None'               THEN 1
                        ELSE 0
                    END AS priority,
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
                    priority,
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
                    MAX(ventilation_status) AS ventilation_status,
                    MAX(priority) AS priority
                FROM vd2
                GROUP BY stay_id, vent_seq
                HAVING MIN(charttime) != MAX(charttime)
            )
            SELECT
                ta.stay_id,
                ta.charttime_floor,
                -- highest-priority status active during this hour
                MAX(CASE vi.priority
                    WHEN 6 THEN 'Tracheostomy'
                    WHEN 5 THEN 'InvasiveVent'
                    WHEN 4 THEN 'NonInvasiveVent'
                    WHEN 3 THEN 'HFNC'
                    WHEN 2 THEN 'SupplementalOxygen'
                    WHEN 1 THEN 'None'
                    END
                    ORDER BY vi.priority DESC
                ) AS ventilation_status
            FROM time_axis ta
            INNER JOIN vent_intervals vi
                ON ta.stay_id = vi.stay_id
               AND ta.starttime < vi.endtime
               AND ta.endtime > vi.starttime
            GROUP BY ta.stay_id, ta.charttime_floor
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
                    CASE WHEN ce.itemid = 225183 THEN ce.valuenum END AS current_goal,
                    CASE WHEN ce.itemid = 224144 THEN ce.valuenum END AS blood_flow,
                    CASE WHEN ce.itemid = 224154 THEN ce.valuenum END AS dialysate_rate,
                    CASE WHEN ce.itemid = 224153 THEN ce.valuenum END AS replacement_rate,
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
                    227290, 224146, 225183, 224144, 224154, 224153, 226457
                )
                  AND ce.value IS NOT NULL
            )
            SELECT
                stay_id,
                date_trunc('hour', charttime) AS charttime_floor,
                MAX(crrt_mode) AS crrt_mode,
                MAX(current_goal) AS current_goal,
                MAX(blood_flow) AS blood_flow,
                MAX(dialysate_rate) AS dialysate_rate,
                MAX(replacement_rate) AS replacement_rate,
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
                WHERE COALESCE(pr.drug_type, '') <> 'BASE'
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
                    COALESCE(pr.stoptime, pr.starttime + INTERVAL 1 DAY) AS stoptime
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
    # Reference: demographics/height.sql + demographics/weight_durations.sql
    # Height: chartevents 226730 (cm) or OMR 'Height' / 'Height (Cm)' rows
    # Weight: chartevents 224639 (daily), 226512 (admit) — forward-filled per stay
    con.execute(f"""
        COPY (
            WITH ht_ce AS (
                SELECT stay_id,
                       AVG(CASE WHEN valuenum BETWEEN 100 AND 250 THEN valuenum END) AS height_cm
                FROM chartevents
                WHERE itemid = 226730
                GROUP BY stay_id
            ),
            ht_omr AS (
                SELECT co.stay_id,
                       AVG(CASE WHEN LOWER(o.result_name) IN ('height','height (cm)')
                                 AND TRY_CAST(o.result_value AS DOUBLE) BETWEEN 100 AND 250
                                THEN TRY_CAST(o.result_value AS DOUBLE) END) AS height_cm
                FROM omr o
                INNER JOIN cohort co ON o.subject_id = co.subject_id
                GROUP BY co.stay_id
            ),
            height AS (
                SELECT stay_id,
                       COALESCE(h.height_cm, ho.height_cm) AS height
                FROM cohort co
                LEFT JOIN ht_ce h  USING (stay_id)
                LEFT JOIN ht_omr ho USING (stay_id)
            ),
            wt_raw AS (
                SELECT
                    ce.stay_id,
                    ce.charttime,
                    CASE WHEN ce.itemid = 226512 THEN 'admit' ELSE 'daily' END AS wt_type,
                    ce.valuenum AS weight
                FROM chartevents ce
                WHERE ce.itemid IN (224639, 226512)
                  AND ce.valuenum BETWEEN 1 AND 1000
            ),
            wt_intervals AS (
                SELECT
                    stay_id,
                    charttime AS starttime,
                    wt_type,
                    COALESCE(
                        LEAD(charttime) OVER (PARTITION BY stay_id ORDER BY charttime),
                        (SELECT outtime FROM cohort c WHERE c.stay_id = wt_raw.stay_id) + INTERVAL 2 HOUR
                    ) AS endtime,
                    weight
                FROM wt_raw
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
            LEFT JOIN height ht ON ta.stay_id = ht.stay_id
            LEFT JOIN wt_intervals wi
                ON ta.stay_id = wi.stay_id
               AND ta.charttime_floor >= wi.starttime
               AND ta.charttime_floor < wi.endtime
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
                SELECT ad.hadm_id,
                  MAX(CASE WHEN icd9 LIKE '410%' OR icd9 LIKE '412%'
                            OR icd10 LIKE 'I21%' OR icd10 LIKE 'I22%' OR icd10='I252'
                           THEN 1 ELSE 0 END) AS mi,
                  MAX(CASE WHEN icd9 IN ('4280','4281','42820','42821','42822','42823',
                                         '42830','42831','42832','42833','42840','42841',
                                         '42842','42843','4289')
                            OR icd9 LIKE '402%' OR icd9 LIKE '404%'
                            OR icd10 IN ('I43','I50','I099','I110','I130','I132',
                                          'I255','I420','I425','I426','I427','I428',
                                          'I429','P290')
                           THEN 1 ELSE 0 END) AS chf,
                  MAX(CASE WHEN icd9 LIKE '440%' OR icd9 LIKE '441%'
                            OR icd9 IN ('0930','4373','4471','5571','5579','V434')
                            OR icd9 LIKE '443%'
                            OR icd10 LIKE 'I70%' OR icd10 LIKE 'I71%'
                            OR icd10 IN ('I731','I738','I739','I771','I790','I791',
                                          'I792','K551','K558','K559','Z958','Z959')
                           THEN 1 ELSE 0 END) AS pvd,
                  MAX(CASE WHEN icd9 BETWEEN '430' AND '438' OR icd9='36234'
                            OR icd10 LIKE 'G45%' OR icd10 LIKE 'G46%'
                            OR icd10 LIKE 'I60%' OR icd10 LIKE 'I61%'
                            OR icd10 LIKE 'I62%' OR icd10 LIKE 'I63%'
                            OR icd10 LIKE 'I64%' OR icd10 LIKE 'I65%'
                            OR icd10 LIKE 'I66%' OR icd10 LIKE 'I67%'
                            OR icd10 LIKE 'I68%' OR icd10 LIKE 'I69%'
                            OR icd10='H340'
                           THEN 1 ELSE 0 END) AS cvd,
                  MAX(CASE WHEN icd9 LIKE '290%' OR icd9 IN ('2941','3312')
                            OR icd10 LIKE 'F00%' OR icd10 LIKE 'F01%'
                            OR icd10 LIKE 'F02%' OR icd10 LIKE 'F03%' OR icd10='F051'
                            OR icd10 LIKE 'G30%' OR icd10='G311'
                           THEN 1 ELSE 0 END) AS dementia,
                  MAX(CASE WHEN icd9 LIKE '490%' OR icd9 LIKE '491%'
                            OR icd9 LIKE '492%' OR icd9 LIKE '493%'
                            OR icd9 LIKE '494%' OR icd9 LIKE '495%'
                            OR icd9 LIKE '496%' OR icd9 LIKE '500%'
                            OR icd9 LIKE '501%' OR icd9 LIKE '502%'
                            OR icd9 LIKE '503%' OR icd9 LIKE '504%'
                            OR icd9 LIKE '505%' OR icd9='5064'
                            OR icd10 LIKE 'J40%' OR icd10 LIKE 'J41%'
                            OR icd10 LIKE 'J42%' OR icd10 LIKE 'J43%'
                            OR icd10 LIKE 'J44%' OR icd10 LIKE 'J45%'
                            OR icd10 LIKE 'J46%' OR icd10 LIKE 'J47%'
                            OR icd10 LIKE 'J60%' OR icd10 LIKE 'J61%'
                            OR icd10 LIKE 'J62%' OR icd10 LIKE 'J63%'
                            OR icd10 LIKE 'J64%' OR icd10 LIKE 'J65%'
                            OR icd10 LIKE 'J66%' OR icd10 LIKE 'J67%'
                            OR icd10='J684' OR icd10='J701' OR icd10='J703'
                           THEN 1 ELSE 0 END) AS copd,
                  MAX(CASE WHEN icd9 IN ('4465','7100','7101','7102','7103','7104',
                                          '7140','7141','7142','71481','7148','725')
                            OR icd10 IN ('M05','M06','M315','M32','M33','M334',
                                          'M335','M336','M34','M351','M353','M360')
                           THEN 1 ELSE 0 END) AS rheum,
                  MAX(CASE WHEN icd9 LIKE '531%' OR icd9 LIKE '532%'
                            OR icd9 LIKE '533%' OR icd9 LIKE '534%'
                            OR icd10 LIKE 'K25%' OR icd10 LIKE 'K26%'
                            OR icd10 LIKE 'K27%' OR icd10 LIKE 'K28%'
                           THEN 1 ELSE 0 END) AS pud,
                  MAX(CASE WHEN icd9 IN ('07022','07023','07032','07033',
                                          '07044','07054','0706','0709',
                                          '5733','5734','5738','5739','V427')
                            OR icd9 LIKE '570%' OR icd9 LIKE '571%'
                            OR icd10 IN ('B18','K700','K701','K702','K703','K709',
                                          'K713','K714','K715','K717','K73','K74',
                                          'K760','K762','K763','K764','K768',
                                          'K769','Z944')
                           THEN 1 ELSE 0 END) AS liver_mild,
                  MAX(CASE WHEN icd9 LIKE '250%'
                            OR icd10 LIKE 'E10%' OR icd10 LIKE 'E11%'
                            OR icd10 LIKE 'E12%' OR icd10 LIKE 'E13%'
                            OR icd10 LIKE 'E14%'
                           THEN 1 ELSE 0 END) AS dm_no_comp,
                  MAX(CASE WHEN icd9 LIKE '250%'
                            OR icd10 IN ('E100','E101','E102','E103','E104',
                                          'E105','E106','E107','E108','E109',
                                          'E110','E111','E112','E113','E114',
                                          'E115','E116','E117','E118','E119')
                           THEN 1 ELSE 0 END) AS dm_comp,
                  MAX(CASE WHEN icd9 IN ('34200','34201','34202','34203',
                                          '34204','34205','34206','34207',
                                          '34209','3430','3431','3432',
                                          '3433','3434','3435','3438','3439',
                                          '34400','34401','34402','34403',
                                          '34404','34405','34406','34408',
                                          '34410','34411','34412','34413',
                                          '34414','34415','34416','34418',
                                          '34419','3449')
                            OR icd10 LIKE 'G041%' OR icd10='G114' OR icd10='G801'
                            OR icd10='G802' OR icd10 LIKE 'G81%' OR icd10 LIKE 'G82%'
                            OR icd10 IN ('G830','G831','G832','G833','G834','G839')
                           THEN 1 ELSE 0 END) AS para,
                  MAX(CASE WHEN icd9 IN ('40301','40311','40391','40402',
                                          '40403','40412','40413','40492',
                                          '40493','5880','V420','V451','V56')
                            OR icd9 LIKE '582%' OR icd9 LIKE '583%'
                            OR icd9 LIKE '585%' OR icd9 LIKE '586%'
                            OR icd10 IN ('N18','N19','I120','I131',
                                          'N032','N033','N034','N035','N036','N037',
                                          'N052','N053','N054','N055','N056','N057',
                                          'N25','N250','Z490','Z491','Z492','Z940','Z992')
                           THEN 1 ELSE 0 END) AS renal,
                  MAX(CASE WHEN icd9 LIKE '140%' OR icd9 LIKE '141%'
                            OR icd9 LIKE '142%' OR icd9 LIKE '143%'
                            OR icd9 LIKE '144%' OR icd9 LIKE '145%'
                            OR icd9 LIKE '146%' OR icd9 LIKE '147%'
                            OR icd9 LIKE '148%' OR icd9 LIKE '149%'
                            OR icd9 LIKE '150%' OR icd9 LIKE '151%'
                            OR icd9 LIKE '152%' OR icd9 LIKE '153%'
                            OR icd9 LIKE '154%' OR icd9 LIKE '155%'
                            OR icd9 LIKE '156%' OR icd9 LIKE '157%'
                            OR icd9 LIKE '158%' OR icd9 LIKE '159%'
                            OR icd9 LIKE '160%' OR icd9 LIKE '161%'
                            OR icd9 LIKE '162%' OR icd9 LIKE '163%'
                            OR icd9 LIKE '164%' OR icd9 LIKE '165%'
                            OR icd9 LIKE '170%' OR icd9 LIKE '171%'
                            OR icd9 LIKE '172%' OR icd9 LIKE '174%'
                            OR icd9 LIKE '175%' OR icd9 LIKE '179%'
                            OR icd9 LIKE '180%' OR icd9 LIKE '181%'
                            OR icd9 LIKE '182%' OR icd9 LIKE '183%'
                            OR icd9 LIKE '184%' OR icd9 LIKE '185%'
                            OR icd9 LIKE '186%' OR icd9 LIKE '187%'
                            OR icd9 LIKE '188%' OR icd9 LIKE '189%'
                            OR icd9 LIKE '190%' OR icd9 LIKE '191%'
                            OR icd9 LIKE '192%' OR icd9 LIKE '193%'
                            OR icd9 LIKE '194%' OR icd9 LIKE '195%'
                            OR icd9 LIKE '200%' OR icd9 LIKE '201%'
                            OR icd9 LIKE '202%' OR icd9='2386' OR icd9='2733'
                            OR icd10 LIKE 'C0%'  OR icd10 LIKE 'C1%'
                            OR icd10 LIKE 'C2%'  OR icd10 LIKE 'C3%'
                            OR icd10 LIKE 'C40%' OR icd10 LIKE 'C41%'
                            OR icd10 LIKE 'C43%' OR icd10 LIKE 'C45%'
                            OR icd10 LIKE 'C46%' OR icd10 LIKE 'C47%'
                            OR icd10 LIKE 'C48%' OR icd10 LIKE 'C49%'
                            OR icd10 LIKE 'C5%'  OR icd10 LIKE 'C6%'
                            OR icd10 LIKE 'C70%' OR icd10 LIKE 'C71%'
                            OR icd10 LIKE 'C72%' OR icd10 LIKE 'C73%'
                            OR icd10 LIKE 'C74%' OR icd10 LIKE 'C75%'
                            OR icd10 LIKE 'C76%' OR icd10 LIKE 'C81%'
                            OR icd10 LIKE 'C82%' OR icd10 LIKE 'C83%'
                            OR icd10 LIKE 'C84%' OR icd10 LIKE 'C85%'
                            OR icd10 LIKE 'C88%' OR icd10 LIKE 'C9%'
                           THEN 1 ELSE 0 END) AS malignancy,
                  MAX(CASE WHEN icd9 IN ('4560','4561','4562',
                                          '5722','5723','5724',
                                          '5725','5726','5727','5728')
                            OR icd10 IN ('K704','K711','K721','K729',
                                          'K765','K766','K767','I850',
                                          'I859','I864','I982')
                           THEN 1 ELSE 0 END) AS liver_severe,
                  MAX(CASE WHEN icd9 LIKE '196%' OR icd9 LIKE '197%'
                            OR icd9 LIKE '198%' OR icd9 LIKE '199%'
                            OR icd10 LIKE 'C77%' OR icd10 LIKE 'C78%'
                            OR icd10 LIKE 'C79%' OR icd10='C800'
                           THEN 1 ELSE 0 END) AS metastatic,
                  MAX(CASE WHEN icd9 LIKE '042%' OR icd9 LIKE '043%' OR icd9 LIKE '044%'
                            OR icd10 IN ('B20','B21','B22','B24')
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
                 + com.mi
                 + com.chf
                 + com.pvd
                 + com.cvd
                 + com.dementia
                 + com.copd
                 + com.rheum
                 + com.pud
                 + GREATEST(com.liver_mild, com.liver_severe * 3)
                 + GREATEST(com.dm_no_comp, com.dm_comp * 2)
                 + com.para * 2
                 + com.renal * 2
                 + GREATEST(com.malignancy * 2, com.metastatic * 6)
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
    # Follow the official structure more closely:
    # 1) compute hourly component scores from current-hour concept rows
    # 2) take the rolling 24h MAX of each component score
    # 3) sum those rolling component maxima into sofa_24hours
    con.execute(f"CREATE OR REPLACE VIEW vitals_p AS SELECT * FROM read_parquet('{inter('03_vitals')}')")
    con.execute(f"CREATE OR REPLACE VIEW bg_p    AS SELECT * FROM read_parquet('{inter('05_bg')}')")
    con.execute(f"CREATE OR REPLACE VIEW cbc_p   AS SELECT * FROM read_parquet('{inter('07_cbc')}')")
    con.execute(f"CREATE OR REPLACE VIEW enzyme_p AS SELECT * FROM read_parquet('{inter('08_enzyme')}')")
    con.execute(f"CREATE OR REPLACE VIEW vaso_p  AS SELECT * FROM read_parquet('{inter('13_vaso')}')")
    con.execute(f"CREATE OR REPLACE VIEW gcs_p   AS SELECT * FROM read_parquet('{inter('04_gcs')}')")
    con.execute(f"CREATE OR REPLACE VIEW chem_p  AS SELECT * FROM read_parquet('{inter('06_chemistry')}')")
    con.execute(f"CREATE OR REPLACE VIEW uo_p    AS SELECT * FROM read_parquet('{inter('12_uo')}')")
    con.execute(f"CREATE OR REPLACE VIEW vent_p  AS SELECT * FROM read_parquet('{inter('14_vent')}')")
    con.execute(f"""
        COPY (
            WITH icu_time_axis AS (
                SELECT *
                FROM time_axis
                WHERE hr >= 0
            ),
            base AS (
                SELECT
                    ta.stay_id,
                    ta.hr,
                    ta.charttime_floor,
                    ta.endtime,
                    CASE
                        WHEN ve.ventilation_status = 'InvasiveVent' THEN bg.pao2fio2ratio_art
                        ELSE NULL
                    END AS pao2fio2ratio_vent,
                    CASE
                        WHEN ve.ventilation_status != 'InvasiveVent' OR ve.ventilation_status IS NULL
                        THEN bg.pao2fio2ratio_art
                        ELSE NULL
                    END AS pao2fio2ratio_novent,
                    cbc.platelet AS platelet_min,
                    en.bilirubin_total AS bilirubin_max,
                    gc.gcs_total AS gcs_min,
                    ch.creatinine AS creatinine_max,
                    vt.mbp AS meanbp_min,
                    uo.uo_24hr AS uo_24hr,
                    va.norepi_rate AS rate_norepinephrine,
                    va.epi_rate AS rate_epinephrine,
                    va.dopa_rate AS rate_dopamine,
                    va.dobu_rate AS rate_dobutamine,
                    ve.ventilation_status
                FROM icu_time_axis ta
                LEFT JOIN vitals_p vt  ON ta.stay_id = vt.stay_id  AND ta.charttime_floor = vt.charttime_floor
                LEFT JOIN bg_p     bg  ON ta.stay_id = bg.stay_id  AND ta.charttime_floor = bg.charttime_floor
                LEFT JOIN cbc_p    cbc ON ta.stay_id = cbc.stay_id AND ta.charttime_floor = cbc.charttime_floor
                LEFT JOIN enzyme_p en  ON ta.stay_id = en.stay_id  AND ta.charttime_floor = en.charttime_floor
                LEFT JOIN gcs_p    gc  ON ta.stay_id = gc.stay_id  AND ta.charttime_floor = gc.charttime_floor
                LEFT JOIN chem_p   ch  ON ta.stay_id = ch.stay_id  AND ta.charttime_floor = ch.charttime_floor
                LEFT JOIN uo_p     uo  ON ta.stay_id = uo.stay_id  AND ta.charttime_floor = uo.charttime_floor
                LEFT JOIN vaso_p   va  ON ta.stay_id = va.stay_id  AND ta.charttime_floor = va.charttime_floor
                LEFT JOIN vent_p   ve  ON ta.stay_id = ve.stay_id  AND ta.charttime_floor = ve.charttime_floor
            ),
            scorecomp AS (
                SELECT
                    stay_id,
                    hr,
                    charttime_floor,
                    endtime,
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
                          OR rate_norepinephrine > 0.1 THEN 4
                        WHEN rate_dopamine > 5
                          OR (rate_epinephrine > 0 AND rate_epinephrine <= 0.1)
                          OR (rate_norepinephrine > 0 AND rate_norepinephrine <= 0.1) THEN 3
                        WHEN rate_dopamine > 0 OR rate_dobutamine > 0 THEN 2
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
                        WHEN gcs_min < 6 THEN 4
                        WHEN gcs_min < 10 THEN 3
                        WHEN gcs_min < 13 THEN 2
                        WHEN gcs_min < 15 THEN 1
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
                FROM base
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
                FROM scorecomp
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
            WHERE COALESCE(pr.drug_type, '') <> 'BASE'
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
                COALESCE(pr.stoptime, pr.starttime + INTERVAL 1 DAY) AS antibiotic_stoptime
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
                    ORDER BY me72.chartdate NULLS FIRST, me72.charttime
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
                    ORDER BY me24.chartdate NULLS FIRST, me24.charttime
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
    # Load all intermediate parquets and left-join onto time_axis
    for step_name in [
        "03_vitals","04_gcs","05_bg","06_chemistry","07_cbc","08_enzyme",
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
                co.first_careunit,
                co.last_careunit,
                co.age,
                co.gender,
                co.dod,
                co.anchor_year_group,
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
                co.hospadmtime,
                adm.edregtime,
                adm.edouttime,
                co.los_hospital,
                co.los_icu,
                co.hospstay_seq,
                co.first_hosp_stay,
                co.icustay_seq,
                co.first_icu_stay,
                ch18.charlson_score,
                -- hospital service (time-varying)
                sv.curr_service,
                -- vitals (ICU chartevents only → NULL for pre-ICU)
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.heart_rate    END AS heart_rate,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.sbp           END AS sbp,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.dbp           END AS dbp,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.mbp           END AS mbp,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.sbp_ni        END AS sbp_ni,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.dbp_ni        END AS dbp_ni,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.mbp_ni        END AS mbp_ni,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.resp_rate     END AS resp_rate,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.temperature   END AS temperature,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.spo2          END AS spo2,
                CASE WHEN ta.hr < 0 THEN NULL ELSE vt.glucose_vital END AS glucose_vital,
                -- GCS (ICU chartevents only → NULL for pre-ICU)
                CASE WHEN ta.hr < 0 THEN NULL ELSE gc.gcs_motor   END AS gcs_motor,
                CASE WHEN ta.hr < 0 THEN NULL ELSE gc.gcs_verbal  END AS gcs_verbal,
                CASE WHEN ta.hr < 0 THEN NULL ELSE gc.gcs_eyes    END AS gcs_eyes,
                CASE WHEN ta.hr < 0 THEN NULL ELSE gc.gcs_unable  END AS gcs_unable,
                CASE WHEN ta.hr < 0 THEN NULL ELSE gc.gcs_total   END AS gcs_total,
                -- blood gas: ph/po2/pco2 can come from hosp labevents (OK for pre-ICU)
                --            fio2_chartevents is ICU chartevents only → NULL for pre-ICU
                bg.ph, bg.pco2, bg.po2, bg.so2,
                bg.aado2, bg.aado2_calc, bg.pao2fio2ratio, bg.pao2fio2ratio_art, bg.fio2,
                CASE WHEN ta.hr < 0 THEN NULL ELSE bg.fio2_chartevents END AS fio2_chartevents,
                bg.arterial_bg_flag,
                bg.baseexcess, bg.lactate,
                bg.carboxyhemoglobin, bg.methemoglobin,
                bg.o2flow, bg.peep, bg.requiredo2, bg.calcium_ionized,
                -- chemistry
                ch.creatinine, ch.sodium, ch.potassium,
                ch.bicarbonate, ch.bun, ch.calcium_total,
                ch.chloride, ch.glucose_lab, ch.anion_gap, ch.albumin,
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
                -- vasopressors
                va.norepi_rate, va.epi_rate, va.dopa_rate, va.dobu_rate,
                va.phenyl_rate, va.vaso_rate,
                -- ventilation
                ve.ventilation_status,
                -- CRRT
                cr.crrt_mode,
                cr.current_goal AS crrt_current_goal,
                cr.blood_flow AS crrt_blood_flow,
                cr.dialysate_rate AS crrt_dialysate_rate,
                cr.replacement_rate AS crrt_replacement_rate,
                cr.ultrafiltrate_output AS crrt_ultrafiltrate_output,
                CASE WHEN ta.hr < 0 THEN NULL ELSE COALESCE(cr.system_active, 0) END AS crrt_system_active,
                CASE WHEN ta.hr < 0 THEN NULL ELSE COALESCE(cr.clots, 0) END AS crrt_clots,
                CASE WHEN ta.hr < 0 THEN NULL ELSE COALESCE(cr.clots_increasing, 0) END AS crrt_clots_increasing,
                CASE WHEN ta.hr < 0 THEN NULL ELSE COALESCE(cr.clotted, 0) END AS crrt_clotted,
                CASE WHEN ta.hr < 0 THEN NULL ELSE COALESCE(cr.crrt_flag, 0) END AS crrt_flag,
                -- antibiotic
                CASE WHEN ta.hr < 0 THEN NULL ELSE COALESCE(ab.antibiotic_flag, 0) END AS antibiotic_flag,
                -- height / weight
                hw.height,
                hw.weight_type,
                hw.admit_weight,
                hw.daily_weight,
                hw.weight,
                -- SOFA
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_respiration    END AS sofa_respiration,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_coagulation    END AS sofa_coagulation,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_liver          END AS sofa_liver,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_cardiovascular END AS sofa_cardiovascular,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_cns            END AS sofa_cns,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_renal          END AS sofa_renal,
                CASE WHEN ta.hr < 0 THEN NULL ELSE sf.sofa_24hours        END AS sofa_24hours,
                -- sepsis metadata: stay-level constants → NULL for pre-ICU to prevent leakage
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
            -- vitals
            LEFT JOIN v_03_vitals         vt   ON ta.stay_id = vt.stay_id  AND ta.charttime_floor = vt.charttime_floor
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
    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT
                    le.subject_id, le.hadm_id, le.charttime,
                    CASE WHEN le.itemid = 50912 AND le.valuenum BETWEEN 0   AND 150   THEN le.valuenum END AS creatinine,
                    CASE WHEN le.itemid = 50983 AND le.valuenum BETWEEN 100 AND 200   THEN le.valuenum END AS sodium,
                    CASE WHEN le.itemid = 50971 AND le.valuenum BETWEEN 1   AND 15    THEN le.valuenum END AS potassium,
                    CASE WHEN le.itemid = 50882 AND le.valuenum BETWEEN 0   AND 60    THEN le.valuenum END AS bicarbonate,
                    CASE WHEN le.itemid = 51006 AND le.valuenum BETWEEN 0   AND 300   THEN le.valuenum END AS bun,
                    CASE WHEN le.itemid = 50893 AND le.valuenum BETWEEN 0   AND 20    THEN le.valuenum END AS calcium_total,
                    CASE WHEN le.itemid = 50902 AND le.valuenum BETWEEN 50  AND 160   THEN le.valuenum END AS chloride,
                    CASE WHEN le.itemid = 50931 AND le.valuenum BETWEEN 0   AND 10000 THEN le.valuenum END AS glucose_lab,
                    CASE WHEN le.itemid = 50868 AND le.valuenum BETWEEN 0   AND 50    THEN le.valuenum END AS anion_gap,
                    CASE WHEN le.itemid = 50862 AND le.valuenum BETWEEN 0   AND 10    THEN le.valuenum END AS albumin
                FROM labevents le
                INNER JOIN cohort co ON le.hadm_id = co.hadm_id
                WHERE le.itemid IN (50912,50983,50971,50882,51006,50893,50902,50931,50868,50862)
            )
            SELECT
                co.stay_id,
                date_trunc('hour', raw.charttime) AS charttime_floor,
                AVG(creatinine)   AS creatinine,
                AVG(sodium)       AS sodium,
                AVG(potassium)    AS potassium,
                AVG(bicarbonate)  AS bicarbonate,
                AVG(bun)          AS bun,
                AVG(calcium_total)AS calcium_total,
                AVG(chloride)     AS chloride,
                AVG(glucose_lab)  AS glucose_lab,
                AVG(anion_gap)    AS anion_gap,
                AVG(albumin)      AS albumin
            FROM raw
            INNER JOIN cohort co
                ON raw.hadm_id = co.hadm_id
               AND raw.charttime >= co.intime - INTERVAL '24' HOUR
               AND raw.charttime <= co.outtime
            GROUP BY co.stay_id, date_trunc('hour', raw.charttime)
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
    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT
                    le.subject_id, le.hadm_id, le.charttime,
                    CASE WHEN le.itemid = 51265 AND le.valuenum BETWEEN 0   AND 10000 THEN le.valuenum END AS platelet,
                    CASE WHEN le.itemid = 51222 AND le.valuenum BETWEEN 0   AND 50    THEN le.valuenum END AS hemoglobin,
                    CASE WHEN le.itemid = 51221 AND le.valuenum BETWEEN 0   AND 100   THEN le.valuenum END AS hematocrit,
                    CASE WHEN le.itemid = 51301 AND le.valuenum BETWEEN 0   AND 1000  THEN le.valuenum END AS wbc,
                    CASE WHEN le.itemid = 51248 AND le.valuenum BETWEEN 0   AND 50    THEN le.valuenum END AS mch,
                    CASE WHEN le.itemid = 51249 AND le.valuenum BETWEEN 0   AND 50    THEN le.valuenum END AS mchc,
                    CASE WHEN le.itemid = 51250 AND le.valuenum BETWEEN 0   AND 150   THEN le.valuenum END AS mcv,
                    CASE WHEN le.itemid = 51279 AND le.valuenum BETWEEN 0   AND 10    THEN le.valuenum END AS rbc,
                    CASE WHEN le.itemid = 51277 AND le.valuenum BETWEEN 0   AND 50    THEN le.valuenum END AS rdw,
                    CASE WHEN le.itemid = 52159 AND le.valuenum BETWEEN 0   AND 50    THEN le.valuenum END AS rdwsd
                FROM labevents le
                INNER JOIN cohort co ON le.hadm_id = co.hadm_id
                WHERE le.itemid IN (51265,51222,51221,51301,51248,51249,51250,51279,51277,52159)
            )
            SELECT
                co.stay_id,
                date_trunc('hour', raw.charttime) AS charttime_floor,
                AVG(platelet)   AS platelet,
                AVG(hemoglobin) AS hemoglobin,
                AVG(hematocrit) AS hematocrit,
                AVG(wbc)        AS wbc,
                AVG(mch)        AS mch,
                AVG(mchc)       AS mchc,
                AVG(mcv)        AS mcv,
                AVG(rbc)        AS rbc,
                AVG(rdw)        AS rdw,
                AVG(rdwsd)      AS rdwsd
            FROM raw
            INNER JOIN cohort co
                ON raw.hadm_id = co.hadm_id
               AND raw.charttime >= co.intime - INTERVAL '24' HOUR
               AND raw.charttime <= co.outtime
            GROUP BY co.stay_id, date_trunc('hour', raw.charttime)
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
    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT
                    le.subject_id, le.hadm_id, le.charttime,
                    CASE WHEN le.itemid = 50861 AND le.valuenum BETWEEN 0 AND 10000 THEN le.valuenum END AS alt,
                    CASE WHEN le.itemid = 50863 AND le.valuenum BETWEEN 0 AND 10000 THEN le.valuenum END AS alp,
                    CASE WHEN le.itemid = 50878 AND le.valuenum BETWEEN 0 AND 10000 THEN le.valuenum END AS ast,
                    CASE WHEN le.itemid = 50867 AND le.valuenum BETWEEN 0 AND 10000 THEN le.valuenum END AS amylase,
                    CASE WHEN le.itemid = 50885 AND le.valuenum BETWEEN 0 AND 150   THEN le.valuenum END AS bilirubin_total,
                    CASE WHEN le.itemid = 50883 AND le.valuenum BETWEEN 0 AND 150   THEN le.valuenum END AS bilirubin_direct,
                    CASE WHEN le.itemid = 50884 AND le.valuenum BETWEEN 0 AND 150   THEN le.valuenum END AS bilirubin_indirect,
                    CASE WHEN le.itemid = 50910 AND le.valuenum BETWEEN 0 AND 10000 THEN le.valuenum END AS ck_cpk,
                    CASE WHEN le.itemid = 50911 AND le.valuenum BETWEEN 0 AND 10000 THEN le.valuenum END AS ck_mb,
                    CASE WHEN le.itemid = 50927 AND le.valuenum BETWEEN 0 AND 10000 THEN le.valuenum END AS ggt,
                    CASE WHEN le.itemid = 50954 AND le.valuenum BETWEEN 0 AND 100000 THEN le.valuenum END AS ldh
                FROM labevents le
                INNER JOIN cohort co ON le.hadm_id = co.hadm_id
                WHERE le.itemid IN (50861,50863,50878,50867,50885,50883,50884,50910,50911,50927,50954)
            )
            SELECT
                co.stay_id,
                date_trunc('hour', raw.charttime) AS charttime_floor,
                AVG(alt)               AS alt,
                AVG(alp)               AS alp,
                AVG(ast)               AS ast,
                AVG(amylase)           AS amylase,
                AVG(bilirubin_total)   AS bilirubin_total,
                AVG(bilirubin_direct)  AS bilirubin_direct,
                AVG(bilirubin_indirect)AS bilirubin_indirect,
                AVG(ck_cpk)            AS ck_cpk,
                AVG(ck_mb)             AS ck_mb,
                AVG(ggt)               AS ggt,
                AVG(ldh)               AS ldh
            FROM raw
            INNER JOIN cohort co
                ON raw.hadm_id = co.hadm_id
               AND raw.charttime >= co.intime - INTERVAL '24' HOUR
               AND raw.charttime <= co.outtime
            GROUP BY co.stay_id, date_trunc('hour', raw.charttime)
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
    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT
                    le.subject_id, le.hadm_id, le.charttime,
                    CASE WHEN le.itemid = 51196 AND le.valuenum BETWEEN 0 AND 10000 THEN le.valuenum END AS d_dimer,
                    CASE WHEN le.itemid = 51214 AND le.valuenum BETWEEN 0 AND 10000 THEN le.valuenum END AS fibrinogen,
                    CASE WHEN le.itemid = 51297 AND le.valuenum BETWEEN 0 AND 300   THEN le.valuenum END AS thrombin,
                    CASE WHEN le.itemid = 51237 AND le.valuenum BETWEEN 0 AND 50    THEN le.valuenum END AS inr,
                    CASE WHEN le.itemid = 51274 AND le.valuenum BETWEEN 0 AND 150   THEN le.valuenum END AS pt,
                    CASE WHEN le.itemid = 51275 AND le.valuenum BETWEEN 0 AND 200   THEN le.valuenum END AS ptt
                FROM labevents le
                INNER JOIN cohort co ON le.hadm_id = co.hadm_id
                WHERE le.itemid IN (51196,51214,51297,51237,51274,51275)
            )
            SELECT
                co.stay_id,
                date_trunc('hour', raw.charttime) AS charttime_floor,
                AVG(d_dimer)    AS d_dimer,
                AVG(fibrinogen) AS fibrinogen,
                AVG(thrombin)   AS thrombin,
                AVG(inr)        AS inr,
                AVG(pt)         AS pt,
                AVG(ptt)        AS ptt
            FROM raw
            INNER JOIN cohort co
                ON raw.hadm_id = co.hadm_id
               AND raw.charttime >= co.intime - INTERVAL '24' HOUR
               AND raw.charttime <= co.outtime
            GROUP BY co.stay_id, date_trunc('hour', raw.charttime)
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
    # Percentages and absolute counts; impute_abs = pct * wbc / 100 when abs missing
    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT
                    le.subject_id, le.charttime,
                    CASE WHEN le.itemid IN (51256,52075) AND le.valuenum BETWEEN 0 AND 100  THEN le.valuenum END AS neutrophils_pct,
                    CASE WHEN le.itemid = 51244        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS lymphocytes_pct,
                    CASE WHEN le.itemid IN (51254,52074)AND le.valuenum BETWEEN 0 AND 100   THEN le.valuenum END AS monocytes_pct,
                    CASE WHEN le.itemid IN (51200,52073)AND le.valuenum BETWEEN 0 AND 100   THEN le.valuenum END AS eosinophils_pct,
                    CASE WHEN le.itemid IN (51146,52069)AND le.valuenum BETWEEN 0 AND 100   THEN le.valuenum END AS basophils_pct,
                    CASE WHEN le.itemid = 52073        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS eosinophils_abs,
                    CASE WHEN le.itemid = 52069        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS basophils_abs,
                    CASE WHEN le.itemid = 52075        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS neutrophils_abs,
                    CASE WHEN le.itemid IN (51133,52769)AND le.valuenum BETWEEN 0 AND 100   THEN le.valuenum END AS lymphocytes_abs,
                    CASE WHEN le.itemid = 52074        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS monocytes_abs,
                    CASE WHEN le.itemid = 51144        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS bands,
                    CASE WHEN le.itemid = 52135        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS immature_granulocytes,
                    CASE WHEN le.itemid = 51143        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS atypical_lymphocytes,
                    CASE WHEN le.itemid = 51251        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS metamyelocytes,
                    CASE WHEN le.itemid = 51257        AND le.valuenum BETWEEN 0 AND 100    THEN le.valuenum END AS nrbc
                FROM labevents le
                INNER JOIN cohort co ON le.subject_id = co.subject_id
                WHERE le.itemid IN (51256,52075,51244,51254,52074,51200,52073,
                                    51146,52069,51133,52769,51144,52135,51143,51251,51257)
            ),
            hourly AS (
                SELECT
                    co.stay_id,
                    date_trunc('hour', raw.charttime) AS charttime_floor,
                    AVG(neutrophils_pct)       AS neutrophils_pct,
                    AVG(neutrophils_abs)       AS neutrophils_abs,
                    AVG(lymphocytes_pct)       AS lymphocytes_pct,
                    AVG(lymphocytes_abs)       AS lymphocytes_abs,
                    AVG(monocytes_pct)         AS monocytes_pct,
                    AVG(monocytes_abs)         AS monocytes_abs,
                    AVG(eosinophils_pct)       AS eosinophils_pct,
                    AVG(eosinophils_abs)       AS eosinophils_abs,
                    AVG(basophils_pct)         AS basophils_pct,
                    AVG(basophils_abs)         AS basophils_abs,
                    AVG(bands)                 AS bands,
                    AVG(immature_granulocytes) AS immature_granulocytes,
                    AVG(atypical_lymphocytes)  AS atypical_lymphocytes,
                    AVG(metamyelocytes)        AS metamyelocytes,
                    AVG(nrbc)                  AS nrbc
                FROM raw
                INNER JOIN cohort co
                    ON raw.subject_id = co.subject_id
                   AND raw.charttime >= co.intime - INTERVAL '24' HOUR
                   AND raw.charttime <= co.outtime
                GROUP BY co.stay_id, date_trunc('hour', raw.charttime)
            ),
            cbc_p AS (
                SELECT stay_id, charttime_floor, wbc
                FROM read_parquet('{inter("07_cbc")}')
            )
            SELECT
                h.stay_id,
                h.charttime_floor,
                h.neutrophils_pct,
                COALESCE(h.neutrophils_abs, h.neutrophils_pct * cbc.wbc / 100.0) AS neutrophils_abs,
                h.lymphocytes_pct,
                COALESCE(h.lymphocytes_abs, h.lymphocytes_pct * cbc.wbc / 100.0) AS lymphocytes_abs,
                h.monocytes_pct,
                COALESCE(h.monocytes_abs, h.monocytes_pct * cbc.wbc / 100.0) AS monocytes_abs,
                h.eosinophils_pct,
                COALESCE(h.eosinophils_abs, h.eosinophils_pct * cbc.wbc / 100.0) AS eosinophils_abs,
                h.basophils_pct,
                COALESCE(h.basophils_abs, h.basophils_pct * cbc.wbc / 100.0) AS basophils_abs,
                h.bands,
                h.immature_granulocytes,
                h.atypical_lymphocytes,
                h.metamyelocytes,
                h.nrbc
            FROM hourly h
            LEFT JOIN cbc_p cbc
                ON h.stay_id = cbc.stay_id
               AND h.charttime_floor = cbc.charttime_floor
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
    # troponin_I not in MIMIC-IV; ck_mb already in enzyme step
    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT
                    le.subject_id, le.charttime,
                    CASE WHEN le.itemid = 51003  AND le.valuenum >= 0             THEN le.valuenum END AS troponin_t,
                    CASE WHEN le.itemid = 50963  AND le.valuenum BETWEEN 0 AND 100000 THEN le.valuenum END AS ntprobnp,
                    CASE WHEN le.itemid = 50889  AND le.valuenum BETWEEN 0 AND 10000  THEN le.valuenum END AS crp
                FROM labevents le
                INNER JOIN cohort co ON le.subject_id = co.subject_id
                WHERE le.itemid IN (51003, 50963, 50889)
            )
            SELECT
                co.stay_id,
                date_trunc('hour', raw.charttime) AS charttime_floor,
                AVG(troponin_t) AS troponin_t,
                AVG(ntprobnp)   AS ntprobnp,
                AVG(crp)        AS crp
            FROM raw
            INNER JOIN cohort co
                ON raw.subject_id = co.subject_id
               AND raw.charttime >= co.intime - INTERVAL '24' HOUR
               AND raw.charttime <= co.outtime
            GROUP BY co.stay_id, date_trunc('hour', raw.charttime)
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
                        bg.baseexcess,
                        bg.carboxyhemoglobin,
                        bg.calcium_ionized,
                        bg.lactate,
                        bg.methemoglobin,
                        bg.o2flow,
                        bg.peep,
                        bg.requiredo2,
                        bg.so2,
                        bg.pco2,
                        bg.ph,
                        bg.po2,
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
                    baseexcess,
                    carboxyhemoglobin,
                    calcium_ionized,
                    lactate,
                    methemoglobin,
                    o2flow,
                    peep,
                    requiredo2,
                    so2,
                    pco2,
                    ph,
                    po2
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
                baseexcess,
                carboxyhemoglobin,
                calcium_ionized,
                lactate,
                methemoglobin,
                o2flow,
                peep,
                requiredo2,
                so2,
                pco2,
                ph,
                po2
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

    con = duckdb.connect(DB_PATH)
    register_views(con)

    step01_cohort(con)
    step02_time_axis(con)
    step03_vitals(con)
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
