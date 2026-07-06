"""
Build MIMIC-III wide table: rows = ICUSTAY_ID x HOUR, columns = clinical variables.
SepsisLabel follows PhysioNet 2019 Challenge / Sepsis-3 definition.

Output: D:/ESILV_S2/Intern/build_mimic/mimiciii/output/mimic3_wide.parquet
"""

import os
import sys
import time
import shutil
import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

RAW_DIR = r"D:\ESILV_S2\mimic\raw\physionet.org\files\mimiciii\1.4\mimiciii csv"
INTER_DIR = r"D:\ESILV_S2\Intern\build_mimic\mimiciii\intermediate\mimiciii"
DB_PATH = r"D:\ESILV_S2\Intern\build_mimic\mimiciii\output\mimic3_build.duckdb"
OUT_PATH = r"D:\ESILV_S2\Intern\build_mimic\mimiciii\output\mimic3_wide.parquet"
BACKUP_DIR = r"D:\ESILV_S2\Intern\build_mimic\mimiciii\output\backups"

os.makedirs(INTER_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)


def inter(name: str) -> str:
    return os.path.join(INTER_DIR, f"{name}.parquet")


def exists(name: str) -> bool:
    return os.path.exists(inter(name))


def backup_existing_output(path: str) -> str | None:
    if not os.path.exists(path):
        return None

    base, ext = os.path.splitext(os.path.basename(path))
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"{base}_before_rebuild_{timestamp}{ext}")
    suffix = 1
    while os.path.exists(backup_path):
        backup_path = os.path.join(
            BACKUP_DIR, f"{base}_before_rebuild_{timestamp}_{suffix}{ext}"
        )
        suffix += 1

    shutil.copy2(path, backup_path)
    log.info("backed up previous final output to %s", backup_path)
    return backup_path


def final_output_is_stale() -> bool:
    if not os.path.exists(OUT_PATH):
        return True

    dependencies = [inter("11_joined"), inter("12_sepsislabel"), inter("severity_scores_firstday"), inter("12c_oasis")]
    dep_mtime = max(os.path.getmtime(path) for path in dependencies if os.path.exists(path))
    return dep_mtime > os.path.getmtime(OUT_PATH)


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB_PATH)
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA memory_limit='12GB'")
    return con


def register_views(con: duckdb.DuckDBPyConnection):
    tables = [
        "ADMISSIONS", "CALLOUT", "CAREGIVERS", "CPTEVENTS",
        "DATETIMEEVENTS", "DIAGNOSES_ICD", "DRGCODES", "D_CPT",
        "D_ICD_DIAGNOSES", "D_ICD_PROCEDURES", "D_ITEMS", "D_LABITEMS",
        "ICUSTAYS", "INPUTEVENTS_CV", "INPUTEVENTS_MV", "LABEVENTS",
        "MICROBIOLOGYEVENTS", "NOTEEVENTS", "OUTPUTEVENTS", "PATIENTS", "PRESCRIPTIONS",
        "PROCEDUREEVENTS_MV", "PROCEDURES_ICD", "SERVICES", "TRANSFERS",
    ]
    for t in tables:
        path = os.path.join(RAW_DIR, f"{t}.csv").replace("\\", "/")
        con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_csv_auto('{path}', header=True, ignore_errors=True)")

    # CHARTEVENTS: force VALUE as VARCHAR — read_csv_auto infers it as DOUBLE
    # because most values are numeric, which would silently drop text values
    # like 'Ventilator' needed for ventilation flag detection.
    ce_path = os.path.join(RAW_DIR, "CHARTEVENTS.csv").replace("\\", "/")
    con.execute(
        f"CREATE OR REPLACE VIEW CHARTEVENTS AS "
        f"SELECT * FROM read_csv_auto('{ce_path}', header=True, ignore_errors=True, "
        f"types={{'VALUE': 'VARCHAR'}})"
    )
    log.info("registered %d CSV views", len(tables) + 1)


# ---------------------------------------------------------------------------
# Step 1: cohort — all ICU stays, age >= 18
# ---------------------------------------------------------------------------
def step01_cohort(con):
    name = "01_cohort"
    if exists(name):
        log.info("step01 cached"); return
    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT
                ie.subject_id,
                ie.hadm_id,
                ie.icustay_id,
                ie.intime,
                ie.outtime,
                CAST(ie.los * 24 AS INTEGER) AS los_hours,
                p.gender,
                CAST(date_diff('year', p.dob, ie.intime) AS INTEGER) AS age
            FROM ICUSTAYS ie
            INNER JOIN PATIENTS p ON ie.subject_id = p.subject_id
            INNER JOIN ADMISSIONS a ON ie.hadm_id = a.hadm_id
            WHERE
                date_diff('year', p.dob, ie.intime) >= 18
                AND COALESCE(a.has_chartevents_data, 0) = 1
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step01 done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 2: time axis — one row per ICUSTAY_ID per hour (-24 to los_hours-1)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Step 1b: icustay_times
# Source: concepts/demographics/icustay_times.sql
# ---------------------------------------------------------------------------
def step_icustay_times(con):
    name = "icustay_times"
    if exists(name):
        log.info("step_icustay_times cached"); return
    t0 = time.time()
    log.info("step_icustay_times: scanning CHARTEVENTS for HR (itemid 211, 220045)...")
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"""
        COPY (
            WITH h AS (
                -- lag/lead 拿相邻住院的出/入院时间
                SELECT
                    subject_id, hadm_id, admittime, dischtime,
                    LAG(dischtime)  OVER (PARTITION BY subject_id ORDER BY admittime) AS dischtime_lag,
                    LEAD(admittime) OVER (PARTITION BY subject_id ORDER BY admittime) AS admittime_lead
                FROM ADMISSIONS
            ),
            adm AS (
                -- fuzzy 边界：相邻住院间隔 < 24h 时取中间点，否则 ±12h
                SELECT
                    h.subject_id,
                    h.hadm_id,
                    CASE
                        WHEN h.dischtime_lag IS NOT NULL
                         AND h.dischtime_lag > h.admittime - INTERVAL '24' HOUR
                            THEN h.admittime
                               - CAST(date_diff('second', h.dischtime_lag, h.admittime) / 2 AS INTEGER)
                               * INTERVAL '1' SECOND
                        ELSE h.admittime - INTERVAL '12' HOUR
                    END AS data_start,
                    CASE
                        WHEN h.admittime_lead IS NOT NULL
                         AND h.admittime_lead < h.dischtime + INTERVAL '24' HOUR
                            THEN h.dischtime
                               + CAST(date_diff('second', h.dischtime, h.admittime_lead) / 2 AS INTEGER)
                               * INTERVAL '1' SECOND
                        ELSE h.dischtime + INTERVAL '12' HOUR
                    END AS data_end
                FROM h
            ),
            t1 AS (
                -- 在 fuzzy 边界内找首/末次心率
                SELECT
                    ce.icustay_id,
                    MIN(ce.charttime) AS intime_hr,
                    MAX(ce.charttime) AS outtime_hr
                FROM CHARTEVENTS ce
                INNER JOIN adm
                    ON  ce.hadm_id    = adm.hadm_id
                    AND ce.charttime >= adm.data_start
                    AND ce.charttime <  adm.data_end
                WHERE ce.itemid IN (211, 220045)
                  AND ce.icustay_id IS NOT NULL
                GROUP BY ce.icustay_id
            )
            SELECT
                c.subject_id,
                c.hadm_id,
                c.icustay_id,
                t1.intime_hr,
                t1.outtime_hr
            FROM cohort c
            LEFT JOIN t1 ON c.icustay_id = t1.icustay_id
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step_icustay_times done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 2: time axis
# Source: concepts/demographics/icustay_hours.sql
# ---------------------------------------------------------------------------
def step02_time_axis(con):
    name = "02_time_axis"
    if exists(name):
        log.info("step02 cached"); return
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE VIEW cohort       AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"CREATE OR REPLACE VIEW icu_times    AS SELECT * FROM read_parquet('{inter('icustay_times')}')")
    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT
                    it.subject_id,
                    it.hadm_id,
                    it.icustay_id,
                    -- 官方：ceiling intime_hr 到整点（+59min 再 truncate）
                    date_trunc('hour', it.intime_hr + INTERVAL '59' MINUTE) AS endtime,
                    -- 官方：小时数从 -24 到 CEIL((outtime_hr - intime_hr) 小时)
                    CAST(CEIL(
                        date_diff('minute', it.intime_hr, it.outtime_hr) / 60.0
                    ) AS INTEGER) AS max_hr
                FROM icu_times it
                WHERE it.intime_hr IS NOT NULL
                  AND it.outtime_hr IS NOT NULL
            )
            SELECT
                b.subject_id,
                b.hadm_id,
                b.icustay_id,
                CAST(h.generate_series AS INTEGER)                                        AS hr,
                b.endtime + CAST(h.generate_series AS INTEGER) * INTERVAL '1' HOUR       AS charttime_floor
            FROM base b,
                 generate_series(-24, b.max_hr) AS h
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step02 done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 3+6+10: single CHARTEVENTS scan → vitals, GCS, ventilation flags
# ---------------------------------------------------------------------------
def step03_06_10_chartevents(con):
    v_name    = "03_vitals_raw"
    g_name    = "06_gcs_raw"
    vent_name = "10_vent_raw"
    hw_name   = "hw"
    fio2_name = "fio2_chart"
    crrt_name = "crrt_cv"
    icp_name  = "icp"
    lines_name = "invasive_lines"
    code_name = "code_status"
    all_done  = all(exists(n) for n in [v_name, g_name, vent_name, hw_name, fio2_name, crrt_name, icp_name, lines_name, code_name])
    if all_done:
        log.info("step03/06/10 cached"); return
    t0 = time.time()
    log.info("scanning CHARTEVENTS (33GB) — this will take a while...")

    # Vitals itemids (from mimic-code pivoted_vital.sql BigQuery)
    vital_items = {
        "heartrate":  [211, 220045],
        "sysbp":      [51, 442, 455, 6701, 220179, 220050],
        "diasbp":     [8368, 8440, 8441, 8555, 220180, 220051],
        "meanbp":     [456, 52, 6702, 443, 220052, 220181, 225312],
        "resprate":   [618, 615, 220210, 224690],
        "tempc":      [223762, 676],
        "tempf":      [223761, 678],
        "spo2":       [646, 220277],
        "glucose":    [807, 811, 1529, 3745, 3744, 225664, 220621, 226537],
        "etco2":      [1817, 228232],
    }
    all_vital_ids = [i for ids in vital_items.values() for i in ids]

    # GCS itemids (from mimic-code pivoted_gcs.sql)
    gcs_motor_ids   = [454, 223901]
    gcs_verbal_ids  = [723, 223900]
    gcs_eyes_ids    = [184, 220739]
    all_gcs_ids     = gcs_motor_ids + gcs_verbal_ids + gcs_eyes_ids

    # Ventilation / oxygen-delivery / significant-event itemids
    # Source: durations/ventilation_classification.sql (WHERE clause, exact order)
    vent_ids = [
        720, 223849,                                              # vent mode
        223848,                                                   # vent type
        445, 448, 449, 450, 1340, 1486, 1600, 224687,            # minute volume
        639, 654, 681, 682, 683, 684, 224685, 224684, 224686,    # tidal volume
        218, 436, 535, 444, 224697, 224695, 224696, 224746, 224747,  # RespPressure
        221, 1, 1211, 1655, 2000, 226873, 224738, 224419, 224750, 227187,  # Insp pressure
        543,                                                      # PlateauPressure
        5865, 5866, 224707, 224709, 224705, 224706,              # APRV pressure
        60, 437, 505, 506, 686, 220339, 224700,                  # PEEP
        3459,                                                     # high pressure relief
        501, 502, 503, 224702,                                    # PCV
        223, 667, 668, 669, 670, 671, 672,                       # TCPCV
        224701,                                                   # PSVlevel
        640,                                                      # extubated
        468, 469, 470, 471, 227287,                              # O2 Delivery Device#2/Mode/Flow
        226732,                                                   # O2 Delivery Device(s)
        223834,                                                   # O2 Flow
        467,                                                      # O2 Delivery Device (vent+O2)
    ]

    # Height itemids (mimic-code pivoted_height.sql)
    # CareVue inches: 920,1394,4187,3486 | CareVue cm: 3485,4188
    # MV inches: 226707 | MV cm: 226730
    height_ids = [920, 1394, 4187, 3486, 3485, 4188, 226707, 226730]

    # Weight itemids (mimic-code weight_durations.sql)
    # 224639 = MetaVision daily weight (was missing); 4183 = birth weight free-text
    weight_ids = [762, 763, 3723, 3580, 226512, 3581, 3582, 224639, 4183]

    # FiO2 from CHARTEVENTS (mimic-code pivoted_bg.sql — chart FiO2)
    fio2_chart_ids = [3420, 3422, 190, 223835]

    # CHARTEVENTS itemids for dialysis detection (Source: pivot/pivoted_rrt.sql)
    crrt_cv_ids = [
        # CareVue dialysis items
        146, 147, 148, 149, 150, 151, 152, 582,
        # CareVue invasive line type items (value='Dialysis Line')
        229, 235, 241, 247, 253, 259, 265, 271,
        # MV checkboxes
        226118, 227357, 225725,
        # MV numeric
        226499, 224154, 225810, 225959, 227639, 225183, 227438, 224191,
        225806, 225807, 228004, 228005, 228006, 224144, 224145, 224149,
        224150, 224151, 224152, 224153, 224404, 224406, 226457,
        # MV text
        224135, 224139, 224146, 225323, 225740, 225776, 225951, 225952,
        225953, 225954, 225956, 225958, 225961, 225963, 225965, 225976,
        225977, 227124, 227290, 227638, 227640, 227753,
    ]

    code_status_ids = [128, 223758]
    icp_numeric_ids = [226, 1374, 2045, 2635, 2660, 2733, 2745, 2870, 2956, 2985, 5856, 7116, 8218, 8298, 8299, 8305, 220765, 227989]
    arterial_line_ids = [
        1221, 1322, 2256, 2308, 2424, 2627, 2744, 2952, 2969,
        3177, 3178, 5638, 5676, 5797, 5803, 5854, 6391, 46659,
        224284, 224285, 224287, 224288, 224289, 224290, 224291,
        225210, 225556, 225575, 225722, 225737, 225752, 226008,
        226107, 227292, 228022, 228023, 228026, 228027,
    ]
    cvl_ids = [
        3344, 4304, 5991, 7645, 224336, 224337, 224339, 224340,
        224341, 224345, 224347, 224348, 224349, 224351, 224468,
        224629, 225578, 225580, 225581, 225700, 225701, 225702,
        225704, 225705, 225706, 225707, 226005, 226230, 226232,
        227105, 228030, 228031, 228032, 228033, 228034, 228035,
        228036, 228037, 228038, 228039, 228040, 228384,
    ]
    pa_catheter_ids = [
        1704, 223773, 224469, 224472, 224476, 224478, 224479,
        224560, 224604, 224605, 224617, 224618, 224619, 224624,
        225351, 225352, 225353, 225354, 225355, 225356, 225357,
        225358, 225607, 225608, 225609, 225615, 225623, 225629,
        225630, 225631, 225633, 225646, 225647, 225648, 225745,
        226007, 226114, 226162, 227351, 227757, 228113, 228114,
        228115, 228116, 228117, 228118, 228119, 228120, 228121,
        228122, 228123, 228124,
    ]
    trauma_line_ids = [224268, 225213, 225214, 225216, 225217, 225218, 225317, 225735, 225750, 226119, 227353, 227762]
    ava_line_ids = [227719, 227725, 227726, 227727, 227728, 227731, 227732]
    icp_catheter_ids = [226124, 226125, 226126, 226127, 226128, 226129, 226130, 226131, 226132, 226133, 226134, 226474, 227363]
    official_line_type_site_ids = [229, 235, 241, 247, 253, 259, 265, 271, 8392, 8393, 8394, 8395, 8396, 8397, 8398, 8399]

    all_ids = list(set(
        all_vital_ids + all_gcs_ids + vent_ids +
        height_ids + weight_ids + fio2_chart_ids + crrt_cv_ids +
        code_status_ids + icp_numeric_ids + arterial_line_ids +
        cvl_ids + pa_catheter_ids + trauma_line_ids + ava_line_ids +
        icp_catheter_ids + official_line_type_site_ids
    ))
    ids_str = ",".join(str(i) for i in all_ids)

    con.execute(f"""
        CREATE OR REPLACE VIEW ce_filtered AS
        SELECT
            ce.icustay_id,
            ce.itemid,
            ce.charttime,
            ce.valuenum,
            ce.value,
            ce.error
        FROM CHARTEVENTS ce
        WHERE ce.itemid IN ({ids_str})
          AND (ce.error IS NULL OR ce.error != 1)
          AND ce.icustay_id IS NOT NULL
    """)

    # --- vitals pivot ---
    if not exists(v_name):
        heartrate_ids = ",".join(str(i) for i in vital_items["heartrate"])
        sysbp_ids     = ",".join(str(i) for i in vital_items["sysbp"])
        diasbp_ids    = ",".join(str(i) for i in vital_items["diasbp"])
        meanbp_ids    = ",".join(str(i) for i in vital_items["meanbp"])
        resprate_ids  = ",".join(str(i) for i in vital_items["resprate"])
        tempc_ids     = ",".join(str(i) for i in vital_items["tempc"])
        tempf_ids     = ",".join(str(i) for i in vital_items["tempf"])
        spo2_ids      = ",".join(str(i) for i in vital_items["spo2"])
        glucose_ids   = ",".join(str(i) for i in vital_items["glucose"])
        etco2_ids     = ",".join(str(i) for i in vital_items["etco2"])

        con.execute(f"""
            COPY (
                SELECT
                    icustay_id,
                    date_trunc('hour', charttime) AS charttime_floor,
                    AVG(CASE WHEN itemid IN ({heartrate_ids}) AND valuenum > 0 AND valuenum < 300 THEN valuenum END) AS heartrate,
                    AVG(CASE WHEN itemid IN ({sysbp_ids})    AND valuenum > 0 AND valuenum < 400 THEN valuenum END) AS sysbp,
                    AVG(CASE WHEN itemid IN ({diasbp_ids})   AND valuenum > 0 AND valuenum < 300 THEN valuenum END) AS diasbp,
                    AVG(CASE WHEN itemid IN ({meanbp_ids})   AND valuenum > 0 AND valuenum < 300 THEN valuenum END) AS meanbp,
                    AVG(CASE WHEN itemid IN ({resprate_ids}) AND valuenum > 0 AND valuenum < 70  THEN valuenum END) AS resprate,
                    AVG(CASE WHEN itemid IN ({tempc_ids})    AND valuenum > 10 AND valuenum < 50 THEN valuenum END) AS tempc,
                    AVG(CASE WHEN itemid IN ({tempf_ids})    AND valuenum > 70 AND valuenum < 120
                             THEN (valuenum - 32.0) / 1.8 END) AS tempc_fromf,
                    AVG(CASE WHEN itemid IN ({spo2_ids})     AND valuenum > 0 AND valuenum <= 100 THEN valuenum END) AS spo2,
                    AVG(CASE WHEN itemid IN ({glucose_ids})  AND valuenum > 0                     THEN valuenum END) AS glucose,
                    AVG(CASE WHEN itemid IN ({etco2_ids})    AND valuenum > 0 AND valuenum < 100  THEN valuenum END) AS etco2
                FROM ce_filtered
                WHERE itemid IN ({",".join(str(i) for i in all_vital_ids)})
                GROUP BY icustay_id, date_trunc('hour', charttime)
            ) TO '{inter(v_name)}' (FORMAT PARQUET)
        """)
        log.info("step03 vitals done")

    # --- GCS pivot ---
    # Source: pivot/pivoted_gcs.sql + firstday/gcs_first_day.sql
    if not exists(g_name):
        all_g_ids = ",".join(str(i) for i in all_gcs_ids)

        con.execute(f"""
            COPY (
                WITH base AS (
                    SELECT
                        ce.icustay_id,
                        ce.charttime,
                        MAX(CASE WHEN ce.itemid IN (454, 223901) THEN ce.valuenum ELSE NULL END) AS gcsmotor,
                        MAX(CASE
                            WHEN ce.itemid = 723    AND ce.value = '1.0 ET/Trach'    THEN 0
                            WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 0
                            WHEN ce.itemid IN (723, 223900) THEN ce.valuenum
                            ELSE NULL
                        END) AS gcsverbal,
                        MAX(CASE WHEN ce.itemid IN (184, 220739) THEN ce.valuenum ELSE NULL END) AS gcseyes,
                        MAX(CASE
                            WHEN ce.itemid = 723    AND ce.value = '1.0 ET/Trach'    THEN 1
                            WHEN ce.itemid = 223900 AND ce.value = 'No Response-ETT' THEN 1
                            ELSE 0
                        END) AS endotrachflag,
                        ROW_NUMBER() OVER (
                            PARTITION BY ce.icustay_id ORDER BY ce.charttime ASC
                        ) AS rn
                    FROM ce_filtered ce
                    WHERE ce.itemid IN ({all_g_ids})
                    GROUP BY ce.icustay_id, ce.charttime
                ),
                gcs_stg0 AS (
                    SELECT
                        b.icustay_id, b.charttime, b.endotrachflag,
                        b.gcsmotor,  b.gcsverbal,  b.gcseyes,
                        b2.gcsmotor  AS gcsmotorprev,
                        b2.gcsverbal AS gcsverbalprev,
                        b2.gcseyes   AS gcseyesprev,
                        CASE
                            WHEN b.gcsverbal = 0
                                THEN 15
                            WHEN b.gcsverbal IS NULL AND b2.gcsverbal = 0
                                THEN 15
                            WHEN b2.gcsverbal = 0
                                THEN COALESCE(b.gcsmotor, 6)
                                   + COALESCE(b.gcsverbal, 5)
                                   + COALESCE(b.gcseyes,  4)
                            ELSE
                                  COALESCE(b.gcsmotor,  COALESCE(b2.gcsmotor,  6))
                                + COALESCE(b.gcsverbal, COALESCE(b2.gcsverbal, 5))
                                + COALESCE(b.gcseyes,   COALESCE(b2.gcseyes,   4))
                        END AS gcs
                    FROM base b
                    LEFT JOIN base b2
                        ON  b.icustay_id = b2.icustay_id
                        AND b.rn = b2.rn + 1
                        AND b2.charttime > b.charttime - INTERVAL '6' HOUR
                ),
                gcs_stg1 AS (
                    SELECT
                        icustay_id, charttime, gcs, endotrachflag,
                        COALESCE(gcsmotor,  gcsmotorprev)  AS gcsmotor,
                        COALESCE(gcsverbal, gcsverbalprev) AS gcsverbal,
                        COALESCE(gcseyes,   gcseyesprev)   AS gcseyes,
                        CASE WHEN COALESCE(gcsmotor,  gcsmotorprev)  IS NULL THEN 0 ELSE 1 END
                      + CASE WHEN COALESCE(gcsverbal, gcsverbalprev) IS NULL THEN 0 ELSE 1 END
                      + CASE WHEN COALESCE(gcseyes,   gcseyesprev)   IS NULL THEN 0 ELSE 1 END
                            AS components_measured
                    FROM gcs_stg0
                ),
                gcs_priority AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY icustay_id, charttime
                            ORDER BY components_measured DESC, endotrachflag, gcs, charttime DESC
                        ) AS rn
                    FROM gcs_stg1
                )
                SELECT
                    icustay_id,
                    date_trunc('hour', charttime) AS charttime_floor,
                    MIN(gcs)           AS gcs_total,
                    MIN(gcsmotor)      AS gcs_motor,
                    MIN(gcsverbal)     AS gcs_verbal,
                    MIN(gcseyes)       AS gcs_eyes,
                    MAX(endotrachflag) AS gcs_sedated
                FROM gcs_priority
                WHERE rn = 1
                GROUP BY icustay_id, date_trunc('hour', charttime)
            ) TO '{inter(g_name)}' (FORMAT PARQUET)
        """)
        log.info("step06 GCS done")

    # --- ventilation flags ---
    if not exists(vent_name):
        vent_ids_str = ",".join(str(i) for i in vent_ids)
        con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
        con.execute(f"""
            COPY (
                WITH vent_cls AS (
                    SELECT
                        icustay_id,
                        charttime,
                        MAX(CASE
                            WHEN itemid IS NULL OR value IS NULL THEN 0
                            WHEN itemid = 720 AND value != 'Other/Remarks' THEN 1
                            WHEN itemid = 223848 AND value != 'Other' THEN 1
                            WHEN itemid = 223849 THEN 1
                            WHEN itemid = 467 AND value = 'Ventilator' THEN 1
                            WHEN itemid IN (
                                445, 448, 449, 450, 1340, 1486, 1600, 224687,
                                639, 654, 681, 682, 683, 684, 224685, 224684, 224686,
                                218, 436, 535, 444, 459, 224697, 224695, 224696, 224746, 224747,
                                221, 1, 1211, 1655, 2000, 226873, 224738, 224419, 224750, 227187,
                                543, 5865, 5866, 224707, 224709, 224705, 224706,
                                60, 437, 505, 506, 686, 220339, 224700, 3459,
                                501, 502, 503, 224702, 223, 667, 668, 669, 670, 671, 672, 224701
                            ) THEN 1
                            ELSE 0
                        END) AS mechvent,
                        MAX(CASE
                            WHEN itemid = 226732 AND value IN (
                                'Nasal cannula', 'Face tent', 'Aerosol-cool', 'Trach mask ',
                                'High flow neb', 'Non-rebreather', 'Venti mask ', 'Medium conc mask ',
                                'T-piece', 'High flow nasal cannula', 'Ultrasonic neb', 'Vapomist'
                            ) THEN 1
                            WHEN itemid = 467 AND value IN (
                                'Cannula', 'Nasal Cannula', 'Face Tent', 'Aerosol-Cool', 'Trach Mask',
                                'Hi Flow Neb', 'Non-Rebreather', 'Venti Mask', 'Medium Conc Mask',
                                'Vapotherm', 'T-Piece', 'Hood', 'Hut', 'TranstrachealCat',
                                'Heated Neb', 'Ultrasonic Neb'
                            ) THEN 1
                            ELSE 0
                        END) AS oxygentherapy,
                        MAX(CASE
                            WHEN itemid = 640 AND value = 'Extubated' THEN 1
                            WHEN itemid = 640 AND value = 'Self Extubation' THEN 1
                            ELSE 0
                        END) AS extubated,
                        MAX(CASE WHEN itemid = 640 AND value = 'Self Extubation' THEN 1 ELSE 0 END) AS selfextubated,
                        MAX(CASE WHEN itemid IN (467, 226732) AND value IN (
                                'Bipap', 'Bipap Mask', 'Bipap mask', 'Bipap mask ',
                                'BiPap', 'BiPap Mode', 'BiPap Mask'
                            ) THEN 1 ELSE 0 END) AS niv_event,
                        MAX(CASE WHEN itemid IN (467, 226732) AND value IN (
                                'CPAP mask', 'CPAP mask ', 'CPAP', 'Cpap', 'Autoset/CPAP'
                            ) THEN 1 ELSE 0 END) AS cpap_event
                    FROM ce_filtered
                    WHERE itemid IN ({vent_ids_str})
                      AND value IS NOT NULL
                    GROUP BY icustay_id, charttime
                    UNION DISTINCT
                    SELECT
                        icustay_id,
                        starttime AS charttime,
                        0 AS mechvent,
                        0 AS oxygentherapy,
                        1 AS extubated,
                        CASE WHEN itemid = 225468 THEN 1 ELSE 0 END AS selfextubated,
                        0 AS niv_event,
                        0 AS cpap_event
                    FROM PROCEDUREEVENTS_MV
                    WHERE itemid IN (227194, 225468, 225477)
                      AND icustay_id IS NOT NULL
                      AND starttime IS NOT NULL
                      AND statusdescription != 'Rewritten'
                ),
                vd0 AS (
                    SELECT
                        icustay_id,
                        CASE WHEN mechvent = 1
                            THEN LAG(charttime, 1) OVER (PARTITION BY icustay_id, mechvent ORDER BY charttime)
                            ELSE NULL
                        END AS charttime_lag,
                        charttime,
                        mechvent,
                        oxygentherapy,
                        extubated,
                        selfextubated,
                        niv_event,
                        cpap_event
                    FROM vent_cls
                ),
                vd1 AS (
                    SELECT
                        *,
                        CASE WHEN mechvent = 1
                            THEN date_diff('minute', charttime_lag, charttime) / 60.0
                            ELSE NULL
                        END AS ventduration,
                        LAG(extubated, 1) OVER (
                            PARTITION BY icustay_id, CASE WHEN mechvent = 1 OR extubated = 1 THEN 1 ELSE 0 END
                            ORDER BY charttime
                        ) AS extubatedlag,
                        CASE
                            -- extubation is not a new vent event; the subsequent row is
                            WHEN LAG(extubated, 1) OVER (
                                PARTITION BY icustay_id, CASE WHEN mechvent = 1 OR extubated = 1 THEN 1 ELSE 0 END
                                ORDER BY charttime
                            ) = 1 THEN 1
                            WHEN mechvent = 0 AND oxygentherapy = 1 THEN 1
                            WHEN charttime > charttime_lag + INTERVAL '8' HOUR THEN 1
                            ELSE 0
                        END AS newvent
                    FROM vd0
                ),
                vd2 AS (
                    SELECT
                        CASE
                            WHEN mechvent = 1 OR extubated = 1
                            THEN SUM(newvent) OVER (PARTITION BY icustay_id ORDER BY charttime)
                            ELSE NULL
                        END AS ventnum,
                        *
                    FROM vd1
                ),
                vent_durations AS (
                    SELECT
                        icustay_id,
                        ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY ventnum) AS ventnum_seq,
                        MIN(charttime) AS starttime,
                        MAX(charttime) AS endtime
                    FROM vd2
                    GROUP BY icustay_id, ventnum
                    HAVING MIN(charttime) != MAX(charttime)
                       AND MAX(mechvent) = 1
                ),
                invasive_hours AS (
                    SELECT
                        t.icustay_id,
                        t.charttime_floor,
                        1 AS vent_invasive_flag
                    FROM time_axis t
                    JOIN vent_durations vd
                      ON t.icustay_id = vd.icustay_id
                     AND t.charttime_floor >= vd.starttime
                     AND t.charttime_floor <= vd.endtime
                    GROUP BY t.icustay_id, t.charttime_floor
                ),
                point_hours AS (
                    SELECT
                        icustay_id,
                        date_trunc('hour', charttime) AS charttime_floor,
                        MAX(cpap_event) AS cpap_flag,
                        MAX(niv_event) AS niv_only_flag,
                        MAX(oxygentherapy) AS oxygen_therapy_flag,
                        MAX(extubated) AS extubated_flag,
                        MAX(selfextubated) AS self_extubated_flag
                    FROM vent_cls
                    GROUP BY icustay_id, date_trunc('hour', charttime)
                ),
                vent_hourly AS (
                    SELECT
                        COALESCE(ih.icustay_id, ph.icustay_id) AS icustay_id,
                        COALESCE(ih.charttime_floor, ph.charttime_floor) AS charttime_floor,
                        COALESCE(ih.vent_invasive_flag, 0) AS vent_invasive_flag,
                        CASE WHEN COALESCE(ph.cpap_flag, 0) = 1 OR COALESCE(ph.niv_only_flag, 0) = 1 THEN 1 ELSE 0 END AS vent_noninvasive_flag,
                        COALESCE(ph.cpap_flag, 0) AS cpap_flag,
                        COALESCE(ph.oxygen_therapy_flag, 0) AS oxygen_therapy_flag,
                        COALESCE(ph.extubated_flag, 0) AS extubated_flag,
                        COALESCE(ph.self_extubated_flag, 0) AS self_extubated_flag
                    FROM invasive_hours ih
                    FULL OUTER JOIN point_hours ph
                      ON ih.icustay_id = ph.icustay_id
                     AND ih.charttime_floor = ph.charttime_floor
                )
                SELECT
                    icustay_id,
                    charttime_floor,
                    vent_invasive_flag,
                    vent_noninvasive_flag,
                    cpap_flag,
                    oxygen_therapy_flag,
                    CASE
                        WHEN vent_invasive_flag = 1 THEN 1
                        WHEN vent_noninvasive_flag = 1 THEN 1
                        ELSE 0
                    END AS vent_flag,
                    CASE
                        WHEN vent_invasive_flag = 1 THEN 'InvasiveVent'
                        WHEN cpap_flag = 1 THEN 'CPAP'
                        WHEN vent_noninvasive_flag = 1 THEN 'NonInvasiveVent'
                        WHEN oxygen_therapy_flag = 1 THEN 'SupplementalOxygen'
                        ELSE 'None'
                    END AS vent_status,
                    extubated_flag,
                    self_extubated_flag
                FROM vent_hourly
            ) TO '{inter(vent_name)}' (FORMAT PARQUET)
        """)
        log.info("step10 vent done")

    # --- height / weight per ICU stay
    # Source: concepts/demographics/heightweight.sql + concepts/durations/weight_durations.sql ---
    if not exists(hw_name):
        con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
        ht_ids_str = ",".join(str(i) for i in height_ids)
        con.execute(f"""
            COPY (
                -- Source: pivot/pivoted_height.sql
                WITH ht_raw AS (
                    SELECT
                        ce.icustay_id,
                        ce.charttime,
                        pt.dob,
                        CASE
                            WHEN ce.itemid IN (920, 1394, 4187, 3486, 226707)
                                THEN ce.valuenum * 2.54
                            WHEN ce.itemid IN (3485, 4188, 226730)
                                THEN ce.valuenum
                        END AS height_raw
                    FROM ce_filtered ce
                    JOIN ICUSTAYS ie ON ce.icustay_id = ie.icustay_id
                    JOIN PATIENTS pt ON ie.subject_id = pt.subject_id
                    WHERE ce.itemid IN ({ht_ids_str})
                      AND ce.valuenum IS NOT NULL
                      AND ce.valuenum != 0
                ),
                ht_chartevents AS (
                    SELECT icustay_id, charttime,
                        CASE
                            WHEN date_diff('year', dob, charttime) <= 1
                             AND height_raw < 80
                                THEN height_raw
                            WHEN date_diff('year', dob, charttime) > 1
                             AND height_raw > 120 AND height_raw < 230
                                THEN height_raw
                        END AS height_cm
                    FROM ht_raw
                ),
                echo_ht_raw AS (
                    SELECT
                        ne.hadm_id,
                        TRY_CAST(
                            regexp_extract(ne.text, 'Date/Time: .+? at ([0-9]+:[0-9]{{2}})', 1)
                            AS VARCHAR
                        ) AS echo_time_str,
                        CAST(ne.chartdate AS DATE) AS chartdate,
                        TRY_CAST(
                            regexp_extract(ne.text, 'Height: \\(in\\) ([0-9]+\\.?[0-9]*)', 1)
                            AS DOUBLE
                        ) AS height_in
                    FROM NOTEEVENTS ne
                    WHERE ne.category = 'Echo'
                      AND ne.hadm_id IS NOT NULL
                ),
                echo_ht AS (
                    SELECT
                        ie.icustay_id,
                        CASE
                            WHEN echo_time_str IS NOT NULL AND echo_time_str != ''
                                THEN strptime(
                                    strftime(chartdate, '%Y-%m-%d') || echo_time_str || ':00',
                                    '%Y-%m-%d%H:%M:%S'
                                )
                            ELSE CAST(chartdate AS TIMESTAMP)
                        END AS charttime,
                        height_in * 2.54 AS height_cm
                    FROM echo_ht_raw er
                    JOIN ICUSTAYS ie ON er.hadm_id = ie.hadm_id
                    WHERE er.height_in IS NOT NULL AND er.height_in > 0
                ),
                ibw_raw AS (
                    SELECT
                        ne.hadm_id,
                        COALESCE(TRY_CAST(ne.charttime AS TIMESTAMP), CAST(ne.chartdate AS TIMESTAMP)) AS charttime,
                        TRY_CAST(
                            regexp_extract(ne.text, 'Ideal body weight: ([0-9]+\\.?[0-9]*)', 1)
                            AS DOUBLE
                        ) AS ibw
                    FROM NOTEEVENTS ne
                    WHERE ne.text LIKE '%Ideal body weight:%'
                      AND ne.category != 'Echo'
                      AND ne.hadm_id IS NOT NULL
                ),
                ht_from_ibw AS (
                    SELECT
                        ie.icustay_id,
                        ir.charttime,
                        CASE
                            WHEN pt.gender = 'F' THEN (ir.ibw - 45.5) / 0.91 + 152.4
                            ELSE                      (ir.ibw - 50.0) / 0.91 + 152.4
                        END AS height_cm
                    FROM ibw_raw ir
                    JOIN ICUSTAYS ie ON ir.hadm_id = ie.hadm_id
                    JOIN PATIENTS pt ON ie.subject_id = pt.subject_id
                    WHERE ir.ibw IS NOT NULL AND ir.ibw != 0
                ),
                ht_nutrition AS (
                    SELECT
                        ie.icustay_id,
                        COALESCE(TRY_CAST(ne.charttime AS TIMESTAMP), CAST(ne.chartdate AS TIMESTAMP)) AS charttime,
                        CASE
                            WHEN TRY_CAST(regexp_extract(ne.text, '([0-9]+) cm', 1) AS DOUBLE) < 80
                                THEN TRY_CAST(regexp_extract(ne.text, '([0-9]+) cm', 1) AS DOUBLE) * 2.54
                            ELSE
                                TRY_CAST(regexp_extract(ne.text, '([0-9]+) cm', 1) AS DOUBLE)
                        END AS height_cm
                    FROM NOTEEVENTS ne
                    JOIN ICUSTAYS ie ON ne.hadm_id = ie.hadm_id
                    WHERE ne.category = 'Nutrition'
                      AND lower(ne.text) LIKE '%height%'
                      AND ne.hadm_id IS NOT NULL
                      AND TRY_CAST(regexp_extract(ne.text, '([0-9]+) cm', 1) AS DOUBLE) > 0
                ),
                ht_stg AS (
                    SELECT icustay_id, charttime, height_cm FROM ht_chartevents
                        WHERE height_cm IS NOT NULL
                    UNION ALL
                    SELECT icustay_id, charttime, height_cm FROM echo_ht
                        WHERE height_cm IS NOT NULL AND height_cm > 0
                    UNION ALL
                    SELECT icustay_id, charttime, height_cm FROM ht_from_ibw
                        WHERE height_cm IS NOT NULL AND height_cm > 0
                    UNION ALL
                    SELECT icustay_id, charttime, height_cm FROM ht_nutrition
                        WHERE height_cm IS NOT NULL AND height_cm > 0
                ),
                -- Source: durations/weight_durations.sql
                wt_neonate AS (
                    SELECT
                        ce.icustay_id, ce.charttime,
                        MAX(CASE WHEN ce.itemid = 3580 THEN ce.valuenum END) AS wt_kg,
                        MAX(CASE WHEN ce.itemid = 3581 THEN ce.valuenum END) AS wt_lb,
                        MAX(CASE WHEN ce.itemid = 3582 THEN ce.valuenum END) AS wt_oz
                    FROM ce_filtered ce
                    WHERE ce.itemid IN (3580, 3581, 3582)
                      AND ce.valuenum > 0
                    GROUP BY ce.icustay_id, ce.charttime
                ),
                birth_wt AS (
                    SELECT
                        ce.icustay_id, ce.charttime,
                        MAX(CASE
                            WHEN ce.itemid = 4183 THEN
                                CASE
                                    WHEN regexp_matches(ce.value, '[^0-9.]') THEN NULL
                                    WHEN TRY_CAST(ce.value AS DOUBLE) > 100
                                        THEN TRY_CAST(ce.value AS DOUBLE) / 1000
                                    WHEN TRY_CAST(ce.value AS DOUBLE) < 10
                                        THEN TRY_CAST(ce.value AS DOUBLE)
                                    ELSE NULL
                                END
                            WHEN ce.itemid = 3723 AND ce.valuenum < 10 THEN ce.valuenum
                            ELSE NULL
                        END) AS wt_kg
                    FROM ce_filtered ce
                    WHERE ce.itemid IN (3723, 4183)
                    GROUP BY ce.icustay_id, ce.charttime
                ),
                wt_stg AS (
                    SELECT
                        ce.icustay_id,
                        ce.charttime,
                        CASE
                            WHEN ce.itemid IN (762, 226512) THEN 'admit'
                            ELSE 'daily'
                        END AS weight_type,
                        ce.valuenum AS weight
                    FROM ce_filtered ce
                    WHERE ce.itemid IN (762, 226512, 763, 224639)
                      AND ce.valuenum IS NOT NULL
                      AND ce.valuenum > 0
                    UNION ALL
                    SELECT
                        icustay_id, charttime,
                        'daily' AS weight_type,
                        CASE
                            WHEN wt_kg IS NOT NULL THEN wt_kg
                            WHEN wt_lb IS NOT NULL THEN wt_lb * 0.45359237 + wt_oz * 0.0283495231
                            ELSE NULL
                        END AS weight
                    FROM wt_neonate
                    UNION ALL
                    SELECT icustay_id, charttime, 'admit' AS weight_type, wt_kg AS weight
                    FROM birth_wt
                    WHERE wt_kg IS NOT NULL
                ),
                -- 官方 echo_data.sql：从 NOTEEVENTS 解析心超记录中的体重
                -- 仅用于无任何 CHARTEVENTS 体重记录的患者（官方注释：约补全 2500 人）
                echo_raw AS (
                    SELECT
                        ne.hadm_id,
                        TRY_CAST(
                            regexp_extract(ne.text, 'Date/Time: .+? at ([0-9]+:[0-9]{{2}})', 1)
                            AS VARCHAR
                        ) AS echo_time_str,
                        CAST(ne.chartdate AS DATE) AS chartdate,
                        TRY_CAST(
                            regexp_extract(ne.text, 'Weight \\(lb\\): ([0-9]+)', 1)
                            AS DOUBLE
                        ) AS weight_lb
                    FROM NOTEEVENTS ne
                    WHERE ne.category = 'Echo'
                      AND ne.hadm_id IS NOT NULL
                ),
                echo AS (
                    SELECT
                        ie.icustay_id,
                        CASE
                            WHEN echo_time_str IS NOT NULL AND echo_time_str != ''
                                THEN strptime(
                                    strftime(chartdate, '%Y-%m-%d') || echo_time_str || ':00',
                                    '%Y-%m-%d%H:%M:%S'
                                )
                            ELSE CAST(chartdate AS TIMESTAMP)
                        END AS charttime,
                        'echo' AS weight_type,
                        weight_lb * 0.453592 AS weight
                    FROM echo_raw er
                    JOIN ICUSTAYS ie ON er.hadm_id = ie.hadm_id
                    WHERE er.weight_lb IS NOT NULL
                      AND er.weight_lb > 0
                      -- 官方：只用于无 CHARTEVENTS 体重记录的患者
                      AND ie.icustay_id NOT IN (
                          SELECT DISTINCT icustay_id FROM wt_stg
                      )
                ),
                wt_stg0 AS (
                    SELECT icustay_id, charttime, weight_type, weight FROM wt_stg
                    UNION ALL
                    SELECT icustay_id, charttime, weight_type, weight FROM echo
                ),
                wt_stg1 AS (
                    SELECT
                        icustay_id,
                        charttime,
                        weight_type,
                        weight,
                        ROW_NUMBER() OVER (
                            PARTITION BY icustay_id, weight_type
                            ORDER BY charttime
                        ) AS rn
                    FROM wt_stg0
                    WHERE weight BETWEEN 20 AND 300
                ),
                wt_stg2 AS (
                    -- 官方：第一条 admit weight 的 starttime 设为 intime - 2h
                    SELECT
                        s.icustay_id,
                        c.intime,
                        c.outtime,
                        CASE
                            WHEN s.weight_type = 'admit' AND s.rn = 1
                                THEN c.intime - INTERVAL '2' HOUR
                            ELSE s.charttime
                        END AS starttime,
                        s.weight
                    FROM wt_stg1 s
                    JOIN ICUSTAYS c ON s.icustay_id = c.icustay_id
                ),
                wt_stg3 AS (
                    -- 官方：endtime = 下一条记录的 starttime，最后一条用 MAX(outtime,starttime)+2h
                    SELECT
                        icustay_id,
                        intime,
                        outtime,
                        starttime,
                        COALESCE(
                            LEAD(starttime) OVER (PARTITION BY icustay_id ORDER BY starttime),
                            GREATEST(outtime, starttime) + INTERVAL '2' HOUR
                        ) AS endtime,
                        weight
                    FROM wt_stg2
                ),
                wt1 AS (
                    SELECT
                        icustay_id,
                        starttime,
                        COALESCE(
                            endtime,
                            LEAD(starttime) OVER (PARTITION BY icustay_id ORDER BY starttime),
                            outtime + INTERVAL '2' HOUR
                        ) AS endtime,
                        weight
                    FROM wt_stg3
                ),
                wt_fix AS (
                    -- 官方：若 intime < 第一条 weight 的 starttime，回填该时段
                    SELECT
                        c.icustay_id,
                        c.intime - INTERVAL '2' HOUR AS starttime,
                        first_wt.starttime           AS endtime,
                        first_wt.weight
                    FROM ICUSTAYS c
                    JOIN (
                        SELECT
                            icustay_id,
                            starttime,
                            weight,
                            ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY starttime) AS rn
                        FROM wt1
                    ) first_wt
                      ON c.icustay_id = first_wt.icustay_id
                     AND first_wt.rn  = 1
                     AND c.intime     < first_wt.starttime
                ),
                weight_durations AS (
                    SELECT icustay_id, starttime, endtime, weight FROM wt1
                    UNION ALL
                    SELECT icustay_id, starttime, endtime, weight FROM wt_fix
                ),
                -- 官方 heightweight.sql：从 weight_durations 取 first/min/max
                wt_agg AS (
                    SELECT
                        icustay_id,
                        weight,
                        ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY starttime) AS rn
                    FROM weight_durations
                ),
                ht_ranked AS (
                    SELECT
                        icustay_id,
                        height_cm,
                        ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY charttime) AS rn
                    FROM ht_stg
                    WHERE height_cm IS NOT NULL
                )
                SELECT
                    c.icustay_id,
                    ROUND(CAST(MIN(CASE WHEN h.rn = 1 THEN h.height_cm END) AS DOUBLE), 2) AS height_first,
                    ROUND(CAST(MIN(h.height_cm)                             AS DOUBLE), 2) AS height_min,
                    ROUND(CAST(MAX(h.height_cm)                             AS DOUBLE), 2) AS height_max,
                    ROUND(CAST(MIN(CASE WHEN w.rn = 1 THEN w.weight    END) AS DOUBLE), 2) AS weight_first,
                    ROUND(CAST(MIN(w.weight)                                AS DOUBLE), 2) AS weight_min,
                    ROUND(CAST(MAX(w.weight)                                AS DOUBLE), 2) AS weight_max
                FROM cohort c
                LEFT JOIN ht_ranked h ON c.icustay_id = h.icustay_id
                LEFT JOIN wt_agg    w ON c.icustay_id = w.icustay_id
                GROUP BY c.icustay_id
            ) TO '{inter(hw_name)}' (FORMAT PARQUET)
        """)
        log.info("step_hw height/weight done")

    # --- FiO2 from CHARTEVENTS per hour ---
    if not exists(fio2_name):
        fio2_ids_str = ",".join(str(i) for i in fio2_chart_ids)
        con.execute(f"""
            COPY (
                SELECT
                    icustay_id,
                    date_trunc('hour', charttime) AS charttime_floor,
                    AVG(CASE
                        -- 223835: MV Inspired O2 Fraction (官方 pivoted_fio2.sql)
                        WHEN itemid = 223835 AND valuenum > 0   AND valuenum <= 1   THEN valuenum * 100
                        WHEN itemid = 223835 AND valuenum > 1   AND valuenum < 21   THEN NULL
                        WHEN itemid = 223835 AND valuenum >= 21 AND valuenum <= 100  THEN valuenum
                        -- 190: CV FiO2 set (小数格式)
                        WHEN itemid = 190    AND valuenum > 0.20 AND valuenum < 1   THEN valuenum * 100
                        -- 3420, 3422: CV FiO2，值已是百分比，直接用
                        WHEN itemid IN (3420, 3422) AND valuenum > 0 AND valuenum < 100 THEN valuenum
                        ELSE NULL
                    END) AS fio2_chartevents
                FROM ce_filtered
                WHERE itemid IN ({fio2_ids_str})
                GROUP BY icustay_id, date_trunc('hour', charttime)
            ) TO '{inter(fio2_name)}' (FORMAT PARQUET)
        """)
        log.info("step_fio2_chart done")

    # --- CRRT CHARTEVENTS rows — save itemid+value+valuenum for pivoted_rrt.sql CASE logic ---
    # Source: pivot/pivoted_rrt.sql (ce CTE)
    if not exists(crrt_name):
        crrt_ids_str = ",".join(str(i) for i in crrt_cv_ids)
        con.execute(f"""
            COPY (
                SELECT
                    icustay_id,
                    date_trunc('hour', charttime) AS charttime_floor,
                    itemid,
                    value,
                    valuenum
                FROM ce_filtered
                WHERE itemid IN ({crrt_ids_str})
                  AND value IS NOT NULL
            ) TO '{inter(crrt_name)}' (FORMAT PARQUET)
        """)
        log.info("step_crrt_cv done")

    if not exists(icp_name):
        icp_ids_str = ",".join(str(i) for i in icp_numeric_ids)
        con.execute(f"""
            COPY (
                SELECT
                    icustay_id,
                    date_trunc('hour', charttime) AS charttime_floor,
                    MAX(CASE WHEN itemid IN ({icp_ids_str}) AND valuenum > 0 AND valuenum < 100 THEN valuenum END) AS icp
                FROM ce_filtered
                WHERE itemid IN ({icp_ids_str})
                GROUP BY icustay_id, date_trunc('hour', charttime)
            ) TO '{inter(icp_name)}' (FORMAT PARQUET)
        """)
        log.info("step_icp done")

    if not exists(lines_name):
        official_pe_line_ids = [224263, 224264, 224267, 224268, 224270, 224272, 225199, 225202, 225203, 225315, 225752, 225789, 227719, 228286]
        arterial_mv_ids = [225752, 224272]
        pa_start_ids = [224560, 225354, 226114] + [1704, 223773]
        pa_stop_ids = [225745]
        trauma_start_ids = [224268, 225317, 226119]
        trauma_stop_ids = [225750]
        ava_start_ids = [227719, 227727]
        ava_stop_ids = [227725]
        icp_catheter_start_ids = [226124, 226128, 226129, 226474]
        icp_catheter_stop_ids = [226125]

        official_pe_line_ids_str = ",".join(str(i) for i in official_pe_line_ids)
        arterial_mv_ids_str = ",".join(str(i) for i in arterial_mv_ids)
        pa_start_str = ",".join(str(i) for i in pa_start_ids)
        pa_stop_str = ",".join(str(i) for i in pa_stop_ids)
        trauma_start_str = ",".join(str(i) for i in trauma_start_ids)
        trauma_stop_str = ",".join(str(i) for i in trauma_stop_ids)
        ava_start_str = ",".join(str(i) for i in ava_start_ids)
        ava_stop_str = ",".join(str(i) for i in ava_stop_ids)
        icp_catheter_start_str = ",".join(str(i) for i in icp_catheter_start_ids)
        icp_catheter_stop_str = ",".join(str(i) for i in icp_catheter_stop_ids)

        con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
        con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
        con.execute(f"""
            COPY (
                WITH line_starts AS (
                    SELECT c.icustay_id, MIN(ce.charttime) AS starttime, c.outtime, 'pa' AS line_type
                    FROM cohort c
                    JOIN ce_filtered ce ON c.icustay_id = ce.icustay_id
                    WHERE ce.itemid IN ({pa_start_str})
                    GROUP BY c.icustay_id, c.outtime
                    UNION ALL
                    SELECT c.icustay_id, MIN(ce.charttime) AS starttime, c.outtime, 'trauma' AS line_type
                    FROM cohort c
                    JOIN ce_filtered ce ON c.icustay_id = ce.icustay_id
                    WHERE ce.itemid IN ({trauma_start_str})
                    GROUP BY c.icustay_id, c.outtime
                    UNION ALL
                    SELECT c.icustay_id, MIN(ce.charttime) AS starttime, c.outtime, 'ava' AS line_type
                    FROM cohort c
                    JOIN ce_filtered ce ON c.icustay_id = ce.icustay_id
                    WHERE ce.itemid IN ({ava_start_str})
                    GROUP BY c.icustay_id, c.outtime
                    UNION ALL
                    SELECT c.icustay_id, MIN(ce.charttime) AS starttime, c.outtime, 'icp_catheter' AS line_type
                    FROM cohort c
                    JOIN ce_filtered ce ON c.icustay_id = ce.icustay_id
                    WHERE ce.itemid IN ({icp_catheter_start_str})
                    GROUP BY c.icustay_id, c.outtime
                ),
                art_cv_grp AS (
                    SELECT
                        ce.icustay_id,
                        ce.charttime,
                        MAX(CASE WHEN itemid = 229 THEN value ELSE NULL END) AS inv1_type,
                        MAX(CASE WHEN itemid = 235 THEN value ELSE NULL END) AS inv2_type,
                        MAX(CASE WHEN itemid = 241 THEN value ELSE NULL END) AS inv3_type,
                        MAX(CASE WHEN itemid = 247 THEN value ELSE NULL END) AS inv4_type,
                        MAX(CASE WHEN itemid = 253 THEN value ELSE NULL END) AS inv5_type,
                        MAX(CASE WHEN itemid = 259 THEN value ELSE NULL END) AS inv6_type,
                        MAX(CASE WHEN itemid = 265 THEN value ELSE NULL END) AS inv7_type,
                        MAX(CASE WHEN itemid = 271 THEN value ELSE NULL END) AS inv8_type
                    FROM ce_filtered ce
                    WHERE ce.itemid IN (229, 235, 241, 247, 253, 259, 265, 271)
                      AND ce.value IS NOT NULL
                    GROUP BY ce.icustay_id, ce.charttime
                ),
                art_cv AS (
                    SELECT DISTINCT icustay_id, charttime
                    FROM art_cv_grp
                    WHERE inv1_type IN ('A-Line', 'IABP')
                       OR inv2_type IN ('A-Line', 'IABP')
                       OR inv3_type IN ('A-Line', 'IABP')
                       OR inv4_type IN ('A-Line', 'IABP')
                       OR inv5_type IN ('A-Line', 'IABP')
                       OR inv6_type IN ('A-Line', 'IABP')
                       OR inv7_type IN ('A-Line', 'IABP')
                       OR inv8_type IN ('A-Line', 'IABP')
                ),
                art_cv0 AS (
                    SELECT
                        icustay_id,
                        LAG(charttime, 1) OVER (PARTITION BY icustay_id ORDER BY charttime) AS charttime_lag,
                        charttime
                    FROM art_cv
                ),
                art_cv1 AS (
                    SELECT
                        icustay_id,
                        charttime,
                        CASE WHEN date_diff('hour', charttime_lag, charttime) > 16 THEN 1 ELSE 0 END AS line_new
                    FROM art_cv0
                ),
                art_cv2 AS (
                    SELECT
                        *,
                        SUM(line_new) OVER (PARTITION BY icustay_id ORDER BY charttime) AS line_num
                    FROM art_cv1
                ),
                art_cv_dur AS (
                    SELECT
                        icustay_id,
                        MIN(charttime) AS starttime,
                        MAX(charttime) AS endtime
                    FROM art_cv2
                    GROUP BY icustay_id, line_num
                    HAVING MIN(charttime) != MAX(charttime)
                ),
                -- Source: concepts/durations/arterial_line_durations.sql
                art_mv_dur AS (
                    SELECT
                        pe.icustay_id,
                        pe.starttime,
                        pe.endtime
                    FROM PROCEDUREEVENTS_MV pe
                    WHERE pe.itemid IN ({official_pe_line_ids_str})
                      AND pe.icustay_id IS NOT NULL
                      AND pe.starttime IS NOT NULL
                      AND pe.endtime IS NOT NULL
                      AND (
                            pe.itemid IN ({arterial_mv_ids_str})
                         OR pe.locationcategory = 'Invasive Arterial'
                         OR (pe.itemid = 225789 AND pe.locationcategory IS NULL)
                      )
                      AND pe.statusdescription != 'Rewritten'
                ),
                arterial_intervals AS (
                    SELECT icustay_id, starttime, endtime FROM art_cv_dur
                    UNION ALL
                    SELECT icustay_id, starttime, endtime FROM art_mv_dur
                ),
                cvl_cv_grp AS (
                    SELECT
                        ce.icustay_id,
                        ce.charttime,
                        MAX(CASE WHEN itemid = 229 THEN value ELSE NULL END) AS inv1_type,
                        MAX(CASE WHEN itemid = 235 THEN value ELSE NULL END) AS inv2_type,
                        MAX(CASE WHEN itemid = 241 THEN value ELSE NULL END) AS inv3_type,
                        MAX(CASE WHEN itemid = 247 THEN value ELSE NULL END) AS inv4_type,
                        MAX(CASE WHEN itemid = 253 THEN value ELSE NULL END) AS inv5_type,
                        MAX(CASE WHEN itemid = 259 THEN value ELSE NULL END) AS inv6_type,
                        MAX(CASE WHEN itemid = 265 THEN value ELSE NULL END) AS inv7_type,
                        MAX(CASE WHEN itemid = 271 THEN value ELSE NULL END) AS inv8_type
                    FROM ce_filtered ce
                    WHERE ce.itemid IN (229, 235, 241, 247, 253, 259, 265, 271)
                      AND ce.value IS NOT NULL
                    GROUP BY ce.icustay_id, ce.charttime
                ),
                cvl_cv AS (
                    SELECT DISTINCT icustay_id, charttime
                    FROM cvl_cv_grp
                    WHERE inv1_type IN ('Multi-lumen', 'PICC line', 'Dialysis Line', 'Introducer', 'Trauma Line', 'Portacath', 'Venous Access', 'Hickman', 'PacerIntroducer', 'TripleIntroducer')
                       OR inv2_type IN ('Multi-lumen', 'PICC line', 'Dialysis Line', 'Introducer', 'Trauma Line', 'Portacath', 'Venous Access', 'Hickman', 'PacerIntroducer', 'TripleIntroducer')
                       OR inv3_type IN ('Multi-lumen', 'PICC line', 'Dialysis Line', 'Introducer', 'Trauma Line', 'Portacath', 'Venous Access', 'Hickman', 'PacerIntroducer', 'TripleIntroducer')
                       OR inv4_type IN ('Multi-lumen', 'PICC line', 'Dialysis Line', 'Introducer', 'Trauma Line', 'Portacath', 'Venous Access', 'Hickman', 'PacerIntroducer', 'TripleIntroducer')
                       OR inv5_type IN ('Multi-lumen', 'PICC line', 'Dialysis Line', 'Introducer', 'Trauma Line', 'Portacath', 'Venous Access', 'Hickman', 'PacerIntroducer', 'TripleIntroducer')
                       OR inv6_type IN ('Multi-lumen', 'PICC line', 'Dialysis Line', 'Introducer', 'Trauma Line', 'Portacath', 'Venous Access', 'Hickman', 'PacerIntroducer', 'TripleIntroducer')
                       OR inv7_type IN ('Multi-lumen', 'PICC line', 'Dialysis Line', 'Introducer', 'Trauma Line', 'Portacath', 'Venous Access', 'Hickman', 'PacerIntroducer', 'TripleIntroducer')
                       OR inv8_type IN ('Multi-lumen', 'PICC line', 'Dialysis Line', 'Introducer', 'Trauma Line', 'Portacath', 'Venous Access', 'Hickman', 'PacerIntroducer', 'TripleIntroducer')
                ),
                cvl_cv0 AS (
                    SELECT
                        icustay_id,
                        LAG(charttime, 1) OVER (PARTITION BY icustay_id ORDER BY charttime) AS charttime_lag,
                        charttime
                    FROM cvl_cv
                ),
                cvl_cv1 AS (
                    SELECT
                        icustay_id,
                        charttime,
                        CASE WHEN date_diff('hour', charttime_lag, charttime) > 16 THEN 1 ELSE 0 END AS line_new
                    FROM cvl_cv0
                ),
                cvl_cv2 AS (
                    SELECT
                        *,
                        SUM(line_new) OVER (PARTITION BY icustay_id ORDER BY charttime) AS line_num
                    FROM cvl_cv1
                ),
                cvl_cv_dur AS (
                    SELECT
                        icustay_id,
                        MIN(charttime) AS starttime,
                        MAX(charttime) AS endtime
                    FROM cvl_cv2
                    GROUP BY icustay_id, line_num
                    HAVING MIN(charttime) != MAX(charttime)
                ),
                -- Source: concepts/durations/central_line_durations.sql
                cvl_mv_dur AS (
                    SELECT
                        pe.icustay_id,
                        pe.starttime,
                        pe.endtime
                    FROM PROCEDUREEVENTS_MV pe
                    WHERE pe.itemid IN ({official_pe_line_ids_str})
                      AND pe.icustay_id IS NOT NULL
                      AND pe.starttime IS NOT NULL
                      AND pe.endtime IS NOT NULL
                      AND (pe.locationcategory != 'Invasive Arterial' OR pe.locationcategory IS NULL)
                      AND pe.itemid NOT IN (224272, 225789, 228286)
                      AND pe.statusdescription != 'Rewritten'
                ),
                cvl_intervals AS (
                    SELECT icustay_id, starttime, endtime FROM cvl_cv_dur
                    UNION ALL
                    SELECT icustay_id, starttime, endtime FROM cvl_mv_dur
                ),
                line_intervals AS (
                    SELECT
                        ls.icustay_id,
                        ls.starttime,
                        COALESCE(
                            CASE
                                WHEN ls.line_type = 'pa' THEN (
                                    SELECT MIN(ce2.charttime) FROM ce_filtered ce2
                                    WHERE ce2.icustay_id = ls.icustay_id
                                      AND ce2.itemid IN ({pa_stop_str})
                                      AND ce2.charttime > ls.starttime
                                )
                                WHEN ls.line_type = 'trauma' THEN (
                                    SELECT MIN(ce2.charttime) FROM ce_filtered ce2
                                    WHERE ce2.icustay_id = ls.icustay_id
                                      AND ce2.itemid IN ({trauma_stop_str})
                                      AND ce2.charttime > ls.starttime
                                )
                                WHEN ls.line_type = 'ava' THEN (
                                    SELECT MIN(ce2.charttime) FROM ce_filtered ce2
                                    WHERE ce2.icustay_id = ls.icustay_id
                                      AND ce2.itemid IN ({ava_stop_str})
                                      AND ce2.charttime > ls.starttime
                                )
                                WHEN ls.line_type = 'icp_catheter' THEN (
                                    SELECT MIN(ce2.charttime) FROM ce_filtered ce2
                                    WHERE ce2.icustay_id = ls.icustay_id
                                      AND ce2.itemid IN ({icp_catheter_stop_str})
                                      AND ce2.charttime > ls.starttime
                                )
                                ELSE NULL
                            END,
                            ls.outtime
                        ) AS endtime,
                        ls.line_type
                    FROM line_starts ls
                    WHERE ls.starttime IS NOT NULL
                ),
                official_line_hours AS (
                    SELECT
                        t.icustay_id,
                        t.charttime_floor,
                        MAX(CASE WHEN ai.icustay_id IS NOT NULL THEN 1 ELSE 0 END) AS arterial_line_flag,
                        MAX(CASE WHEN ci.icustay_id IS NOT NULL THEN 1 ELSE 0 END) AS cvl_flag
                    FROM time_axis t
                    LEFT JOIN arterial_intervals ai
                      ON t.icustay_id = ai.icustay_id
                     AND t.charttime_floor >= ai.starttime
                     AND t.charttime_floor <= ai.endtime
                    LEFT JOIN cvl_intervals ci
                      ON t.icustay_id = ci.icustay_id
                     AND t.charttime_floor >= ci.starttime
                     AND t.charttime_floor <= ci.endtime
                    GROUP BY t.icustay_id, t.charttime_floor
                ),
                hourly_lines AS (
                    SELECT
                        t.icustay_id,
                        t.charttime_floor,
                        li.line_type
                    FROM time_axis t
                    JOIN line_intervals li
                      ON t.icustay_id = li.icustay_id
                     AND t.charttime_floor < li.endtime
                     AND t.charttime_floor + INTERVAL '1' HOUR > li.starttime
                )
                SELECT
                    COALESCE(olh.icustay_id, hl.icustay_id) AS icustay_id,
                    COALESCE(olh.charttime_floor, hl.charttime_floor) AS charttime_floor,
                    COALESCE(olh.arterial_line_flag, 0) AS arterial_line_flag,
                    COALESCE(olh.cvl_flag, 0) AS cvl_flag,
                    COALESCE(MAX(CASE WHEN hl.line_type = 'pa' THEN 1 ELSE 0 END), 0) AS pa_catheter_flag,
                    COALESCE(MAX(CASE WHEN hl.line_type = 'trauma' THEN 1 ELSE 0 END), 0) AS trauma_line_flag,
                    COALESCE(MAX(CASE WHEN hl.line_type = 'ava' THEN 1 ELSE 0 END), 0) AS ava_line_flag,
                    COALESCE(MAX(CASE WHEN hl.line_type = 'icp_catheter' THEN 1 ELSE 0 END), 0) AS icp_catheter_flag,
                    CASE
                        WHEN COALESCE(olh.arterial_line_flag, 0) = 1
                          OR COALESCE(olh.cvl_flag, 0) = 1
                          OR COALESCE(MAX(CASE WHEN hl.line_type IS NOT NULL THEN 1 ELSE 0 END), 0) = 1
                        THEN 1 ELSE 0
                    END AS any_invasive_line_flag
                FROM official_line_hours olh
                FULL OUTER JOIN hourly_lines hl
                  ON olh.icustay_id = hl.icustay_id
                 AND olh.charttime_floor = hl.charttime_floor
                GROUP BY
                    COALESCE(olh.icustay_id, hl.icustay_id),
                    COALESCE(olh.charttime_floor, hl.charttime_floor),
                    COALESCE(olh.arterial_line_flag, 0),
                    COALESCE(olh.cvl_flag, 0)
            ) TO '{inter(lines_name)}' (FORMAT PARQUET)
        """)
        log.info("step_invasive_lines done")

    if not exists(code_name):
        code_ids_str = ",".join(str(i) for i in code_status_ids)
        con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
        con.execute(f"""
            COPY (
                WITH t1 AS (
                    SELECT
                        icustay_id,
                        charttime,
                        value,
                        ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY charttime) AS rnfirst,
                        ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY charttime DESC) AS rnlast,
                        CASE WHEN value IN ('Full Code', 'Full code') THEN 1 ELSE 0 END AS fullcode,
                        CASE WHEN value IN ('Comfort Measures', 'Comfort measures only') THEN 1 ELSE 0 END AS cmo,
                        CASE WHEN value = 'CPR Not Indicate' THEN 1 ELSE 0 END AS dncpr,
                        CASE WHEN value IN ('Do Not Intubate', 'DNI (do not intubate)', 'DNR / DNI') THEN 1 ELSE 0 END AS dni,
                        CASE WHEN value IN ('Do Not Resuscita', 'DNR (do not resuscitate)', 'DNR / DNI') THEN 1 ELSE 0 END AS dnr
                    FROM ce_filtered
                    WHERE itemid IN ({code_ids_str})
                      AND value IS NOT NULL
                      AND value != 'Other/Remarks'
                ),
                code_points AS (
                    SELECT
                        icustay_id,
                        date_trunc('hour', charttime) AS charttime_floor,
                        arg_max(value, charttime) AS code_status
                    FROM t1
                    GROUP BY icustay_id, date_trunc('hour', charttime)
                ),
                code_summary AS (
                    SELECT
                        icustay_id,
                        MAX(CASE WHEN rnfirst = 1 THEN fullcode ELSE NULL END) AS fullcode_first,
                        MAX(CASE WHEN rnfirst = 1 THEN cmo ELSE NULL END) AS cmo_first,
                        MAX(CASE WHEN rnfirst = 1 THEN dnr ELSE NULL END) AS dnr_first,
                        MAX(CASE WHEN rnfirst = 1 THEN dni ELSE NULL END) AS dni_first,
                        MAX(CASE WHEN rnfirst = 1 THEN dncpr ELSE NULL END) AS dncpr_first,
                        MAX(CASE WHEN rnlast = 1 THEN fullcode ELSE NULL END) AS fullcode_last,
                        MAX(CASE WHEN rnlast = 1 THEN cmo ELSE NULL END) AS cmo_last,
                        MAX(CASE WHEN rnlast = 1 THEN dnr ELSE NULL END) AS dnr_last,
                        MAX(CASE WHEN rnlast = 1 THEN dni ELSE NULL END) AS dni_last,
                        MAX(CASE WHEN rnlast = 1 THEN dncpr ELSE NULL END) AS dncpr_last,
                        MAX(fullcode) AS fullcode,
                        MAX(cmo) AS cmo,
                        MAX(dnr) AS dnr,
                        MAX(dni) AS dni,
                        MAX(dncpr) AS dncpr,
                        MIN(CASE WHEN dnr = 1 THEN charttime ELSE NULL END) AS dnr_first_charttime,
                        MIN(CASE WHEN dni = 1 THEN charttime ELSE NULL END) AS dni_first_charttime,
                        MIN(CASE WHEN dncpr = 1 THEN charttime ELSE NULL END) AS dncpr_first_charttime,
                        MIN(CASE WHEN cmo = 1 THEN charttime ELSE NULL END) AS timecmo_chart
                    FROM t1
                    GROUP BY icustay_id
                ),
                code_state AS (
                    SELECT
                        t.icustay_id,
                        t.charttime_floor,
                        LAST_VALUE(cp.code_status IGNORE NULLS) OVER (
                            PARTITION BY t.icustay_id
                            ORDER BY t.charttime_floor
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS code_status
                    FROM time_axis t
                    LEFT JOIN code_points cp
                      ON t.icustay_id = cp.icustay_id
                     AND t.charttime_floor = cp.charttime_floor
                )
                SELECT
                    cs.icustay_id,
                    cs.charttime_floor,
                    cs.code_status,
                    CASE WHEN cs.code_status IN ('Full Code', 'Full code') THEN 1 ELSE 0 END AS full_code_flag,
                    CASE
                        WHEN cs.code_status IN ('Do Not Resuscita', 'DNR (do not resuscitate)', 'DNR / DNI', 'CPR Not Indicate')
                        THEN 1 ELSE 0
                    END AS dnr_flag,
                    CASE WHEN cs.code_status IN ('Do Not Intubate', 'DNI (do not intubate)', 'DNR / DNI') THEN 1 ELSE 0 END AS dni_flag,
                    CASE WHEN cs.code_status IN ('Comfort Measures', 'Comfort measures only') THEN 1 ELSE 0 END AS cmo_flag,
                    sm.fullcode_first,
                    sm.cmo_first,
                    sm.dnr_first,
                    sm.dni_first,
                    sm.dncpr_first,
                    sm.fullcode_last,
                    sm.cmo_last,
                    sm.dnr_last,
                    sm.dni_last,
                    sm.dncpr_last,
                    sm.fullcode AS fullcode_ever,
                    sm.cmo AS cmo_ever,
                    sm.dnr AS dnr_ever,
                    sm.dni AS dni_ever,
                    sm.dncpr AS dncpr_ever,
                    sm.dnr_first_charttime,
                    sm.dni_first_charttime,
                    sm.dncpr_first_charttime,
                    sm.timecmo_chart
                FROM code_state cs
                LEFT JOIN code_summary sm
                  ON cs.icustay_id = sm.icustay_id
                WHERE cs.code_status IS NOT NULL
            ) TO '{inter(code_name)}' (FORMAT PARQUET)
        """)
        log.info("step_code_status done")

    log.info("CHARTEVENTS scan complete %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 4: labs from LABEVENTS (no icustay_id → boundary join)
# ---------------------------------------------------------------------------
def step04_labs(con):
    name = "04_labs"
    if exists(name):
        log.info("step04 cached"); return
    t0 = time.time()
    log.info("step04 labs — fuzzy join on LABEVENTS (large)...")

    # itemids from mimic-code pivoted_lab.sql + extended concept groups
    lab_items = {
        # core (pivoted_lab.sql)
        "albumin":            [50862],
        "aniongap":           [50868],
        "bicarbonate":        [50882],
        "bilirubin":          [50885],
        "bilirubin_direct":   [50883],
        "bilirubin_indirect": [50884],
        "bun":                [51006],
        "calcium":            [50893],
        "chloride":           [50902],
        "creatinine":         [50912],
        "glucose_lab":        [50931],
        "hematocrit":         [51221],
        "hemoglobin":         [51222],
        "inr":                [51237],
        "lactate":            [50813],
        "magnesium":          [50960],
        "phosphate":          [50970],
        "platelet":           [51265],
        "potassium":          [50971],
        "ptt":                [51275],
        "sodium":             [50983],
        "wbc":                [51300, 51301],
        # enzyme group
        "alt":                [50861],
        "alp":                [50863],
        "ast":                [50878],
        "amylase":            [50867],
        "ck_cpk":             [50910],
        "ck_mb":              [50911],
        "ggt":                [50927],
        "ldh":                [50954],
        "lipase":             [50956],
        # coagulation
        "fibrinogen":         [50856],
        "pt":                 [51274],
        "d_dimer":            [51214],
        "thrombin":           [51196],
        # cardiac / inflammation
        "troponin_i":         [51002],
        "troponin_t":         [51003],
        "ntprobnp":           [50963],
        "crp":                [50889],
        # CBC extended
        "mch":                [51245],
        "mchc":               [51248],
        "mcv":                [51250],
        "rbc":                [51279],
        "rdw":                [51277],
        # blood differential %
        "neutrophils_pct":    [51200],
        "lymphocytes_pct":    [51244],
        "monocytes_pct":      [51254],
        "eosinophils_pct":    [51199],
        "basophils_pct":      [51146],
        "bands":              [51144],
        # blood differential absolute
        "neutrophils_abs":    [51256],
        "lymphocytes_abs":    [51133],
        "monocytes_abs":      [51137],
    }
    all_lab_ids = [i for ids in lab_items.values() for i in ids]
    ids_str = ",".join(str(i) for i in all_lab_ids)

    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")

    # Source: pivot/pivoted_lab.sql — subject_id partition with halfway boundary split
    con.execute(f"""
        COPY (
            WITH icu_windows AS (
                SELECT
                    c.subject_id,
                    c.hadm_id,
                    c.icustay_id,
                    c.intime,
                    c.outtime,
                    LAG(c.outtime)  OVER (PARTITION BY c.subject_id ORDER BY c.intime) AS prev_outtime,
                    LEAD(c.intime)  OVER (PARTITION BY c.subject_id ORDER BY c.intime) AS next_intime
                FROM cohort c
            ),
            iid_assign AS (
                SELECT
                    subject_id, hadm_id, icustay_id, intime, outtime,
                    CASE
                        WHEN prev_outtime IS NOT NULL
                         AND prev_outtime > intime - INTERVAL '24' HOUR
                        THEN intime - (intime - prev_outtime) / 2
                        ELSE intime - INTERVAL '12' HOUR
                    END AS data_start,
                    CASE
                        WHEN next_intime IS NOT NULL
                         AND next_intime < outtime + INTERVAL '24' HOUR
                        THEN outtime + (next_intime - outtime) / 2
                        ELSE outtime + INTERVAL '12' HOUR
                    END AS data_end
                FROM icu_windows
            ),
            lab_raw AS (
                SELECT subject_id, itemid, charttime, valuenum
                FROM LABEVENTS
                WHERE itemid IN ({ids_str})
                  AND valuenum IS NOT NULL AND valuenum > 0
            ),
            assigned AS (
                SELECT
                    iid.icustay_id,
                    l.itemid,
                    l.charttime,
                    l.valuenum,
                    ROW_NUMBER() OVER (
                        PARTITION BY l.subject_id, l.charttime, l.itemid
                        ORDER BY
                            CASE WHEN l.charttime BETWEEN iid.intime AND iid.outtime THEN 0 ELSE 1 END,
                            ABS(EPOCH(l.charttime) - EPOCH(iid.intime))
                    ) AS rn
                FROM lab_raw l
                JOIN iid_assign iid
                  ON l.subject_id = iid.subject_id
                WHERE l.charttime >= iid.data_start
                  AND l.charttime <  iid.data_end
            )
            SELECT
                icustay_id,
                date_trunc('hour', charttime) AS charttime_floor,
                -- core chemistry (Source: pivot/pivoted_lab.sql sanity check upper bounds)
                AVG(CASE WHEN itemid = 50862 AND valuenum BETWEEN 0 AND 10    THEN valuenum END) AS albumin,
                AVG(CASE WHEN itemid = 50868 AND valuenum BETWEEN 0 AND 10000 THEN valuenum END) AS aniongap,
                AVG(CASE WHEN itemid = 50882 AND valuenum BETWEEN 0 AND 10000 THEN valuenum END) AS bicarbonate,
                AVG(CASE WHEN itemid = 50885 AND valuenum BETWEEN 0 AND 150   THEN valuenum END) AS bilirubin,
                AVG(CASE WHEN itemid = 50883 AND valuenum BETWEEN 0 AND 150   THEN valuenum END) AS bilirubin_direct,
                AVG(CASE WHEN itemid = 50884 AND valuenum BETWEEN 0 AND 150   THEN valuenum END) AS bilirubin_indirect,
                AVG(CASE WHEN itemid = 51006 AND valuenum BETWEEN 0 AND 300   THEN valuenum END) AS bun,
                AVG(CASE WHEN itemid = 50893 AND valuenum BETWEEN 0 AND 20    THEN valuenum END) AS calcium,
                AVG(CASE WHEN itemid = 50902 AND valuenum BETWEEN 0 AND 10000 THEN valuenum END) AS chloride,
                AVG(CASE WHEN itemid = 50912 AND valuenum BETWEEN 0 AND 150   THEN valuenum END) AS creatinine,
                AVG(CASE WHEN itemid = 50931 AND valuenum BETWEEN 0 AND 10000 THEN valuenum END) AS glucose_lab,
                AVG(CASE WHEN itemid = 51221 AND valuenum BETWEEN 0 AND 100   THEN valuenum END) AS hematocrit,
                AVG(CASE WHEN itemid = 51222 AND valuenum BETWEEN 0 AND 50    THEN valuenum END) AS hemoglobin,
                AVG(CASE WHEN itemid = 51237 AND valuenum BETWEEN 0 AND 50    THEN valuenum END) AS inr,
                AVG(CASE WHEN itemid = 50813 AND valuenum BETWEEN 0 AND 50    THEN valuenum END) AS lactate,
                AVG(CASE WHEN itemid = 50960 AND valuenum BETWEEN 0 AND 20    THEN valuenum END) AS magnesium,
                AVG(CASE WHEN itemid = 50970 AND valuenum BETWEEN 0 AND 20    THEN valuenum END) AS phosphate,
                AVG(CASE WHEN itemid = 51265 AND valuenum BETWEEN 0 AND 10000 THEN valuenum END) AS platelet,
                AVG(CASE WHEN itemid = 50971 AND valuenum BETWEEN 0 AND 30    THEN valuenum END) AS potassium,
                AVG(CASE WHEN itemid = 51275 AND valuenum BETWEEN 0 AND 150   THEN valuenum END) AS ptt,
                AVG(CASE WHEN itemid = 50983 AND valuenum BETWEEN 0 AND 200   THEN valuenum END) AS sodium,
                AVG(CASE WHEN itemid IN (51300,51301) AND valuenum BETWEEN 0 AND 1000 THEN valuenum END) AS wbc,
                -- enzyme group
                AVG(CASE WHEN itemid = 50861 AND valuenum BETWEEN 0 AND 10000  THEN valuenum END) AS alt,
                AVG(CASE WHEN itemid = 50863 AND valuenum BETWEEN 0 AND 10000  THEN valuenum END) AS alp,
                AVG(CASE WHEN itemid = 50878 AND valuenum BETWEEN 0 AND 10000  THEN valuenum END) AS ast,
                AVG(CASE WHEN itemid = 50867 AND valuenum BETWEEN 0 AND 10000  THEN valuenum END) AS amylase,
                AVG(CASE WHEN itemid = 50910 AND valuenum BETWEEN 0 AND 150000 THEN valuenum END) AS ck_cpk,
                AVG(CASE WHEN itemid = 50911 AND valuenum BETWEEN 0 AND 2000   THEN valuenum END) AS ck_mb,
                AVG(CASE WHEN itemid = 50927 AND valuenum BETWEEN 0 AND 10000  THEN valuenum END) AS ggt,
                AVG(CASE WHEN itemid = 50954 AND valuenum BETWEEN 0 AND 50000  THEN valuenum END) AS ldh,
                AVG(CASE WHEN itemid = 50956 AND valuenum BETWEEN 0 AND 50000  THEN valuenum END) AS lipase,
                -- coagulation
                AVG(CASE WHEN itemid = 50856 AND valuenum BETWEEN 0 AND 2000   THEN valuenum END) AS fibrinogen,
                AVG(CASE WHEN itemid = 51274 AND valuenum BETWEEN 0 AND 150    THEN valuenum END) AS pt,
                AVG(CASE WHEN itemid = 51214 AND valuenum BETWEEN 0 AND 20000  THEN valuenum END) AS d_dimer,
                AVG(CASE WHEN itemid = 51196 AND valuenum BETWEEN 0 AND 150    THEN valuenum END) AS thrombin,
                -- cardiac / inflammation
                AVG(CASE WHEN itemid = 51002 AND valuenum BETWEEN 0 AND 100    THEN valuenum END) AS troponin_i,
                AVG(CASE WHEN itemid = 51003 AND valuenum BETWEEN 0 AND 100    THEN valuenum END) AS troponin_t,
                AVG(CASE WHEN itemid = 50963 AND valuenum BETWEEN 0 AND 100000 THEN valuenum END) AS ntprobnp,
                AVG(CASE WHEN itemid = 50889 AND valuenum BETWEEN 0 AND 500    THEN valuenum END) AS crp,
                -- CBC extended
                AVG(CASE WHEN itemid = 51245 AND valuenum BETWEEN 0 AND 50     THEN valuenum END) AS mch,
                AVG(CASE WHEN itemid = 51248 AND valuenum BETWEEN 0 AND 50     THEN valuenum END) AS mchc,
                AVG(CASE WHEN itemid = 51250 AND valuenum BETWEEN 0 AND 150    THEN valuenum END) AS mcv,
                AVG(CASE WHEN itemid = 51279 AND valuenum BETWEEN 0 AND 10     THEN valuenum END) AS rbc,
                AVG(CASE WHEN itemid = 51277 AND valuenum BETWEEN 0 AND 40     THEN valuenum END) AS rdw,
                -- blood differential %
                AVG(CASE WHEN itemid = 51200 AND valuenum BETWEEN 0 AND 100    THEN valuenum END) AS neutrophils_pct,
                AVG(CASE WHEN itemid = 51244 AND valuenum BETWEEN 0 AND 100    THEN valuenum END) AS lymphocytes_pct,
                AVG(CASE WHEN itemid = 51254 AND valuenum BETWEEN 0 AND 100    THEN valuenum END) AS monocytes_pct,
                AVG(CASE WHEN itemid = 51199 AND valuenum BETWEEN 0 AND 100    THEN valuenum END) AS eosinophils_pct,
                AVG(CASE WHEN itemid = 51146 AND valuenum BETWEEN 0 AND 100    THEN valuenum END) AS basophils_pct,
                AVG(CASE WHEN itemid = 51144 AND valuenum BETWEEN 0 AND 100    THEN valuenum END) AS bands,
                -- blood differential absolute (10^3/uL)
                AVG(CASE WHEN itemid = 51256 AND valuenum BETWEEN 0 AND 100    THEN valuenum END) AS neutrophils_abs,
                AVG(CASE WHEN itemid = 51133 AND valuenum BETWEEN 0 AND 50     THEN valuenum END) AS lymphocytes_abs,
                AVG(CASE WHEN itemid = 51137 AND valuenum BETWEEN 0 AND 20     THEN valuenum END) AS monocytes_abs
            FROM assigned
            WHERE rn = 1
            GROUP BY icustay_id, date_trunc('hour', charttime)
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step04 done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 5: blood gases from LABEVENTS
# ---------------------------------------------------------------------------
def step05_bg(con):
    name = "05_bg"
    if exists(name):
        log.info("step05 cached"); return
    t0 = time.time()
    log.info("step05 blood gases...")

    # itemids from mimic-code pivoted_bg.sql
    bg_ids = [
        50800,  # specimen type
        50820,  # ph
        50818,  # pco2
        50821,  # po2
        50816,  # fio2 (lab)
        50801,  # aado2
        50802,  # base excess
        50803,  # bicarbonate (blood gas)
        50804,  # total co2
        50805,  # carboxyhemoglobin
        50806,  # chloride (blood gas)
        50808,  # ionized calcium
        50809,  # glucose (blood gas)
        50810,  # hematocrit (blood gas)
        50811,  # hemoglobin (blood gas)
        50812,  # intubated
        50819,  # peep
        50817,  # so2
        50814,  # methemoglobin
        50815,  # o2flow
        50822,  # potassium (blood gas)
        50823,  # requiredo2
        50824,  # sodium (blood gas)
        50825,  # temperature
        50826,  # tidal volume
        50827,  # ventilation rate
        50828,  # ventilator
        50813,  # lactate (used by LR arterial specimen classifier in pivoted_bg_art.sql)
        51545,  # (官方 pivoted_bg.sql 包含，用于 specimen filter)
    ]
    ids_str = ",".join(str(i) for i in bg_ids)

    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"CREATE OR REPLACE VIEW fio2_chart AS SELECT * FROM read_parquet('{inter('fio2_chart')}')")

    con.execute(f"""
        COPY (
            WITH icu_windows AS (
                SELECT
                    c.subject_id,
                    c.hadm_id,
                    c.icustay_id,
                    c.intime,
                    c.outtime,
                    LAG(c.outtime) OVER (
                        PARTITION BY c.hadm_id
                        ORDER BY c.intime, c.outtime, c.icustay_id
                    ) AS prev_outtime,
                    LEAD(c.intime) OVER (
                        PARTITION BY c.hadm_id
                        ORDER BY c.intime, c.outtime, c.icustay_id
                    ) AS next_intime
                FROM cohort c
            ),
            bg_raw AS (
                SELECT
                    subject_id,
                    hadm_id,
                    itemid,
                    charttime,
                    CAST(value AS VARCHAR) AS value,
                    valuenum
                FROM LABEVENTS
                WHERE itemid IN ({ids_str})
                  AND hadm_id IS NOT NULL
                  AND (valuenum IS NOT NULL OR value IS NOT NULL)
            ),
            assigned AS (
                SELECT
                    c.icustay_id,
                    l.hadm_id,
                    l.itemid,
                    l.charttime,
                    l.value,
                    l.valuenum,
                    ROW_NUMBER() OVER (
                        PARTITION BY l.hadm_id, l.charttime, l.itemid
                        ORDER BY
                            CASE
                                WHEN l.charttime BETWEEN c.intime AND c.outtime THEN 0
                                ELSE 1
                            END,
                            ABS(EPOCH(l.charttime) - EPOCH(c.intime))
                    ) AS rn
                FROM bg_raw l
                JOIN icu_windows c
                  ON l.hadm_id = c.hadm_id
                WHERE l.charttime >= COALESCE(
                        GREATEST(c.intime - INTERVAL '12' HOUR, c.prev_outtime),
                        c.intime - INTERVAL '12' HOUR
                    )
                  AND l.charttime <= COALESCE(
                        LEAST(c.outtime + INTERVAL '12' HOUR, c.next_intime),
                        c.outtime + INTERVAL '12' HOUR
                    )
            ),
            bg_valid_draws AS (
                -- 官方 pivoted_bg.sql: 过滤 specimen 记录数 >= 2 的重复血气（HAVING ... < 2）
                SELECT icustay_id, charttime
                FROM assigned
                WHERE rn = 1
                GROUP BY icustay_id, charttime
                HAVING sum(CASE WHEN itemid = 50800 THEN 1 ELSE 0 END) < 2
            ),
            draw_values AS (
                -- Per-draw pivot of BG features needed for LR arterial specimen classifier
                -- Source: pivot/pivoted_bg_art.sql
                SELECT
                    a.icustay_id, a.hadm_id, a.charttime,
                    MAX(CASE WHEN a.itemid = 50800 THEN a.value END) AS specimen,
                    MAX(CASE WHEN a.itemid = 50821 AND a.valuenum BETWEEN 0   AND 800  THEN a.valuenum END) AS po2,
                    MAX(CASE WHEN a.itemid = 50817 AND a.valuenum BETWEEN 0   AND 100  THEN a.valuenum END) AS so2,
                    MAX(CASE WHEN a.itemid = 50818 AND a.valuenum BETWEEN 0   AND 200  THEN a.valuenum END) AS pco2,
                    MAX(CASE WHEN a.itemid = 50816 AND a.valuenum BETWEEN 20  AND 100  THEN a.valuenum END) AS fio2,
                    MAX(CASE WHEN a.itemid = 50801 AND a.valuenum BETWEEN 0   AND 800  THEN a.valuenum END) AS aado2,
                    MAX(CASE WHEN a.itemid = 50803 AND a.valuenum BETWEEN 0   AND 60   THEN a.valuenum END) AS bicarbonate_bg,
                    MAX(CASE WHEN a.itemid = 50804 AND a.valuenum BETWEEN 0   AND 80   THEN a.valuenum END) AS totalco2,
                    MAX(CASE WHEN a.itemid = 50811 AND a.valuenum BETWEEN 0   AND 40   THEN a.valuenum END) AS hemoglobin,
                    MAX(CASE WHEN a.itemid = 50815 AND a.valuenum BETWEEN 0   AND 70   THEN a.valuenum END) AS o2flow,
                    MAX(CASE WHEN a.itemid = 50820 AND a.valuenum BETWEEN 6.5 AND 8.0  THEN a.valuenum END) AS ph,
                    MAX(CASE WHEN a.itemid = 50813 AND a.valuenum > 0                  THEN a.valuenum END) AS lactate
                FROM assigned a
                INNER JOIN bg_valid_draws v ON a.icustay_id = v.icustay_id AND a.charttime = v.charttime
                WHERE a.rn = 1
                GROUP BY a.icustay_id, a.hadm_id, a.charttime
            ),
            stg_spo2_draws AS (
                -- SpO2 from CHARTEVENTS grouped by hadm_id per event, for 2h lookback
                -- Source: pivot/pivoted_bg_art.sql stg_spo2 (official joins by hadm_id)
                SELECT hadm_id, charttime, AVG(valuenum) AS spo2
                FROM CHARTEVENTS
                WHERE itemid IN (646, 220277)
                  AND valuenum > 0 AND valuenum <= 100
                GROUP BY hadm_id, charttime
            ),
            stg_fio2_draws AS (
                -- FiO2 from CHARTEVENTS grouped by hadm_id, with unit normalization, for 4h lookback
                -- Source: pivot/pivoted_bg_art.sql stg_fio2 (official joins by hadm_id)
                SELECT hadm_id, charttime,
                    MAX(CASE
                        WHEN itemid = 223835 THEN
                            CASE WHEN valuenum > 0 AND valuenum <= 1     THEN valuenum * 100
                                 WHEN valuenum > 1 AND valuenum < 21     THEN NULL
                                 WHEN valuenum >= 21 AND valuenum <= 100  THEN valuenum
                                 ELSE NULL END
                        WHEN itemid IN (3420, 3422) THEN valuenum
                        WHEN itemid = 190 AND valuenum > 0.20 AND valuenum < 1 THEN valuenum * 100
                        ELSE NULL END
                    ) AS fio2_ce
                FROM CHARTEVENTS
                WHERE itemid IN (3420, 190, 223835, 3422)
                  AND valuenum > 0 AND valuenum < 100
                  AND (error IS NULL OR error != 1)
                GROUP BY hadm_id, charttime
            ),
            draw_spo2 AS (
                -- Most recent SpO2 within 2h before each blood gas draw, joined by hadm_id
                -- Source: pivot/pivoted_bg_art.sql stg2
                SELECT dv.icustay_id, dv.charttime,
                    arg_max(s.spo2, s.charttime) AS spo2
                FROM draw_values dv
                LEFT JOIN stg_spo2_draws s
                    ON dv.hadm_id = s.hadm_id
                    AND s.charttime BETWEEN dv.charttime - INTERVAL '2' HOUR AND dv.charttime
                GROUP BY dv.icustay_id, dv.charttime
            ),
            draw_fio2 AS (
                -- Most recent FiO2 within 4h before each blood gas draw, joined by hadm_id
                -- Source: pivot/pivoted_bg_art.sql stg3
                SELECT dv.icustay_id, dv.charttime,
                    arg_max(f.fio2_ce, f.charttime) AS fio2_ce
                FROM draw_values dv
                LEFT JOIN stg_fio2_draws f
                    ON dv.hadm_id = f.hadm_id
                    AND f.charttime BETWEEN dv.charttime - INTERVAL '4' HOUR AND dv.charttime
                    AND f.fio2_ce > 0
                GROUP BY dv.icustay_id, dv.charttime
            ),
            art_draws AS (
                -- Flag each draw arterial: explicit ART label OR LR specimen_prob > 0.75
                -- Source: pivot/pivoted_bg_art.sql final WHERE clause
                SELECT dv.icustay_id, dv.charttime,
                    CASE
                        WHEN UPPER(dv.specimen) LIKE '%ART%' THEN 1
                        WHEN dv.po2 IS NOT NULL AND
                            1.0/(1.0+EXP(-(-0.02544
                                + 0.04598  * dv.po2
                                + COALESCE(-0.15356 * ds.spo2,           -0.15356 * 97.49420 +  0.13429)
                                + COALESCE( 0.00621 * df.fio2_ce,         0.00621 * 51.49550 + -0.24958)
                                + COALESCE( 0.10559 * dv.hemoglobin,      0.10559 * 10.32307 +  0.05954)
                                + COALESCE( 0.13251 * dv.so2,             0.13251 * 93.66539 + -0.23172)
                                + COALESCE(-0.01511 * dv.pco2,           -0.01511 * 42.08866 + -0.01630)
                                + COALESCE( 0.01480 * dv.fio2,            0.01480 * 63.97836 + -0.31142)
                                + COALESCE(-0.00200 * dv.aado2,          -0.00200 *442.21186 + -0.01328)
                                + COALESCE(-0.03220 * dv.bicarbonate_bg, -0.03220 * 22.96894 + -0.06535)
                                + COALESCE( 0.05384 * dv.totalco2,        0.05384 * 24.72632 + -0.01405)
                                + COALESCE( 0.08202 * dv.lactate,         0.08202 *  3.06436 +  0.06038)
                                + COALESCE( 0.10956 * dv.ph,              0.10956 *  7.36233 + -0.00617)
                                + COALESCE( 0.00848 * dv.o2flow,          0.00848 *  7.59362 + -0.35803)
                            ))) > 0.75 THEN 1
                        ELSE 0
                    END AS is_arterial
                FROM draw_values dv
                LEFT JOIN draw_spo2 ds ON dv.icustay_id = ds.icustay_id AND dv.charttime = ds.charttime
                LEFT JOIN draw_fio2 df ON dv.icustay_id = df.icustay_id AND dv.charttime = df.charttime
            ),
            bg_hourly AS (
                SELECT
                    a.icustay_id,
                    date_trunc('hour', a.charttime) AS charttime_floor,
                    arg_max(CASE WHEN a.itemid = 50800 THEN a.value END, a.charttime) AS specimen_bg,
                    AVG(CASE WHEN a.itemid = 50820 AND a.valuenum BETWEEN 6.5 AND 8.0  THEN a.valuenum END) AS ph,
                    AVG(CASE WHEN a.itemid = 50818 AND a.valuenum BETWEEN 0 AND 200     THEN a.valuenum END) AS pco2,
                    AVG(CASE WHEN a.itemid = 50821 AND a.valuenum BETWEEN 0 AND 800
                              AND ds.is_arterial = 1                                    THEN a.valuenum END) AS po2_art,
                    AVG(CASE WHEN a.itemid = 50821 AND a.valuenum BETWEEN 0 AND 800     THEN a.valuenum END) AS po2,
                    AVG(CASE WHEN a.itemid = 50816 AND a.valuenum BETWEEN 20 AND 100    THEN a.valuenum END) AS fio2_lab,
                    AVG(CASE WHEN a.itemid = 50801 AND a.valuenum BETWEEN 0 AND 800     THEN a.valuenum END) AS aado2,
                    AVG(CASE WHEN a.itemid = 50802 AND a.valuenum BETWEEN -30 AND 30    THEN a.valuenum END) AS baseexcess,
                    AVG(CASE WHEN a.itemid = 50803 AND a.valuenum BETWEEN 0 AND 60      THEN a.valuenum END) AS bicarbonate_bg,
                    AVG(CASE WHEN a.itemid = 50804 AND a.valuenum BETWEEN 0 AND 80      THEN a.valuenum END) AS totalco2,
                    AVG(CASE WHEN a.itemid = 50805 AND a.valuenum BETWEEN 0 AND 100     THEN a.valuenum END) AS carboxyhemoglobin,
                    AVG(CASE WHEN a.itemid = 50806 AND a.valuenum BETWEEN 0 AND 200     THEN a.valuenum END) AS chloride_bg,
                    AVG(CASE WHEN a.itemid = 50808 AND a.valuenum BETWEEN 0 AND 20      THEN a.valuenum END) AS calcium_bg,
                    AVG(CASE WHEN a.itemid = 50809 AND a.valuenum BETWEEN 0 AND 1000    THEN a.valuenum END) AS glucose_bg,
                    AVG(CASE WHEN a.itemid = 50810 AND a.valuenum BETWEEN 0 AND 100     THEN a.valuenum END) AS hematocrit_bg,
                    AVG(CASE WHEN a.itemid = 50811 AND a.valuenum BETWEEN 0 AND 40      THEN a.valuenum END) AS hemoglobin_bg,
                    arg_max(CASE WHEN a.itemid = 50812 THEN a.value END, a.charttime) AS intubated_bg,
                    AVG(CASE WHEN a.itemid = 50819 AND a.valuenum BETWEEN 0 AND 50      THEN a.valuenum END) AS peep_bg,
                    AVG(CASE WHEN a.itemid = 50817 AND a.valuenum BETWEEN 0 AND 100     THEN a.valuenum END) AS so2,
                    AVG(CASE WHEN a.itemid = 50814 AND a.valuenum BETWEEN 0 AND 100     THEN a.valuenum END) AS methemoglobin,
                    AVG(CASE WHEN a.itemid = 50815 AND a.valuenum BETWEEN 0 AND 70      THEN a.valuenum END) AS o2flow,
                    AVG(CASE WHEN a.itemid = 50822 AND a.valuenum BETWEEN 0 AND 20      THEN a.valuenum END) AS potassium_bg,
                    AVG(CASE WHEN a.itemid = 50823 AND a.valuenum BETWEEN 0 AND 100     THEN a.valuenum END) AS requiredo2,
                    AVG(CASE WHEN a.itemid = 50824 AND a.valuenum BETWEEN 0 AND 200     THEN a.valuenum END) AS sodium_bg,
                    AVG(CASE WHEN a.itemid = 50825 AND a.valuenum BETWEEN 25 AND 45     THEN a.valuenum END) AS temperature_bg,
                    AVG(CASE WHEN a.itemid = 50826 AND a.valuenum BETWEEN 0 AND 3000    THEN a.valuenum END) AS tidalvolume_bg,
                    arg_max(CASE WHEN a.itemid = 50827 THEN a.value END, a.charttime) AS ventilationrate_bg,
                    arg_max(CASE WHEN a.itemid = 50828 THEN a.value END, a.charttime) AS ventilator_bg
                FROM assigned a
                INNER JOIN bg_valid_draws v
                    ON a.icustay_id = v.icustay_id AND a.charttime = v.charttime
                LEFT JOIN art_draws ds
                    ON a.icustay_id = ds.icustay_id AND a.charttime = ds.charttime
                WHERE a.rn = 1
                GROUP BY a.icustay_id, date_trunc('hour', a.charttime)
            )
            SELECT
                b.icustay_id,
                b.charttime_floor,
                b.specimen_bg,
                b.ph,
                b.pco2,
                b.po2_art,
                b.po2,
                COALESCE(b.fio2_lab, f.fio2_chartevents) AS fio2_bg,
                b.aado2,
                b.baseexcess,
                b.bicarbonate_bg,
                b.totalco2,
                b.chloride_bg,
                b.calcium_bg,
                b.glucose_bg,
                b.hematocrit_bg,
                b.hemoglobin_bg,
                b.intubated_bg,
                b.peep_bg,
                b.so2,
                b.carboxyhemoglobin,
                b.methemoglobin,
                b.o2flow,
                b.potassium_bg,
                b.requiredo2,
                b.sodium_bg,
                b.temperature_bg,
                b.tidalvolume_bg,
                b.ventilationrate_bg,
                b.ventilator_bg
            FROM bg_hourly b
            LEFT JOIN fio2_chart f
              ON b.icustay_id = f.icustay_id
             AND b.charttime_floor = f.charttime_floor
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step05 done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 7: urine output from OUTPUTEVENTS
# ---------------------------------------------------------------------------
def step07_uo(con):
    name = "07_uo"
    if exists(name):
        log.info("step07 cached"); return
    t0 = time.time()
    # itemids from mimic-code pivoted_uo.sql
    # GU irrigant (227488) counts negative
    uo_ids = [
        40055, 43175, 40069, 40094, 40715, 40473, 40085, 40057, 40056, 40405,
        40428, 40086, 40096, 40651, 226559, 226560, 226561, 226584, 226563,
        226564, 226565, 226567, 226557, 226558, 227488, 227489,
    ]
    ids_str = ",".join(str(i) for i in uo_ids)

    con.execute(f"""
        COPY (
            WITH uo_raw AS (
                SELECT
                    oe.icustay_id,
                    oe.charttime,
                    CASE WHEN oe.itemid = 227488 THEN -oe.value ELSE oe.value END AS urineoutput
                FROM OUTPUTEVENTS oe
                INNER JOIN cohort c ON oe.icustay_id = c.icustay_id
                -- Source: pivot/pivoted_uo.sql
                WHERE oe.itemid IN ({ids_str})
                  AND oe.icustay_id IS NOT NULL
                  AND (oe.iserror IS NULL OR oe.iserror != 1)
            ),
            uo_tm AS (
                -- time since last UO event (or since ICU intime for the first event)
                SELECT
                    c.icustay_id,
                    c.intime,
                    c.outtime,
                    ur.charttime,
                    ur.urineoutput,
                    CASE
                        WHEN LAG(ur.charttime) OVER w IS NULL
                            THEN date_diff('minute', c.intime, ur.charttime)
                        ELSE date_diff('minute', LAG(ur.charttime) OVER w, ur.charttime)
                    END AS tm_since_last_uo
                FROM cohort c
                INNER JOIN uo_raw ur ON c.icustay_id = ur.icustay_id
                WINDOW w AS (PARTITION BY c.icustay_id ORDER BY ur.charttime)
            ),
            ur_stg AS (
                SELECT
                    io.icustay_id,
                    io.charttime,
                    SUM(io.urineoutput) AS uo,
                    SUM(iosum.urineoutput)           AS urineoutput_24hr,
                    SUM(iosum.tm_since_last_uo)/60.0 AS uo_tm_24hr
                FROM uo_tm io
                LEFT JOIN uo_tm iosum
                    ON  io.icustay_id = iosum.icustay_id
                    AND io.charttime  >= iosum.charttime
                    AND io.charttime  <= iosum.charttime + INTERVAL 23 HOUR
                GROUP BY io.icustay_id, io.charttime
            ),
            uo_event AS (
                -- apply 22h observation guard: uo_24hr only set when the 24h window is sufficiently covered
                SELECT
                    icustay_id,
                    charttime,
                    uo,
                    CASE
                        WHEN uo_tm_24hr >= 22 AND uo_tm_24hr <= 30
                            THEN urineoutput_24hr / uo_tm_24hr * 24.0
                    END AS uo_24hr
                FROM ur_stg
            ),
            hour_last AS (
                -- keep the last event within each hour for uo_24hr (latest = most complete 24h window)
                SELECT
                    ue.*,
                    date_trunc('hour', ue.charttime) AS charttime_floor,
                    ROW_NUMBER() OVER (
                        PARTITION BY ue.icustay_id, date_trunc('hour', ue.charttime)
                        ORDER BY ue.charttime DESC
                    ) AS hour_seq
                FROM uo_event ue
            )
            SELECT
                c.icustay_id,
                hl.charttime_floor,
                SUM(hl.uo)                                    AS urineoutput,
                MAX(CASE WHEN hl.hour_seq = 1 THEN hl.uo_24hr END) AS uo_24hr
            FROM hour_last hl
            INNER JOIN cohort c
                ON  hl.icustay_id   = c.icustay_id
                AND hl.charttime   >= c.intime
                AND hl.charttime   <= c.outtime
            GROUP BY c.icustay_id, hl.charttime_floor
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step07 done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 8: vasopressors from INPUTEVENTS_MV + INPUTEVENTS_CV
# Source: concepts/durations/vasopressor_durations.sql + individual *_dose.sql files
# ---------------------------------------------------------------------------
def step08_vaso(con):
    name = "08_vaso"
    if exists(name):
        log.info("step08 cached"); return
    t0 = time.time()

    # Vasopressors
    mv_norepi  = [221906];  cv_norepi  = [30047, 30120]
    mv_epi     = [221289];  cv_epi     = [30044, 30119, 30309]
    mv_dopa    = [221662];  cv_dopa    = [30043, 30307]
    mv_dobu    = [221653];  cv_dobu    = [30042, 30306]
    mv_vaso    = [222315];  cv_vaso    = [30051, 42273, 42802]
    mv_phenyl  = [221749];  cv_phenyl  = [30128, 30127]
    mv_milri   = [221986];  cv_milri   = [30125]
    # Sedation / analgesia
    mv_prop    = [222168];  cv_prop    = [30131]
    mv_mida    = [221668];  cv_mida    = [30124]
    mv_dex     = [225150];  cv_dex     = []
    mv_fent    = [225942, 221744]; cv_fent = [30118]
    # Metabolic
    mv_ins     = [223258, 223257, 223260]; cv_ins = [30045, 30100]
    # Neuromuscular blockade
    # Source: concepts/durations/neuroblock_dose.sql
    mv_nmb     = [221555, 222062]
    cv_nmb     = [30114, 30138, 30113, 42174, 42385, 41916, 42100, 42045, 42246, 42291, 42590, 42284, 45096]
    # Crystalloids (NS, LR, D5W)
    mv_cryst   = [225158, 225944, 225828, 220964, 225159, 225161]
    cv_cryst   = [30018, 30021, 30056, 30057, 30015, 30060, 30023, 30020, 30162]
    # Colloids (albumin, PRBC, FFP, platelets)
    mv_colloid = [220862, 220864, 225170, 220970, 226368, 226369]
    cv_colloid = [30008, 30011, 30012, 30001, 30104, 30005, 30006]

    # Official fluid-balance concept families
    # Source: fluid_balance/crystalloid_bolus.sql (225161/30143 = hypertonic saline, excluded per official)
    mv_cryst_bolus = [225158, 225828, 225944, 225797, 225159, 225823, 225825, 225827, 225941, 226089]
    cv_cryst_bolus = [
        30015, 30018, 30020, 30021, 30058, 30060, 30061, 30063, 30065,
        30159, 30160, 30169, 30190, 40850, 41491, 42639, 42187,
        43819, 41430, 40712, 44160, 42383, 42297, 42453, 40872, 41915,
        41490, 46501, 45045, 41984, 41371, 41582, 41322, 40778, 41896,
        41428, 43936, 44200, 41619, 40424, 41457, 41581, 42844, 42429,
        41356, 40532, 42548, 44184, 44521, 44741, 44126, 44110, 44633,
        44983, 44815, 43986, 45079, 46781, 45155, 43909, 41467, 44367,
        41743, 40423, 44263, 42749, 45480, 44491, 41695, 46169, 41580,
        41392, 45989, 45137, 45154, 44053, 41416, 44761, 41237, 44426,
        43975, 44894, 41380, 42671
    ]
    # Source: fluid_balance/colloid_bolus.sql
    mv_colloid_bolus = [220864, 220862, 225174, 225795, 225796]
    cv_colloid_bolus = [30008, 30009, 42832, 40548, 45403, 44203, 30181, 46564, 43237, 43353, 30012, 46313, 30011, 30016, 42975, 42944, 46336, 46729, 40033, 45410, 42731]
    ce_colloid_bolus = [2510, 3087, 6937, 3088]  # colloids charted in CHARTEVENTS (t3)
    # Source: fluid_balance/rbc_transfusion.sql (raw_rbc + pre_icu_rbc)
    cv_rbc = [30179, 30001, 30004, 42324, 42588, 42239, 46407, 46612, 46124, 42740]
    mv_rbc = [225168, 227070]
    # Source: fluid_balance/ffp_transfusion.sql
    cv_ffp = [30005, 30180, 42323, 44044, 44172, 44236, 44819, 45669, 46122, 46410, 46418, 46530, 46684]
    mv_ffp = [220970, 227072]

    all_mv = list(set(
        mv_norepi + mv_epi + mv_dopa + mv_dobu + mv_vaso + mv_phenyl + mv_milri +
        mv_prop + mv_mida + mv_dex + mv_fent + mv_ins + mv_nmb + mv_cryst + mv_colloid +
        mv_cryst_bolus + mv_colloid_bolus + mv_rbc + mv_ffp
    ))
    all_cv = list(set(
        cv_norepi + cv_epi + cv_dopa + cv_dobu + cv_vaso + cv_phenyl + cv_milri +
        cv_prop + cv_mida + cv_dex + cv_fent + cv_ins + cv_nmb + cv_cryst + cv_colloid +
        cv_cryst_bolus + cv_colloid_bolus + cv_rbc + cv_ffp
    ))

    def ids(lst): return ",".join(str(i) for i in lst)
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")

    con.execute(f"""
        COPY (
            WITH wt AS (
                SELECT
                    ie.icustay_id,
                    AVG(CASE
                        WHEN c.itemid IN (762,763,3723,3580,226512) THEN c.valuenum
                        WHEN c.itemid = 3581 THEN c.valuenum * 0.45359237
                        WHEN c.itemid = 3582 THEN c.valuenum * 0.0283495231
                        ELSE NULL
                    END) AS weight_kg
                FROM ICUSTAYS ie
                LEFT JOIN CHARTEVENTS c ON ie.icustay_id = c.icustay_id
                WHERE c.valuenum IS NOT NULL
                  AND c.itemid IN (762,763,3723,3580,3581,3582,226512)
                  AND c.valuenum != 0
                  AND c.charttime BETWEEN ie.intime - INTERVAL '1' DAY AND ie.intime + INTERVAL '1' DAY
                  AND (c.error IS NULL OR c.error = 0)
                GROUP BY ie.icustay_id
            ),
            mv_base AS (
                SELECT
                    icustay_id,
                    itemid,
                    starttime,
                    COALESCE(
                        NULLIF(endtime, starttime),
                        starttime + INTERVAL '1' MINUTE
                    ) AS endtime,
                    TRY_CAST(rate AS DOUBLE) AS rate,
                    TRY_CAST(amount AS DOUBLE) AS amount,
                    CAST(rateuom AS VARCHAR) AS rateuom,
                    CAST(amountuom AS VARCHAR) AS amountuom,
                    TRY_CAST(patientweight AS DOUBLE) AS patientweight
                FROM INPUTEVENTS_MV
                WHERE itemid IN ({ids(all_mv)})
                  AND statusdescription != 'Rewritten'
                  AND icustay_id IS NOT NULL
                  AND starttime IS NOT NULL
            ),
            mv_rates AS (
                SELECT
                    t.icustay_id,
                    t.charttime_floor,
                    MAX(CASE WHEN itemid IN ({ids(mv_norepi)}) THEN TRY_CAST(rate AS DOUBLE) END) AS rate_norepinephrine,
                    MAX(CASE WHEN itemid IN ({ids(mv_epi)})    THEN TRY_CAST(rate AS DOUBLE) END) AS rate_epinephrine,
                    MAX(CASE WHEN itemid IN ({ids(mv_dopa)})   THEN TRY_CAST(rate AS DOUBLE) END) AS rate_dopamine,
                    MAX(CASE WHEN itemid IN ({ids(mv_dobu)})   THEN TRY_CAST(rate AS DOUBLE) END) AS rate_dobutamine,
                    MAX(CASE WHEN itemid IN ({ids(mv_vaso)})   THEN TRY_CAST(rate AS DOUBLE) END) AS rate_vasopressin,
                    MAX(CASE WHEN itemid IN ({ids(mv_phenyl)}) THEN TRY_CAST(rate AS DOUBLE) END) AS rate_phenylephrine,
                    MAX(CASE WHEN itemid IN ({ids(mv_milri)})  THEN TRY_CAST(rate AS DOUBLE) END) AS rate_milrinone,
                    MAX(CASE WHEN itemid IN ({ids(mv_prop)})   THEN TRY_CAST(rate AS DOUBLE) END) AS rate_propofol,
                    MAX(CASE WHEN itemid IN ({ids(mv_mida)})   THEN TRY_CAST(rate AS DOUBLE) END) AS rate_midazolam,
                    MAX(CASE WHEN itemid IN ({ids(mv_dex)})    THEN TRY_CAST(rate AS DOUBLE) END) AS rate_dexmedetomidine,
                    MAX(CASE WHEN itemid IN ({ids(mv_fent)})   THEN TRY_CAST(rate AS DOUBLE) END) AS rate_fentanyl,
                    MAX(CASE WHEN itemid IN ({ids(mv_ins)})    THEN TRY_CAST(rate AS DOUBLE) END) AS rate_insulin,
                    MAX(CASE WHEN itemid IN ({ids(mv_nmb)})    AND TRY_CAST(rate AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS nmb_flag,
                    CAST(NULL AS DOUBLE) AS crystalloid_bolus_ml,
                    CAST(NULL AS DOUBLE) AS colloid_bolus_ml,
                    CAST(NULL AS DOUBLE) AS rbc_transfusion_ml,
                    CAST(NULL AS DOUBLE) AS ffp_transfusion_ml,
                    CAST(NULL AS DOUBLE) AS crystalloid_ml,
                    CAST(NULL AS DOUBLE) AS colloid_ml
                FROM time_axis t
                JOIN mv_base mv
                  ON t.icustay_id = mv.icustay_id
                 AND t.charttime_floor < mv.endtime
                 AND t.charttime_floor + INTERVAL '1' HOUR > mv.starttime
                GROUP BY t.icustay_id, t.charttime_floor
            ),
            mv_amounts AS (
                SELECT
                    icustay_id,
                    date_trunc('hour', starttime) AS charttime_floor,
                    CAST(NULL AS DOUBLE) AS rate_norepinephrine,
                    CAST(NULL AS DOUBLE) AS rate_epinephrine,
                    CAST(NULL AS DOUBLE) AS rate_dopamine,
                    CAST(NULL AS DOUBLE) AS rate_dobutamine,
                    CAST(NULL AS DOUBLE) AS rate_vasopressin,
                    CAST(NULL AS DOUBLE) AS rate_phenylephrine,
                    CAST(NULL AS DOUBLE) AS rate_milrinone,
                    CAST(NULL AS DOUBLE) AS rate_propofol,
                    CAST(NULL AS DOUBLE) AS rate_midazolam,
                    CAST(NULL AS DOUBLE) AS rate_dexmedetomidine,
                    CAST(NULL AS DOUBLE) AS rate_fentanyl,
                    CAST(NULL AS DOUBLE) AS rate_insulin,
                    0 AS nmb_flag,
                    SUM(CASE
                        WHEN itemid IN ({ids(mv_cryst_bolus)})
                         AND (
                            (rate IS NOT NULL AND lower(rateuom) = 'ml/hour' AND rate > 248)
                            OR (rate IS NOT NULL AND lower(rateuom) = 'ml/min' AND rate > (248.0/60.0))
                            OR (rate IS NULL AND lower(amountuom) = 'l'  AND amount > 0.248)
                            OR (rate IS NULL AND lower(amountuom) = 'ml' AND amount > 248)
                         )
                         AND (
                            CASE
                                WHEN lower(amountuom) = 'l' THEN ROUND(amount * 1000.0)
                                WHEN lower(amountuom) = 'ml' THEN ROUND(amount)
                                ELSE NULL
                            END
                         ) > 248
                        THEN CASE
                            WHEN lower(amountuom) = 'l' THEN ROUND(amount * 1000.0)
                            WHEN lower(amountuom) = 'ml' THEN ROUND(amount)
                            ELSE 0
                        END
                        ELSE 0
                    END) AS crystalloid_bolus_ml,
                    SUM(CASE
                        WHEN itemid IN ({ids(mv_colloid_bolus)})
                         AND (
                            (lower(rateuom) = 'ml/hour' AND rate > 100)
                            OR (lower(rateuom) = 'ml/min' AND rate > (100.0/60.0))
                            OR (lower(rateuom) = 'ml/kg/hour' AND (rate * COALESCE(patientweight, 0)) > 100)
                         )
                         AND (
                            CASE
                                WHEN lower(amountuom) = 'l' THEN ROUND(amount * 1000.0)
                                WHEN lower(amountuom) = 'ml' THEN ROUND(amount)
                                ELSE NULL
                            END
                         ) > 100
                        THEN CASE
                            WHEN lower(amountuom) = 'l' THEN ROUND(amount * 1000.0)
                            WHEN lower(amountuom) = 'ml' THEN ROUND(amount)
                            ELSE 0
                        END
                        ELSE 0
                    END) AS colloid_bolus_ml,
                    SUM(CASE
                        WHEN itemid IN ({ids(mv_rbc)}) AND amount > 0 THEN COALESCE(amount, 0)
                        ELSE 0
                    END) AS rbc_transfusion_ml,
                    SUM(CASE
                        WHEN itemid IN ({ids(mv_ffp)}) AND amount > 0 THEN COALESCE(amount, 0)
                        ELSE 0
                    END) AS ffp_transfusion_ml,
                    SUM(CASE WHEN itemid IN ({ids(mv_cryst)})   THEN COALESCE(amount, 0) ELSE 0 END) AS crystalloid_ml,
                    SUM(CASE WHEN itemid IN ({ids(mv_colloid)}) THEN COALESCE(amount, 0) ELSE 0 END) AS colloid_ml
                FROM mv_base
                GROUP BY icustay_id, date_trunc('hour', starttime)
            ),
            mv AS (
                SELECT
                    icustay_id,
                    charttime_floor,
                    MAX(rate_norepinephrine) AS rate_norepinephrine,
                    MAX(rate_epinephrine) AS rate_epinephrine,
                    MAX(rate_dopamine) AS rate_dopamine,
                    MAX(rate_dobutamine) AS rate_dobutamine,
                    MAX(rate_vasopressin) AS rate_vasopressin,
                    MAX(rate_phenylephrine) AS rate_phenylephrine,
                    MAX(rate_milrinone) AS rate_milrinone,
                    MAX(rate_propofol) AS rate_propofol,
                    MAX(rate_midazolam) AS rate_midazolam,
                    MAX(rate_dexmedetomidine) AS rate_dexmedetomidine,
                    MAX(rate_fentanyl) AS rate_fentanyl,
                    MAX(rate_insulin) AS rate_insulin,
                    MAX(nmb_flag) AS nmb_flag,
                    SUM(COALESCE(crystalloid_bolus_ml, 0.0)) AS crystalloid_bolus_ml,
                    SUM(COALESCE(colloid_bolus_ml, 0.0)) AS colloid_bolus_ml,
                    SUM(COALESCE(rbc_transfusion_ml, 0.0)) AS rbc_transfusion_ml,
                    SUM(COALESCE(ffp_transfusion_ml, 0.0)) AS ffp_transfusion_ml,
                    SUM(COALESCE(crystalloid_ml, 0.0)) AS crystalloid_ml,
                    SUM(COALESCE(colloid_ml, 0.0)) AS colloid_ml
                FROM (
                    SELECT * FROM mv_rates
                    UNION ALL
                    SELECT * FROM mv_amounts
                ) mv_union
                GROUP BY icustay_id, charttime_floor
            ),
            cv AS (
                SELECT
                    cv.icustay_id,
                    date_trunc('hour', cv.charttime) AS charttime_floor,
                    MAX(CASE WHEN cv.itemid = 30047 THEN TRY_CAST(cv.rate AS DOUBLE) / NULLIF(COALESCE(wt.weight_kg, 80.0), 0)
                             WHEN cv.itemid = 30120 THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_norepinephrine,
                    MAX(CASE WHEN cv.itemid = 30044 THEN TRY_CAST(cv.rate AS DOUBLE) / NULLIF(COALESCE(wt.weight_kg, 80.0), 0)
                             WHEN cv.itemid IN (30119,30309) THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_epinephrine,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_dopa)})   THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_dopamine,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_dobu)})   THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_dobutamine,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_vaso)})
                             -- itemids 42273/42802 accidentally store rate in amount column
                             THEN CASE WHEN cv.itemid IN (42273, 42802)
                                       THEN TRY_CAST(cv.amount AS DOUBLE)
                                       ELSE TRY_CAST(cv.rate AS DOUBLE) END
                             END) AS rate_vasopressin,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_phenyl)}) THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_phenylephrine,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_milri)})  THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_milrinone,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_prop)})   THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_propofol,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_mida)})   THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_midazolam,
                    CAST(NULL AS DOUBLE) AS rate_dexmedetomidine,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_fent)})   THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_fentanyl,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_ins)})    THEN TRY_CAST(cv.rate AS DOUBLE) END) AS rate_insulin,
                    MAX(CASE WHEN cv.itemid IN ({ids(cv_nmb)})    AND TRY_CAST(cv.rate AS DOUBLE) > 0 THEN 1 ELSE 0 END) AS nmb_flag,
                    SUM(CASE
                        WHEN cv.itemid IN ({ids(cv_cryst_bolus)})
                         AND COALESCE(TRY_CAST(cv.amount AS DOUBLE), 0) > 248
                         AND COALESCE(TRY_CAST(cv.amount AS DOUBLE), 0) <= 2000
                         AND lower(CAST(cv.amountuom AS VARCHAR)) = 'ml'
                        THEN ROUND(TRY_CAST(cv.amount AS DOUBLE))
                        ELSE 0
                    END) AS crystalloid_bolus_ml,
                    SUM(CASE
                        WHEN cv.itemid IN ({ids(cv_colloid_bolus)})
                         AND COALESCE(TRY_CAST(cv.amount AS DOUBLE), 0) > 100
                         AND COALESCE(TRY_CAST(cv.amount AS DOUBLE), 0) < 2000
                        THEN ROUND(TRY_CAST(cv.amount AS DOUBLE))
                        ELSE 0
                    END) AS colloid_bolus_ml,
                    SUM(CASE
                        WHEN cv.itemid IN ({ids(cv_rbc)})
                        THEN COALESCE(
                            TRY_CAST(cv.amount AS DOUBLE),
                            CASE WHEN cv.stopped IS NOT NULL THEN 0 ELSE 375 END
                        )
                        ELSE 0
                    END) AS rbc_transfusion_ml,
                    SUM(CASE
                        WHEN cv.itemid IN ({ids(cv_ffp)}) AND TRY_CAST(cv.amount AS DOUBLE) > 0
                        THEN COALESCE(TRY_CAST(cv.amount AS DOUBLE), 0)
                        ELSE 0
                    END) AS ffp_transfusion_ml,
                    SUM(CASE WHEN cv.itemid IN ({ids(cv_cryst)})  THEN COALESCE(TRY_CAST(cv.amount AS DOUBLE), 0) ELSE 0 END) AS crystalloid_ml,
                    SUM(CASE WHEN cv.itemid IN ({ids(cv_colloid)}) THEN COALESCE(TRY_CAST(cv.amount AS DOUBLE), 0) ELSE 0 END) AS colloid_ml
                FROM INPUTEVENTS_CV cv
                LEFT JOIN wt ON cv.icustay_id = wt.icustay_id
                WHERE cv.itemid IN ({ids(all_cv)})
                GROUP BY cv.icustay_id, date_trunc('hour', cv.charttime)
            ),
            -- Source: fluid_balance/colloid_bolus.sql t3 (colloids charted in CHARTEVENTS)
            ce_colloid AS (
                SELECT
                    ce.icustay_id,
                    date_trunc('hour', ce.charttime) AS charttime_floor,
                    SUM(CASE
                        WHEN ce.valuenum IS NOT NULL
                         AND ce.valuenum > 100
                         AND ce.valuenum < 2000
                        THEN ROUND(ce.valuenum) ELSE 0
                    END) AS colloid_bolus_ml
                FROM CHARTEVENTS ce
                WHERE ce.itemid IN ({ids(ce_colloid_bolus)})
                  AND ce.icustay_id IS NOT NULL
                  AND (ce.error IS NULL OR ce.error != 1)
                GROUP BY ce.icustay_id, date_trunc('hour', ce.charttime)
            )
            SELECT
                COALESCE(mv.icustay_id, cv.icustay_id)             AS icustay_id,
                COALESCE(mv.charttime_floor, cv.charttime_floor)    AS charttime_floor,
                COALESCE(mv.rate_norepinephrine, cv.rate_norepinephrine, 0.0) AS rate_norepinephrine,
                COALESCE(mv.rate_epinephrine,    cv.rate_epinephrine,    0.0) AS rate_epinephrine,
                COALESCE(mv.rate_dopamine,       cv.rate_dopamine,       0.0) AS rate_dopamine,
                COALESCE(mv.rate_dobutamine,     cv.rate_dobutamine,     0.0) AS rate_dobutamine,
                COALESCE(mv.rate_vasopressin,    cv.rate_vasopressin,    0.0) AS rate_vasopressin,
                COALESCE(mv.rate_phenylephrine,  cv.rate_phenylephrine,  0.0) AS rate_phenylephrine,
                COALESCE(mv.rate_milrinone,      cv.rate_milrinone,      0.0) AS rate_milrinone,
                COALESCE(mv.rate_propofol,       cv.rate_propofol,       0.0) AS rate_propofol,
                COALESCE(mv.rate_midazolam,      cv.rate_midazolam,      0.0) AS rate_midazolam,
                COALESCE(mv.rate_dexmedetomidine,cv.rate_dexmedetomidine,0.0) AS rate_dexmedetomidine,
                COALESCE(mv.rate_fentanyl,       cv.rate_fentanyl,       0.0) AS rate_fentanyl,
                COALESCE(mv.rate_insulin,        cv.rate_insulin,        0.0) AS rate_insulin,
                COALESCE(mv.nmb_flag,            cv.nmb_flag,            0)   AS nmb_flag,
                COALESCE(mv.crystalloid_bolus_ml,cv.crystalloid_bolus_ml,0.0) AS crystalloid_bolus_ml,
                COALESCE(mv.colloid_bolus_ml, 0.0) + COALESCE(cv.colloid_bolus_ml, 0.0) + COALESCE(ce_colloid.colloid_bolus_ml, 0.0) AS colloid_bolus_ml,
                COALESCE(mv.rbc_transfusion_ml,  cv.rbc_transfusion_ml,  0.0) AS rbc_transfusion_ml,
                COALESCE(mv.ffp_transfusion_ml,  cv.ffp_transfusion_ml,  0.0) AS ffp_transfusion_ml,
                COALESCE(mv.crystalloid_ml,      cv.crystalloid_ml,      0.0) AS crystalloid_ml,
                COALESCE(mv.colloid_ml,          cv.colloid_ml,          0.0) AS colloid_ml,
                CASE WHEN COALESCE(mv.rate_norepinephrine, cv.rate_norepinephrine, 0.0) > 0 THEN 1 ELSE 0 END AS norepi_flag,
                CASE WHEN COALESCE(mv.rate_epinephrine,    cv.rate_epinephrine,    0.0) > 0 THEN 1 ELSE 0 END AS epi_flag,
                CASE WHEN COALESCE(mv.rate_dopamine,       cv.rate_dopamine,       0.0) > 0 THEN 1 ELSE 0 END AS dopa_flag,
                CASE WHEN COALESCE(mv.rate_dobutamine,     cv.rate_dobutamine,     0.0) > 0 THEN 1 ELSE 0 END AS dobu_flag,
                CASE WHEN COALESCE(mv.rate_vasopressin,    cv.rate_vasopressin,    0.0) > 0 THEN 1 ELSE 0 END AS vaso_flag,
                CASE WHEN COALESCE(mv.rate_phenylephrine,  cv.rate_phenylephrine,  0.0) > 0 THEN 1 ELSE 0 END AS phenyl_flag
            FROM mv
            FULL OUTER JOIN cv
              ON mv.icustay_id = cv.icustay_id
             AND mv.charttime_floor = cv.charttime_floor
            LEFT JOIN ce_colloid
              ON COALESCE(mv.icustay_id, cv.icustay_id) = ce_colloid.icustay_id
             AND COALESCE(mv.charttime_floor, cv.charttime_floor) = ce_colloid.charttime_floor
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step08 done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 9: suspicion of infection (PhysioNet 2019 / Sepsis-3)
# ---------------------------------------------------------------------------
def step09_suspinfect(con):
    name = "09_suspinfect"
    all_name = "09_suspinfect_all"
    if exists(name) and exists(all_name):
        log.info("step09 cached"); return
    t0 = time.time()
    log.info("step09 suspicion of infection...")

    # Antibiotic LIKE terms from mimic-code abx_prescriptions_list.sql (exact)
    abx_like_terms = [
        'adoxa', 'ala-tet', 'alodox', 'amikacin', 'amikin', 'amoxicillin',
        'ampicillin', 'augmentin', 'avelox', 'avidoxy', 'azactam', 'azithromycin',
        'aztreonam', 'axetil', 'bactocill', 'bactrim', 'bethkis', 'biaxin',
        'bicillin l-a', 'cayston', 'cefazolin', 'cedax', 'cefoxitin', 'ceftazidime',
        'cefaclor', 'cefadroxil', 'cefdinir', 'cefditoren', 'cefepime', 'cefotetan',
        'cefotaxime', 'cefpodoxime', 'cefprozil', 'ceftibuten', 'ceftin',
        'cefuroxime', 'cephalexin', 'chloramphenicol', 'cipro', 'ciprofloxacin',
        'claforan', 'clarithromycin', 'cleocin', 'clindamycin', 'cubicin',
        'dicloxacillin', 'doryx', 'doxycycline', 'duricef', 'dynacin',
        'ery-tab', 'eryped', 'eryc', 'erythrocin', 'erythromycin',
        'factive', 'flagyl', 'fortaz', 'furadantin', 'garamycin', 'gentamicin',
        'kanamycin', 'keflex', 'ketek', 'levaquin', 'levofloxacin', 'lincocin',
        'macrobid', 'macrodantin', 'maxipime', 'mefoxin', 'metronidazole',
        'minocin', 'minocycline', 'monodox', 'monurol', 'morgidox', 'moxatag',
        'moxifloxacin', 'myrac', 'nafcillin sodium', 'nicazel doxy 30', 'nitrofurantoin',
        'noroxin', 'ocudox', 'ofloxacin', 'omnicef', 'oracea', 'oraxyl',
        'oxacillin', 'pc pen vk', 'pce dispertab', 'panixine', 'pediazole',
        'penicillin', 'periostat', 'pfizerpen', 'piperacillin', 'tazobactam', 'clavulanate', 'trimethoprim', 'primsol',
        'proquin', 'raniclor', 'rifadin', 'rifampin', 'rocephin', 'smz-tmp',
        'septra', 'solodyn', 'spectracef', 'streptomycin sulfate', 'sulfadiazine',
        'sulfamethoxazole', 'sulfatrim', 'sulfisoxazole', 'suprax', 'synercid',
        'tazicef', 'tetracycline', 'timentin', 'tobi', 'tobramycin', 'unasyn',
        'vancocin', 'vancomycin', 'vantin', 'vibativ', 'vibra-tabs', 'vibramycin',
        'zinacef', 'zithromax', 'zmax', 'zosyn', 'zyvox',
    ]

    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")

    # Official MIMIC-III mimic-code logic:
    # suspected_infection_time is the culture time (if present in the prior 72h
    # or next 24h relative to antibiotic time), not the antibiotic time itself.
    # We persist:
    # - all qualifying events for step12 sepsis evaluation
    # - earliest event per ICU stay for stable final-wide-table metadata columns
    con.execute(f"""
        COPY (
            WITH abx AS (
                SELECT
                    p.hadm_id,
                    p.drug AS antibiotic_name,
                    p.startdate AS antibiotic_time,
                    p.enddate AS antibiotic_endtime
                FROM PRESCRIPTIONS p
                WHERE p.drug_type IN ('MAIN', 'ADDITIVE')
                  AND p.route NOT IN ('OU','OS','OD','AU','AS','AD','TP')
                  AND LOWER(p.route) NOT LIKE '%ear%'
                  AND LOWER(p.route) NOT LIKE '%eye%'
                  AND LOWER(p.drug) NOT LIKE '%cream%'
                  AND LOWER(p.drug) NOT LIKE '%desensitization%'
                  AND LOWER(p.drug) NOT LIKE '%ophth oint%'
                  AND LOWER(p.drug) NOT LIKE '%gel%'
                  AND ({' OR '.join(f"LOWER(p.drug) LIKE '%{t}%'" for t in abx_like_terms)})
                  AND p.startdate IS NOT NULL
            ),
            ab_tbl AS (
                SELECT
                    ie.subject_id,
                    ie.hadm_id,
                    ie.icustay_id,
                    ie.intime,
                    ie.outtime,
                    abx.antibiotic_name,
                    abx.antibiotic_time,
                    abx.antibiotic_endtime
                FROM ICUSTAYS ie
                LEFT JOIN abx
                  ON ie.hadm_id = abx.hadm_id
            ),
            me AS (
                SELECT
                    hadm_id,
                    chartdate,
                    charttime,
                    spec_type_desc,
                    MAX(CASE WHEN org_name IS NOT NULL AND org_name != '' THEN 1 ELSE 0 END) AS positiveculture
                FROM MICROBIOLOGYEVENTS
                WHERE hadm_id IS NOT NULL
                GROUP BY hadm_id, chartdate, charttime, spec_type_desc
            ),
            ab_fnl AS (
                SELECT
                    ab_tbl.icustay_id,
                    ab_tbl.antibiotic_name,
                    ab_tbl.antibiotic_time,
                    COALESCE(me72.charttime, me72.chartdate) AS last72_charttime,
                    COALESCE(me24.charttime, me24.chartdate) AS next24_charttime,
                    me72.positiveculture AS last72_positiveculture,
                    me72.spec_type_desc AS last72_specimen,
                    me24.positiveculture AS next24_positiveculture,
                    me24.spec_type_desc AS next24_specimen
                FROM ab_tbl
                LEFT JOIN me me72
                  ON ab_tbl.hadm_id = me72.hadm_id
                 AND ab_tbl.antibiotic_time IS NOT NULL
                 AND (
                    (
                        me72.charttime IS NOT NULL
                        AND ab_tbl.antibiotic_time >= me72.charttime
                        AND ab_tbl.antibiotic_time <= me72.charttime + INTERVAL '72' HOUR
                    )
                    OR
                    (
                        me72.charttime IS NULL
                        AND ab_tbl.antibiotic_time >= me72.chartdate
                        AND ab_tbl.antibiotic_time <= me72.chartdate + INTERVAL '96' HOUR
                    )
                 )
                LEFT JOIN me me24
                  ON ab_tbl.hadm_id = me24.hadm_id
                 AND ab_tbl.antibiotic_time IS NOT NULL
                 AND (
                    (
                        me24.charttime IS NOT NULL
                        AND ab_tbl.antibiotic_time <= me24.charttime
                        AND ab_tbl.antibiotic_time >= me24.charttime - INTERVAL '24' HOUR
                    )
                    OR
                    (
                        me24.charttime IS NULL
                        AND ab_tbl.antibiotic_time <= me24.chartdate
                        AND ab_tbl.antibiotic_time >= me24.chartdate - INTERVAL '24' HOUR
                    )
                 )
            ),
            suspicion_ranked AS (
                SELECT
                    ROW_NUMBER() OVER (
                        PARTITION BY icustay_id
                        ORDER BY COALESCE(last72_charttime, next24_charttime), antibiotic_time
                    ) AS suspicion_id,
                    icustay_id,
                    antibiotic_name,
                    antibiotic_time,
                    last72_charttime,
                    next24_charttime,
                    COALESCE(last72_charttime, next24_charttime) AS t_suspicion,
                    COALESCE(last72_charttime, next24_charttime) - INTERVAL '24' HOUR AS si_starttime,
                    COALESCE(last72_charttime, next24_charttime) + INTERVAL '12' HOUR AS si_endtime,
                    CASE
                        WHEN last72_charttime IS NOT NULL THEN last72_specimen
                        WHEN next24_charttime IS NOT NULL THEN next24_specimen
                        ELSE NULL
                    END AS specimen,
                    CASE
                        WHEN last72_charttime IS NOT NULL THEN last72_positiveculture
                        WHEN next24_charttime IS NOT NULL THEN next24_positiveculture
                        ELSE NULL
                    END AS positiveculture
                FROM ab_fnl
                WHERE COALESCE(last72_charttime, next24_charttime) IS NOT NULL
            )
            SELECT
                suspicion_id,
                icustay_id,
                t_suspicion,
                si_starttime,
                si_endtime,
                antibiotic_name,
                antibiotic_time,
                specimen,
                positiveculture
            FROM suspicion_ranked s
        ) TO '{inter(all_name)}' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY icustay_id
                        ORDER BY t_suspicion, antibiotic_time, suspicion_id
                    ) AS rn
                FROM read_parquet('{inter(all_name)}')
            )
            SELECT
                icustay_id,
                t_suspicion,
                si_starttime,
                si_endtime,
                antibiotic_name,
                antibiotic_time,
                specimen,
                positiveculture
            FROM ranked
            WHERE rn = 1
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step09 done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 10b: static features per ICU stay
# Source: concepts/demographics/icustay_detail.sql
# ---------------------------------------------------------------------------
def step_static(con):
    name = "static"
    if exists(name):
        log.info("step_static cached"); return
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"""
        COPY (
            SELECT
                c.icustay_id,
                c.age,
                c.gender,
                ie.dbsource,
                ie.first_careunit,
                ie.last_careunit,
                ROUND(date_diff('minute', a.admittime, c.intime) / 60.0, 2) AS hospadmtime,
                COALESCE(a.hospital_expire_flag, 0) AS hospital_expire_flag,
                a.admission_type,
                a.admission_location,
                a.discharge_location,
                a.deathtime,
                a.insurance,
                a.ethnicity,
                CASE
                    WHEN a.ethnicity IN ('WHITE','WHITE - RUSSIAN','WHITE - OTHER EUROPEAN','WHITE - BRAZILIAN','WHITE - EASTERN EUROPEAN') THEN 'white'
                    WHEN a.ethnicity IN ('BLACK/AFRICAN AMERICAN','BLACK/CAPE VERDEAN','BLACK/HAITIAN','BLACK/AFRICAN','CARIBBEAN ISLAND') THEN 'black'
                    WHEN a.ethnicity IN ('HISPANIC OR LATINO','HISPANIC/LATINO - PUERTO RICAN','HISPANIC/LATINO - DOMINICAN','HISPANIC/LATINO - GUATEMALAN','HISPANIC/LATINO - CUBAN','HISPANIC/LATINO - SALVADORAN','HISPANIC/LATINO - CENTRAL AMERICAN (OTHER)','HISPANIC/LATINO - MEXICAN','HISPANIC/LATINO - COLOMBIAN','HISPANIC/LATINO - HONDURAN') THEN 'hispanic'
                    WHEN a.ethnicity IN ('ASIAN','ASIAN - CHINESE','ASIAN - ASIAN INDIAN','ASIAN - VIETNAMESE','ASIAN - FILIPINO','ASIAN - CAMBODIAN','ASIAN - OTHER','ASIAN - KOREAN','ASIAN - JAPANESE','ASIAN - THAI') THEN 'asian'
                    WHEN a.ethnicity IN ('AMERICAN INDIAN/ALASKA NATIVE','AMERICAN INDIAN/ALASKA NATIVE FEDERALLY RECOGNIZED TRIBE') THEN 'native'
                    WHEN a.ethnicity IN ('UNKNOWN/NOT SPECIFIED','UNABLE TO OBTAIN','PATIENT DECLINED TO ANSWER') THEN 'unknown'
                    ELSE 'other'
                END AS ethnicity_grouped,
                a.admittime,
                a.dischtime,
                a.marital_status,
                p.dod,
                p.dod_hosp,
                p.expire_flag,
                -- icustay_detail.sql: hospstay_seq / first_hosp_stay
                DENSE_RANK() OVER (PARTITION BY a.subject_id ORDER BY a.admittime) AS hospstay_seq,
                CASE WHEN DENSE_RANK() OVER (PARTITION BY a.subject_id ORDER BY a.admittime) = 1
                     THEN TRUE ELSE FALSE END AS first_hosp_stay,
                -- icustay_detail.sql: icustay_seq / first_icu_stay
                DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) AS icustay_seq,
                CASE WHEN DENSE_RANK() OVER (PARTITION BY ie.hadm_id ORDER BY ie.intime) = 1
                     THEN TRUE ELSE FALSE END AS first_icu_stay,
                -- icustay_detail.sql: los_hospital / los_icu / intime / outtime
                date_diff('day', a.admittime, a.dischtime) AS los_hospital,
                date_diff('day', c.intime, c.outtime)      AS los_icu,
                c.intime,
                c.outtime
            FROM cohort c
            JOIN ICUSTAYS ie   ON c.icustay_id = ie.icustay_id
            LEFT JOIN ADMISSIONS a ON c.hadm_id  = a.hadm_id
            LEFT JOIN PATIENTS  p  ON c.subject_id = p.subject_id
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step_static done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Elixhauser comorbidities from DIAGNOSES_ICD (official Quan logic + score)
# ---------------------------------------------------------------------------
def step_elixhauser(con):
    name = "elixhauser"
    if exists(name):
        log.info("step_elixhauser cached"); return
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"""
        COPY (
            WITH diag AS (
                SELECT d.hadm_id, d.seq_num, d.icd9_code
                FROM DIAGNOSES_ICD d
                JOIN cohort c ON d.hadm_id = c.hadm_id
                WHERE d.icd9_code IS NOT NULL
                  AND d.seq_num != 1
            ),
            eliflg AS (
                SELECT
                    hadm_id,
                    CASE
                        WHEN icd9_code IN ('39891','40201','40211','40291','40401','40403','40411','40413','40491','40493') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('4254','4255','4257','4258','4259') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('428') THEN 1
                        ELSE 0
                    END AS chf,
                    CASE
                        WHEN icd9_code IN ('42613','42610','42612','99601','99604') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('4260','4267','4269','4270','4271','4272','4273','4274','4276','4278','4279','7850','V450','V533') THEN 1
                        ELSE 0
                    END AS arrhy,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('0932','7463','7464','7465','7466','V422','V433') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('394','395','396','397','424') THEN 1
                        ELSE 0
                    END AS valve,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('4150','4151','4170','4178','4179') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('416') THEN 1
                        ELSE 0
                    END AS pulmcirc,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('0930','4373','4431','4432','4438','4439','4471','5571','5579','V434') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('440','441') THEN 1
                        ELSE 0
                    END AS perivasc,
                    CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('401') THEN 1 ELSE 0 END AS htn,
                    CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('402','403','404','405') THEN 1 ELSE 0 END AS htncx,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('3341','3440','3441','3442','3443','3444','3445','3446','3449') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('342','343') THEN 1
                        ELSE 0
                    END AS para,
                    CASE
                        WHEN icd9_code IN ('33392') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('3319','3320','3321','3334','3335','3362','3481','3483','7803','7843') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('334','335','340','341','345') THEN 1
                        ELSE 0
                    END AS neuro,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('4168','4169','5064','5081','5088') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('490','491','492','493','494','495','496','500','501','502','503','504','505') THEN 1
                        ELSE 0
                    END AS chrnlung,
                    CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('2500','2501','2502','2503') THEN 1 ELSE 0 END AS dm,
                    CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('2504','2505','2506','2507','2508','2509') THEN 1 ELSE 0 END AS dmcx,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('2409','2461','2468') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('243','244') THEN 1
                        ELSE 0
                    END AS hypothy,
                    CASE
                        WHEN icd9_code IN ('40301','40311','40391','40402','40403','40412','40413','40492','40493') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('5880','V420','V451') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('585','586','V56') THEN 1
                        ELSE 0
                    END AS renlfail,
                    CASE
                        WHEN icd9_code IN ('07022','07023','07032','07033','07044','07054') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('0706','0709','4560','4561','4562','5722','5723','5724','5728','5733','5734','5738','5739','V427') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('570','571') THEN 1
                        ELSE 0
                    END AS liver,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('5317','5319','5327','5329','5337','5339','5347','5349') THEN 1
                        ELSE 0
                    END AS ulcer,
                    CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('042','043','044') THEN 1 ELSE 0 END AS aids,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('2030','2386') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('200','201','202') THEN 1
                        ELSE 0
                    END AS lymph,
                    CASE WHEN SUBSTR(icd9_code, 1, 3) IN ('196','197','198','199') THEN 1 ELSE 0 END AS mets,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 3) IN (
                            '140','141','142','143','144','145','146','147','148','149','150','151','152',
                            '153','154','155','156','157','158','159','160','161','162','163','164','165',
                            '166','167','168','169','170','171','172','174','175','176','177','178','179',
                            '180','181','182','183','184','185','186','187','188','189','190','191','192',
                            '193','194','195'
                        ) THEN 1
                        ELSE 0
                    END AS tumor,
                    CASE
                        WHEN icd9_code IN ('72889','72930') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('7010','7100','7101','7102','7103','7104','7108','7109','7112','7193','7285') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('446','714','720','725') THEN 1
                        ELSE 0
                    END AS arth,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('2871','2873','2874','2875') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('286') THEN 1
                        ELSE 0
                    END AS coag,
                    CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('2780') THEN 1 ELSE 0 END AS obese,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('7832','7994') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('260','261','262','263') THEN 1
                        ELSE 0
                    END AS wghtloss,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('2536') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('276') THEN 1
                        ELSE 0
                    END AS lytes,
                    CASE WHEN SUBSTR(icd9_code, 1, 4) IN ('2800') THEN 1 ELSE 0 END AS bldloss,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('2801','2808','2809') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('281') THEN 1
                        ELSE 0
                    END AS anemdef,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('2652','2911','2912','2913','2915','2918','2919','3030','3039','3050','3575','4255','5353','5710','5711','5712','5713','V113') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('980') THEN 1
                        ELSE 0
                    END AS alcohol,
                    CASE
                        WHEN icd9_code IN ('V6542') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('3052','3053','3054','3055','3056','3057','3058','3059') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('292','304') THEN 1
                        ELSE 0
                    END AS drug,
                    CASE
                        WHEN icd9_code IN ('29604','29614','29644','29654') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('2938') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('295','297','298') THEN 1
                        ELSE 0
                    END AS psych,
                    CASE
                        WHEN SUBSTR(icd9_code, 1, 4) IN ('2962','2963','2965','3004') THEN 1
                        WHEN SUBSTR(icd9_code, 1, 3) IN ('309','311') THEN 1
                        ELSE 0
                    END AS depress
                FROM diag
            ),
            eligrp AS (
                SELECT
                    hadm_id,
                    MAX(chf) AS chf,
                    MAX(arrhy) AS arrhy,
                    MAX(valve) AS valve,
                    MAX(pulmcirc) AS pulmcirc,
                    MAX(perivasc) AS perivasc,
                    MAX(htn) AS htn,
                    MAX(htncx) AS htncx,
                    MAX(para) AS para,
                    MAX(neuro) AS neuro,
                    MAX(chrnlung) AS chrnlung,
                    MAX(dm) AS dm,
                    MAX(dmcx) AS dmcx,
                    MAX(hypothy) AS hypothy,
                    MAX(renlfail) AS renlfail,
                    MAX(liver) AS liver,
                    MAX(ulcer) AS ulcer,
                    MAX(aids) AS aids,
                    MAX(lymph) AS lymph,
                    MAX(mets) AS mets,
                    MAX(tumor) AS tumor,
                    MAX(arth) AS arth,
                    MAX(coag) AS coag,
                    MAX(obese) AS obese,
                    MAX(wghtloss) AS wghtloss,
                    MAX(lytes) AS lytes,
                    MAX(bldloss) AS bldloss,
                    MAX(anemdef) AS anemdef,
                    MAX(alcohol) AS alcohol,
                    MAX(drug) AS drug,
                    MAX(psych) AS psych,
                    MAX(depress) AS depress
                FROM eliflg
                GROUP BY hadm_id
            ),
            elix_adm AS (
                SELECT
                    c.hadm_id,
                    COALESCE(e.chf, 0) AS congestive_heart_failure,
                    COALESCE(e.arrhy, 0) AS cardiac_arrhythmias,
                    COALESCE(e.valve, 0) AS valvular_disease,
                    COALESCE(e.pulmcirc, 0) AS pulmonary_circulation,
                    COALESCE(e.perivasc, 0) AS peripheral_vascular,
                    CASE WHEN COALESCE(e.htn, 0) = 1 OR COALESCE(e.htncx, 0) = 1 THEN 1 ELSE 0 END AS hypertension,
                    COALESCE(e.para, 0) AS paralysis,
                    COALESCE(e.neuro, 0) AS other_neurological,
                    COALESCE(e.chrnlung, 0) AS chronic_pulmonary,
                    CASE WHEN COALESCE(e.dmcx, 0) = 1 THEN 0 WHEN COALESCE(e.dm, 0) = 1 THEN 1 ELSE 0 END AS diabetes_uncomplicated,
                    COALESCE(e.dmcx, 0) AS diabetes_complicated,
                    COALESCE(e.hypothy, 0) AS hypothyroidism,
                    COALESCE(e.renlfail, 0) AS renal_failure,
                    COALESCE(e.liver, 0) AS liver_disease,
                    COALESCE(e.ulcer, 0) AS peptic_ulcer,
                    COALESCE(e.aids, 0) AS aids,
                    COALESCE(e.lymph, 0) AS lymphoma,
                    COALESCE(e.mets, 0) AS metastatic_cancer,
                    CASE WHEN COALESCE(e.mets, 0) = 1 THEN 0 WHEN COALESCE(e.tumor, 0) = 1 THEN 1 ELSE 0 END AS solid_tumor,
                    COALESCE(e.arth, 0) AS rheumatoid_arthritis,
                    COALESCE(e.coag, 0) AS coagulopathy,
                    COALESCE(e.obese, 0) AS obesity,
                    COALESCE(e.wghtloss, 0) AS weight_loss,
                    COALESCE(e.lytes, 0) AS fluid_electrolyte,
                    COALESCE(e.bldloss, 0) AS blood_loss_anemia,
                    COALESCE(e.anemdef, 0) AS deficiency_anemias,
                    COALESCE(e.alcohol, 0) AS alcohol_abuse,
                    COALESCE(e.drug, 0) AS drug_abuse,
                    COALESCE(e.psych, 0) AS psychoses,
                    COALESCE(e.depress, 0) AS depression
                FROM (SELECT DISTINCT hadm_id FROM cohort) c
                LEFT JOIN eligrp e ON c.hadm_id = e.hadm_id
            )
            SELECT
                c.icustay_id,
                ea.congestive_heart_failure,
                ea.cardiac_arrhythmias,
                ea.valvular_disease,
                ea.pulmonary_circulation,
                ea.peripheral_vascular,
                ea.hypertension,
                ea.paralysis,
                ea.other_neurological,
                ea.chronic_pulmonary,
                ea.diabetes_uncomplicated,
                ea.diabetes_complicated,
                ea.hypothyroidism,
                ea.renal_failure,
                ea.liver_disease,
                ea.peptic_ulcer,
                ea.aids,
                ea.lymphoma,
                ea.metastatic_cancer,
                ea.solid_tumor,
                ea.rheumatoid_arthritis,
                ea.coagulopathy,
                ea.obesity,
                ea.weight_loss,
                ea.fluid_electrolyte,
                ea.blood_loss_anemia,
                ea.deficiency_anemias,
                ea.alcohol_abuse,
                ea.drug_abuse,
                ea.psychoses,
                ea.depression,
                0 * ea.aids
              + 0 * ea.alcohol_abuse
              + -2 * ea.blood_loss_anemia
              + 7 * ea.congestive_heart_failure
              + 3 * ea.chronic_pulmonary
              + 3 * ea.coagulopathy
              + -2 * ea.deficiency_anemias
              + -3 * ea.depression
              + 0 * ea.diabetes_complicated
              + 0 * ea.diabetes_uncomplicated
              + -7 * ea.drug_abuse
              + 5 * ea.fluid_electrolyte
              + 0 * ea.hypertension
              + 0 * ea.hypothyroidism
              + 11 * ea.liver_disease
              + 9 * ea.lymphoma
              + 12 * ea.metastatic_cancer
              + 6 * ea.other_neurological
              + -4 * ea.obesity
              + 7 * ea.paralysis
              + 2 * ea.peripheral_vascular
              + 0 * ea.peptic_ulcer
              + 0 * ea.psychoses
              + 4 * ea.pulmonary_circulation
              + 0 * ea.rheumatoid_arthritis
              + 5 * ea.renal_failure
              + 4 * ea.solid_tumor
              + -1 * ea.valvular_disease
              + 6 * ea.weight_loss AS elixhauser_vanwalraven
              -- Source: comorbidity/elixhauser_score_quan.sql SID29 (cardiac_arrhythmias excluded)
            , 0 * ea.aids
              + -2 * ea.alcohol_abuse
              + -2 * ea.blood_loss_anemia
              + 9 * ea.congestive_heart_failure
              + 3 * ea.chronic_pulmonary
              + 9 * ea.coagulopathy
              + 0 * ea.deficiency_anemias
              + -4 * ea.depression
              + 0 * ea.diabetes_complicated
              + -1 * ea.diabetes_uncomplicated
              + -8 * ea.drug_abuse
              + 9 * ea.fluid_electrolyte
              + -1 * ea.hypertension
              + 0 * ea.hypothyroidism
              + 5 * ea.liver_disease
              + 6 * ea.lymphoma
              + 13 * ea.metastatic_cancer
              + 4 * ea.other_neurological
              + -4 * ea.obesity
              + 3 * ea.paralysis
              + 0 * ea.peptic_ulcer
              + 4 * ea.peripheral_vascular
              + -4 * ea.psychoses
              + 5 * ea.pulmonary_circulation
              + 6 * ea.renal_failure
              + 0 * ea.rheumatoid_arthritis
              + 8 * ea.solid_tumor
              + 0 * ea.valvular_disease
              + 8 * ea.weight_loss AS elixhauser_SID29
              -- Source: comorbidity/elixhauser_score_quan.sql SID30 (cardiac_arrhythmias included)
            , 0 * ea.aids
              + 0 * ea.alcohol_abuse
              + -3 * ea.blood_loss_anemia
              + 8 * ea.cardiac_arrhythmias
              + 9 * ea.congestive_heart_failure
              + 3 * ea.chronic_pulmonary
              + 12 * ea.coagulopathy
              + 0 * ea.deficiency_anemias
              + -5 * ea.depression
              + 1 * ea.diabetes_complicated
              + 0 * ea.diabetes_uncomplicated
              + -11 * ea.drug_abuse
              + 11 * ea.fluid_electrolyte
              + -2 * ea.hypertension
              + 0 * ea.hypothyroidism
              + 7 * ea.liver_disease
              + 8 * ea.lymphoma
              + 17 * ea.metastatic_cancer
              + 5 * ea.other_neurological
              + -5 * ea.obesity
              + 4 * ea.paralysis
              + 0 * ea.peptic_ulcer
              + 4 * ea.peripheral_vascular
              + -6 * ea.psychoses
              + 5 * ea.pulmonary_circulation
              + 7 * ea.renal_failure
              + 0 * ea.rheumatoid_arthritis
              + 10 * ea.solid_tumor
              + 0 * ea.valvular_disease
              + 10 * ea.weight_loss AS elixhauser_SID30
            FROM cohort c
            LEFT JOIN elix_adm ea ON c.hadm_id = ea.hadm_id
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step_elixhauser done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Hospital service per hour from SERVICES (forward-fill)
# ---------------------------------------------------------------------------
def step_service(con):
    name = "service"
    if exists(name):
        log.info("step_service cached"); return
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
    con.execute(f"""
        COPY (
            WITH svc AS (
                SELECT
                    c.icustay_id,
                    s.transfertime,
                    s.curr_service
                FROM SERVICES s
                JOIN cohort c ON s.hadm_id = c.hadm_id
                WHERE s.transfertime <= c.outtime
            ),
            ranked AS (
                SELECT
                    t.icustay_id,
                    t.charttime_floor,
                    s.curr_service,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.icustay_id, t.charttime_floor
                        ORDER BY s.transfertime DESC
                    ) AS rn
                FROM time_axis t
                JOIN svc s ON t.icustay_id = s.icustay_id
                WHERE s.transfertime <= t.charttime_floor + INTERVAL '1' HOUR
            )
            SELECT icustay_id, charttime_floor, curr_service
            FROM ranked
            WHERE rn = 1
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step_service done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Non-urine fluid outputs from OUTPUTEVENTS
# ---------------------------------------------------------------------------
def step_other_outputs(con):
    name = "other_outputs"
    if exists(name):
        log.info("step_other_outputs cached"); return
    t0 = time.time()
    chest_ids  = [226588, 226589, 226590, 226591, 226592]
    drain_ids  = [228105, 226595, 226596, 226597]
    ng_ids     = [226573, 226575, 226576]
    stool_ids  = [226579, 226580]
    all_ids    = chest_ids + drain_ids + ng_ids + stool_ids
    ids_str    = ",".join(str(i) for i in all_ids)
    chest_str  = ",".join(str(i) for i in chest_ids)
    drain_str  = ",".join(str(i) for i in drain_ids)
    ng_str     = ",".join(str(i) for i in ng_ids)
    stool_str  = ",".join(str(i) for i in stool_ids)
    con.execute(f"""
        COPY (
            SELECT
                icustay_id,
                date_trunc('hour', charttime) AS charttime_floor,
                SUM(CASE WHEN itemid IN ({chest_str}) THEN COALESCE(value, 0) END) AS chest_tube_output,
                SUM(CASE WHEN itemid IN ({drain_str}) THEN COALESCE(value, 0) END) AS drain_output,
                SUM(CASE WHEN itemid IN ({ng_str})    THEN COALESCE(value, 0) END) AS ng_tube_output,
                SUM(CASE WHEN itemid IN ({stool_str}) THEN COALESCE(value, 0) END) AS stool_output
            FROM OUTPUTEVENTS
            WHERE itemid IN ({ids_str})
              AND value IS NOT NULL
              AND icustay_id IS NOT NULL
              AND (iserror IS NULL OR iserror = 0)
            GROUP BY icustay_id, date_trunc('hour', charttime)
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step_other_outputs done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Antibiotic and steroid flags from PRESCRIPTIONS (date-level precision)
# ---------------------------------------------------------------------------
def step_prescription_flags(con):
    name = "prescription_flags"
    if exists(name):
        log.info("step_prescription_flags cached"); return
    t0 = time.time()
    abx_drugs = [
        'Vancomycin','Piperacillin-Tazobactam','Cefepime','Meropenem',
        'Metronidazole','Ciprofloxacin','Levofloxacin','Azithromycin',
        'Ampicillin-Sulbactam','Ceftriaxone','Fluconazole','Clindamycin',
        'Linezolid','Daptomycin','Tigecycline','Colistin',
        'Trimethoprim-Sulfamethoxazole','Rifampin','Gentamicin',
        'Amikacin','Tobramycin','Oxacillin','Nafcillin',
        'Imipenem-Cilastatin','Ertapenem','Aztreonam','Cefazolin',
        'Ceftazidime','Ampicillin','Penicillin G','Nitrofurantoin',
        'Tetracycline','Doxycycline','Minocycline',
    ]
    steroid_drugs = [
        'Hydrocortisone','Methylprednisolone','Dexamethasone',
        'Prednisone','Prednisolone','Fludrocortisone',
    ]
    abx_upper    = ",".join(f"UPPER('{d}')" for d in abx_drugs)
    steroid_upper= ",".join(f"UPPER('{d}')" for d in steroid_drugs)
    con.execute(f"CREATE OR REPLACE VIEW cohort AS SELECT * FROM read_parquet('{inter('01_cohort')}')")
    con.execute(f"CREATE OR REPLACE VIEW time_axis AS SELECT * FROM read_parquet('{inter('02_time_axis')}')")
    con.execute(f"""
        COPY (
            WITH abx AS (
                SELECT DISTINCT p.hadm_id, p.startdate, p.enddate
                FROM PRESCRIPTIONS p
                WHERE UPPER(p.drug) IN ({abx_upper})
                  AND p.startdate IS NOT NULL
            ),
            steroids AS (
                SELECT DISTINCT p.hadm_id, p.startdate, p.enddate
                FROM PRESCRIPTIONS p
                WHERE UPPER(p.drug) IN ({steroid_upper})
                  AND p.startdate IS NOT NULL
            )
            SELECT
                t.icustay_id,
                t.charttime_floor,
                MAX(CASE WHEN a.hadm_id IS NOT NULL THEN 1 ELSE 0 END) AS antibiotic_flag,
                MAX(CASE WHEN s.hadm_id IS NOT NULL THEN 1 ELSE 0 END) AS steroid_flag
            FROM time_axis t
            JOIN cohort c ON t.icustay_id = c.icustay_id
            LEFT JOIN abx a
              ON c.hadm_id = a.hadm_id
             AND CAST(t.charttime_floor AS DATE) >= a.startdate
             AND (a.enddate IS NULL OR CAST(t.charttime_floor AS DATE) <= a.enddate)
            LEFT JOIN steroids s
              ON c.hadm_id = s.hadm_id
             AND CAST(t.charttime_floor AS DATE) >= s.startdate
             AND (s.enddate IS NULL OR CAST(t.charttime_floor AS DATE) <= s.enddate)
            GROUP BY t.icustay_id, t.charttime_floor
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step_prescription_flags done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Dialysis — CHARTEVENTS + INPUTEVENTS_CV + OUTPUTEVENTS + MV intervals
# Source: pivot/pivoted_rrt.sql
# ---------------------------------------------------------------------------
def step_crrt(con):
    name = "crrt"
    if exists(name):
        log.info("step_crrt cached"); return
    t0 = time.time()
    log.info("step_crrt dialysis (pivoted_rrt style)...")
    con.execute(f"CREATE OR REPLACE VIEW crrt_cv AS SELECT * FROM read_parquet('{inter('crrt_cv')}')")

    # Source: pivot/pivoted_rrt.sql (cv_ie CTE)
    cv_ie_ids = [
        40788, 40907, 41063, 41147, 41307, 41460, 41620, 41711, 41791, 41792,
        42562, 43829, 44037, 44188, 44526, 44527, 44584, 44591, 44698, 44927,
        44954, 45157, 45268, 45352, 45353, 46012, 46013, 46172, 46173, 46250,
        46262, 46292, 46293, 46311, 46389, 46574, 46681, 46720, 46769, 46773,
    ]
    # Source: pivot/pivoted_rrt.sql (oe CTE)
    oe_ids = [
        40386, 40425, 40426, 40507, 40613, 40624, 40690, 40745, 40789, 40881,
        40910, 41016, 41034, 41069, 41112, 41250, 41374, 41417, 41500, 41527,
        41623, 41635, 41713, 41750, 41829, 41842, 41897, 42289, 42388, 42464,
        42524, 42536, 42868, 42928, 42972, 43016, 43052, 43098, 43115, 43687,
        43941, 44027, 44085, 44193, 44199, 44216, 44286, 44567, 44843, 44845,
        44857, 44901, 44943, 45479, 45828, 46230, 46232, 46394, 46464, 46712,
        46713, 46715, 46741,
    ]
    # Source: pivot/pivoted_rrt.sql (mv_ranges CTE — INPUTEVENTS_MV medications)
    mv_ie_ids  = [227536, 227525]
    # Source: pivot/pivoted_rrt.sql (mv_ranges CTE — PROCEDUREEVENTS_MV procedures)
    mv_pe_ids  = [225441, 225802, 225803, 225805, 224270, 225809, 225955, 225436]

    cv_ie_str  = ",".join(str(i) for i in cv_ie_ids)
    oe_str     = ",".join(str(i) for i in oe_ids)
    mv_ie_str  = ",".join(str(i) for i in mv_ie_ids)
    mv_pe_str  = ",".join(str(i) for i in mv_pe_ids)

    con.execute(f"""
        COPY (
            -- Source: pivot/pivoted_rrt.sql
            WITH ce AS (
                SELECT
                    icustay_id,
                    charttime_floor,
                    CASE
                        WHEN itemid IN (146,147,148,149,150,151,152) THEN 1
                        WHEN itemid = 582 AND value IN (
                            'CAVH Start','CVVHD Start','Hemodialysis st',
                            'CAVH D/C','CVVHD D/C','Hemodialysis end','Peritoneal Dial'
                        ) THEN 1
                        WHEN itemid IN (229,235,241,247,253,259,265,271) AND value = 'Dialysis Line' THEN 1
                        WHEN itemid IN (226118,227357,225725) THEN 1
                        WHEN itemid IN (
                            226499,224154,225810,225959,227639,225183,227438,224191,
                            225806,225807,228004,228005,228006,224144,224145,224149,
                            224150,224151,224152,224153,224404,224406,226457
                        ) THEN 1
                        WHEN itemid IN (
                            224135,224139,224146,225323,225740,225776,225951,225952,
                            225953,225954,225956,225958,225961,225963,225965,225976,
                            225977,227124,227290,227638,227640,227753
                        ) THEN 1
                        ELSE 0
                    END AS dialysis_present,
                    CASE
                        WHEN itemid = 582 AND value IN ('CAVH Start','CVVHD Start','Hemodialysis st','Peritoneal Dial') THEN 1
                        WHEN itemid = 582 AND value IN ('CAVH D/C','CVVHD D/C','Hemodialysis end') THEN 0
                        WHEN itemid = 147  AND value = 'Yes'    THEN 1
                        WHEN itemid = 225965 AND value = 'In use' THEN 1
                        WHEN itemid IN (
                            146,226499,224154,225183,227438,224191,
                            225806,225807,228004,228005,228006,224144,224145,224153,226457
                        ) THEN 1
                        ELSE 0
                    END AS dialysis_active,
                    CASE
                        WHEN itemid IN (152,227290) THEN
                            CASE value
                                WHEN 'CVVH'       THEN 'CVVH'
                                WHEN 'CVVHD'      THEN 'CVVHD'
                                WHEN 'CVVHDF'     THEN 'CVVHDF'
                                WHEN 'SCUF'       THEN 'SCUF'
                                WHEN 'Peritoneal' THEN 'Peritoneal'
                                ELSE NULL
                            END
                        WHEN itemid IN (
                            225810,225806,225807,227639,225959,225951,225952,
                            225961,225953,225963,225965,227638,227640
                        ) THEN 'Peritoneal'
                        WHEN itemid = 226499 THEN 'IHD'
                        WHEN itemid = 582 THEN
                            CASE
                                WHEN value IN ('CAVH Start','CAVH D/C')       THEN 'CAVH'
                                WHEN value IN ('CVVHD Start','CVVHD D/C')     THEN 'CVVHD'
                                WHEN value IN ('Hemodialysis st','Hemodialysis end') THEN NULL
                                ELSE NULL
                            END
                        ELSE NULL
                    END AS dialysis_type
                FROM crrt_cv
            ),
            cv_ie AS (
                SELECT
                    icustay_id,
                    date_trunc('hour', charttime) AS charttime_floor,
                    1 AS dialysis_present,
                    CASE WHEN itemid NOT IN (44954) THEN 1 ELSE 0 END AS dialysis_active,
                    CASE
                        WHEN itemid IN (40788,41063,41307,43829,44698,46720) THEN 'Peritoneal'
                        WHEN itemid IN (45352,45353)                         THEN 'CVVH'
                        WHEN itemid IN (45268,46769,46773)                   THEN 'CVVHD'
                        WHEN itemid IN (46012,46013,46172,46173)             THEN 'CVVHDF'
                        ELSE NULL
                    END AS dialysis_type
                FROM INPUTEVENTS_CV
                WHERE itemid IN ({cv_ie_str})
                  AND amount > 0
                  AND icustay_id IS NOT NULL
            ),
            oe AS (
                SELECT
                    icustay_id,
                    date_trunc('hour', charttime) AS charttime_floor,
                    1 AS dialysis_present,
                    CASE WHEN itemid NOT IN (41897) THEN 1 ELSE 0 END AS dialysis_active,
                    CASE
                        WHEN itemid IN (40789,40910,41069,44843,46394) THEN 'Peritoneal'
                        ELSE NULL
                    END AS dialysis_type
                FROM OUTPUTEVENTS
                WHERE itemid IN ({oe_str})
                  AND value > 0
                  AND icustay_id IS NOT NULL
            ),
            mv_ranges AS (
                SELECT icustay_id, starttime, endtime,
                    1 AS dialysis_present,
                    1 AS dialysis_active,
                    'CRRT' AS dialysis_type
                FROM INPUTEVENTS_MV
                WHERE itemid IN ({mv_ie_str})
                  AND amount > 0
                  AND icustay_id IS NOT NULL
                UNION DISTINCT
                SELECT icustay_id, starttime, endtime,
                    1 AS dialysis_present,
                    CASE WHEN itemid NOT IN (224270,225436) THEN 1 ELSE 0 END AS dialysis_active,
                    CASE
                        WHEN itemid = 225441 THEN 'IHD'
                        WHEN itemid = 225802 THEN 'CRRT'
                        WHEN itemid = 225803 THEN 'CVVHD'
                        WHEN itemid = 225805 THEN 'Peritoneal'
                        WHEN itemid = 225809 THEN 'CVVHDF'
                        WHEN itemid = 225955 THEN 'SCUF'
                        ELSE NULL
                    END AS dialysis_type
                FROM PROCEDUREEVENTS_MV
                WHERE itemid IN ({mv_pe_str})
                  AND value IS NOT NULL
                  AND icustay_id IS NOT NULL
            ),
            stg0 AS (
                SELECT icustay_id, charttime_floor, dialysis_present, dialysis_active, dialysis_type
                FROM ce WHERE dialysis_present = 1
                UNION DISTINCT
                SELECT icustay_id, charttime_floor, dialysis_present, dialysis_active, dialysis_type
                FROM cv_ie WHERE dialysis_present = 1
                UNION DISTINCT
                SELECT icustay_id, charttime_floor, dialysis_present, dialysis_active, dialysis_type
                FROM oe WHERE dialysis_present = 1
                UNION DISTINCT
                SELECT icustay_id, date_trunc('hour', starttime) AS charttime_floor,
                    dialysis_present, dialysis_active, dialysis_type
                FROM mv_ranges
                UNION DISTINCT
                SELECT icustay_id, date_trunc('hour', endtime) AS charttime_floor,
                    dialysis_present, dialysis_active, dialysis_type
                FROM mv_ranges
            )
            -- Collapse to one row per (icustay_id, charttime_floor) for the hourly wide-table join.
            -- Multiple stg0 sources can produce different dialysis_type for the same hour;
            -- mv.dialysis_type takes precedence (COALESCE), then MAX picks the non-NULL winner.
            SELECT
                icustay_id,
                charttime_floor,
                MAX(dialysis_present) AS dialysis_present,
                MAX(dialysis_active)  AS dialysis_active,
                MAX(dialysis_type)    AS dialysis_type
            FROM (
                SELECT
                    stg0.icustay_id,
                    stg0.charttime_floor,
                    COALESCE(mv.dialysis_present, stg0.dialysis_present) AS dialysis_present,
                    COALESCE(mv.dialysis_active,  stg0.dialysis_active)  AS dialysis_active,
                    COALESCE(mv.dialysis_type,    stg0.dialysis_type)    AS dialysis_type
                FROM stg0
                LEFT JOIN mv_ranges mv
                  ON stg0.icustay_id = mv.icustay_id
                 AND stg0.charttime_floor >= date_trunc('hour', mv.starttime)
                 AND stg0.charttime_floor <= date_trunc('hour', mv.endtime)
                WHERE stg0.icustay_id IS NOT NULL
            ) sub
            GROUP BY icustay_id, charttime_floor
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step_crrt done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 11: join all features onto time axis
# ---------------------------------------------------------------------------
def step11_join(con):
    name = "11_joined"
    if exists(name):
        log.info("step11 cached"); return
    t0 = time.time()
    log.info("step11 joining all features...")

    views = {
        "02_time_axis":       "taxis",
        "03_vitals_raw":      "vitals",
        "04_labs":            "labs",
        "05_bg":              "bg",
        "06_gcs_raw":         "gcs",
        "07_uo":              "uo",
        "08_vaso":            "vaso",
        "09_suspinfect":      "suspinfect",
        "10_vent_raw":        "vent",
        "static":             "sf",
        "hw":                 "hw",
        "elixhauser":         "elix",
        "service":            "svc",
        "other_outputs":      "oo",
        "prescription_flags": "rx",
        "crrt":               "crrt",
        "icp":                "icp",
        "invasive_lines":     "ln",
        "code_status":        "cs",
    }
    for n, alias in views.items():
        con.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM read_parquet('{inter(n)}')")

    con.execute(f"""
        COPY (
            SELECT
                t.subject_id,
                t.hadm_id,
                t.icustay_id,
                t.hr,
                t.charttime_floor,

                -- static demographics
                sf.age,
                sf.gender,
                sf.dbsource,
                sf.first_careunit,
                sf.last_careunit,
                sf.hospadmtime,
                sf.hospital_expire_flag,
                sf.admission_type,
                sf.admission_location,
                sf.discharge_location,
                sf.deathtime,
                sf.insurance,
                sf.ethnicity,
                sf.ethnicity_grouped,
                sf.admittime,
                sf.dischtime,
                sf.marital_status,
                sf.dod,
                sf.dod_hosp,
                sf.expire_flag,
                sf.hospstay_seq,
                sf.first_hosp_stay,
                sf.icustay_seq,
                sf.first_icu_stay,
                sf.los_hospital,
                sf.los_icu,
                sf.intime,
                sf.outtime,

                -- comorbidity
                elix.congestive_heart_failure,
                elix.cardiac_arrhythmias,
                elix.valvular_disease,
                elix.pulmonary_circulation,
                elix.peripheral_vascular,
                elix.hypertension,
                elix.paralysis,
                elix.other_neurological,
                elix.chronic_pulmonary,
                elix.diabetes_uncomplicated,
                elix.diabetes_complicated,
                elix.hypothyroidism,
                elix.renal_failure,
                elix.liver_disease,
                elix.peptic_ulcer,
                elix.aids,
                elix.lymphoma,
                elix.metastatic_cancer,
                elix.solid_tumor,
                elix.rheumatoid_arthritis,
                elix.coagulopathy,
                elix.obesity,
                elix.weight_loss,
                elix.fluid_electrolyte,
                elix.blood_loss_anemia,
                elix.deficiency_anemias,
                elix.alcohol_abuse,
                elix.drug_abuse,
                elix.psychoses,
                elix.depression,
                elix.elixhauser_vanwalraven,
                elix.elixhauser_SID29,
                elix.elixhauser_SID30,

                -- hospital service (time-varying)
                svc.curr_service,

                -- anthropometrics (官方 heightweight.sql: first/min/max)
                hw.height_first,
                hw.height_min,
                hw.height_max,
                hw.weight_first,
                hw.weight_min,
                hw.weight_max,

                -- vitals
                v.heartrate,
                COALESCE(v.tempc, v.tempc_fromf) AS tempc,
                v.sysbp,
                v.diasbp,
                v.meanbp,
                v.resprate,
                v.spo2,
                v.glucose,
                v.etco2,

                -- GCS (carry-forward + sedation imputation per pivoted_gcs.sql)
                g.gcs_total,
                g.gcs_motor,
                g.gcs_verbal,
                g.gcs_eyes,
                g.gcs_sedated,

                -- blood gas
                bg.specimen_bg,
                bg.ph,
                bg.pco2,
                bg.po2,
                bg.fio2_bg AS fio2,
                bg.aado2,
                bg.baseexcess,
                bg.bicarbonate_bg,
                bg.totalco2,
                bg.chloride_bg,
                bg.calcium_bg,
                bg.glucose_bg,
                bg.hematocrit_bg,
                bg.hemoglobin_bg,
                bg.intubated_bg,
                bg.peep_bg AS peep,
                bg.so2,
                bg.carboxyhemoglobin,
                bg.methemoglobin,
                bg.o2flow,
                bg.potassium_bg,
                bg.requiredo2,
                bg.sodium_bg,
                bg.temperature_bg,
                bg.tidalvolume_bg,
                bg.ventilationrate_bg,
                bg.ventilator_bg,

                -- chemistry / core labs
                l.albumin,
                l.aniongap,
                l.bicarbonate,
                l.bilirubin,
                l.bilirubin_direct,
                l.bilirubin_indirect,
                l.bun,
                l.calcium,
                l.chloride,
                l.creatinine,
                l.glucose_lab,
                l.hematocrit,
                l.hemoglobin,
                l.inr,
                l.lactate,
                l.magnesium,
                l.phosphate,
                l.platelet,
                l.potassium,
                l.ptt,
                l.sodium,
                l.wbc,

                -- enzyme group
                l.alt,
                l.alp,
                l.ast,
                l.amylase,
                l.ck_cpk,
                l.ck_mb,
                l.ggt,
                l.ldh,
                l.lipase,

                -- coagulation extended
                l.fibrinogen,
                l.pt,
                l.d_dimer,
                l.thrombin,

                -- cardiac / inflammation
                l.troponin_i,
                l.troponin_t,
                l.ntprobnp,
                l.crp,

                -- CBC extended
                l.mch,
                l.mchc,
                l.mcv,
                l.rbc,
                l.rdw,

                -- blood differential
                l.neutrophils_pct,
                l.lymphocytes_pct,
                l.monocytes_pct,
                l.eosinophils_pct,
                l.basophils_pct,
                l.bands,
                l.neutrophils_abs,
                l.lymphocytes_abs,
                l.monocytes_abs,

                -- urine output
                COALESCE(uo.urineoutput, 0.0) AS urineoutput,
                uo.uo_24hr AS uo_24hr,

                -- other fluid outputs
                COALESCE(oo.chest_tube_output, 0.0) AS chest_tube_output,
                COALESCE(oo.drain_output,      0.0) AS drain_output,
                COALESCE(oo.ng_tube_output,    0.0) AS ng_tube_output,
                COALESCE(oo.stool_output,      0.0) AS stool_output,

                -- vasopressors
                COALESCE(va.rate_norepinephrine,  0.0) AS rate_norepinephrine,
                COALESCE(va.rate_epinephrine,     0.0) AS rate_epinephrine,
                COALESCE(va.rate_dopamine,        0.0) AS rate_dopamine,
                COALESCE(va.rate_dobutamine,      0.0) AS rate_dobutamine,
                COALESCE(va.rate_vasopressin,     0.0) AS rate_vasopressin,
                COALESCE(va.rate_phenylephrine,   0.0) AS rate_phenylephrine,
                COALESCE(va.rate_milrinone,       0.0) AS rate_milrinone,
                COALESCE(va.norepi_flag,  0) AS norepi_flag,
                COALESCE(va.epi_flag,     0) AS epi_flag,
                COALESCE(va.dopa_flag,    0) AS dopa_flag,
                COALESCE(va.dobu_flag,    0) AS dobu_flag,
                COALESCE(va.vaso_flag,    0) AS vaso_flag,
                COALESCE(va.phenyl_flag,  0) AS phenyl_flag,

                -- sedation / analgesia / metabolic
                COALESCE(va.rate_propofol,        0.0) AS rate_propofol,
                COALESCE(va.rate_midazolam,       0.0) AS rate_midazolam,
                COALESCE(va.rate_dexmedetomidine, 0.0) AS rate_dexmedetomidine,
                COALESCE(va.rate_fentanyl,        0.0) AS rate_fentanyl,
                COALESCE(va.rate_insulin,         0.0) AS rate_insulin,
                COALESCE(va.nmb_flag,  0) AS nmb_flag,

                -- fluid balance inputs
                COALESCE(va.crystalloid_bolus_ml, 0.0) AS crystalloid_bolus_ml,
                COALESCE(va.colloid_bolus_ml,     0.0) AS colloid_bolus_ml,
                COALESCE(va.rbc_transfusion_ml,   0.0) AS rbc_transfusion_ml,
                COALESCE(va.ffp_transfusion_ml,   0.0) AS ffp_transfusion_ml,
                COALESCE(va.crystalloid_ml, 0.0) AS crystalloid_ml,
                COALESCE(va.colloid_ml,     0.0) AS colloid_ml,

                -- ventilation
                COALESCE(ve.vent_invasive_flag,    0)      AS vent_invasive_flag,
                COALESCE(ve.vent_noninvasive_flag, 0)      AS vent_noninvasive_flag,
                COALESCE(ve.cpap_flag,             0)      AS cpap_flag,
                COALESCE(ve.oxygen_therapy_flag,   0)      AS oxygen_therapy_flag,
                COALESCE(ve.vent_flag,             0)      AS vent_flag,
                COALESCE(ve.extubated_flag,        0)      AS extubated_flag,
                COALESCE(ve.self_extubated_flag,   0)      AS self_extubated_flag,
                COALESCE(ve.vent_status,           'None') AS vent_status,

                -- ICP / invasive lines / code status
                icp.icp,
                COALESCE(ln.arterial_line_flag, 0) AS arterial_line_flag,
                COALESCE(ln.cvl_flag, 0) AS cvl_flag,
                COALESCE(ln.pa_catheter_flag, 0) AS pa_catheter_flag,
                COALESCE(ln.trauma_line_flag, 0) AS trauma_line_flag,
                COALESCE(ln.ava_line_flag, 0) AS ava_line_flag,
                COALESCE(ln.icp_catheter_flag, 0) AS icp_catheter_flag,
                COALESCE(ln.any_invasive_line_flag, 0) AS any_invasive_line_flag,
                cs.code_status,
                COALESCE(cs.full_code_flag, 0) AS full_code_flag,
                COALESCE(cs.dnr_flag, 0) AS dnr_flag,
                COALESCE(cs.dni_flag, 0) AS dni_flag,
                COALESCE(cs.cmo_flag, 0) AS cmo_flag,
                cs.fullcode_first,
                cs.cmo_first,
                cs.dnr_first,
                cs.dni_first,
                cs.dncpr_first,
                cs.fullcode_last,
                cs.cmo_last,
                cs.dnr_last,
                cs.dni_last,
                cs.dncpr_last,
                cs.fullcode_ever,
                cs.cmo_ever,
                cs.dnr_ever,
                cs.dni_ever,
                cs.dncpr_ever,
                cs.dnr_first_charttime,
                cs.dni_first_charttime,
                cs.dncpr_first_charttime,
                cs.timecmo_chart,

                -- dialysis (Source: pivot/pivoted_rrt.sql)
                COALESCE(crrt.dialysis_present, 0) AS dialysis_present,
                COALESCE(crrt.dialysis_active,  0) AS dialysis_active,
                crrt.dialysis_type                 AS dialysis_type,

                -- prescriptions
                COALESCE(rx.antibiotic_flag, 0) AS antibiotic_flag,
                COALESCE(rx.steroid_flag,    0) AS steroid_flag,

                -- suspicion of infection anchor
                si.t_suspicion,
                si.si_starttime,
                si.si_endtime

            FROM taxis t
            LEFT JOIN vitals v   ON t.icustay_id = v.icustay_id   AND t.charttime_floor = v.charttime_floor
            LEFT JOIN labs   l   ON t.icustay_id = l.icustay_id   AND t.charttime_floor = l.charttime_floor
            LEFT JOIN bg         ON t.icustay_id = bg.icustay_id  AND t.charttime_floor = bg.charttime_floor
            LEFT JOIN gcs    g   ON t.icustay_id = g.icustay_id   AND t.charttime_floor = g.charttime_floor
            LEFT JOIN uo         ON t.icustay_id = uo.icustay_id  AND t.charttime_floor = uo.charttime_floor
            LEFT JOIN vaso   va  ON t.icustay_id = va.icustay_id  AND t.charttime_floor = va.charttime_floor
            LEFT JOIN vent   ve  ON t.icustay_id = ve.icustay_id  AND t.charttime_floor = ve.charttime_floor
            LEFT JOIN suspinfect si ON t.icustay_id = si.icustay_id
            LEFT JOIN sf         ON t.icustay_id = sf.icustay_id
            LEFT JOIN hw         ON t.icustay_id = hw.icustay_id
            LEFT JOIN elix       ON t.icustay_id = elix.icustay_id
            LEFT JOIN svc        ON t.icustay_id = svc.icustay_id AND t.charttime_floor = svc.charttime_floor
            LEFT JOIN oo         ON t.icustay_id = oo.icustay_id  AND t.charttime_floor = oo.charttime_floor
            LEFT JOIN rx         ON t.icustay_id = rx.icustay_id  AND t.charttime_floor = rx.charttime_floor
            LEFT JOIN icp        ON t.icustay_id = icp.icustay_id AND t.charttime_floor = icp.charttime_floor
            LEFT JOIN ln         ON t.icustay_id = ln.icustay_id AND t.charttime_floor = ln.charttime_floor
            LEFT JOIN cs         ON t.icustay_id = cs.icustay_id AND t.charttime_floor = cs.charttime_floor
            LEFT JOIN crrt       ON t.icustay_id = crrt.icustay_id AND t.charttime_floor = crrt.charttime_floor
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step11 done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 12: SOFA score + SepsisLabel
# Reads raw intermediate parquets directly to apply LOCF before scoring.
# GCS default fill in step11 (for model features) is intentionally NOT used here.
# ---------------------------------------------------------------------------
def step12_sepsis_label(con):
    name = "12_sepsislabel"
    if exists(name):
        log.info("step12 cached"); return
    t0 = time.time()
    log.info("step12 SOFA + SepsisLabel (with LOCF)...")

    for alias, src in [
        ("taxis",     "02_time_axis"),
        ("vitals",    "03_vitals_raw"),
        ("labs",      "04_labs"),
        ("bg",        "05_bg"),
        ("gcs_raw",   "06_gcs_raw"),
        ("uo",        "07_uo"),
        ("vaso",      "08_vaso"),
        ("vent",      "10_vent_raw"),
    ]:
        con.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM read_parquet('{inter(src)}')")
    con.execute(f"CREATE OR REPLACE VIEW suspinfect_first AS SELECT * FROM read_parquet('{inter('09_suspinfect')}')")
    con.execute(f"CREATE OR REPLACE VIEW suspinfect_all AS SELECT * FROM read_parquet('{inter('09_suspinfect_all')}')")

    con.execute(f"""
        COPY (
            WITH
            icu_taxis AS (
                SELECT *
                FROM taxis
                WHERE hr >= 0
            ),
            sofa_raw AS (
                SELECT
                    t.icustay_id,
                    t.hr,
                    t.charttime_floor,
                    v.meanbp,
                    va.rate_norepinephrine,
                    va.rate_epinephrine,
                    va.rate_dopamine,
                    va.rate_dobutamine,
                    va.rate_vasopressin,
                    va.rate_phenylephrine,
                    l.creatinine,
                    l.bilirubin,
                    l.platelet,
                    bg.po2_art AS po2,
                    bg.fio2_bg AS fio2,
                    COALESCE(ve.vent_invasive_flag, 0) AS vent_invasive_flag,
                    COALESCE(ve.vent_noninvasive_flag, 0) AS vent_noninvasive_flag,
                    COALESCE(ve.oxygen_therapy_flag, 0) AS oxygen_therapy_flag,
                    COALESCE(ve.vent_flag, 0) AS vent_flag,
                    COALESCE(ve.vent_status, 'None') AS vent_status,
                    uo.urineoutput,
                    uo.uo_24hr,
                    CASE
                        WHEN g.gcs_sedated = 1 THEN 15
                        WHEN g.gcs_motor IS NOT NULL
                         AND g.gcs_verbal IS NOT NULL
                         AND g.gcs_eyes   IS NOT NULL
                        THEN g.gcs_motor + g.gcs_verbal + g.gcs_eyes
                        ELSE NULL
                    END AS gcs_measured
                FROM icu_taxis t
                LEFT JOIN vitals  v  ON t.icustay_id = v.icustay_id  AND t.charttime_floor = v.charttime_floor
                LEFT JOIN vaso    va ON t.icustay_id = va.icustay_id AND t.charttime_floor = va.charttime_floor
                LEFT JOIN labs    l  ON t.icustay_id = l.icustay_id  AND t.charttime_floor = l.charttime_floor
                LEFT JOIN bg         ON t.icustay_id = bg.icustay_id AND t.charttime_floor = bg.charttime_floor
                LEFT JOIN gcs_raw g  ON t.icustay_id = g.icustay_id  AND t.charttime_floor = g.charttime_floor
                LEFT JOIN uo         ON t.icustay_id = uo.icustay_id AND t.charttime_floor = uo.charttime_floor
                LEFT JOIN vent    ve ON t.icustay_id = ve.icustay_id AND t.charttime_floor = ve.charttime_floor
            ),
            -- Pass sofa_raw through unchanged; uo_24hr from step07 already carries the
            -- observation-time-aware 24h normalised UO (NULL until >=22h coverage).
            uo_rolling AS (
                SELECT * FROM sofa_raw
            ),
            -- Score each hour from raw values (no LOCF); renal uses uo_24hr (MIMIC-IV style,
            -- observation-time-aware) instead of official MIMIC-III rolling sum.
            -- MAX of per-hour scores over 24 PRECEDING window applied in score_final below.
            scorecomp AS (
                SELECT
                    icustay_id,
                    hr,
                    charttime_floor,
                    vent_invasive_flag,
                    vent_noninvasive_flag,
                    oxygen_therapy_flag,
                    vent_flag,
                    vent_status,
                    meanbp,
                    rate_norepinephrine,
                    rate_epinephrine,
                    rate_dopamine,
                    rate_dobutamine,
                    rate_vasopressin,
                    rate_phenylephrine,
                    urineoutput,
                    -- pafi split by ventilation (kept for feature output)
                    CASE WHEN vent_invasive_flag = 1
                          AND po2 IS NOT NULL AND fio2 IS NOT NULL AND fio2 > 0
                         THEN 100.0 * po2 / fio2 ELSE NULL
                    END AS pafi_vent,
                    CASE WHEN COALESCE(vent_invasive_flag, 0) = 0
                          AND po2 IS NOT NULL AND fio2 IS NOT NULL AND fio2 > 0
                         THEN 100.0 * po2 / fio2 ELSE NULL
                    END AS pafi_novent,
                    -- per-hour SOFA component scores (official pivoted_sofa.sql logic)
                    CASE
                        WHEN vent_invasive_flag = 1 AND po2 IS NOT NULL AND fio2 IS NOT NULL AND fio2 > 0
                             AND 100.0 * po2 / fio2 < 100 THEN 4
                        WHEN vent_invasive_flag = 1 AND po2 IS NOT NULL AND fio2 IS NOT NULL AND fio2 > 0
                             AND 100.0 * po2 / fio2 < 200 THEN 3
                        WHEN COALESCE(vent_invasive_flag, 0) = 0 AND po2 IS NOT NULL AND fio2 IS NOT NULL AND fio2 > 0
                             AND 100.0 * po2 / fio2 < 300 THEN 2
                        WHEN COALESCE(vent_invasive_flag, 0) = 0 AND po2 IS NOT NULL AND fio2 IS NOT NULL AND fio2 > 0
                             AND 100.0 * po2 / fio2 < 400 THEN 1
                        WHEN po2 IS NULL OR fio2 IS NULL OR fio2 = 0 THEN NULL
                        ELSE 0
                    END AS respiration,
                    CASE
                        WHEN platelet < 20  THEN 4
                        WHEN platelet < 50  THEN 3
                        WHEN platelet < 100 THEN 2
                        WHEN platelet < 150 THEN 1
                        WHEN platelet IS NULL THEN NULL
                        ELSE 0
                    END AS coagulation,
                    CASE
                        WHEN bilirubin >= 12.0 THEN 4
                        WHEN bilirubin >= 6.0  THEN 3
                        WHEN bilirubin >= 2.0  THEN 2
                        WHEN bilirubin >= 1.2  THEN 1
                        WHEN bilirubin IS NULL THEN NULL
                        ELSE 0
                    END AS liver,
                    -- cardiovascular: official epi/norepi/dopa/dobu only.
                    -- rates are COALESCE'd to 0 in step11; guard with > 0 so 0-rate does not trigger score 3.
                    -- NULL check uses meanbp IS NULL because all rates are non-null (0 when not given).
                    CASE
                        WHEN rate_dopamine > 15
                             OR rate_epinephrine > 0.1
                             OR rate_norepinephrine > 0.1 THEN 4
                        WHEN rate_dopamine > 5
                             OR (rate_epinephrine    > 0 AND rate_epinephrine    <= 0.1)
                             OR (rate_norepinephrine > 0 AND rate_norepinephrine <= 0.1) THEN 3
                        WHEN rate_dopamine > 0 OR rate_dobutamine > 0 THEN 2
                        WHEN meanbp < 70 THEN 1
                        WHEN meanbp IS NULL THEN NULL
                        ELSE 0
                    END AS cardiovascular,
                    CASE
                        WHEN gcs_measured >= 13 AND gcs_measured <= 14 THEN 1
                        WHEN gcs_measured >= 10 AND gcs_measured <= 12 THEN 2
                        WHEN gcs_measured >= 6  AND gcs_measured <= 9  THEN 3
                        WHEN gcs_measured < 6   THEN 4
                        WHEN gcs_measured IS NULL THEN NULL
                        ELSE 0
                    END AS cns,
                    -- renal: use uo_24hr from step07 (observation-time-aware; NULL until >=22h coverage)
                    CASE
                        WHEN creatinine >= 5.0 THEN 4
                        WHEN uo_24hr < 200 THEN 4
                        WHEN creatinine >= 3.5 THEN 3
                        WHEN uo_24hr < 500 THEN 3
                        WHEN creatinine >= 2.0 THEN 2
                        WHEN creatinine >= 1.2 THEN 1
                        WHEN COALESCE(uo_24hr, creatinine) IS NULL THEN NULL
                        ELSE 0
                    END AS renal
                FROM uo_rolling
            ),
            score_final AS (
                -- MAX of per-hour scores over 24 PRECEDING (official pivoted_sofa.sql)
                SELECT
                    icustay_id,
                    hr,
                    charttime_floor,
                    vent_invasive_flag,
                    vent_noninvasive_flag,
                    oxygen_therapy_flag,
                    vent_flag,
                    vent_status,
                    -- feature columns preserved for downstream wide table (no LOCF)
                    meanbp                        AS meanbp_lf,
                    MAX(rate_norepinephrine) OVER w24 AS rate_norepinephrine_24h,
                    MAX(rate_epinephrine)    OVER w24 AS rate_epinephrine_24h,
                    MAX(rate_dopamine)       OVER w24 AS rate_dopamine_24h,
                    MAX(rate_dobutamine)     OVER w24 AS rate_dobutamine_24h,
                    MAX(rate_vasopressin)    OVER w24 AS rate_vasopressin_24h,
                    MAX(rate_phenylephrine)  OVER w24 AS rate_phenylephrine_24h,
                    SUM(urineoutput)         OVER w24 AS urineoutput_24h,
                    MIN(pafi_vent)           OVER w24 AS pafi_vent_min_24h,
                    MIN(pafi_novent)         OVER w24 AS pafi_novent_min_24h,
                    -- SOFA component scores: MAX over 24 PRECEDING
                    COALESCE(MAX(respiration)    OVER w24, 0) AS sofa_resp,
                    COALESCE(MAX(coagulation)    OVER w24, 0) AS sofa_coag,
                    COALESCE(MAX(liver)          OVER w24, 0) AS sofa_liver,
                    COALESCE(MAX(cardiovascular) OVER w24, 0) AS sofa_cv,
                    COALESCE(MAX(cns)            OVER w24, 0) AS sofa_cns,
                    COALESCE(MAX(renal)          OVER w24, 0) AS sofa_renal
                FROM scorecomp
                WINDOW w24 AS (
                    PARTITION BY icustay_id ORDER BY hr
                    ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
                )
            ),
            sofa_totals AS (
                SELECT
                    *,
                    sofa_resp + sofa_coag + sofa_liver + sofa_cv + sofa_cns + sofa_renal AS sofa_total
                FROM score_final
            ),
            icu_start AS (
                -- Use ICUSTAYS.intime (official ICU admission timestamp) as the
                -- suspicion filter threshold. This avoids using intime_hr (ceiling
                -- of first HR recording) which can predate the official intime in
                -- edge cases, causing t_suspicion to appear before ICU admission.
                SELECT icustay_id, intime AS icu_start_time
                FROM ICUSTAYS
            ),
            sepsis_event_rows AS (
                SELECT
                    st.*,
                    si.suspicion_id,
                    si.t_suspicion,
                    si.si_starttime,
                    si.si_endtime
                FROM sofa_totals st
                JOIN suspinfect_all si
                  ON st.icustay_id = si.icustay_id
                JOIN icu_start us
                  ON si.icustay_id = us.icustay_id
                WHERE st.charttime_floor >= si.si_starttime
                  AND st.charttime_floor <= si.si_endtime
                  -- only consider suspicion events where culture time is on or
                  -- after official ICU admission (excludes pre-ICU suspicions)
                  AND si.t_suspicion >= us.icu_start_time
            ),
            sofa_windowed AS (
                SELECT
                    ser.*,
                    date_diff('hour', ser.si_starttime, ser.charttime_floor) AS time_window,
                    MIN(ser.sofa_total) OVER (
                        PARTITION BY ser.icustay_id, ser.suspicion_id
                        ORDER BY ser.charttime_floor
                        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                    ) AS sofa_running_min,
                    ser.sofa_total - MIN(ser.sofa_total) OVER (
                        PARTITION BY ser.icustay_id, ser.suspicion_id
                        ORDER BY ser.charttime_floor
                        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                    ) AS sofa_delta_24h
                FROM sepsis_event_rows ser
            ),
            t_sofa_per_event AS (
                SELECT
                    icustay_id,
                    suspicion_id,
                    MIN(charttime_floor) AS t_sofa
                FROM sofa_windowed
                WHERE sofa_delta_24h >= 2
                GROUP BY icustay_id, suspicion_id
            ),
            sofa_window_summary AS (
                SELECT
                    icustay_id,
                    charttime_floor,
                    MAX(sofa_24h_max) AS sofa_24h_max,
                    MAX(sofa_delta_24h) AS sofa_delta_24h
                FROM (
                    SELECT
                        icustay_id,
                        suspicion_id,
                        charttime_floor,
                        MAX(sofa_total) OVER (
                            PARTITION BY icustay_id, suspicion_id
                            ORDER BY charttime_floor
                            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                        ) AS sofa_24h_max,
                        sofa_delta_24h
                    FROM sofa_windowed
                ) sw
                GROUP BY icustay_id, charttime_floor
            ),
            -- Pair t_suspicion and t_sofa from the SAME suspicion event, then pick
            -- the earliest valid t_sepsis across all events (Challenge definition).
            t_sepsis_per_event AS (
                SELECT
                    sa.icustay_id,
                    sa.suspicion_id,
                    sa.t_suspicion,
                    sa.si_starttime,
                    sa.si_endtime,
                    te.t_sofa,
                    LEAST(sa.t_suspicion, te.t_sofa) AS t_sepsis
                FROM suspinfect_all sa
                INNER JOIN t_sofa_per_event te
                    ON sa.icustay_id   = te.icustay_id
                   AND sa.suspicion_id = te.suspicion_id
            ),
            t_sepsis_per_stay AS (
                SELECT icustay_id, t_suspicion, si_starttime, si_endtime, t_sofa, t_sepsis
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY icustay_id ORDER BY t_sepsis
                           ) AS rn
                    FROM t_sepsis_per_event
                )
                WHERE rn = 1
            ),
            with_sepsis AS (
                SELECT
                    st.*,
                    ts.t_suspicion,
                    ts.si_starttime,
                    ts.si_endtime,
                    sws.sofa_24h_max,
                    sws.sofa_delta_24h,
                    ts.t_sofa,
                    ts.t_sepsis
                FROM sofa_totals st
                LEFT JOIN t_sepsis_per_stay ts
                  ON st.icustay_id = ts.icustay_id
                LEFT JOIN sofa_window_summary sws
                  ON st.icustay_id = sws.icustay_id
                 AND st.charttime_floor = sws.charttime_floor
            )
            SELECT
                icustay_id,
                hr,
                charttime_floor,
                si_starttime,
                si_endtime,
                vent_invasive_flag,
                vent_noninvasive_flag,
                oxygen_therapy_flag,
                vent_flag,
                vent_status,
                meanbp_lf,
                rate_norepinephrine_24h,
                rate_epinephrine_24h,
                rate_dopamine_24h,
                rate_dobutamine_24h,
                rate_vasopressin_24h,
                rate_phenylephrine_24h,
                urineoutput_24h,
                pafi_vent_min_24h,
                pafi_novent_min_24h,
                sofa_resp,
                sofa_coag,
                sofa_liver,
                sofa_cv,
                sofa_cns,
                sofa_renal,
                sofa_total,
                sofa_24h_max,
                0 AS sofa_baseline,
                sofa_delta_24h,
                t_suspicion,
                t_sofa,
                t_sepsis,
                CASE
                    WHEN t_sepsis IS NOT NULL
                         AND charttime_floor >= t_sepsis
                    THEN 1 ELSE 0
                END AS SepsisLabel
            FROM with_sepsis
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step12 done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 12b: official-style first-day severity scores from joined hourly table
# qSOFA/SIRS are persisted per ICU stay and later repeated across hourly rows.
# ---------------------------------------------------------------------------
def step_severity_scores_firstday(con):
    name = "severity_scores_firstday"
    if exists(name):
        log.info("step12b cached"); return
    t0 = time.time()
    log.info("step12b first-day qSOFA/SIRS/OASIS...")

    con.execute(f"CREATE OR REPLACE VIEW joined AS SELECT * FROM read_parquet('{inter('11_joined')}')")

    con.execute(f"""
        COPY (
            WITH firstday AS (
                SELECT *
                FROM joined
                WHERE hr BETWEEN 0 AND 23
            ),
            diag_comorb AS (
                SELECT
                    j.hadm_id,
                    MAX(CASE
                        WHEN SUBSTR(d.icd9_code, 1, 3) BETWEEN '042' AND '044' THEN 1
                        ELSE 0
                    END) AS saps_aids,
                    MAX(CASE
                        WHEN d.icd9_code BETWEEN '20000' AND '20238' THEN 1
                        WHEN d.icd9_code BETWEEN '20240' AND '20248' THEN 1
                        WHEN d.icd9_code BETWEEN '20250' AND '20302' THEN 1
                        WHEN d.icd9_code BETWEEN '20310' AND '20312' THEN 1
                        WHEN d.icd9_code BETWEEN '20302' AND '20382' THEN 1
                        WHEN d.icd9_code BETWEEN '20400' AND '20522' THEN 1
                        WHEN d.icd9_code BETWEEN '20580' AND '20702' THEN 1
                        WHEN d.icd9_code BETWEEN '20720' AND '20892' THEN 1
                        WHEN SUBSTR(d.icd9_code, 1, 4) = '2386' THEN 1
                        WHEN SUBSTR(d.icd9_code, 1, 4) = '2733' THEN 1
                        ELSE 0
                    END) AS saps_hem,
                    MAX(CASE
                        WHEN SUBSTR(d.icd9_code, 1, 4) BETWEEN '1960' AND '1991' THEN 1
                        WHEN d.icd9_code BETWEEN '20970' AND '20975' THEN 1
                        WHEN d.icd9_code = '20979' THEN 1
                        WHEN d.icd9_code = '78951' THEN 1
                        ELSE 0
                    END) AS saps_mets
                FROM (SELECT DISTINCT hadm_id FROM firstday) j
                LEFT JOIN DIAGNOSES_ICD d
                  ON j.hadm_id = d.hadm_id
                GROUP BY j.hadm_id
            ),
            agg AS (
                SELECT
                    ANY_VALUE(subject_id) AS subject_id,
                    ANY_VALUE(hadm_id) AS hadm_id,
                    icustay_id,
                    ANY_VALUE(age) AS age,
                    ANY_VALUE(admission_type) AS admission_type,
                    MAX(CASE
                        WHEN lower(COALESCE(curr_service, '')) LIKE '%surg%' OR curr_service = 'ORTHO'
                        THEN 1 ELSE 0
                    END) AS surgical,
                    MIN(hospadmtime) AS preiculos_hours,
                    MIN(tempc) AS tempc_min,
                    MAX(tempc) AS tempc_max,
                    MAX(heartrate) AS heartrate_max,
                    MIN(heartrate) AS heartrate_min,
                    MAX(meanbp) AS meanbp_max,
                    MIN(meanbp) AS meanbp_min,
                    MAX(resprate) AS resprate_max,
                    MIN(resprate) AS resprate_min,
                    MIN(CASE
                        WHEN upper(COALESCE(specimen_bg, '')) LIKE 'ART%'
                         AND (COALESCE(vent_invasive_flag, 0) = 1 OR COALESCE(cpap_flag, 0) = 1)
                         AND po2 IS NOT NULL AND fio2 IS NOT NULL AND fio2 > 0
                        THEN po2 / (fio2 / 100.0)
                        ELSE NULL
                    END) AS pao2fio2_vent_min,
                    MIN(CASE
                        WHEN upper(COALESCE(specimen_bg, '')) LIKE 'ART%'
                        THEN pco2
                        ELSE NULL
                    END) AS paco2_min,
                    MIN(bun) AS bun_min,
                    MAX(bun) AS bun_max,
                    MIN(wbc) AS wbc_min,
                    MAX(wbc) AS wbc_max,
                    MIN(potassium) AS potassium_min,
                    MAX(potassium) AS potassium_max,
                    MIN(sodium) AS sodium_min,
                    MAX(sodium) AS sodium_max,
                    MIN(bicarbonate) AS bicarbonate_min,
                    MAX(bicarbonate) AS bicarbonate_max,
                    MIN(bilirubin) AS bilirubin_min,
                    MAX(bilirubin) AS bilirubin_max,
                    MAX(bands) AS bands_max,
                    MIN(sysbp) AS sysbp_min,
                    MAX(sysbp) AS sysbp_max,
                    MIN(gcs_total) AS gcs_min,
                    SUM(COALESCE(urineoutput, 0.0)) AS urineoutput_firstday
                FROM firstday
                GROUP BY icustay_id
            ),
            scorecalc AS (
                SELECT
                    a.subject_id,
                    a.hadm_id,
                    a.icustay_id,
                    CASE
                        WHEN a.sysbp_min <= 100 THEN 1
                        WHEN a.sysbp_min IS NULL THEN NULL
                        ELSE 0
                    END AS qsofa_sysbp_score,
                    CASE
                        WHEN a.resprate_max >= 22 THEN 1
                        WHEN a.resprate_max IS NULL THEN NULL
                        ELSE 0
                    END AS qsofa_resprate_score,
                    CASE
                        WHEN a.gcs_min <= 13 THEN 1
                        WHEN a.gcs_min IS NULL THEN NULL
                        ELSE 0
                    END AS qsofa_gcs_score,
                    CASE
                        WHEN a.tempc_min < 36.0 THEN 1
                        WHEN a.tempc_max > 38.0 THEN 1
                        WHEN a.tempc_min IS NULL AND a.tempc_max IS NULL THEN NULL
                        ELSE 0
                    END AS sirs_temp_score,
                    CASE
                        WHEN a.heartrate_max > 90.0 THEN 1
                        WHEN a.heartrate_max IS NULL THEN NULL
                        ELSE 0
                    END AS sirs_heartrate_score,
                    CASE
                        WHEN a.resprate_max > 20.0 THEN 1
                        WHEN a.paco2_min < 32.0 THEN 1
                        WHEN COALESCE(a.resprate_max, a.paco2_min) IS NULL THEN NULL
                        ELSE 0
                    END AS sirs_resp_score,
                    CASE
                        WHEN a.wbc_min < 4.0 THEN 1
                        WHEN a.wbc_max > 12.0 THEN 1
                        WHEN a.bands_max > 10.0 THEN 1
                        WHEN COALESCE(a.wbc_min, a.wbc_max, a.bands_max) IS NULL THEN NULL
                        ELSE 0
                    END AS sirs_wbc_score,
                    CASE
                        WHEN a.age IS NULL THEN NULL
                        WHEN a.age < 40 THEN 0
                        WHEN a.age < 60 THEN 7
                        WHEN a.age < 70 THEN 12
                        WHEN a.age < 75 THEN 15
                        WHEN a.age < 80 THEN 16
                        WHEN a.age >= 80 THEN 18
                        ELSE 0
                    END AS sapsii_age_score,
                    CASE
                        WHEN a.heartrate_max IS NULL THEN NULL
                        WHEN a.heartrate_min < 40 THEN 11
                        WHEN a.heartrate_max >= 160 THEN 7
                        WHEN a.heartrate_max >= 120 THEN 4
                        WHEN a.heartrate_min < 70 THEN 2
                        WHEN a.heartrate_max >= 70 AND a.heartrate_max < 120
                         AND a.heartrate_min >= 70 AND a.heartrate_min < 120 THEN 0
                        ELSE NULL
                    END AS sapsii_hr_score,
                    CASE
                        WHEN a.sysbp_min IS NULL THEN NULL
                        WHEN a.sysbp_min < 70 THEN 13
                        WHEN a.sysbp_min < 100 THEN 5
                        WHEN a.sysbp_max >= 200 THEN 2
                        WHEN a.sysbp_max >= 100 AND a.sysbp_max < 200
                         AND a.sysbp_min >= 100 AND a.sysbp_min < 200 THEN 0
                        ELSE NULL
                    END AS sapsii_sysbp_score,
                    CASE
                        WHEN a.tempc_max IS NULL THEN NULL
                        WHEN a.tempc_min < 39.0 THEN 0
                        WHEN a.tempc_max >= 39.0 THEN 3
                        ELSE NULL
                    END AS sapsii_temp_score,
                    CASE
                        WHEN a.pao2fio2_vent_min IS NULL THEN NULL
                        WHEN a.pao2fio2_vent_min < 100 THEN 11
                        WHEN a.pao2fio2_vent_min < 200 THEN 9
                        WHEN a.pao2fio2_vent_min >= 200 THEN 6
                        ELSE NULL
                    END AS sapsii_pao2fio2_score,
                    CASE
                        WHEN a.urineoutput_firstday IS NULL THEN NULL
                        WHEN a.urineoutput_firstday < 500 THEN 11
                        WHEN a.urineoutput_firstday < 1000 THEN 4
                        WHEN a.urineoutput_firstday >= 1000 THEN 0
                        ELSE NULL
                    END AS sapsii_uo_score,
                    CASE
                        WHEN a.bun_max IS NULL THEN NULL
                        WHEN a.bun_max < 28 THEN 0
                        WHEN a.bun_max < 84 THEN 6
                        WHEN a.bun_max >= 84 THEN 10
                        ELSE NULL
                    END AS sapsii_bun_score,
                    CASE
                        WHEN a.wbc_max IS NULL THEN NULL
                        WHEN a.wbc_min < 1 THEN 12
                        WHEN a.wbc_max >= 20 THEN 3
                        WHEN a.wbc_max >= 1 AND a.wbc_max < 20
                         AND a.wbc_min >= 1 AND a.wbc_min < 20 THEN 0
                        ELSE NULL
                    END AS sapsii_wbc_score,
                    CASE
                        WHEN a.potassium_max IS NULL THEN NULL
                        WHEN a.potassium_min < 3 THEN 3
                        WHEN a.potassium_max >= 5 THEN 3
                        WHEN a.potassium_max >= 3 AND a.potassium_max < 5
                         AND a.potassium_min >= 3 AND a.potassium_min < 5 THEN 0
                        ELSE NULL
                    END AS sapsii_potassium_score,
                    CASE
                        WHEN a.sodium_max IS NULL THEN NULL
                        WHEN a.sodium_min < 125 THEN 5
                        WHEN a.sodium_max >= 145 THEN 1
                        WHEN a.sodium_max >= 125 AND a.sodium_max < 145
                         AND a.sodium_min >= 125 AND a.sodium_min < 145 THEN 0
                        ELSE NULL
                    END AS sapsii_sodium_score,
                    CASE
                        WHEN a.bicarbonate_max IS NULL THEN NULL
                        WHEN a.bicarbonate_min < 15 THEN 6
                        WHEN a.bicarbonate_min < 20 THEN 3
                        WHEN a.bicarbonate_max >= 20 AND a.bicarbonate_min >= 20 THEN 0
                        ELSE NULL
                    END AS sapsii_bicarbonate_score,
                    CASE
                        WHEN a.bilirubin_max IS NULL THEN NULL
                        WHEN a.bilirubin_max < 4 THEN 0
                        WHEN a.bilirubin_max < 6 THEN 4
                        WHEN a.bilirubin_max >= 6 THEN 9
                        ELSE NULL
                    END AS sapsii_bilirubin_score,
                    CASE
                        WHEN a.gcs_min IS NULL THEN NULL
                        WHEN a.gcs_min < 3 THEN NULL
                        WHEN a.gcs_min < 6 THEN 26
                        WHEN a.gcs_min < 9 THEN 13
                        WHEN a.gcs_min < 11 THEN 7
                        WHEN a.gcs_min < 14 THEN 5
                        WHEN a.gcs_min >= 14 AND a.gcs_min <= 15 THEN 0
                        ELSE NULL
                    END AS sapsii_gcs_score,
                    CASE
                        WHEN COALESCE(dc.saps_aids, 0) = 1 THEN 17
                        WHEN COALESCE(dc.saps_hem, 0) = 1 THEN 10
                        WHEN COALESCE(dc.saps_mets, 0) = 1 THEN 9
                        ELSE 0
                    END AS sapsii_comorbidity_score,
                    CASE
                        WHEN a.admission_type = 'ELECTIVE' AND a.surgical = 1 THEN 0
                        WHEN a.admission_type != 'ELECTIVE' AND a.surgical = 1 THEN 8
                        ELSE 6
                    END AS sapsii_admissiontype_score
                FROM agg a
                LEFT JOIN diag_comorb dc
                  ON a.hadm_id = dc.hadm_id
            )
            SELECT
                subject_id,
                hadm_id,
                icustay_id,
                COALESCE(qsofa_sysbp_score, 0)
              + COALESCE(qsofa_resprate_score, 0)
              + COALESCE(qsofa_gcs_score, 0) AS qsofa,
                qsofa_sysbp_score,
                qsofa_resprate_score,
                qsofa_gcs_score,
                COALESCE(sirs_temp_score, 0)
              + COALESCE(sirs_heartrate_score, 0)
              + COALESCE(sirs_resp_score, 0)
              + COALESCE(sirs_wbc_score, 0) AS sirs,
                sirs_temp_score,
                sirs_heartrate_score,
                sirs_resp_score,
                sirs_wbc_score,
                COALESCE(sapsii_age_score, 0)
              + COALESCE(sapsii_hr_score, 0)
              + COALESCE(sapsii_sysbp_score, 0)
              + COALESCE(sapsii_temp_score, 0)
              + COALESCE(sapsii_pao2fio2_score, 0)
              + COALESCE(sapsii_uo_score, 0)
              + COALESCE(sapsii_bun_score, 0)
              + COALESCE(sapsii_wbc_score, 0)
              + COALESCE(sapsii_potassium_score, 0)
              + COALESCE(sapsii_sodium_score, 0)
              + COALESCE(sapsii_bicarbonate_score, 0)
              + COALESCE(sapsii_bilirubin_score, 0)
              + COALESCE(sapsii_gcs_score, 0)
              + COALESCE(sapsii_comorbidity_score, 0)
              + COALESCE(sapsii_admissiontype_score, 0) AS sapsii,
                1 / (1 + exp(-(-7.7631
                  + 0.0737 * (
                    COALESCE(sapsii_age_score, 0)
                  + COALESCE(sapsii_hr_score, 0)
                  + COALESCE(sapsii_sysbp_score, 0)
                  + COALESCE(sapsii_temp_score, 0)
                  + COALESCE(sapsii_pao2fio2_score, 0)
                  + COALESCE(sapsii_uo_score, 0)
                  + COALESCE(sapsii_bun_score, 0)
                  + COALESCE(sapsii_wbc_score, 0)
                  + COALESCE(sapsii_potassium_score, 0)
                  + COALESCE(sapsii_sodium_score, 0)
                  + COALESCE(sapsii_bicarbonate_score, 0)
                  + COALESCE(sapsii_bilirubin_score, 0)
                  + COALESCE(sapsii_gcs_score, 0)
                  + COALESCE(sapsii_comorbidity_score, 0)
                  + COALESCE(sapsii_admissiontype_score, 0)
                )
                  + 0.9971 * ln(
                    (
                      COALESCE(sapsii_age_score, 0)
                    + COALESCE(sapsii_hr_score, 0)
                    + COALESCE(sapsii_sysbp_score, 0)
                    + COALESCE(sapsii_temp_score, 0)
                    + COALESCE(sapsii_pao2fio2_score, 0)
                    + COALESCE(sapsii_uo_score, 0)
                    + COALESCE(sapsii_bun_score, 0)
                    + COALESCE(sapsii_wbc_score, 0)
                    + COALESCE(sapsii_potassium_score, 0)
                    + COALESCE(sapsii_sodium_score, 0)
                    + COALESCE(sapsii_bicarbonate_score, 0)
                    + COALESCE(sapsii_bilirubin_score, 0)
                    + COALESCE(sapsii_gcs_score, 0)
                    + COALESCE(sapsii_comorbidity_score, 0)
                    + COALESCE(sapsii_admissiontype_score, 0)
                    ) + 1
                  )
                ))) AS sapsii_prob,
                sapsii_age_score,
                sapsii_hr_score,
                sapsii_sysbp_score,
                sapsii_temp_score,
                sapsii_pao2fio2_score,
                sapsii_uo_score,
                sapsii_bun_score,
                sapsii_wbc_score,
                sapsii_potassium_score,
                sapsii_sodium_score,
                sapsii_bicarbonate_score,
                sapsii_bilirubin_score,
                sapsii_gcs_score,
                sapsii_comorbidity_score,
                sapsii_admissiontype_score
            FROM scorecalc
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step12b done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 12c: hourly OASIS with 24h rolling window (Source: pivot/pivoted_oasis.sql)
# ---------------------------------------------------------------------------
def step12c_oasis(con):
    name = "12c_oasis"
    if exists(name):
        log.info("step12c cached"); return
    t0 = time.time()
    log.info("step12c hourly OASIS (24h rolling window)...")

    con.execute(f"CREATE OR REPLACE VIEW joined AS SELECT * FROM read_parquet('{inter('11_joined')}')")

    con.execute(f"""
        COPY (
            -- Source: pivot/pivoted_oasis.sql
            -- Structure: scorecomp (per-hour values) → scorecalc (per-hour scores, UO uses SUM OVER W24)
            --            → score_final (MAX(per-hour score) OVER W24 for dynamic components)
            WITH surgical_flag AS (
                SELECT icustay_id,
                    MAX(CASE WHEN LOWER(COALESCE(curr_service, '')) LIKE '%surg%'
                             OR curr_service = 'ORTHO' THEN 1 ELSE 0 END) AS surgical
                FROM joined
                GROUP BY icustay_id
            ),
            scorecomp AS (
                SELECT
                    j.icustay_id,
                    j.hr,
                    j.hospadmtime   AS preiculos_hours,
                    j.age,
                    j.admission_type,
                    sf.surgical,
                    j.gcs_total,
                    j.heartrate,
                    j.meanbp,
                    j.resprate,
                    j.tempc,
                    CASE WHEN COALESCE(j.vent_invasive_flag, 0) = 1 THEN 1 ELSE 0 END AS mechvent,
                    COALESCE(j.urineoutput, 0) AS uo_hr
                FROM joined j
                LEFT JOIN surgical_flag sf ON j.icustay_id = sf.icustay_id
            ),
            scorecalc AS (
                -- per-hour component scores; UO score based on 24h rolling sum (per official scorecalc)
                SELECT
                    icustay_id, hr,
                    CASE
                        WHEN preiculos_hours IS NULL THEN NULL
                        WHEN preiculos_hours * 60.0 < 10.2    THEN 5
                        WHEN preiculos_hours * 60.0 < 297.0   THEN 3
                        WHEN preiculos_hours * 60.0 < 1440.0  THEN 0
                        WHEN preiculos_hours * 60.0 < 18708.0 THEN 1
                        ELSE 2
                    END AS oasis_preiculos_score,
                    CASE
                        WHEN age IS NULL THEN NULL
                        WHEN age < 24  THEN 0
                        WHEN age <= 53 THEN 3
                        WHEN age <= 77 THEN 6
                        WHEN age <= 89 THEN 9
                        WHEN age >= 90 THEN 7
                        ELSE 0
                    END AS oasis_age_score,
                    CASE
                        WHEN admission_type = 'ELECTIVE' AND surgical = 1 THEN 0
                        WHEN admission_type IS NULL OR surgical IS NULL   THEN NULL
                        ELSE 6
                    END AS oasis_electivesurgery_score,
                    CASE
                        WHEN gcs_total IS NULL THEN NULL
                        WHEN gcs_total <= 7  THEN 10
                        WHEN gcs_total < 14  THEN 4
                        WHEN gcs_total = 14  THEN 3
                        ELSE 0
                    END AS gcs_score_hr,
                    CASE
                        WHEN heartrate IS NULL THEN NULL
                        WHEN heartrate > 125               THEN 6
                        WHEN heartrate < 33                THEN 4
                        WHEN heartrate BETWEEN 107 AND 125 THEN 3
                        WHEN heartrate BETWEEN 89  AND 106 THEN 1
                        ELSE 0
                    END AS heartrate_score_hr,
                    CASE
                        WHEN meanbp IS NULL THEN NULL
                        WHEN meanbp < 20.65                    THEN 4
                        WHEN meanbp < 51                       THEN 3
                        WHEN meanbp > 143.44                   THEN 3
                        WHEN meanbp >= 51 AND meanbp < 61.33   THEN 2
                        ELSE 0
                    END AS meanbp_score_hr,
                    CASE
                        WHEN resprate IS NULL THEN NULL
                        WHEN resprate < 6   THEN 10
                        WHEN resprate > 44  THEN 9
                        WHEN resprate > 30  THEN 6
                        WHEN resprate > 22  THEN 1
                        WHEN resprate < 13  THEN 1
                        ELSE 0
                    END AS resprate_score_hr,
                    CASE
                        WHEN tempc IS NULL THEN NULL
                        WHEN tempc > 39.88                        THEN 6
                        WHEN tempc BETWEEN 33.22 AND 35.93        THEN 4
                        WHEN tempc < 33.22                         THEN 3
                        WHEN tempc > 35.93 AND tempc <= 36.39     THEN 2
                        WHEN tempc >= 36.89 AND tempc <= 39.88    THEN 2
                        ELSE 0
                    END AS temp_score_hr,
                    CASE WHEN mechvent = 1 THEN 9 ELSE 0 END AS mechvent_score_hr,
                    CASE
                        WHEN SUM(uo_hr) OVER W24 < 671.09                    THEN 10
                        WHEN SUM(uo_hr) OVER W24 > 6896.80                   THEN 8
                        WHEN SUM(uo_hr) OVER W24 BETWEEN 671.09 AND 1426.99  THEN 5
                        WHEN SUM(uo_hr) OVER W24 BETWEEN 1427.00 AND 2544.14 THEN 1
                        ELSE 0
                    END AS uo_score_hr
                FROM scorecomp
                WINDOW W24 AS (
                    PARTITION BY icustay_id ORDER BY hr
                    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                )
            ),
            score_final AS (
                -- worst score over last 24h for each dynamic component (per official score_final)
                SELECT
                    icustay_id, hr,
                    oasis_preiculos_score,
                    oasis_electivesurgery_score,
                    COALESCE(MAX(oasis_age_score)      OVER W24, 0) AS oasis_age_score,
                    COALESCE(MAX(gcs_score_hr)         OVER W24, 0) AS oasis_gcs_score,
                    COALESCE(MAX(heartrate_score_hr)   OVER W24, 0) AS oasis_heartrate_score,
                    COALESCE(MAX(meanbp_score_hr)      OVER W24, 0) AS oasis_meanbp_score,
                    COALESCE(MAX(resprate_score_hr)    OVER W24, 0) AS oasis_resprate_score,
                    COALESCE(MAX(temp_score_hr)        OVER W24, 0) AS oasis_temp_score,
                    COALESCE(MAX(mechvent_score_hr)    OVER W24, 0) AS oasis_mechvent_score,
                    COALESCE(MAX(uo_score_hr)          OVER W24, 0) AS oasis_urineoutput_score
                FROM scorecalc
                WINDOW W24 AS (
                    PARTITION BY icustay_id ORDER BY hr
                    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                )
            )
            SELECT
                icustay_id, hr,
                COALESCE(oasis_age_score, 0)
              + COALESCE(oasis_preiculos_score, 0)
              + oasis_gcs_score
              + oasis_heartrate_score
              + oasis_meanbp_score
              + oasis_resprate_score
              + oasis_temp_score
              + oasis_urineoutput_score
              + oasis_mechvent_score
              + COALESCE(oasis_electivesurgery_score, 0) AS oasis,
                1 / (1 + EXP(-(-6.1746 + 0.1275 * (
                    COALESCE(oasis_age_score, 0)
                  + COALESCE(oasis_preiculos_score, 0)
                  + oasis_gcs_score
                  + oasis_heartrate_score
                  + oasis_meanbp_score
                  + oasis_resprate_score
                  + oasis_temp_score
                  + oasis_urineoutput_score
                  + oasis_mechvent_score
                  + COALESCE(oasis_electivesurgery_score, 0)
                )))) AS oasis_prob,
                oasis_age_score,
                oasis_preiculos_score,
                oasis_gcs_score,
                oasis_heartrate_score,
                oasis_meanbp_score,
                oasis_resprate_score,
                oasis_temp_score,
                oasis_urineoutput_score,
                oasis_mechvent_score,
                oasis_electivesurgery_score
            FROM score_final
        ) TO '{inter(name)}' (FORMAT PARQUET)
    """)
    log.info("step12c done %.1fs", time.time() - t0)


# ---------------------------------------------------------------------------
# Step 13: final wide table
# ---------------------------------------------------------------------------
def step13_final(con):
    if os.path.exists(OUT_PATH) and not final_output_is_stale():
        log.info("final output is up to date, skipping"); return

    if os.path.exists(OUT_PATH):
        backup_existing_output(OUT_PATH)
        os.remove(OUT_PATH)
        log.info("removed stale final output so it can be rebuilt")

    t0 = time.time()
    log.info("step13 building final wide table...")

    con.execute(f"CREATE OR REPLACE VIEW joined       AS SELECT * FROM read_parquet('{inter('11_joined')}')")
    con.execute(f"CREATE OR REPLACE VIEW sepsis_lbl   AS SELECT * FROM read_parquet('{inter('12_sepsislabel')}')")
    con.execute(f"CREATE OR REPLACE VIEW sev_first    AS SELECT * FROM read_parquet('{inter('severity_scores_firstday')}')")
    con.execute(f"CREATE OR REPLACE VIEW oasis_hourly AS SELECT * FROM read_parquet('{inter('12c_oasis')}')")

    out_path = OUT_PATH.replace("\\", "/")
    con.execute(f"""
        COPY (
            SELECT
                j.subject_id,
                j.hadm_id,
                j.icustay_id,
                j.hr,
                j.charttime_floor,

                -- static demographics + outcomes
                j.age,
                j.gender,
                j.dbsource,
                j.first_careunit,
                j.last_careunit,
                j.hospadmtime,
                j.hospital_expire_flag,
                j.admission_type,
                j.admission_location,
                j.discharge_location,
                j.deathtime,
                j.insurance,
                j.ethnicity,
                j.ethnicity_grouped,
                j.marital_status,
                j.dod,
                j.dod_hosp,
                j.expire_flag,
                j.hospstay_seq,
                j.first_hosp_stay,
                j.icustay_seq,
                j.first_icu_stay,
                j.los_hospital,
                j.los_icu,
                j.intime,
                j.outtime,

                -- comorbidity + service + anthropometrics
                j.congestive_heart_failure,
                j.cardiac_arrhythmias,
                j.valvular_disease,
                j.pulmonary_circulation,
                j.peripheral_vascular,
                j.hypertension,
                j.paralysis,
                j.other_neurological,
                j.chronic_pulmonary,
                j.diabetes_uncomplicated,
                j.diabetes_complicated,
                j.hypothyroidism,
                j.renal_failure,
                j.liver_disease,
                j.peptic_ulcer,
                j.aids,
                j.lymphoma,
                j.metastatic_cancer,
                j.solid_tumor,
                j.rheumatoid_arthritis,
                j.coagulopathy,
                j.obesity,
                j.weight_loss,
                j.fluid_electrolyte,
                j.blood_loss_anemia,
                j.deficiency_anemias,
                j.alcohol_abuse,
                j.drug_abuse,
                j.psychoses,
                j.depression,
                j.elixhauser_vanwalraven,
                j.elixhauser_SID29,
                j.elixhauser_SID30,
                j.curr_service,
                j.height_first,
                j.height_min,
                j.height_max,
                j.weight_first,
                j.weight_min,
                j.weight_max,

                -- vitals
                j.heartrate,
                j.tempc,
                j.sysbp,
                j.diasbp,
                j.meanbp,
                j.resprate,
                j.spo2,
                j.glucose,
                j.etco2,

                -- GCS
                j.gcs_motor,
                j.gcs_verbal,
                j.gcs_eyes,
                j.gcs_total,
                j.gcs_sedated,

                -- blood gas
                j.specimen_bg,
                j.ph,
                j.pco2,
                j.po2,
                j.fio2,
                j.aado2,
                j.baseexcess,
                j.bicarbonate_bg,
                j.totalco2,
                j.chloride_bg,
                j.calcium_bg,
                j.glucose_bg,
                j.hematocrit_bg,
                j.hemoglobin_bg,
                j.intubated_bg,
                j.peep,
                j.so2,
                j.carboxyhemoglobin,
                j.methemoglobin,
                j.o2flow,
                j.potassium_bg,
                j.requiredo2,
                j.sodium_bg,
                j.temperature_bg,
                j.tidalvolume_bg,
                j.ventilationrate_bg,
                j.ventilator_bg,

                -- chemistry / core labs
                j.albumin,
                j.aniongap,
                j.bicarbonate,
                j.bilirubin,
                j.bilirubin_direct,
                j.bilirubin_indirect,
                j.bun,
                j.calcium,
                j.chloride,
                j.creatinine,
                j.glucose_lab,
                j.hematocrit,
                j.hemoglobin,
                j.inr,
                j.lactate,
                j.magnesium,
                j.phosphate,
                j.platelet,
                j.potassium,
                j.ptt,
                j.sodium,
                j.wbc,

                -- enzyme group
                j.alt,
                j.alp,
                j.ast,
                j.amylase,
                j.ck_cpk,
                j.ck_mb,
                j.ggt,
                j.ldh,
                j.lipase,

                -- coagulation extended
                j.fibrinogen,
                j.pt,
                j.d_dimer,
                j.thrombin,

                -- cardiac / inflammation
                j.troponin_i,
                j.troponin_t,
                j.ntprobnp,
                j.crp,

                -- CBC extended
                j.mch,
                j.mchc,
                j.mcv,
                j.rbc,
                j.rdw,

                -- blood differential
                j.neutrophils_pct,
                j.lymphocytes_pct,
                j.monocytes_pct,
                j.eosinophils_pct,
                j.basophils_pct,
                j.bands,
                j.neutrophils_abs,
                j.lymphocytes_abs,
                j.monocytes_abs,

                -- urine + other fluid outputs (UO nulled for pre-ICU hours)
                CASE WHEN j.hr < 0 THEN NULL ELSE j.urineoutput END AS urineoutput,
                j.chest_tube_output,
                j.drain_output,
                j.ng_tube_output,
                j.stool_output,

                -- vasopressors (nulled for pre-ICU hours)
                CASE WHEN j.hr < 0 THEN NULL ELSE j.rate_norepinephrine  END AS rate_norepinephrine,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.rate_epinephrine     END AS rate_epinephrine,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.rate_dopamine        END AS rate_dopamine,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.rate_dobutamine      END AS rate_dobutamine,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.rate_vasopressin     END AS rate_vasopressin,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.rate_phenylephrine   END AS rate_phenylephrine,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.rate_milrinone       END AS rate_milrinone,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.norepi_flag          END AS norepi_flag,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.epi_flag             END AS epi_flag,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.dopa_flag            END AS dopa_flag,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.dobu_flag            END AS dobu_flag,
                CASE WHEN j.hr < 0 THEN NULL ELSE j.vaso_flag            END AS vaso_flag,

                -- sedation / analgesia / metabolic
                j.rate_propofol,
                j.rate_midazolam,
                j.rate_dexmedetomidine,
                j.rate_fentanyl,
                j.rate_insulin,
                j.nmb_flag,

                -- fluid balance inputs
                j.crystalloid_bolus_ml,
                j.colloid_bolus_ml,
                j.rbc_transfusion_ml,
                j.ffp_transfusion_ml,
                j.crystalloid_ml,
                j.colloid_ml,

                -- ventilation
                j.vent_invasive_flag,
                j.vent_noninvasive_flag,
                j.cpap_flag,
                j.oxygen_therapy_flag,
                j.vent_flag,
                j.extubated_flag,
                j.self_extubated_flag,
                j.vent_status,

                -- ICP / invasive lines / code status
                j.icp,
                j.arterial_line_flag,
                j.cvl_flag,
                j.pa_catheter_flag,
                j.trauma_line_flag,
                j.ava_line_flag,
                j.icp_catheter_flag,
                j.any_invasive_line_flag,
                j.code_status,
                j.full_code_flag,
                j.dnr_flag,
                j.dni_flag,
                j.cmo_flag,
                j.fullcode_first,
                j.cmo_first,
                j.dnr_first,
                j.dni_first,
                j.dncpr_first,
                j.fullcode_last,
                j.cmo_last,
                j.dnr_last,
                j.dni_last,
                j.dncpr_last,
                j.fullcode_ever,
                j.cmo_ever,
                j.dnr_ever,
                j.dni_ever,
                j.dncpr_ever,
                j.dnr_first_charttime,
                j.dni_first_charttime,
                j.dncpr_first_charttime,
                j.timecmo_chart,

                -- official-style first-day severity scores (nulled for pre-ICU hours)
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.qsofa                    END AS qsofa,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.qsofa_sysbp_score        END AS qsofa_sysbp_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.qsofa_resprate_score     END AS qsofa_resprate_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.qsofa_gcs_score          END AS qsofa_gcs_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sirs                     END AS sirs,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sirs_temp_score          END AS sirs_temp_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sirs_heartrate_score     END AS sirs_heartrate_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sirs_resp_score          END AS sirs_resp_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sirs_wbc_score           END AS sirs_wbc_score,
                -- hourly OASIS (24h rolling window per pivoted_oasis.sql, nulled for pre-ICU hours)
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis                    END AS oasis,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_prob               END AS oasis_prob,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_age_score          END AS oasis_age_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_preiculos_score    END AS oasis_preiculos_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_gcs_score          END AS oasis_gcs_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_heartrate_score    END AS oasis_heartrate_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_meanbp_score       END AS oasis_meanbp_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_resprate_score     END AS oasis_resprate_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_temp_score         END AS oasis_temp_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_urineoutput_score  END AS oasis_urineoutput_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_mechvent_score     END AS oasis_mechvent_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE oh.oasis_electivesurgery_score END AS oasis_electivesurgery_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii                   END AS sapsii,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_prob              END AS sapsii_prob,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_age_score         END AS sapsii_age_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_hr_score          END AS sapsii_hr_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_sysbp_score       END AS sapsii_sysbp_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_temp_score        END AS sapsii_temp_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_pao2fio2_score    END AS sapsii_pao2fio2_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_uo_score          END AS sapsii_uo_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_bun_score         END AS sapsii_bun_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_wbc_score         END AS sapsii_wbc_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_potassium_score   END AS sapsii_potassium_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_sodium_score      END AS sapsii_sodium_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_bicarbonate_score END AS sapsii_bicarbonate_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_bilirubin_score   END AS sapsii_bilirubin_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_gcs_score         END AS sapsii_gcs_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_comorbidity_score END AS sapsii_comorbidity_score,
                CASE WHEN j.hr < 0 THEN NULL ELSE sf.sapsii_admissiontype_score END AS sapsii_admissiontype_score,

                -- dialysis + prescriptions
                j.dialysis_present,
                j.dialysis_active,
                j.dialysis_type,
                j.antibiotic_flag,
                j.steroid_flag,

                -- SOFA components (from step12, LOCF-based; nulled for pre-ICU hours)
                CASE WHEN j.hr < 0 THEN NULL ELSE s.meanbp_lf               END AS meanbp_lf,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.rate_norepinephrine_24h  END AS rate_norepinephrine_24h,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.rate_epinephrine_24h     END AS rate_epinephrine_24h,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.rate_dopamine_24h        END AS rate_dopamine_24h,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.rate_dobutamine_24h      END AS rate_dobutamine_24h,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.rate_vasopressin_24h     END AS rate_vasopressin_24h,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.rate_phenylephrine_24h   END AS rate_phenylephrine_24h,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.urineoutput_24h          END AS urineoutput_24h,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.pafi_vent_min_24h        END AS pafi_vent_min_24h,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.pafi_novent_min_24h      END AS pafi_novent_min_24h,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.sofa_resp                END AS sofa_resp,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.sofa_coag                END AS sofa_coag,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.sofa_liver               END AS sofa_liver,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.sofa_cv                  END AS sofa_cv,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.sofa_cns                 END AS sofa_cns,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.sofa_renal               END AS sofa_renal,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.sofa_total               END AS sofa_total,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.sofa_delta_24h           END AS sofa_delta_24h,

                -- sepsis timing (nulled for pre-ICU hours)
                CASE WHEN j.hr < 0 THEN NULL ELSE s.t_suspicion   END AS t_suspicion,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.si_starttime   END AS si_starttime,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.si_endtime     END AS si_endtime,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.t_sofa         END AS t_sofa,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.t_sepsis       END AS t_sepsis,
                CASE WHEN j.hr < 0 THEN NULL ELSE s.SepsisLabel    END AS SepsisLabel

            FROM joined j
            LEFT JOIN sev_first sf
              ON j.icustay_id = sf.icustay_id
            LEFT JOIN oasis_hourly oh
              ON j.icustay_id = oh.icustay_id AND j.hr = oh.hr
            LEFT JOIN sepsis_lbl s
              ON j.icustay_id = s.icustay_id AND j.hr = s.hr
            ORDER BY j.icustay_id, j.hr
        ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    log.info("step13 done %.1fs  →  %s", time.time() - t0, OUT_PATH)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def _invalidate_stale_caches():
    stale = []
    # These intermediate files have new columns — delete them to force rebuild
    pass  # no schema-migration checks currently needed
    for name in stale:
        p = inter(name)
        os.remove(p)
        log.info("removed stale cache: %s", p)


def main():
    con = connect()
    register_views(con)

    _invalidate_stale_caches()

    step01_cohort(con)
    step_icustay_times(con)
    step02_time_axis(con)
    step03_06_10_chartevents(con)   # single 33GB scan: vitals, GCS, vent, hw, fio2_chart, crrt_cv
    step04_labs(con)
    step05_bg(con)                  # uses fio2_chart produced above
    step07_uo(con)
    step08_vaso(con)
    step09_suspinfect(con)
    step_static(con)
    step_elixhauser(con)
    step_service(con)
    step_other_outputs(con)
    step_prescription_flags(con)
    step_crrt(con)
    step11_join(con)
    step12_sepsis_label(con)
    step_severity_scores_firstday(con)
    step12c_oasis(con)
    step13_final(con)

    result = con.execute(
        f"SELECT COUNT(*) AS rows, COUNT(DISTINCT icustay_id) AS stays, "
        f"SUM(SepsisLabel) AS sepsis_rows, COUNT(*) AS total_cols "
        f"FROM read_parquet('{OUT_PATH}')"
    ).fetchone()
    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{OUT_PATH}') LIMIT 0").fetchall()
    log.info("FINAL: %d rows, %d ICU stays, %d sepsis rows, %d columns",
             result[0], result[1], result[2], len(cols))
    con.close()
    log.info("done")


if __name__ == "__main__":
    main()
