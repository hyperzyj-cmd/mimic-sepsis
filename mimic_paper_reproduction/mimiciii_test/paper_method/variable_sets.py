"""
Current variable-set specification for MIMIC-III full-wide experiments.

This file keeps a single active feature mainline aligned to the rebuilt
`mimic3_wide.parquet` table. The goal is to keep as many columns as possible
while removing direct leakage, post-outcome information, and label-builder
artifacts.
"""

from __future__ import annotations

import duckdb
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PARQUET = REPO_ROOT / "build_mimic" / "mimiciii" / "output" / "mimic3_wide.parquet"


ID_COLS = [
    "SUBJECT_ID",
    "HADM_ID",
    "ICUSTAY_ID",
]

TIME_INDEX_COLS = [
    "hr",
    "charttime_floor",
]

LABEL_BUILDER_COLS = [
    "SepsisLabel",
    "t_suspicion",
    "si_starttime",
    "si_endtime",
    "t_sofa",
    "t_sepsis",
]


STATIC_CANDIDATES = [
    "age",
    "GENDER",
    "DBSOURCE",
    "FIRST_CAREUNIT",
    "ADMISSION_TYPE",
    "ADMISSION_LOCATION",
    "INSURANCE",
    "ETHNICITY",
    "ethnicity_grouped",
    "MARITAL_STATUS",
    "congestive_heart_failure",
    "cardiac_arrhythmias",
    "valvular_disease",
    "pulmonary_circulation",
    "peripheral_vascular",
    "hypertension",
    "paralysis",
    "other_neurological",
    "chronic_pulmonary",
    "diabetes_uncomplicated",
    "diabetes_complicated",
    "hypothyroidism",
    "renal_failure",
    "liver_disease",
    "peptic_ulcer",
    "aids",
    "lymphoma",
    "metastatic_cancer",
    "solid_tumor",
    "rheumatoid_arthritis",
    "coagulopathy",
    "obesity",
    "weight_loss",
    "fluid_electrolyte",
    "blood_loss_anemia",
    "deficiency_anemias",
    "alcohol_abuse",
    "drug_abuse",
    "psychoses",
    "depression",
    "elixhauser_vanwalraven",
    "elixhauser_SID29",
    "elixhauser_SID30",
    "height_first",
    "height_min",
    "height_max",
    "weight_first",
    "weight_min",
    "weight_max",
]


# Direct leakage, post-outcome variables, or score columns that collapse the
# target definition into a handcrafted summary.
FORBIDDEN_LEAKAGE_COLS = [
    *ID_COLS,
    *TIME_INDEX_COLS,
    *LABEL_BUILDER_COLS,
    "hospadmtime",
    "hospital_expire_flag",
    "DISCHARGE_LOCATION",
    "DEATHTIME",
    "DOD",
    "DOD_HOSP",
    "EXPIRE_FLAG",
    "hospstay_seq",
    "first_hosp_stay",
    "icustay_seq",
    "first_icu_stay",
    "los_hospital",
    "los_icu",
    "INTIME",
    "OUTTIME",
    "LAST_CAREUNIT",
    "antibiotic_flag",
    "code_status",
    "full_code_flag",
    "dnr_flag",
    "dni_flag",
    "cmo_flag",
    "fullcode_first",
    "cmo_first",
    "dnr_first",
    "dni_first",
    "dncpr_first",
    "fullcode_last",
    "cmo_last",
    "dnr_last",
    "dni_last",
    "dncpr_last",
    "fullcode_ever",
    "cmo_ever",
    "dnr_ever",
    "dni_ever",
    "dncpr_ever",
    "dnr_first_charttime",
    "dni_first_charttime",
    "dncpr_first_charttime",
    "timecmo_chart",
    "qsofa",
    "qsofa_sysbp_score",
    "qsofa_resprate_score",
    "qsofa_gcs_score",
    "sirs",
    "sirs_temp_score",
    "sirs_heartrate_score",
    "sirs_resp_score",
    "sirs_wbc_score",
    "oasis",
    "oasis_prob",
    "oasis_age_score",
    "oasis_preiculos_score",
    "oasis_gcs_score",
    "oasis_heartrate_score",
    "oasis_meanbp_score",
    "oasis_resprate_score",
    "oasis_temp_score",
    "oasis_urineoutput_score",
    "oasis_mechvent_score",
    "oasis_electivesurgery_score",
    "sapsii",
    "sapsii_prob",
    "sapsii_age_score",
    "sapsii_hr_score",
    "sapsii_sysbp_score",
    "sapsii_temp_score",
    "sapsii_pao2fio2_score",
    "sapsii_uo_score",
    "sapsii_bun_score",
    "sapsii_wbc_score",
    "sapsii_potassium_score",
    "sapsii_sodium_score",
    "sapsii_bicarbonate_score",
    "sapsii_bilirubin_score",
    "sapsii_gcs_score",
    "sapsii_comorbidity_score",
    "sapsii_admissiontype_score",
    "rate_norepinephrine_24h",
    "rate_epinephrine_24h",
    "rate_dopamine_24h",
    "rate_dobutamine_24h",
    "rate_vasopressin_24h",
    "rate_phenylephrine_24h",
    "urineoutput_24h",
    "pafi_vent_min_24h",
    "pafi_novent_min_24h",
    "sofa_resp",
    "sofa_coag",
    "sofa_liver",
    "sofa_cv",
    "sofa_cns",
    "sofa_renal",
    "sofa_total",
    "sofa_delta_24h",
]


def unique_preserve_order(columns: list[str]) -> list[str]:
    seen = set()
    out = []
    for col in columns:
        if col not in seen:
            out.append(col)
            seen.add(col)
    return out


def load_all_columns() -> list[str]:
    con = duckdb.connect()
    path = str(PARQUET).replace("\\", "/")
    df = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}')"
    ).fetchdf()
    con.close()
    return df["column_name"].tolist()


ALL_COLUMNS = load_all_columns()

STATIC_COLS = unique_preserve_order(
    [col for col in STATIC_CANDIDATES if col in ALL_COLUMNS]
)

DYNAMIC_COLS = unique_preserve_order(
    [
        col
        for col in ALL_COLUMNS
        if col not in FORBIDDEN_LEAKAGE_COLS
        and col not in STATIC_COLS
    ]
)


def validate_no_leakage(static_cols: list[str], dynamic_cols: list[str]) -> None:
    final_cols = set(static_cols) | set(dynamic_cols)
    forbidden_in_final = sorted(final_cols & set(FORBIDDEN_LEAKAGE_COLS))
    if forbidden_in_final:
        raise ValueError(
            "Forbidden leakage columns present in final variable set: "
            + ", ".join(forbidden_in_final)
        )


validate_no_leakage(STATIC_COLS, DYNAMIC_COLS)


VARIABLE_SETS = {
    "near_full_wide": {
        "static": STATIC_COLS,
        "dynamic": DYNAMIC_COLS,
    },
}
