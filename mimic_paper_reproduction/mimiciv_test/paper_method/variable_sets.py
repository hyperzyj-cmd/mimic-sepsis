"""
Current variable-set specification for MIMIC-IV full-wide experiments.

This file defines the single active feature mainline used by the current
MIMIC-IV task suite.
"""

from __future__ import annotations


ID_COLS = [
    "subject_id",
    "hadm_id",
    "stay_id",
]

TIME_INDEX_COLS = [
    "hr",
    "charttime_floor",
]

LABEL_BUILDER_COLS = [
    "t_suspicion",
    "onset_hr",
    "SepsisLabel",
]

# Columns excluded because they either encode the label construction process,
# expose post-outcome timeline context, or directly represent treatment/state
# variables too tightly coupled to sepsis definition for the current setup.
FORBIDDEN_LEAKAGE_COLS = [
    "t_suspicion",
    "onset_hr",
    "SepsisLabel",
    "sepsis3",
    "sofa_time",
    "sofa_score",
    "antibiotic_time",
    "culture_time",
    "specimen",
    "positive_culture",
    "hospital_expire_flag",
    "dod",
    "deathtime",
    "hospadmtime",
    "admittime",
    "dischtime",
    "edregtime",
    "edouttime",
    "starttime",
    "endtime",
    "intime",
    "outtime",
    "los_hospital",
    "los_icu",
    "hospstay_seq",
    "first_hosp_stay",
    "icustay_seq",
    "first_icu_stay",
    "antibiotic_flag",
    "sofa_respiration",
    "sofa_coagulation",
    "sofa_liver",
    "sofa_cardiovascular",
    "sofa_cns",
    "sofa_renal",
    "sofa_24hours",
    "crrt_flag",
    "dobu_rate",
    "norepi_rate",
    "epi_rate",
    "dopa_rate",
    "phenyl_rate",
    "vaso_rate",
    "ventilation_status",
    "crrt_mode",
    "crrt_current_goal",
    "crrt_blood_flow",
    "crrt_dialysate_rate",
    "crrt_replacement_rate",
    "crrt_ultrafiltrate_output",
    "crrt_system_active",
    "crrt_clots",
    "crrt_clots_increasing",
    "crrt_clotted",
    "first_careunit",
    "last_careunit",
    "curr_service",
    "anchor_year_group",
    "discharge_location",
]


STATIC_COLS = [
    "age",
    "gender",
    "charlson_score",
    "admission_type",
    "admission_location",
    "insurance",
    "language",
    "marital_status",
    "race",
    "height",
    "weight",
]

DYNAMIC_COLS = [
    "hr",
    "heart_rate",
    "sbp",
    "dbp",
    "mbp",
    "sbp_ni",
    "dbp_ni",
    "mbp_ni",
    "resp_rate",
    "respiratory_rate_set",
    "respiratory_rate_total",
    "respiratory_rate_spontaneous",
    "spo2",
    "temperature",
    "temperature_site",
    "glucose_vital",
    "heart_rhythm",
    "ectopy_type",
    "ectopy_frequency",
    "ectopy_type_secondary",
    "ectopy_frequency_secondary",
    "gcs_motor",
    "gcs_verbal",
    "gcs_eyes",
    "gcs_unable",
    "gcs_total",
    "ph",
    "pco2",
    "po2",
    "so2",
    "aado2",
    "aado2_calc",
    "pao2fio2ratio",
    "pao2fio2ratio_art",
    "fio2",
    "fio2_chartevents",
    "arterial_bg_flag",
    "baseexcess",
    "bicarbonate_bg",
    "totalco2_bg",
    "lactate",
    "carboxyhemoglobin",
    "methemoglobin",
    "o2flow",
    "flow_rate",
    "minute_volume",
    "tidal_volume_set",
    "tidal_volume_observed",
    "tidal_volume_spontaneous",
    "plateau_pressure",
    "peep_vent",
    "fio2_vent",
    "ventilator_mode",
    "ventilator_mode_hamilton",
    "ventilator_type",
    "peep",
    "requiredo2",
    "bg_specimen",
    "calcium_ionized",
    "creatinine",
    "sodium",
    "sodium_bg",
    "potassium",
    "potassium_bg",
    "bicarbonate",
    "bun",
    "calcium_total",
    "chloride",
    "chloride_bg",
    "glucose_lab",
    "glucose_bg",
    "anion_gap",
    "albumin",
    "globulin",
    "total_protein",
    "platelet",
    "hemoglobin",
    "hemoglobin_bg",
    "hematocrit",
    "hematocrit_bg",
    "wbc",
    "wbc_diff",
    "mch",
    "mchc",
    "mcv",
    "rbc",
    "rdw",
    "rdwsd",
    "alt",
    "alp",
    "ast",
    "amylase",
    "bilirubin_total",
    "bilirubin_direct",
    "bilirubin_indirect",
    "ck_cpk",
    "ck_mb",
    "ggt",
    "ldh",
    "d_dimer",
    "fibrinogen",
    "thrombin",
    "inr",
    "pt",
    "ptt",
    "temperature_bg",
    "neutrophils_pct",
    "neutrophils_abs",
    "lymphocytes_pct",
    "lymphocytes_abs",
    "monocytes_pct",
    "monocytes_abs",
    "eosinophils_pct",
    "eosinophils_abs",
    "basophils_pct",
    "basophils_abs",
    "bands",
    "immature_granulocytes",
    "atypical_lymphocytes",
    "metamyelocytes",
    "nrbc",
    "troponin_t",
    "ntprobnp",
    "crp",
    "invasive_line_count",
    "invasive_line_types",
    "invasive_line_sites",
    "weight_type",
    "admit_weight",
    "daily_weight",
    "uo_weight",
    "urine_output",
    "urine_output_24h",
]


def unique_preserve_order(columns: list[str]) -> list[str]:
    seen = set()
    out = []
    for col in columns:
        if col not in seen:
            out.append(col)
            seen.add(col)
    return out


STATIC_COLS = unique_preserve_order(STATIC_COLS)
DYNAMIC_COLS = unique_preserve_order(DYNAMIC_COLS)


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
