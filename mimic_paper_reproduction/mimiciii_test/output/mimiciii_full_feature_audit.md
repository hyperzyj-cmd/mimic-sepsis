# MIMIC-III Full Feature Audit

Wide table source: `build_mimic/mimiciii/output/mimic3_wide.parquet`

- total columns audited: `301`
- kept static columns: `49`
- kept dynamic columns: `145`
- excluded columns: `107`

## Category counts

- excluded_derived_acuity_score: `45`
- excluded_future_aggregate: `10`
- excluded_goals_of_care: `23`
- excluded_identifier: `3`
- excluded_label_builder: `6`
- excluded_label_proxy_treatment: `1`
- excluded_manual_leakage_rule: `1`
- excluded_post_outcome_metadata: `16`
- excluded_time_index_structural: `2`
- retained_dynamic_intervention_signal: `43`
- retained_dynamic_other: `10`
- retained_dynamic_physiology_lab: `91`
- retained_dynamic_workflow: `1`
- retained_static_admission_context: `8`
- retained_static_comorbidity: `33`
- retained_static_demographic_anthropometric: `8`

## Important notes

- `hr` is not in `variable_sets.py`, but the runner still injects it into the hourly relational table as structural sequence context.
- static columns are defined in `variable_sets.py`, but current III/IV notebook defaults keep `include_static_in_root=False`, so they are not used in default training runs.
- retained intervention/workflow signals are not hard leakage under the current rule set, but they are the main remaining boundary group if a more conservative feature set is later desired.

## Boundary retained columns worth extra policy review

- CURR_SERVICE
- ventilationrate_bg
- ventilator_bg
- rate_norepinephrine
- rate_dopamine
- rate_dobutamine
- rate_vasopressin
- rate_milrinone
- norepi_flag
- epi_flag
- dopa_flag
- dobu_flag
- vaso_flag
- rate_propofol
- rate_midazolam
- rate_dexmedetomidine
- rate_fentanyl
- rate_insulin
- nmb_flag
- crystalloid_bolus_ml
- colloid_bolus_ml
- rbc_transfusion_ml
- ffp_transfusion_ml
- crystalloid_ml
- colloid_ml
- vent_invasive_flag
- vent_noninvasive_flag
- cpap_flag
- oxygen_therapy_flag
- vent_flag
- extubated_flag
- self_extubated_flag
- vent_status
- arterial_line_flag
- cvl_flag
- pa_catheter_flag
- trauma_line_flag
- ava_line_flag
- icp_catheter_flag
- any_invasive_line_flag
- dialysis_present
- dialysis_active
- dialysis_type
- steroid_flag
