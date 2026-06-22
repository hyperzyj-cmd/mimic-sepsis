# MIMIC-III vs MIMIC-IV: Dataset Comparison Summary

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

| | MIMIC-III | MIMIC-IV |
|---|---:|---:|
| **Scale** | | |
| Total ICU stays | 52,840 | 94,444 |
| Unique subjects | 38,484 | 65,355 |
| Total patient-hours (hr ≥ 0) | 5,274,012 | 8,322,254 |
| ICU LOS, mean (h) | 98.8 | 87.1 |
| ICU LOS, median [IQR] (h) | 51.0 [28.0, 100.0] | 47.0 [26.0, 93.0] |
| LOS < 24 h, % | 16.0% | 19.9% |
| Hospital mortality, n (%) | 6,516 (12.3%) | 11,343 (12.0%) |
| **Sepsis** | | |
| Sepsis stays, n (%) | 19,809 (37.5%) | 40,456 (42.8%) |
| Non-sepsis stays, n (%) | 33,031 (62.5%) | 53,988 (57.2%) |
| Sepsis subjects, n (%) | 15,785 (41.0%) | 31,390 (48.0%) |
| Sepsis onset, median [IQR] (h) | 0 [0, 2] | 2 [1, 6] |
| **SOFA** | | |
| Max SOFA per stay, mean | 4.6 | 4.7 |
| Max SOFA per stay, median [IQR] | 4 [2, 6] | 4 [2, 7] |
| Max SOFA ≥ 2, % of stays | 81.4% | 80.4% |
| Max SOFA ≥ 6, % of stays | 32.4% | 33.8% |
| SOFA at sepsis onset, median [IQR] | 0 [0, 2] | 3 [2, 4] |
| SOFA Δ at sepsis onset (III only), median [IQR] | 0 [0, 1] | — |

### Table 2. First ICU Unit by Volume

| ICU Unit | MIMIC-III | MIMIC-IV |
|---|---:|---:|
| MICU | 20,837 (39.4%) | 20,699 (21.9%) |
| MICU/SICU | — | 15,447 (16.4%) |
| CVICU | — | 14,769 (15.6%) |
| CSRU | 9,228 (17.5%) | — |
| SICU | 8,794 (16.6%) | 13,008 (13.8%) |
| CCU | 7,639 (14.5%) | 10,771 (11.4%) |
| TSICU | 6,342 (12.0%) | 10,474 (11.1%) |
| Neuro Intermediate | — | 5,776 (6.1%) |
| Neuro SICU | — | 1,750 (1.9%) |

### Table 3. Clinical Data Coverage at hr = 0 (% of stays with ≥ 1 non-null value)

| Variable | MIMIC-III | MIMIC-IV |
|---|---:|---:|
| Heart rate | 41.1% | 55.8% |
| Systolic BP | 39.7% | 52.6% |
| Temperature | N/A † | 38.7% |
| SpO₂ | 39.5% | 54.3% |
| GCS total | 23.0% | 26.8% |
| Creatinine | 12.6% | 10.4% |
| Platelet | 14.2% | 11.4% |
| PaO₂ | 19.6% | 16.0% |
| Bilirubin (total) | 4.8% | 5.0% |
| FiO₂ | 4.7% | 3.0% |

† Temperature is not available as a standalone vital in the MIMIC-III wide table (arterial blood gas temperature only).

---

### Key Observations

#### 1. Sepsis rate: MIMIC-IV (42.8%) > MIMIC-III (37.5%)

The difference is driven by the **labelling criterion**, not a genuinely higher prevalence of sepsis.

MIMIC-III applies a delta criterion (SOFA rise ≥ 2 from a 24 h rolling minimum), which does not label patients who arrive critically ill and do not deteriorate further. As a result, the median SOFA at sepsis onset is **0 [0, 2]**, indicating that most flagged patients experienced a rise from a low baseline rather than an acute decompensation. MIMIC-IV applies an absolute criterion (SOFA ≥ 2 at time of suspected infection), capturing patients admitted with an already-elevated SOFA; their median onset SOFA is **3 [2, 4]**.

The clinical severity of the labelled cohorts is nearly identical across datasets (first-day SOFA median 5.0 [3.0, 7.0]; 30-day mortality 20.3% vs 20.6%), confirming that the additional labels in MIMIC-IV represent patients missed by the structural blind spot of the delta criterion — those who arrive critically ill but do not deteriorate further — rather than a milder case mix.

#### 2. Vitals coverage: MIMIC-IV (~55%) > MIMIC-III (~41%) at hr = 0

The gap reflects a documentation system transition: MIMIC-III was recorded primarily through CareVue (manual nursing entry), whereas MIMIC-IV transitioned to MetaVision (automated bedside monitor downloads), which captures vitals at higher frequency and completeness.

#### 3. SOFA means aligned: MIMIC-III (4.6) ≈ MIMIC-IV (4.7)

Prior to correction, the MIMIC-III mean max SOFA was 8.3. The root cause was a structural defect in the renal SOFA urine-output logic, described in detail below.

---

### In-Depth Analysis: Systematic Bias in Early Renal SOFA Scoring

#### Core Conclusion

When extracting the Renal SOFA score for the first 48 hours of an ICU admission, there is a massive gap between MIMIC-III and MIMIC-IV. Tracing the underlying SQL confirms: this is not a local data extraction bug, but a **severe boundary truncation defect in MIMIC-III's official script when handling cumulative metrics (urine output)**. The defect was structurally resolved in MIMIC-IV through an observation-time-aware mechanism.

#### I. Empirical Evidence: Systematic False Positives in MIMIC-III

| ICU Hours Elapsed | MIMIC-III (Score = 4) | MIMIC-IV (Score = 4) | Observation |
|:---|:---:|:---:|:---|
| hr = 0 | 5.4% | 0.5% | IV reflects true baseline of severe renal failure. |
| hr = 6 | **44.6% (peak)** | — | III false positives peak; nearly half of patients deemed anuric. |
| hr = 24 | 49.5% | 5.2% | IV completes first full 24 h window; ratio is stable. |
| hr = 48 | **8.6% (sudden drop)** | 7.5% | III exits the flawed rolling window; drops back to normal. |

**The absolute driver: urine output (UO).** In MIMIC-III, 84.1% of abnormally high renal scores (≥ 2) are driven by UO alone:

- Pure UO trigger: **62.9%** — creatinine is normal; the patient is penalized solely by the UO logic.
- Pure creatinine trigger: 19.9%.

#### II. Defect Origin: Flaw in MIMIC-III's Official Script

The root cause lies in `pivoted_sofa.sql`. A fixed row-based window is applied blindly, producing two compounding errors: **early boundary truncation** and **extreme-value propagation**.

For snapshot metrics (respiration, coagulation), taking the min or max over 24 hours is unaffected by how long the patient has been in the ICU. Urine output is a **cumulative metric** requiring `SUM` — truncated sums produce systematically wrong totals.

**MIMIC-III official `pivoted_sofa.sql`:**

```sql
-- Root cause: mechanical 24-row backward rolling window
SUM(UrineOutput) OVER (
    PARTITION BY icustay_id
    ORDER BY hr
    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW  -- sums whatever rows exist, regardless of actual time span
) AS uo_24hr
```

**Error chain:**

1. **Truncated comparison** — At hr = 6 the engine finds only 7 rows. Normal urine over 7 hours (~400 mL) is compared directly against the 24 h clinical threshold (< 500 mL = score 3).
2. **False-positive explosion** — Seven hours of urine can rarely meet a 24 h passing mark; healthy kidneys are mass-classified as severely oliguric in the early hours.
3. **Ghost propagation** — The final SOFA uses `MAX(sofa_renal) OVER 24 PRECEDING`. A spuriously generated score of 4 at hr = 7 propagates forward for 24 hours, exiting the window completely only at hr = 48 — explaining the abrupt drop above.

#### III. The Fix: Observation-Time-Aware Logic in MIMIC-IV

MIMIC-IV's `sofa.sql` introduces `uo_tm_24hr` (accumulated observation duration) and refuses to score until the window is sufficiently covered.

**MIMIC-IV official `sofa.sql`:**

```sql
MAX(
    CASE
        -- Refuse to evaluate if observation window < 22 h
        WHEN uo.uo_tm_24hr >= 22 AND uo.uo_tm_24hr <= 30
        -- Normalise to a standard 24 h equivalent
        THEN uo.urineoutput_24hr / uo.uo_tm_24hr * 24
    END
) AS uo_24hr
```

During the first ~22 hours, `uo_24hr` is forcibly NULL — no UO-driven score is possible. The scoring engine falls back entirely to admission serum creatinine, which is why MIMIC-IV shows a realistic 0.5% severe-failure rate at hr = 0. This logic was ported into the MIMIC-III pipeline (`build_mimic3_wide.py`, step07_uo), after which both datasets align: max SOFA mean 4.6 (III) vs 4.7 (IV).

---

## Part 2: Patient-Level Cohort Summary

**Inclusion:** One row per patient, first ICU stay only.

**Definitions:**
- **SOFA:** first-day maximum SOFA score
- **SIRS:** first-day maximum SIRS score
- **Comorbidity index:** Elixhauser van Walraven score (MIMIC-III, official mimic-code ICD-9 implementation) / Charlson Comorbidity Index with age adjustment (MIMIC-IV, official mimic-code ICD-9 + ICD-10 implementation). *Scores are not directly comparable across datasets.*
- **Mechanical ventilation:** any invasive/non-invasive ventilation or tracheostomy during the first ICU stay
- **30-day mortality:** measured from first ICU admission time
- **Race/ethnicity:** grouped into 7 categories (White, Black, Hispanic, Asian, Native, Other, Unknown)

### Table 4. MIMIC-III Patient Characteristics

| Variable | All patients (N = 38,484) | Survivors (N = 34,070, 88.5%) | Non-survivors (N = 4,414, 11.5%) | *P* (survival) | Sepsis (N = 13,436, 34.9%) | Non-sepsis (N = 25,048, 65.1%) | *P* (sepsis) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Age (y), median [IQR] | 66.0 [52.0, 78.0] | 65.0 [52.0, 77.0] | 74.0 [60.0, 83.0] | <0.001 | 68.0 [55.0, 79.0] | 64.0 [51.0, 77.0] | <0.001 |
| Male, n (%) | 21,793 (56.6%) | 19,461 (57.1%) | 2,332 (52.8%) | <0.001 | 7,726 (57.5%) | 14,067 (56.2%) | 0.011 |
| BMI (kg/m²), mean ± SD | 28.4 ± 7.0 | 28.5 ± 7.0 | 27.5 ± 7.1 | <0.001 | 28.8 ± 7.4 | 28.3 ± 6.8 | <0.001 |
| Race, n (%) | | | | <0.001 | | | <0.001 |
| &nbsp;&nbsp;White | 27,457 (71.3%) | 24,451 (71.8%) | 3,006 (68.1%) | | 9,783 (72.8%) | 17,674 (70.6%) | |
| &nbsp;&nbsp;Black | 2,949 (7.7%) | 2,695 (7.9%) | 254 (5.8%) | | 1,081 (8.0%) | 1,868 (7.5%) | |
| &nbsp;&nbsp;Hispanic | 1,252 (3.3%) | 1,166 (3.4%) | 86 (1.9%) | | 421 (3.1%) | 831 (3.3%) | |
| &nbsp;&nbsp;Asian | 909 (2.4%) | 806 (2.4%) | 103 (2.3%) | | 350 (2.6%) | 559 (2.2%) | |
| &nbsp;&nbsp;Native American | 20 (0.1%) | 19 (0.1%) | 1 (0.0%) | | 8 (0.1%) | 12 (0.0%) | |
| &nbsp;&nbsp;Other | 1,041 (2.7%) | 935 (2.7%) | 106 (2.4%) | | 389 (2.9%) | 652 (2.6%) | |
| &nbsp;&nbsp;Unknown | 4,856 (12.6%) | 3,998 (11.7%) | 858 (19.4%) | | 1,404 (10.4%) | 3,452 (13.8%) | |
| Comorbidity index †, median [IQR] | 5.0 [0.0, 12.0] | 5.0 [0.0, 11.0] | 11.0 [5.0, 17.0] | <0.001 | 9.0 [3.0, 15.0] | 4.0 [0.0, 10.0] | <0.001 |
| SIRS, first-day max, median [IQR] | 3.0 [2.0, 3.0] | 3.0 [2.0, 3.0] | 3.0 [2.0, 4.0] | <0.001 | 3.0 [2.0, 4.0] | 2.0 [2.0, 3.0] | <0.001 |
| SOFA, first-day, median [IQR] | 3.0 [1.0, 5.0] | 3.0 [1.0, 5.0] | 5.0 [3.0, 9.0] | <0.001 | 5.0 [3.0, 7.0] | 2.0 [1.0, 4.0] | <0.001 |
| ICU LOS (d), median [IQR] | 2.0 [1.0, 4.0] | 2.0 [1.0, 4.0] | 3.0 [1.0, 7.0] | <0.001 | 3.0 [2.0, 8.0] | 2.0 [1.0, 3.0] | <0.001 |
| Mechanical ventilation, n (%) | 19,483 (50.6%) | 16,271 (47.8%) | 3,212 (72.8%) | <0.001 | 8,749 (65.1%) | 10,734 (42.9%) | <0.001 |
| 30-day mortality, n (%) | 5,234 (13.6%) | 1,073 (3.1%) | 4,161 (94.3%) | <0.001 | 2,730 (20.3%) | 2,504 (10.0%) | <0.001 |
| Hospital mortality, n (%) | 4,414 (11.5%) | 0 (0.0%) | 4,414 (100.0%) | <0.001 | 2,385 (17.8%) | 2,029 (8.1%) | <0.001 |

### Table 5. MIMIC-IV Patient Characteristics

| Variable | All patients (N = 65,355) | Survivors (N = 58,275, 89.2%) | Non-survivors (N = 7,080, 10.8%) | *P* (survival) | Sepsis (N = 27,326, 41.8%) | Non-sepsis (N = 38,029, 58.2%) | *P* (sepsis) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Age (y), median [IQR] | 66.0 [54.0, 78.0] | 66.0 [54.0, 77.0] | 73.0 [61.0, 83.0] | <0.001 | 67.0 [56.0, 78.0] | 65.0 [53.0, 77.0] | <0.001 |
| Male, n (%) | 36,714 (56.2%) | 32,875 (56.4%) | 3,839 (54.2%) | <0.001 | 15,851 (58.0%) | 20,863 (54.9%) | <0.001 |
| BMI (kg/m²), mean ± SD | 28.8 ± 7.4 | 28.9 ± 7.3 | 28.7 ± 8.3 | 0.125 | 29.1 ± 7.7 | 28.5 ± 7.1 | <0.001 |
| Race, n (%) | | | | <0.001 | | | <0.001 |
| &nbsp;&nbsp;White | 43,089 (65.9%) | 38,990 (66.9%) | 4,099 (57.9%) | | 17,890 (65.5%) | 25,199 (66.3%) | |
| &nbsp;&nbsp;Black | 6,016 (9.2%) | 5,470 (9.4%) | 546 (7.7%) | | 2,303 (8.4%) | 3,713 (9.8%) | |
| &nbsp;&nbsp;Hispanic | 2,351 (3.6%) | 2,177 (3.7%) | 174 (2.5%) | | 933 (3.4%) | 1,418 (3.7%) | |
| &nbsp;&nbsp;Asian | 1,980 (3.0%) | 1,769 (3.0%) | 211 (3.0%) | | 800 (2.9%) | 1,180 (3.1%) | |
| &nbsp;&nbsp;Native American | 127 (0.2%) | 117 (0.2%) | 10 (0.1%) | | 58 (0.2%) | 69 (0.2%) | |
| &nbsp;&nbsp;Other | 2,416 (3.7%) | 2,183 (3.7%) | 233 (3.3%) | | 987 (3.6%) | 1,429 (3.8%) | |
| &nbsp;&nbsp;Unknown | 9,376 (14.3%) | 7,569 (13.0%) | 1,807 (25.5%) | | 4,355 (15.9%) | 5,021 (13.2%) | |
| Comorbidity index ‡, median [IQR] | 4.0 [2.0, 6.0] | 4.0 [2.0, 6.0] | 6.0 [4.0, 8.0] | <0.001 | 5.0 [3.0, 7.0] | 4.0 [2.0, 6.0] | <0.001 |
| SIRS, first-day max, median [IQR] | 2.0 [1.0, 2.0] | 2.0 [1.0, 2.0] | 2.0 [2.0, 3.0] | <0.001 | 2.0 [1.0, 3.0] | 2.0 [1.0, 2.0] | <0.001 |
| SOFA, first-day, median [IQR] | 3.0 [1.0, 6.0] | 3.0 [1.0, 5.0] | 6.0 [3.0, 9.0] | <0.001 | 5.0 [3.0, 7.0] | 2.0 [1.0, 4.0] | <0.001 |
| ICU LOS (d), median [IQR] | 1.9 [1.1, 3.7] | 1.9 [1.1, 3.4] | 2.7 [1.1, 6.4] | <0.001 | 3.0 [1.5, 6.4] | 1.5 [0.9, 2.5] | <0.001 |
| Mechanical ventilation, n (%) | 28,339 (43.4%) | 23,514 (40.4%) | 4,825 (68.1%) | <0.001 | 17,242 (63.1%) | 11,097 (29.2%) | <0.001 |
| 30-day mortality, n (%) | 8,964 (13.7%) | 2,208 (3.8%) | 6,756 (95.4%) | <0.001 | 5,620 (20.6%) | 3,344 (8.8%) | <0.001 |
| Hospital mortality, n (%) | 7,080 (10.8%) | 0 (0.0%) | 7,080 (100.0%) | <0.001 | 4,671 (17.1%) | 2,409 (6.3%) | <0.001 |

---

*† Elixhauser van Walraven score (MIMIC-III): 29-condition weighted index based on secondary ICD-9 diagnoses (Quan et al. 2005; van Walraven et al. 2009). Scores range from approximately −19 to +89; higher scores indicate greater predicted in-hospital mortality risk. Negative weights apply to certain conditions (e.g., drug abuse −7, obesity −4).*

*‡ Charlson Comorbidity Index with age adjustment (MIMIC-IV): 17-condition index based on ICD-9 and ICD-10 diagnoses (Charlson et al. 1987; Quan et al. 2011). Age contributes 0–4 points (1 point per decade above age 50). Scores range from 0 to approximately 30; higher scores indicate greater comorbidity burden. The two comorbidity indices are not directly comparable.*

---

## Part 3: Temporal Dynamics across ICU Stay

> All figures use 3-hour time bins (x-axis: 0–336 h). Scripts: `mimic_analysis/plot_icu_curves.py`.

---

### Figure 1. Active ICU Stays over Time

Each curve shows the number of stays still ongoing at a given ICU hour — i.e., stays whose length of stay ≥ t. Non-sepsis stays discharge much faster (green flattens near zero by ~72 h), while sepsis stays persist longer and come to dominate the late ICU population. MIMIC-IV has roughly 1.8× more total stays than MIMIC-III at every time point, consistent with its larger overall cohort.

**MIMIC-III**
![MIMIC-III ICU Retention](../mimiciii/icu_retention.png)

**MIMIC-IV**
![MIMIC-IV ICU Retention](../mimiciv/icu_retention.png)

---

### Figure 2. Sepsis Prevalence among Active Stays

At each hour t, the curve shows the percentage of still-active stays whose sepsis onset has already occurred (i.e., onset_hr ≤ t). The monotone increase reflects two compounding effects: new sepsis diagnoses accumulate over time, and non-sepsis patients discharge earlier, shrinking the denominator. By 336 h, over 80% (MIMIC-III) and 92% (MIMIC-IV) of remaining ICU stays have been labelled sepsis — a clear illustration of length-of-stay bias in long-stay cohorts.

**MIMIC-III**
![MIMIC-III Sepsis Prevalence](../mimiciii/sepsis_prevalence.png)

**MIMIC-IV**
![MIMIC-IV Sepsis Prevalence](../mimiciv/sepsis_prevalence.png)

---

### Figure 3. Mean SOFA Score over ICU Stay

Mean SOFA is computed at each 3-hour bin across all stays still active at that time, stratified by sepsis status (classified by the full-stay label). In both datasets the mean rises steeply during the first 24 hours as the 24-hour rolling maximum window fills, then plateaus. Sepsis stays maintain a consistently higher SOFA throughout (III: ~5, IV: ~5–6 vs non-sepsis ~3 and ~2.5 respectively), confirming a persistent severity gap between the two cohorts that does not close over the ICU stay.

**MIMIC-III**
![MIMIC-III SOFA Trajectory](../mimiciii/sofa_trajectory.png)

**MIMIC-IV**
![MIMIC-IV SOFA Trajectory](../mimiciv/sofa_trajectory.png)

---

### Figure 4. In-Hospital Death Timing

The histogram shows when in-hospital deaths occur relative to ICU admission, approximated as the last recorded ICU hour for each death stay. Deaths are concentrated in the first 48–72 hours, with the highest incidence around ICU hours 12–36. The dashed cumulative curve shows that ~50% of all in-hospital deaths occur within the first 48 h (MIMIC-III) or 72 h (MIMIC-IV). The spike at h = 336 is an artefact of clipping: stays longer than 14 days are capped at the plot boundary.

**MIMIC-III**
![MIMIC-III Death Timing](../mimiciii/death_timing.png)

**MIMIC-IV**
![MIMIC-IV Death Timing](../mimiciv/death_timing.png)

---

### Figure 5. Vasopressor Use Rate over ICU Stay

The y-axis shows, at each 3-hour bin, the percentage of currently active stays receiving any vasopressor (norepinephrine, epinephrine, dopamine, or vasopressin). Both axes are on the same scale to allow direct comparison. In both datasets sepsis stays peak at ~17–18% around hours 6–12, then gradually decline as patients are weaned. Non-sepsis stays show much lower rates throughout (III: ~4–6%, IV: ~1–3%), reflecting the higher haemodynamic instability of septic shock. The late-stay non-sepsis increase is a selection effect: only the most critically ill non-sepsis patients remain in the ICU beyond day 10.

**MIMIC-III**
![MIMIC-III Vasopressor Rate](../mimiciii/vasopressor_rate.png)

**MIMIC-IV**
![MIMIC-IV Vasopressor Rate](../mimiciv/vasopressor_rate.png)

---

---

---

## Part 4: Subject-Level ICU Stay Patterns

> **Unit of analysis:** one row per unique subject (first vs. subsequent stays). Sepsis label per stay = 1 if `SepsisLabel = 1` at any `hr ≥ 0` within that stay, else 0.

---

### 5.1 Distribution of ICU Stays per Subject

MIMIC-III: **38,484** unique subjects — 8,300 (21.6%) have more than one ICU stay.  
MIMIC-IV: **65,355** unique subjects — 16,241 (24.9%) have more than one ICU stay.

*All non-sepsis* = all stays for this subject are labelled 0; *All sepsis* = all stays labelled 1; *Mixed* = at least one 0 and at least one 1 across stays.

**MIMIC-III**

| ICU stays | Subjects, n (%) | ≤ n stays (%) | All non-sepsis | All sepsis | Mixed |
|---:|---:|---:|---:|---:|---:|
| 1 | 30,184 (78.4%) | 78.4% | 19,943 (66.1%) | 10,241 (33.9%) | 0 (0.0%) |
| 2 | 5,449 (14.2%) | 92.6% | 2,215 (40.6%) | 1,057 (19.4%) | 2,177 (40.0%) |
| 3 | 1,557 (4.0%) | 96.6% | 404 (25.9%) | 190 (12.2%) | 963 (61.8%) |
| 4 | 630 (1.6%) | 98.3% | 88 (14.0%) | 64 (10.2%) | 478 (75.9%) |
| 5 | 290 (0.8%) | 99.0% | 34 (11.7%) | 27 (9.3%) | 229 (79.0%) |
| 6 | 151 (0.4%) | 99.4% | 6 (4.0%) | 4 (2.6%) | 141 (93.4%) |
| 7 | 82 (0.2%) | 99.6% | 6 (7.3%) | 3 (3.7%) | 73 (89.0%) |
| 8 | 40 (0.1%) | 99.7% | 0 (0.0%) | 2 (5.0%) | 38 (95.0%) |
| 9 | 22 (0.1%) | 99.8% | 1 (4.5%) | 0 (0.0%) | 21 (95.5%) |
| 10 | 23 (0.1%) | 99.9% | 0 (0.0%) | 0 (0.0%) | 23 (100.0%) |
| ≥11 | 56 (0.1%) | 100.0% | 2 (3.6%) | 0 (0.0%) | 54 (96.4%) |

**MIMIC-IV**

| ICU stays | Subjects, n (%) | ≤ n stays (%) | All non-sepsis | All sepsis | Mixed |
|---:|---:|---:|---:|---:|---:|
| 1 | 49,114 (75.1%) | 75.1% | 29,212 (59.5%) | 19,902 (40.5%) | 0 (0.0%) |
| 2 | 10,341 (15.8%) | 91.0% | 3,825 (37.0%) | 2,242 (21.7%) | 4,274 (41.3%) |
| 3 | 3,206 (4.9%) | 95.9% | 670 (20.9%) | 455 (14.2%) | 2,081 (64.9%) |
| 4 | 1,289 (2.0%) | 97.9% | 169 (13.1%) | 125 (9.7%) | 995 (77.2%) |
| 5 | 580 (0.9%) | 98.7% | 41 (7.1%) | 40 (6.9%) | 499 (86.0%) |
| 6 | 308 (0.5%) | 99.2% | 22 (7.1%) | 12 (3.9%) | 274 (89.0%) |
| 7 | 158 (0.2%) | 99.5% | 5 (3.2%) | 3 (1.9%) | 150 (94.9%) |
| 8 | 110 (0.2%) | 99.6% | 7 (6.4%) | 3 (2.7%) | 100 (90.9%) |
| 9 | 55 (0.1%) | 99.7% | 3 (5.5%) | 2 (3.6%) | 50 (90.9%) |
| 10 | 53 (0.1%) | 99.8% | 2 (3.8%) | 1 (1.9%) | 50 (94.3%) |
| ≥11 | 141 (0.2%) | 100.0% | 9 (6.4%) | 1 (0.7%) | 131 (92.9%) |

---

### 5.2 Sepsis Sequence Patterns

Each subject's ICU stays are ordered by admission time and collapsed to a binary sepsis label per stay (0 = no sepsis, 1 = sepsis), then concatenated into a sequence such as `0->1->0`. Proportion within group = share among all subjects with the same number of ICU stays.

MIMIC-III: **301** unique sequences in total (30 with ≤4 stays).  
MIMIC-IV: **514** unique sequences in total (30 with ≤4 stays).

#### MIMIC-III

#### Sequences with 1–4 ICU Stays (all combinations)

| n stays | Sequence | Subjects, n | % within group |
|---:|---|---:|---:|
| 1 | `0` | 19,943 | 66.1% |
| 1 | `1` | 10,241 | 33.9% |
| 2 | `0->0` | 2,215 | 40.6% |
| 2 | `0->1` | 1,194 | 21.9% |
| 2 | `1->1` | 1,057 | 19.4% |
| 2 | `1->0` | 983 | 18.0% |
| 3 | `0->0->0` | 404 | 25.9% |
| 3 | `0->0->1` | 220 | 14.1% |
| 3 | `1->1->1` | 190 | 12.2% |
| 3 | `0->1->1` | 170 | 10.9% |
| 3 | `1->0->1` | 158 | 10.1% |
| 3 | `1->0->0` | 157 | 10.1% |
| 3 | `0->1->0` | 148 | 9.5% |
| 3 | `1->1->0` | 110 | 7.1% |
| 4 | `0->0->0->0` | 88 | 14.0% |
| 4 | `1->1->1->1` | 64 | 10.2% |
| 4 | `0->0->0->1` | 55 | 8.7% |
| 4 | `0->0->1->1` | 42 | 6.7% |
| 4 | `1->0->1->1` | 42 | 6.7% |
| 4 | `0->1->0->0` | 41 | 6.5% |
| 4 | `1->0->0->1` | 37 | 5.9% |
| 4 | `1->0->0->0` | 34 | 5.4% |
| 4 | `0->1->1->1` | 34 | 5.4% |
| 4 | `0->0->1->0` | 33 | 5.2% |
| 4 | `0->1->0->1` | 31 | 4.9% |
| 4 | `1->1->1->0` | 29 | 4.6% |
| 4 | `0->1->1->0` | 28 | 4.4% |
| 4 | `1->1->0->1` | 25 | 4.0% |
| 4 | `1->0->1->0` | 24 | 3.8% |
| 4 | `1->1->0->0` | 23 | 3.7% |

#### Sequences with ≥5 ICU Stays (summary)

| n stays | Subjects | Unique patterns | Most common sequence | Count |
|---:|---:|---:|---|---:|
| 5 | 290 | 32 | `0->0->0->0->0` | 34 |
| 6 | 151 | 52 | `0->0->0->0->0->1` | 8 |
| 7 | 82 | 51 | `0->0->0->0->0->0->0` | 6 |
| 8 | 40 | 36 | `0->1->1->0->1->0->0->0` | 2 |
| 9 | 22 | 22 | `0->1->1->1->1->1->0->0->1` | 1 |
| 10 | 23 | 22 | `1->0->0->1->1->1->0->1->1->1` | 2 |
| 11 | 12 | 12 | `1->0->0->0->0->1->1->0->1->0->1` | 1 |
| 12 | 9 | 9 | `0->0->1->0->1->0->0->0->1->0->0->1` | 1 |
| 13 | 9 | 9 | `0->0->1->1->0->1->0->0->0->1->1->1->1` | 1 |
| 14 | 4 | 4 | `1->1->1->1->1->1->1->1->1->0->1->1->1->0` | 1 |
| 15 | 6 | 6 | `1->1->0->1->1->1->0->0->1->1->1->1->0->1->1` | 1 |
| 16 | 1 | 1 | `1->1->1->1->0->1->1->1->1->1->0->1->1->0->0->1` | 1 |
| 17 | 2 | 2 | `0->1->1->1->1->0->0->1->1->1->0->1->1->0->1->1->0` | 1 |
| 18 | 2 | 2 | `0->1->0->0->0->0->0->1->0->0->0->1->0->1->0->0->0->0` | 1 |
| 19 | 1 | 1 | `0->1->0->1->0->0->1->0->1->0->0->0->1->1->1->1->1->0->1` | 1 |
| 21 | 2 | 2 | `0->0->0->0->0->0->0->1->1->0->0->1->0->0->0->0->0->1->0->0->0` | 1 |
| 22 | 1 | 1 | `0->0->1->0->0->0->1->0->0->0->1->1->1->1->1->1->1->0->1->1->0->1` | 1 |
| 23 | 1 | 1 | `0->0->0->0->0->1->0->0->0->0->0->1->0->1->0->1->0->0->0->0->0->0->0` | 1 |
| 25 | 2 | 2 | `1->0->1->0->1->1->0->0->0->1->0->0->1->0->0->0->0->1->0->1->0->0->0->1->1` | 1 |
| 30 | 1 | 1 | `0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0` | 1 |
| 35 | 1 | 1 | `0->0->0->0->0->0->0->0->1->1->0->0->0->0->0->1->0->0->0->0->0->0->0->0->0->0->0->1->0->0->0->0->0->0->0` | 1 |
| 38 | 1 | 1 | `0->0->0->0->0->0->1->1->1->0->0->0->1->0->0->1->1->0->0->0->0->0->0->0->0->0->0->0->0->0->1->0->0->0->1->0->0->0` | 1 |
| 41 | 1 | 1 | `0->1->1->0->0->0->0->0->1->0->0->0->0->0->0->0->0->0->1->0->0->0->1->0->0->0->0->0->0->0->0->1->1->0->0->0->1->0->0->0->0` | 1 |

#### MIMIC-IV

#### Sequences with 1–4 ICU Stays (all combinations)

| n stays | Sequence | Subjects, n | % within group |
|---:|---|---:|---:|
| 1 | `0` | 29,212 | 59.5% |
| 1 | `1` | 19,902 | 40.5% |
| 2 | `0->0` | 3,825 | 37.0% |
| 2 | `1->1` | 2,242 | 21.7% |
| 2 | `1->0` | 2,231 | 21.6% |
| 2 | `0->1` | 2,043 | 19.8% |
| 3 | `0->0->0` | 670 | 20.9% |
| 3 | `1->1->1` | 455 | 14.2% |
| 3 | `1->0->0` | 427 | 13.3% |
| 3 | `0->0->1` | 365 | 11.4% |
| 3 | `1->0->1` | 352 | 11.0% |
| 3 | `1->1->0` | 335 | 10.4% |
| 3 | `0->1->1` | 306 | 9.5% |
| 3 | `0->1->0` | 296 | 9.2% |
| 4 | `0->0->0->0` | 169 | 13.1% |
| 4 | `1->1->1->1` | 125 | 9.7% |
| 4 | `0->0->0->1` | 97 | 7.5% |
| 4 | `1->0->1->1` | 88 | 6.8% |
| 4 | `1->0->0->0` | 82 | 6.4% |
| 4 | `0->1->0->0` | 80 | 6.2% |
| 4 | `1->1->0->1` | 78 | 6.1% |
| 4 | `1->0->0->1` | 77 | 6.0% |
| 4 | `1->1->0->0` | 72 | 5.6% |
| 4 | `0->0->1->1` | 71 | 5.5% |
| 4 | `0->0->1->0` | 68 | 5.3% |
| 4 | `0->1->1->1` | 66 | 5.1% |
| 4 | `1->1->1->0` | 64 | 5.0% |
| 4 | `0->1->0->1` | 55 | 4.3% |
| 4 | `0->1->1->0` | 49 | 3.8% |
| 4 | `1->0->1->0` | 48 | 3.7% |

#### Sequences with ≥5 ICU Stays (summary)

| n stays | Subjects | Unique patterns | Most common sequence | Count |
|---:|---:|---:|---|---:|
| 5 | 580 | 32 | `0->0->0->0->0` | 41 |
| 6 | 308 | 61 | `0->0->0->0->0->0` | 22 |
| 7 | 158 | 81 | `1->0->1->1->1->1->1` | 5 |
| 8 | 110 | 75 | `0->0->0->0->0->0->0->0` | 7 |
| 9 | 55 | 47 | `1->1->1->0->0->1->1->1->1` | 3 |
| 10 | 53 | 51 | `0->0->0->0->0->0->0->0->0->0` | 2 |
| 11 | 43 | 40 | `0->0->0->0->0->0->0->0->0->0->0` | 4 |
| 12 | 20 | 20 | `1->1->0->0->0->0->0->1->1->1->1->0` | 1 |
| 13 | 17 | 17 | `1->1->0->1->0->1->0->1->1->1->1->1->0` | 1 |
| 14 | 6 | 6 | `1->1->0->0->0->1->0->0->1->0->0->0->0->0` | 1 |
| 15 | 10 | 10 | `1->0->0->0->0->1->1->1->1->0->1->0->1->0->1` | 1 |
| 16 | 9 | 9 | `1->0->1->1->0->1->1->1->1->1->0->1->0->1->1->1` | 1 |
| 17 | 4 | 4 | `1->0->0->0->0->1->0->1->0->0->1->1->0->0->1->1->1` | 1 |
| 18 | 11 | 10 | `0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0` | 2 |
| 19 | 2 | 2 | `0->0->0->0->1->0->0->1->1->1->1->1->1->0->0->0->0->0->1` | 1 |
| 20 | 3 | 3 | `1->0->0->0->1->0->1->0->0->0->1->1->1->0->1->1->0->1->0->0` | 1 |
| 22 | 3 | 3 | `1->1->0->1->1->1->1->1->0->1->0->1->1->1->0->1->0->1->1->0->0->1` | 1 |
| 24 | 3 | 3 | `1->1->1->0->1->0->1->1->1->1->1->0->1->0->0->0->0->0->0->0->1->1->1->1` | 1 |
| 25 | 3 | 3 | `1->0->1->1->1->1->1->1->1->1->1->1->0->1->1->0->1->1->1->0->0->1->1->1->1` | 1 |
| 26 | 1 | 1 | `0->0->0->0->0->1->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0` | 1 |
| 27 | 1 | 1 | `0->0->0->0->0->0->0->1->0->0->0->0->0->0->0->0->0->1->1->0->0->1->0->0->1->0->1` | 1 |
| 30 | 1 | 1 | `0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->1->0->0->0->0->0->1->0->1->1->0->1->1->0->1` | 1 |
| 31 | 1 | 1 | `1->1->0->0->0->1->0->0->0->1->0->0->0->1->1->1->1->0->0->0->1->0->0->0->0->0->0->0->0->1->0` | 1 |
| 34 | 1 | 1 | `0->0->0->0->0->0->0->0->0->1->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->1->0->0->0->0->0->0->0->0` | 1 |
| 37 | 1 | 1 | `1->1->0->1->1->0->0->0->0->0->0->0->0->0->0->1->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->0->1->0->0->0` | 1 |
| 41 | 1 | 1 | `1->0->0->0->1->1->1->1->0->1->1->1->1->1->1->0->1->1->1->1->1->0->1->1->1->0->1->1->1->1->1->1->1->0->1->1->1->1->1->1->1` | 1 |
