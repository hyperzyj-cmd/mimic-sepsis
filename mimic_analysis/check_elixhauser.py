import duckdb
con = duckdb.connect()
con.execute("CREATE VIEW w3 AS SELECT * FROM read_parquet('D:/ESILV_S2/Intern/build_mimic/mimiciii/output/mimic3_wide.parquet')")

r = con.execute("""
WITH first_stay AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY SUBJECT_ID ORDER BY INTIME) AS rn
    FROM w3 WHERE hr = 0
)
SELECT
    hospital_expire_flag,
    COUNT(*) AS n,
    ROUND(AVG(elixhauser_vanwalraven),1) AS mean_vw,
    ROUND(MEDIAN(elixhauser_vanwalraven),1) AS median_vw,
    ROUND(AVG(liver_disease)*100,1)        AS pct_liver,
    ROUND(AVG(metastatic_cancer)*100,1)    AS pct_mets,
    ROUND(AVG(lymphoma)*100,1)             AS pct_lymphoma,
    ROUND(AVG(congestive_heart_failure)*100,1) AS pct_chf,
    ROUND(AVG(renal_failure)*100,1)        AS pct_renal,
    ROUND(AVG(fluid_electrolyte)*100,1)    AS pct_lytes,
    ROUND(AVG(other_neurological)*100,1)   AS pct_neuro,
    ROUND(AVG(paralysis)*100,1)            AS pct_paralysis,
    ROUND(AVG(obesity)*100,1)              AS pct_obese,
    ROUND(AVG(drug_abuse)*100,1)           AS pct_drug,
    ROUND(AVG(depression)*100,1)           AS pct_depress
FROM first_stay WHERE rn = 1
GROUP BY hospital_expire_flag ORDER BY hospital_expire_flag
""").fetchdf()

print("=== Elixhauser van Walraven by mortality (MIMIC-III, first stay per patient) ===")
print(r.to_string())

# Also check score distribution
print("\n=== Score distribution (non-survivors) ===")
r2 = con.execute("""
WITH first_stay AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY SUBJECT_ID ORDER BY INTIME) AS rn
    FROM w3 WHERE hr = 0
)
SELECT
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY elixhauser_vanwalraven) AS p10,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY elixhauser_vanwalraven) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY elixhauser_vanwalraven) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY elixhauser_vanwalraven) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY elixhauser_vanwalraven) AS p90,
    MIN(elixhauser_vanwalraven) AS min_val,
    MAX(elixhauser_vanwalraven) AS max_val
FROM first_stay WHERE rn = 1 AND hospital_expire_flag = 1
""").fetchdf()
print(r2.to_string())
