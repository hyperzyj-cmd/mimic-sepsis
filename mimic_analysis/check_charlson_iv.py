import duckdb
con = duckdb.connect()
con.execute("CREATE VIEW w4 AS SELECT * FROM read_parquet('D:/ESILV_S2/Intern/build_mimic/mimiciv/output/mimic4_wide.parquet')")

r = con.execute("""
WITH first_stay AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
    FROM w4 WHERE hr = 0
)
SELECT
    hospital_expire_flag,
    COUNT(*) AS n,
    ROUND(AVG(charlson_score),1)  AS mean_cs,
    ROUND(MEDIAN(charlson_score),1) AS median_cs,
    -- age score (0-4)
    ROUND(AVG(CASE WHEN age > 80 THEN 4 WHEN age > 70 THEN 3 WHEN age > 60 THEN 2 WHEN age > 50 THEN 1 ELSE 0 END),2) AS mean_age_score,
    -- high-weight flags (weight >=2)
    ROUND(AVG(CASE WHEN charlson_score >= 0 THEN 1 ELSE 0 END)*100,1) AS pct_any,
    -- individual conditions (not directly stored, but we can infer from score range)
    -- Let's check score distribution instead
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY charlson_score) AS p10,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY charlson_score) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY charlson_score) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY charlson_score) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY charlson_score) AS p90,
    MIN(charlson_score) AS min_val,
    MAX(charlson_score) AS max_val
FROM first_stay WHERE rn = 1
GROUP BY hospital_expire_flag ORDER BY hospital_expire_flag
""").fetchdf()

print("=== Charlson score distribution by mortality (MIMIC-IV, first stay per patient) ===")
print(r.to_string())

# Now check individual flag prevalence by rebuilding from diagnoses
# Since flags aren't stored in wide table, estimate from charlson_score buckets
# Instead, check score distribution by sepsis
r2 = con.execute("""
WITH first_stay AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
    FROM w4 WHERE hr = 0
),
tagged AS (
    SELECT *,
        MAX(SepsisLabel) OVER (PARTITION BY stay_id) AS sepsis_any
    FROM w4
),
first_sepsis AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
    FROM tagged WHERE hr = 0
)
SELECT
    sepsis_any,
    COUNT(*) AS n,
    ROUND(AVG(charlson_score),1)    AS mean_cs,
    ROUND(MEDIAN(charlson_score),1) AS median_cs,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY charlson_score) AS p25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY charlson_score) AS p75
FROM first_sepsis WHERE rn = 1
GROUP BY sepsis_any ORDER BY sepsis_any
""").fetchdf()

print("\n=== Charlson score by sepsis status (MIMIC-IV) ===")
print(r2.to_string())

# Score = 0 rate (no comorbidities beyond age)
r3 = con.execute("""
WITH first_stay AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
    FROM w4 WHERE hr = 0
)
SELECT
    hospital_expire_flag,
    ROUND(AVG(CASE WHEN charlson_score = 0 THEN 1.0 ELSE 0 END)*100,1) AS pct_score0,
    ROUND(AVG(CASE WHEN charlson_score <= 2 THEN 1.0 ELSE 0 END)*100,1) AS pct_score_le2,
    ROUND(AVG(CASE WHEN charlson_score >= 6 THEN 1.0 ELSE 0 END)*100,1) AS pct_score_ge6,
    ROUND(AVG(CASE WHEN charlson_score >= 10 THEN 1.0 ELSE 0 END)*100,1) AS pct_score_ge10
FROM first_stay WHERE rn = 1
GROUP BY hospital_expire_flag ORDER BY hospital_expire_flag
""").fetchdf()

print("\n=== Score category breakdown by mortality (MIMIC-IV) ===")
print(r3.to_string())
