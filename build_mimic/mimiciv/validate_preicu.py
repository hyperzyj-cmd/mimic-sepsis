import duckdb
con = duckdb.connect()
con.execute("CREATE VIEW wide AS SELECT * FROM read_parquet('output/mimic4_wide.parquet')")

r1 = con.execute("""
SELECT hr, COUNT(*) AS rows
FROM wide WHERE hr BETWEEN -24 AND -1
GROUP BY hr ORDER BY hr
""").fetchdf()
print("=== Pre-ICU row counts ===")
print(r1.to_string())

r2 = con.execute("""
SELECT
    CASE WHEN hr < 0 THEN 'pre_icu' ELSE 'icu' END AS period,
    ROUND(100.0 * COUNT(creatinine) / COUNT(*), 1) AS pct_creatinine,
    ROUND(100.0 * COUNT(platelet)   / COUNT(*), 1) AS pct_platelet,
    ROUND(100.0 * COUNT(po2)        / COUNT(*), 1) AS pct_po2,
    ROUND(100.0 * COUNT(heart_rate) / COUNT(*), 1) AS pct_heartrate,
    ROUND(100.0 * COUNT(SepsisLabel)/ COUNT(*), 1) AS pct_sepsislabel_nonnull
FROM wide
WHERE hr BETWEEN -24 AND 48
GROUP BY period
ORDER BY period
""").fetchdf()
print("\n=== Feature coverage ===")
print(r2.to_string())

r3 = con.execute("""
SELECT
    COUNT(*) AS pre_icu_rows,
    SUM(CASE WHEN SepsisLabel IS NULL THEN 1 ELSE 0 END) AS sepsislabel_null,
    SUM(CASE WHEN sofa_24hours IS NULL THEN 1 ELSE 0 END) AS sofa_null,
    SUM(CASE WHEN creatinine IS NOT NULL THEN 1 ELSE 0 END) AS has_creatinine,
    SUM(CASE WHEN heart_rate IS NOT NULL THEN 1 ELSE 0 END) AS has_heartrate
FROM wide WHERE hr < 0
""").fetchdf()
print("\n=== Pre-ICU nulling validation ===")
print(r3.T.to_string())
