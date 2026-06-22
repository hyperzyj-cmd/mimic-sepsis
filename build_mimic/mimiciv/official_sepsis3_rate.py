"""
Replicate official MIMIC-IV sepsis3.sql logic using local parquet files.
Official: sofa_24hours >= 2 AND suspected_infection=1
          within sofa.endtime IN [suspected_infection_time-48h, suspected_infection_time+24h]
"""
import duckdb

con = duckdb.connect()
INTER = "intermediate/mimiciv"

con.execute(f"CREATE VIEW sofa   AS SELECT * FROM read_parquet('{INTER}/20_sofa.parquet')")
con.execute(f"CREATE VIEW sepsis AS SELECT * FROM read_parquet('{INTER}/21_sepsis.parquet')")
con.execute(f"CREATE VIEW wide   AS SELECT * FROM read_parquet('output/mimic4_wide.parquet')")

# Official sepsis3.sql logic
r = con.execute("""
WITH sofa_q AS (
    SELECT stay_id, hr, endtime, sofa_24hours
    FROM sofa
    WHERE sofa_24hours >= 2
),
soi AS (
    -- first suspicion event per stay from step21
    SELECT DISTINCT stay_id, t_suspicion, antibiotic_time, culture_time, specimen, positive_culture
    FROM sepsis
    WHERE t_suspicion IS NOT NULL
),
matched AS (
    SELECT DISTINCT soi.stay_id
    FROM soi
    JOIN sofa_q sq
      ON soi.stay_id = sq.stay_id
     AND sq.endtime >= soi.t_suspicion - INTERVAL '48' HOUR
     AND sq.endtime <= soi.t_suspicion + INTERVAL '24' HOUR
),
-- compare: local wide table
local_sepsis AS (
    SELECT stay_id, MAX(SepsisLabel) AS is_sepsis
    FROM wide WHERE hr >= 0
    GROUP BY stay_id
)
SELECT
    (SELECT COUNT(DISTINCT stay_id) FROM wide WHERE hr=0)     AS total_stays,
    -- official logic rate
    (SELECT COUNT(*) FROM matched)                             AS official_sepsis_stays,
    ROUND(100.0*(SELECT COUNT(*) FROM matched)
              /(SELECT COUNT(DISTINCT stay_id) FROM wide WHERE hr=0), 1) AS official_pct,
    -- local wide table rate
    (SELECT SUM(is_sepsis) FROM local_sepsis)                 AS local_sepsis_stays,
    ROUND(100.0*(SELECT SUM(is_sepsis) FROM local_sepsis)
              /(SELECT COUNT(*) FROM local_sepsis), 1)        AS local_pct
""").fetchdf()

print("=== MIMIC-IV: Official sepsis3 logic vs Local ===")
print(r.T.to_string())
