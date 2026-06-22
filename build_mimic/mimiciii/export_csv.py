import duckdb
import time

IN  = "D:/ESILV_S2/Intern/build_mimic/mimiciii/output/mimic3_wide.parquet"
OUT = "D:/ESILV_S2/Intern/build_mimic/mimiciii/output/mimic3_wide.csv"

t0 = time.time()
con = duckdb.connect()
con.execute(f"COPY (SELECT * FROM read_parquet('{IN}')) TO '{OUT}' (FORMAT CSV, HEADER TRUE)")
print(f"done in {time.time()-t0:.1f}s  ->  {OUT}")
