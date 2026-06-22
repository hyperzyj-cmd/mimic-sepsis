"""
Build Part 5 of Mimic_analysis_summary.md:
  A. ICU stay count distribution per subject (1-10 individually, ≥11 merged)
  B. Sepsis sequence patterns (≤4 stays: all combinations; ≥5 stays: summary)
"""
import duckdb
import pandas as pd
from pathlib import Path

III_PARQUET = "D:/ESILV_S2/Intern/build_mimic/mimiciii/output/mimic3_wide.parquet"
IV_PARQUET  = "D:/ESILV_S2/Intern/build_mimic/mimiciv/output/mimic4_wide.parquet"
SUMMARY_MD  = Path("D:/ESILV_S2/Intern/mimic_analysis/summary_output/Mimic_analysis_summary.md")

con = duckdb.connect()
con.execute(f"CREATE VIEW w3 AS SELECT * FROM read_parquet('{III_PARQUET}')")
con.execute(f"CREATE VIEW w4 AS SELECT * FROM read_parquet('{IV_PARQUET}')")

# ─────────────────────────────────────────────────────────────────────────────
# A. Stay count distribution
# ─────────────────────────────────────────────────────────────────────────────
def get_stay_dist(view, id_col, stay_col, intime_col):
    return con.execute(f"""
        WITH stay_sep AS (
            SELECT {id_col}, {stay_col},
                   MAX(CASE WHEN SepsisLabel=1 THEN 1 ELSE 0 END) AS is_sepsis
            FROM {view} WHERE hr>=0
            GROUP BY {id_col}, {stay_col}
        ),
        subj AS (
            SELECT {id_col},
                   COUNT(DISTINCT {stay_col})  AS n_stays,
                   SUM(is_sepsis)              AS n_sep
            FROM stay_sep GROUP BY {id_col}
        )
        SELECT n_stays,
               COUNT(*)                                                            AS n_subjects,
               SUM(CASE WHEN n_sep = 0            THEN 1 ELSE 0 END)              AS all_non_sep,
               SUM(CASE WHEN n_sep = n_stays       THEN 1 ELSE 0 END)              AS all_sep,
               SUM(CASE WHEN n_sep > 0 AND n_sep < n_stays THEN 1 ELSE 0 END)     AS mixed
        FROM subj
        GROUP BY n_stays ORDER BY n_stays
    """).fetchdf()

dist3 = get_stay_dist("w3", "SUBJECT_ID", "ICUSTAY_ID", "INTIME")
dist4 = get_stay_dist("w4", "subject_id", "stay_id",    "intime")

def collapse_dist(df, cap=10):
    """Rows 1-cap individually; ≥cap+1 merged."""
    top = df[df["n_stays"] <= cap].copy()
    bot = df[df["n_stays"] > cap]
    if len(bot):
        merged = pd.DataFrame([{
            "n_stays":    f"≥{cap+1}",
            "n_subjects": bot["n_subjects"].sum(),
            "all_non_sep":bot["all_non_sep"].sum(),
            "all_sep":    bot["all_sep"].sum(),
            "mixed":      bot["mixed"].sum(),
        }])
        top = pd.concat([top.astype({"n_stays": object}), merged], ignore_index=True)
    return top

def dist_to_md(df):
    total = df["n_subjects"].sum()
    cum   = 0
    lines = []
    lines.append("| ICU stays | Subjects, n (%) | ≤ n stays (%) | All non-sepsis | All sepsis | Mixed |")
    lines.append("|---:|---:|---:|---:|---:|---:|")
    for _, row in df.iterrows():
        n    = int(row["n_subjects"])
        cum += n
        pct  = 100 * n / total
        cpct = 100 * cum / total
        ns   = int(row["all_non_sep"])
        as_  = int(row["all_sep"])
        mx   = int(row["mixed"])
        ns_p = 100*ns/n; as_p = 100*as_/n; mx_p = 100*mx/n
        lines.append(
            f"| {row['n_stays']} | {n:,} ({pct:.1f}%) | {cpct:.1f}% "
            f"| {ns:,} ({ns_p:.1f}%) | {as_:,} ({as_p:.1f}%) | {mx:,} ({mx_p:.1f}%) |"
        )
    return lines

# ─────────────────────────────────────────────────────────────────────────────
# B. Sequence patterns
# ─────────────────────────────────────────────────────────────────────────────
def get_sequences(view, id_col, stay_col, intime_col):
    return con.execute(f"""
        WITH stay_sep AS (
            SELECT {id_col}, {stay_col}, MIN({intime_col}) AS intime,
                   MAX(CASE WHEN SepsisLabel=1 THEN 1 ELSE 0 END) AS is_sepsis
            FROM {view} WHERE hr>=0
            GROUP BY {id_col}, {stay_col}
        ),
        ranked AS (
            SELECT {id_col}, is_sepsis,
                   ROW_NUMBER() OVER (PARTITION BY {id_col} ORDER BY intime) AS rk
            FROM stay_sep
        ),
        seqs AS (
            SELECT {id_col},
                   COUNT(*)  AS n_stays,
                   STRING_AGG(CAST(is_sepsis AS VARCHAR), '->' ORDER BY rk) AS seq
            FROM ranked GROUP BY {id_col}
        )
        SELECT n_stays, seq, COUNT(*) AS n_subjects
        FROM seqs GROUP BY n_stays, seq
        ORDER BY n_stays, n_subjects DESC
    """).fetchdf()

seq3 = get_sequences("w3", "SUBJECT_ID", "ICUSTAY_ID", "INTIME")
seq4 = get_sequences("w4", "subject_id", "stay_id",    "intime")

def seq_to_md(df, cap_stays=4):
    """Full table for n_stays ≤ cap_stays; summary table for > cap_stays."""
    lines = []

    # ── detailed table (≤ cap_stays) ──────────────────────────────────────────
    lines.append(f"#### Sequences with 1–{cap_stays} ICU Stays (all combinations)\n")
    lines.append("| n stays | Sequence | Subjects, n | % within group |")
    lines.append("|---:|---|---:|---:|")
    detail = df[df["n_stays"] <= cap_stays].copy()
    for n_stay in sorted(detail["n_stays"].unique()):
        grp   = detail[detail["n_stays"] == n_stay]
        total = grp["n_subjects"].sum()
        for _, row in grp.iterrows():
            pct = 100 * row["n_subjects"] / total
            lines.append(f"| {int(n_stay)} | `{row['seq']}` | {int(row['n_subjects']):,} | {pct:.1f}% |")

    # ── summary table (> cap_stays) ───────────────────────────────────────────
    above = df[df["n_stays"] > cap_stays]
    if len(above):
        lines.append(f"\n#### Sequences with ≥{cap_stays+1} ICU Stays (summary)\n")
        lines.append("| n stays | Subjects | Unique patterns | Most common sequence | Count |")
        lines.append("|---:|---:|---:|---|---:|")
        for n_stay in sorted(above["n_stays"].unique()):
            grp  = above[above["n_stays"] == n_stay]
            top1 = grp.iloc[0]
            lines.append(
                f"| {int(n_stay)} | {int(grp['n_subjects'].sum()):,} "
                f"| {len(grp)} | `{top1['seq']}` | {int(top1['n_subjects'])} |"
            )
    return lines

# ─────────────────────────────────────────────────────────────────────────────
# Build Part 5 markdown
# ─────────────────────────────────────────────────────────────────────────────
total3 = dist3["n_subjects"].sum()
total4 = dist4["n_subjects"].sum()

# overall stats
n_multi3 = dist3[dist3["n_stays"] > 1]["n_subjects"].sum()
n_multi4 = dist4[dist4["n_stays"] > 1]["n_subjects"].sum()

part5 = []
part5 += [
    "",
    "---",
    "",
    "## Part 4: Subject-Level ICU Stay Patterns",
    "",
    "> **Unit of analysis:** one row per unique subject (first vs. subsequent stays). "
    "Sepsis label per stay = 1 if `SepsisLabel = 1` at any `hr ≥ 0` within that stay, else 0.",
    "",
    "---",
    "",
    "### 5.1 Distribution of ICU Stays per Subject",
    "",
    f"MIMIC-III: **{total3:,}** unique subjects — {n_multi3:,} ({100*n_multi3/total3:.1f}%) "
    f"have more than one ICU stay.  ",
    f"MIMIC-IV: **{total4:,}** unique subjects — {n_multi4:,} ({100*n_multi4/total4:.1f}%) "
    f"have more than one ICU stay.",
    "",
    "*All non-sepsis* = all stays for this subject are labelled 0; "
    "*All sepsis* = all stays labelled 1; "
    "*Mixed* = at least one 0 and at least one 1 across stays.",
    "",
    "**MIMIC-III**",
    "",
]
part5 += dist_to_md(collapse_dist(dist3))
part5 += [
    "",
    "**MIMIC-IV**",
    "",
]
part5 += dist_to_md(collapse_dist(dist4))

# unique sequences count
u3 = seq3["seq"].nunique()
u4 = seq4["seq"].nunique()
u3_le4 = seq3[seq3["n_stays"] <= 4]["seq"].nunique()
u4_le4 = seq4[seq4["n_stays"] <= 4]["seq"].nunique()

part5 += [
    "",
    "---",
    "",
    "### 5.2 Sepsis Sequence Patterns",
    "",
    "Each subject's ICU stays are ordered by admission time and collapsed to a binary "
    "sepsis label per stay (0 = no sepsis, 1 = sepsis), then concatenated into a sequence "
    "such as `0->1->0`. Proportion within group = share among all subjects with the same "
    "number of ICU stays.",
    "",
    f"MIMIC-III: **{u3}** unique sequences in total ({u3_le4} with ≤4 stays).  ",
    f"MIMIC-IV: **{u4}** unique sequences in total ({u4_le4} with ≤4 stays).",
    "",
    "#### MIMIC-III",
    "",
]
part5 += seq_to_md(seq3)
part5 += [
    "",
    "#### MIMIC-IV",
    "",
]
part5 += seq_to_md(seq4)

# ─────────────────────────────────────────────────────────────────────────────
# Append to summary
# ─────────────────────────────────────────────────────────────────────────────
marker = "## Part 4:"
existing = SUMMARY_MD.read_text(encoding="utf-8")
idx = existing.find(marker)
base = existing[:idx].rstrip() if idx != -1 else existing.rstrip()
SUMMARY_MD.write_text(base + "\n" + "\n".join(part5) + "\n", encoding="utf-8")
print("Done →", SUMMARY_MD)
