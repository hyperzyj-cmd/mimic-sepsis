"""
ICU stay time-series plots for MIMIC-III and MIMIC-IV.

Figures (saved to mimic_analysis/mimiciii/ and mimiciv/):
  1. icu_retention.png       — active stays over ICU time (total / sepsis / non-sepsis)
  2. sepsis_prevalence.png   — % of active stays currently in sepsis at each hour
  3. death_timing.png        — histogram of when in-hospital deaths occur (ICU hour)
  4. sofa_trajectory.png     — mean SOFA over time: sepsis vs non-sepsis stays
  5. vasopressor_rate.png    — % of active stays on ANY vasopressor over ICU time
"""
import duckdb
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
III_PARQUET = "D:/ESILV_S2/Intern/build_mimic/mimiciii/output/mimic3_wide.parquet"
IV_PARQUET  = "D:/ESILV_S2/Intern/build_mimic/mimiciv/output/mimic4_wide.parquet"
OUT_III = Path("D:/ESILV_S2/Intern/mimic_analysis/mimiciii")
OUT_IV  = Path("D:/ESILV_S2/Intern/mimic_analysis/mimiciv")
OUT_III.mkdir(parents=True, exist_ok=True)
OUT_IV.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute(f"CREATE VIEW w3 AS SELECT * FROM read_parquet('{III_PARQUET}')")
con.execute(f"CREATE VIEW w4 AS SELECT * FROM read_parquet('{IV_PARQUET}')")

BINS     = list(range(0, 337, 3))   # 0,3,…,336 h
BIN_ARR  = np.array(BINS)

# ── Shared style ──────────────────────────────────────────────────────────────
C = dict(total="#2196F3", sepsis="#FF5722", nonsepsis="#4CAF50",
         sep_sofa="#E53935", ns_sofa="#1E88E5", vaso="#00ACC1")

plt.rcParams.update({
    "figure.dpi": 150, "font.size": 11,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
})

def fmt_k(ax_obj):
    ax_obj.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

def fmt_pct(ax_obj):
    ax_obj.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))

def x_ticks(ax_obj):
    ax_obj.xaxis.set_major_locator(mticker.MultipleLocator(24))


# ═══════════════════════════════════════════════════════════════════════════════
# 1. RETENTION (total / sepsis / non-sepsis only — death line removed)
# ═══════════════════════════════════════════════════════════════════════════════
def build_retention(view, id_col):
    return con.execute(f"""
        SELECT {id_col} AS sid,
               MAX(hr)  AS los,
               MAX(CASE WHEN SepsisLabel=1 THEN 1 ELSE 0 END) AS is_sepsis
        FROM {view} WHERE hr >= 0
        GROUP BY {id_col}
    """).fetchdf()


def plot_retention(df, label, out_path):
    total, sepsis, nonsep = [], [], []
    for t in BINS:
        sub = df[df["los"] >= t]
        total.append(len(sub))
        sepsis.append(int(sub["is_sepsis"].sum()))
        nonsep.append(int((sub["is_sepsis"] == 0).sum()))

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(BINS, total,  label="All stays",        color=C["total"],     lw=2.0)
    ax.plot(BINS, sepsis, label="Sepsis stays",     color=C["sepsis"],    lw=1.8)
    ax.plot(BINS, nonsep, label="Non-sepsis stays", color=C["nonsepsis"], lw=1.8)

    ax.set_title(f"{label} — Active ICU Stays over Time", fontsize=13, pad=10)
    ax.set_xlabel("ICU stay time (hours, 3h bins)")
    ax.set_ylabel("Number of active ICU stays")
    x_ticks(ax); fmt_k(ax)
    ax.legend(framealpha=0.85, fontsize=10)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. SEPSIS PREVALENCE  —  % of active stays currently in sepsis at hour t
#    A stay is "currently sepsis" at hour t if onset_hr <= t AND los >= t
# ═══════════════════════════════════════════════════════════════════════════════
def build_sepsis_prev(view, id_col):
    return con.execute(f"""
        SELECT {id_col}                                                  AS sid,
               MAX(hr)                                                   AS los,
               MIN(CASE WHEN SepsisLabel=1 THEN hr ELSE NULL END)        AS onset_hr
        FROM {view} WHERE hr >= 0
        GROUP BY {id_col}
    """).fetchdf()


def plot_sepsis_prevalence(df, label, out_path):
    pct = []
    for t in BINS:
        active = df[df["los"] >= t]
        n = len(active)
        if n == 0:
            pct.append(np.nan)
            continue
        # currently sepsis = onset has already happened by hour t
        currently_sep = active[active["onset_hr"].notna() & (active["onset_hr"] <= t)]
        pct.append(100 * len(currently_sep) / n)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(BINS, pct, color=C["sepsis"], lw=2.0)
    ax.fill_between(BINS, pct, alpha=0.12, color=C["sepsis"])

    ax.set_title(f"{label} — Sepsis Prevalence among Active ICU Stays", fontsize=13, pad=10)
    ax.set_xlabel("ICU stay time (hours, 3h bins)")
    ax.set_ylabel("% of active stays currently in sepsis")
    x_ticks(ax); fmt_pct(ax)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. DEATH TIMING  —  histogram of when in-hospital deaths occur (≈ MAX(hr))
# ═══════════════════════════════════════════════════════════════════════════════
def build_death_timing(view, id_col):
    return con.execute(f"""
        SELECT MAX(hr) AS death_hr
        FROM {view}
        WHERE hr >= 0
        GROUP BY {id_col}
        HAVING MAX(COALESCE(hospital_expire_flag, 0)) = 1
    """).fetchdf()


def plot_death_timing(df, label, out_path):
    hrs = df["death_hr"].clip(upper=336)
    bin_edges = np.arange(0, 340, 3)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(hrs, bins=bin_edges, color=C["sepsis"], alpha=0.75, edgecolor="none")

    # cumulative % on right axis
    ax2 = ax.twinx()
    sorted_hrs = np.sort(hrs)
    cum = np.arange(1, len(sorted_hrs)+1) / len(sorted_hrs) * 100
    ax2.plot(sorted_hrs, cum, color="#333", lw=1.5, ls="--")
    ax2.set_ylabel("Cumulative % of in-hospital deaths")
    ax2.set_ylim(0, 105)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax2.spines["top"].set_visible(False)

    ax.set_title(f"{label} — In-Hospital Death Timing (ICU hour)", fontsize=13, pad=10)
    ax.set_xlabel("ICU hour of death (3h bins)")
    ax.set_ylabel("Number of deaths")
    x_ticks(ax); fmt_k(ax)

    from matplotlib.lines import Line2D
    legend_items = [
        plt.Rectangle((0,0),1,1, color=C["sepsis"], alpha=0.75),
        Line2D([0],[0], color="#333", ls="--", lw=1.5),
    ]
    ax.legend(legend_items, ["Death count", "Cumulative %"], framealpha=0.85, fontsize=10)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. SOFA TRAJECTORY (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════
def build_sofa_trajectory(view, id_col, sofa_col):
    return con.execute(f"""
        WITH tags AS (
            SELECT {id_col},
                   MAX(CASE WHEN SepsisLabel=1 THEN 1 ELSE 0 END) AS is_sepsis
            FROM {view} WHERE hr >= 0 GROUP BY {id_col}
        )
        SELECT (v.hr/3)*3 AS bin, t.is_sepsis,
               AVG(v.{sofa_col}) AS mean_sofa
        FROM {view} v
        JOIN tags t ON v.{id_col} = t.{id_col}
        WHERE v.hr >= 0 AND v.hr <= 336 AND v.{sofa_col} IS NOT NULL
        GROUP BY bin, t.is_sepsis ORDER BY bin
    """).fetchdf()


def plot_sofa(df, label, out_path):
    sep  = df[df["is_sepsis"]==1].set_index("bin")["mean_sofa"]
    nsep = df[df["is_sepsis"]==0].set_index("bin")["mean_sofa"]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(sep.index,  sep.values,  label="Sepsis stays",     color=C["sep_sofa"], lw=2.0)
    ax.plot(nsep.index, nsep.values, label="Non-sepsis stays", color=C["ns_sofa"],  lw=2.0, ls="--")
    ax.set_title(f"{label} — Mean SOFA Score over ICU Stay", fontsize=13, pad=10)
    ax.set_xlabel("ICU stay time (hours, 3h bins)")
    ax.set_ylabel("Mean SOFA score")
    x_ticks(ax); ax.set_ylim(bottom=0)
    ax.legend(framealpha=0.85, fontsize=10)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. VASOPRESSOR RATE  —  any vasopressor, same y-axis for III and IV
#    III: norepinephrine OR epinephrine OR dopamine OR vasopressin (rate > 0)
#    IV:  norepi_rate OR vaso_rate (rate > 0)
# ═══════════════════════════════════════════════════════════════════════════════
def build_vaso_rate_iii(view, id_col):
    return con.execute(f"""
        WITH tags AS (
            SELECT {id_col},
                   MAX(CASE WHEN SepsisLabel=1 THEN 1 ELSE 0 END) AS is_sepsis
            FROM {view} WHERE hr >= 0 GROUP BY {id_col}
        ),
        los AS (SELECT {id_col}, MAX(hr) AS los FROM {view} WHERE hr>=0 GROUP BY {id_col}),
        hourly AS (
            SELECT (v.hr/3)*3 AS bin, t.is_sepsis,
                   COUNT(DISTINCT v.{id_col})  AS active,
                   COUNT(DISTINCT CASE WHEN (
                       COALESCE(v.rate_norepinephrine,0) > 0 OR
                       COALESCE(v.rate_epinephrine,0)    > 0 OR
                       COALESCE(v.rate_dopamine,0)       > 0 OR
                       COALESCE(v.rate_vasopressin,0)    > 0
                   ) THEN v.{id_col} END) AS on_vaso
            FROM {view} v
            JOIN tags t ON v.{id_col} = t.{id_col}
            JOIN los  l ON v.{id_col} = l.{id_col}
            WHERE v.hr >= 0 AND v.hr <= l.los AND v.hr <= 336
            GROUP BY bin, t.is_sepsis
        )
        SELECT bin, is_sepsis,
               100.0 * on_vaso / NULLIF(active,0) AS vaso_pct
        FROM hourly ORDER BY bin
    """).fetchdf()


def build_vaso_rate_iv(view, id_col):
    return con.execute(f"""
        WITH tags AS (
            SELECT {id_col},
                   MAX(CASE WHEN SepsisLabel=1 THEN 1 ELSE 0 END) AS is_sepsis
            FROM {view} WHERE hr >= 0 GROUP BY {id_col}
        ),
        los AS (SELECT {id_col}, MAX(hr) AS los FROM {view} WHERE hr>=0 GROUP BY {id_col}),
        hourly AS (
            SELECT (v.hr/3)*3 AS bin, t.is_sepsis,
                   COUNT(DISTINCT v.{id_col})  AS active,
                   COUNT(DISTINCT CASE WHEN (
                       COALESCE(v.norepi_rate,0) > 0 OR
                       COALESCE(v.vaso_rate,0)   > 0
                   ) THEN v.{id_col} END) AS on_vaso
            FROM {view} v
            JOIN tags t ON v.{id_col} = t.{id_col}
            JOIN los  l ON v.{id_col} = l.{id_col}
            WHERE v.hr >= 0 AND v.hr <= l.los AND v.hr <= 336
            GROUP BY bin, t.is_sepsis
        )
        SELECT bin, is_sepsis,
               100.0 * on_vaso / NULLIF(active,0) AS vaso_pct
        FROM hourly ORDER BY bin
    """).fetchdf()


def plot_vaso(df3, df4, out3, out4):
    # Compute shared y-axis max across both datasets
    ymax = max(df3["vaso_pct"].max(), df4["vaso_pct"].max()) * 1.1

    for df, label, out_path in [(df3, "MIMIC-III", out3), (df4, "MIMIC-IV", out4)]:
        sep  = df[df["is_sepsis"]==1].set_index("bin")["vaso_pct"]
        nsep = df[df["is_sepsis"]==0].set_index("bin")["vaso_pct"]

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(sep.index,  sep.values,  label="Sepsis stays",     color=C["sep_sofa"], lw=2.0)
        ax.plot(nsep.index, nsep.values, label="Non-sepsis stays", color=C["ns_sofa"],  lw=2.0, ls="--")
        ax.set_title(f"{label} — Vasopressor Use Rate over ICU Stay\n"
                     "(any of: norepinephrine / epinephrine / dopamine / vasopressin)",
                     fontsize=12, pad=10)
        ax.set_xlabel("ICU stay time (hours, 3h bins)")
        ax.set_ylabel("% of active stays on vasopressors")
        x_ticks(ax); fmt_pct(ax)
        ax.set_ylim(0, ymax)                   # shared y-axis
        ax.legend(framealpha=0.85, fontsize=10)
        fig.tight_layout()
        fig.savefig(out_path, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved: {out_path}")


# ═══════════════════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════════════════
print("\n=== MIMIC-III ===")
ret3 = build_retention("w3", "ICUSTAY_ID")
plot_retention(ret3, "MIMIC-III", OUT_III / "icu_retention.png")

prev3 = build_sepsis_prev("w3", "ICUSTAY_ID")
plot_sepsis_prevalence(prev3, "MIMIC-III", OUT_III / "sepsis_prevalence.png")

death3 = build_death_timing("w3", "ICUSTAY_ID")
plot_death_timing(death3, "MIMIC-III", OUT_III / "death_timing.png")

sofa3 = build_sofa_trajectory("w3", "ICUSTAY_ID", "sofa_total")
plot_sofa(sofa3, "MIMIC-III", OUT_III / "sofa_trajectory.png")

print("\n=== MIMIC-IV ===")
ret4 = build_retention("w4", "stay_id")
plot_retention(ret4, "MIMIC-IV", OUT_IV / "icu_retention.png")

prev4 = build_sepsis_prev("w4", "stay_id")
plot_sepsis_prevalence(prev4, "MIMIC-IV", OUT_IV / "sepsis_prevalence.png")

death4 = build_death_timing("w4", "stay_id")
plot_death_timing(death4, "MIMIC-IV", OUT_IV / "death_timing.png")

sofa4 = build_sofa_trajectory("w4", "stay_id", "sofa_24hours")
plot_sofa(sofa4, "MIMIC-IV", OUT_IV / "sofa_trajectory.png")

print("\n=== Vasopressor (shared y-axis) ===")
vaso3 = build_vaso_rate_iii("w3", "ICUSTAY_ID")
vaso4 = build_vaso_rate_iv("w4", "stay_id")
plot_vaso(vaso3, vaso4, OUT_III / "vasopressor_rate.png", OUT_IV / "vasopressor_rate.png")

print("\nAll done.")
