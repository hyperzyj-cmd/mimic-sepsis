r"""
Shared MIMIC-III onset-window experiment runner.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import duckdb
from khiops import core as kh
import matplotlib
import numpy as np
from IPython.display import display

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from khiops.sklearn import KhiopsClassifier
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import StratifiedKFold

from variable_sets import VARIABLE_SETS


REPO_ROOT = Path(__file__).resolve().parents[3]
PARQUET = REPO_ROOT / "build_mimic" / "mimiciii" / "output" / "mimic3_wide.parquet"
EXP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(EXP_DIR, "output")
RUN_OUTPUT_DIR = os.path.join(OUT_DIR, "single_runs")
SUMMARY_OUTPUT_DIR = os.path.join(OUT_DIR, "grid_summary")
EXPLAIN_OUTPUT_DIR = os.path.join(OUT_DIR, "explainability")
DEBUG_OUTPUT_DIR = os.path.join(OUT_DIR, "debug_tables")
for _path in (
    OUT_DIR,
    RUN_OUTPUT_DIR,
    SUMMARY_OUTPUT_DIR,
    EXPLAIN_OUTPUT_DIR,
    DEBUG_OUTPUT_DIR,
):
    os.makedirs(_path, exist_ok=True)

HORIZONS = [3, 6]
N_FEATURES = 1000
N_SPLITS = 10
CV5_SPLITS = 5
RANDOM_SEED = 42
NEG_SAMPLE_CAP = 12000
INCLUDE_STATIC_IN_ROOT = False
EXPORT_GLOBAL_EXPLAINABILITY = True
NEGATIVE_ALIGNMENT_MODE = "paper"

STATIC_COLS = VARIABLE_SETS["near_full_wide"]["static"]
DYNAMIC_COLS = VARIABLE_SETS["near_full_wide"]["dynamic"]


def _artifact_path(base_dir: str, filename: str) -> str:
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, filename)


def _static_sql(active_static_cols: list[str]) -> tuple[str, str]:
    if not active_static_cols:
        return "", ""
    expr = ", ".join(f"MAX({c}) AS {c}" for c in active_static_cols)
    return expr, f", {expr}"


def preload_data(
    cohort_before: int,
    cohort_after: int,
    *,
    include_static_in_root: bool,
):
    con = duckdb.connect()
    path = str(PARQUET).replace("\\", "/")
    dyn = ", ".join(DYNAMIC_COLS)
    dyn_select = f"hr, {dyn}" if dyn else "hr"
    active_static_cols = STATIC_COLS if include_static_in_root else []
    _, stat_select = _static_sql(active_static_cols)

    print("  Positive stays...")
    pos_static = con.execute(
        f"""
        WITH onset AS (
            SELECT ICUSTAY_ID, MIN(hr) AS onset_hr_first
            FROM read_parquet('{path}')
            WHERE SepsisLabel = 1
            GROUP BY ICUSTAY_ID
        ),
        stay_bounds AS (
            SELECT ICUSTAY_ID, MAX(hr) AS max_hr
            FROM read_parquet('{path}')
            GROUP BY ICUSTAY_ID
        ),
        pos_ids AS (
            SELECT o.ICUSTAY_ID, o.onset_hr_first, b.max_hr
            FROM onset o
            JOIN stay_bounds b USING (ICUSTAY_ID)
            WHERE o.onset_hr_first >= {cohort_before}
              AND b.max_hr >= o.onset_hr_first + {cohort_after - 1}
        )
        SELECT s.ICUSTAY_ID, p.onset_hr_first, p.max_hr{stat_select}
        FROM (
            SELECT ICUSTAY_ID{stat_select}
            FROM read_parquet('{path}')
            WHERE ICUSTAY_ID IN (SELECT ICUSTAY_ID FROM pos_ids)
            GROUP BY ICUSTAY_ID
        ) s
        JOIN pos_ids p USING (ICUSTAY_ID)
        GROUP BY s.ICUSTAY_ID, p.onset_hr_first, p.max_hr
    """
    ).fetchdf()

    if pos_static.empty:
        raise RuntimeError("No positive stays available for this cohort/window configuration.")

    pos_ids_str = ",".join(str(i) for i in pos_static["ICUSTAY_ID"].tolist())
    pos_hourly = con.execute(
        f"""
        SELECT ICUSTAY_ID, {dyn_select}
        FROM read_parquet('{path}')
        WHERE ICUSTAY_ID IN ({pos_ids_str})
        ORDER BY ICUSTAY_ID, hr
    """
    ).fetchdf()

    print("  Negative stays...")
    cohort_total = cohort_before + cohort_after
    neg_static = con.execute(
        f"""
        SELECT ICUSTAY_ID, MAX(hr) AS los_hr{stat_select}
        FROM read_parquet('{path}')
        GROUP BY ICUSTAY_ID
        HAVING SUM(CASE WHEN SepsisLabel = 1 THEN 1 ELSE 0 END) = 0
           AND MAX(hr) + 1 >= {cohort_total}
        LIMIT {NEG_SAMPLE_CAP}
    """
    ).fetchdf()

    if neg_static.empty:
        raise RuntimeError("No negative stays available for this cohort/window configuration.")

    neg_ids_str = ",".join(str(i) for i in neg_static["ICUSTAY_ID"].tolist())
    neg_hourly = con.execute(
        f"""
        SELECT ICUSTAY_ID, {dyn_select}
        FROM read_parquet('{path}')
        WHERE ICUSTAY_ID IN ({neg_ids_str})
        ORDER BY ICUSTAY_ID, hr
    """
    ).fetchdf()

    con.close()
    return pos_static, pos_hourly, neg_static, neg_hourly


def _build_negative_cohort(
    neg_static: pd.DataFrame,
    neg_hourly: pd.DataFrame,
    *,
    cohort_before: int,
    cohort_after: int,
    horizon_h: int,
    mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cohort_total = cohort_before + cohort_after
    neg_hr = neg_hourly.copy()

    if mode == "tail":
        neg_los = dict(zip(neg_static["ICUSTAY_ID"], neg_static["los_hr"]))
        neg_hr["los_hr"] = neg_hr["ICUSTAY_ID"].map(neg_los)
        neg_hr["cohort_start"] = neg_hr["los_hr"] - (cohort_total - 1)
        neg_hr["cohort_end"] = neg_hr["los_hr"] + 1
    elif mode == "paper":
        neg_hr["cohort_start"] = 0
        neg_hr["cohort_end"] = cohort_total
    else:
        raise ValueError(f"Unsupported negative_alignment_mode: {mode}")

    neg_cohort = neg_hr[
        (neg_hr["hr"] >= neg_hr["cohort_start"]) & (neg_hr["hr"] < neg_hr["cohort_end"])
    ].copy()
    valid_neg_full = set(
        neg_cohort.groupby("ICUSTAY_ID").size().loc[lambda s: s == cohort_total].index
    )
    neg_cohort = neg_cohort[neg_cohort["ICUSTAY_ID"].isin(valid_neg_full)].copy()
    neg_static_valid = neg_static[neg_static["ICUSTAY_ID"].isin(valid_neg_full)].copy()

    neg_cohort["pseudo_onset"] = neg_cohort["cohort_start"] + cohort_before
    neg_cohort["visible_end"] = neg_cohort["pseudo_onset"] - horizon_h
    neg_visible = neg_cohort[
        (neg_cohort["hr"] >= neg_cohort["cohort_start"])
        & (neg_cohort["hr"] <= neg_cohort["visible_end"])
    ].copy()
    drop_cols = ["cohort_start", "cohort_end", "pseudo_onset", "visible_end"]
    if "los_hr" in neg_visible.columns:
        drop_cols.append("los_hr")
    neg_visible = neg_visible.drop(columns=drop_cols)
    if "los_hr" in neg_static_valid.columns:
        neg_static_valid = neg_static_valid.drop(columns=["los_hr"])

    return neg_static_valid.reset_index(drop=True), neg_visible.reset_index(drop=True)


def extract_horizon_windows(
    pos_static,
    pos_hourly,
    neg_static,
    neg_hourly,
    cohort_before: int,
    cohort_after: int,
    horizon_h: int,
    negative_alignment_mode: str = NEGATIVE_ALIGNMENT_MODE,
):
    cohort_total = cohort_before + cohort_after

    pos_map = dict(zip(pos_static["ICUSTAY_ID"], pos_static["onset_hr_first"]))
    pos_hr = pos_hourly[pos_hourly["ICUSTAY_ID"].isin(pos_map)].copy()
    pos_hr["onset_hr_first"] = pos_hr["ICUSTAY_ID"].map(pos_map)
    pos_hr["cohort_start"] = pos_hr["onset_hr_first"] - cohort_before
    pos_hr["cohort_end"] = pos_hr["onset_hr_first"] + cohort_after

    pos_cohort = pos_hr[
        (pos_hr["hr"] >= pos_hr["cohort_start"]) & (pos_hr["hr"] < pos_hr["cohort_end"])
    ].copy()
    valid_pos_full = set(
        pos_cohort.groupby("ICUSTAY_ID").size().loc[lambda s: s == cohort_total].index
    )

    pos_cohort = pos_cohort[pos_cohort["ICUSTAY_ID"].isin(valid_pos_full)].copy()
    pos_cohort["visible_end"] = pos_cohort["onset_hr_first"] - horizon_h
    pos_visible = pos_cohort[
        (pos_cohort["hr"] >= pos_cohort["cohort_start"])
        & (pos_cohort["hr"] <= pos_cohort["visible_end"])
    ].drop(columns=["onset_hr_first", "cohort_start", "cohort_end", "visible_end"])

    pos_root = pos_static[pos_static["ICUSTAY_ID"].isin(valid_pos_full)].drop(
        columns=["onset_hr_first", "max_hr"]
    ).reset_index(drop=True)

    neg_root_all, neg_visible_all = _build_negative_cohort(
        neg_static,
        neg_hourly,
        cohort_before=cohort_before,
        cohort_after=cohort_after,
        horizon_h=horizon_h,
        mode=negative_alignment_mode,
    )

    n_target = len(pos_root)
    neg_root = neg_root_all.sample(
        min(n_target, len(neg_root_all)), random_state=RANDOM_SEED
    ).copy()
    neg_visible = neg_visible_all[
        neg_visible_all["ICUSTAY_ID"].isin(set(neg_root["ICUSTAY_ID"]))
    ].reset_index(drop=True)

    root = pd.concat([pos_root, neg_root], ignore_index=True)
    hourly = pd.concat([pos_visible, neg_visible], ignore_index=True)
    y = pd.Series([1] * len(pos_root) + [0] * len(neg_root), name="label")
    return root, hourly, y


def sanitize_khiops_frame(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    for col in clean.columns:
        series = clean[col]
        if pd.api.types.is_numeric_dtype(series.dtype) or pd.api.types.is_bool_dtype(series.dtype):
            clean[col] = pd.to_numeric(series, errors="coerce")
        else:
            clean[col] = series.astype(object).where(pd.notna(series), None)
    return clean.replace({pd.NA: np.nan})


def make_dataset(root_df, hourly_df):
    root_df = sanitize_khiops_frame(root_df)
    hourly_df = sanitize_khiops_frame(hourly_df)
    return {
        "main_table": (root_df, ["ICUSTAY_ID"]),
        "additional_data_tables": {"hourly": (hourly_df, ["ICUSTAY_ID"])},
    }


def export_relational_tables(root_df, hourly_df, prefix: str, horizon_h: int):
    root_path = _artifact_path(DEBUG_OUTPUT_DIR, f"{prefix}_h{horizon_h}_root.parquet")
    hourly_path = _artifact_path(
        DEBUG_OUTPUT_DIR, f"{prefix}_h{horizon_h}_secondary_hourly.parquet"
    )
    root_df.to_parquet(root_path, index=False)
    hourly_df.to_parquet(hourly_path, index=False)
    print(f"  Root table saved      -> {root_path}")
    print(f"  Secondary table saved -> {hourly_path}")
    return {"root_path": root_path, "hourly_path": hourly_path}


def _get_predictor_dictionary_name(clf: KhiopsClassifier) -> str:
    for dictionary in clf.model_.dictionaries:
        if getattr(dictionary, "root", False):
            return dictionary.name
    raise RuntimeError("Could not locate the root predictor dictionary in the Khiops model.")


def export_global_explainability(
    clf: KhiopsClassifier,
    *,
    prefix: str,
    task_name: str,
    horizon_h: int,
    n_pos: int,
    n_neg: int,
    eval_df: pd.DataFrame,
    stay_ids: pd.Series,
):
    predictor = clf.model_report_.modeling_report.trained_predictors[0]
    selected_variables = predictor.selected_variables or []
    prep = clf.model_report_.preparation_report

    rows = [
        {
            "rank": i + 1,
            "name": var.name,
            "prepared_name": var.prepared_name,
            "level": var.level,
            "weight": var.weight,
            "importance": var.importance,
        }
        for i, var in enumerate(selected_variables)
    ]
    selected_df = pd.DataFrame(rows)

    csv_path = _artifact_path(
        EXPLAIN_OUTPUT_DIR, f"{prefix}_h{horizon_h}_selected_variables.csv"
    )
    md_path = _artifact_path(
        EXPLAIN_OUTPUT_DIR, f"{prefix}_h{horizon_h}_global_explainability.md"
    )
    json_path = _artifact_path(
        EXPLAIN_OUTPUT_DIR, f"{prefix}_h{horizon_h}_analysis_results.json"
    )
    model_kdic_path = _artifact_path(
        EXPLAIN_OUTPUT_DIR, f"{prefix}_h{horizon_h}_predictor_model.kdic"
    )
    interpretor_kdic_path = os.path.join(
        EXPLAIN_OUTPUT_DIR, f"{prefix}_h{horizon_h}_interpretation_model.kdic"
    )
    cases_path = _artifact_path(
        EXPLAIN_OUTPUT_DIR, f"{prefix}_h{horizon_h}_explainability_cases.csv"
    )

    selected_df.to_csv(csv_path, index=False)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(clf.model_report_.to_dict(), f, ensure_ascii=False, indent=2)
    with open(model_kdic_path, "w", encoding="utf-8") as f:
        f.write(str(clf.model_))

    predictor_name = _get_predictor_dictionary_name(clf)
    kh.interpret_predictor(
        clf.model_,
        predictor_name,
        interpretor_kdic_path,
        max_variable_importances=min(len(selected_variables), 100) or 100,
    )

    case_df = pd.DataFrame(
        {
            "ICUSTAY_ID": stay_ids.reset_index(drop=True),
            "y_true": eval_df["y_true"].reset_index(drop=True),
            "y_prob_oof": eval_df["y_prob"].reset_index(drop=True),
        }
    )
    case_df["error_abs"] = (case_df["y_true"] - case_df["y_prob_oof"]).abs()
    case_df["confidence"] = (case_df["y_prob_oof"] - 0.5).abs()
    case_df = case_df.sort_values(
        ["error_abs", "confidence"], ascending=[False, False]
    ).reset_index(drop=True)
    case_df.to_csv(cases_path, index=False)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# {task_name} - Global Explainability (h={horizon_h}h)\n\n")
        f.write(f"- stays: `{n_pos + n_neg}`\n")
        f.write(f"- positives: `{n_pos}`\n")
        f.write(f"- negatives: `{n_neg}`\n")
        f.write(
            f"- informative variables: `{prep.informative_variable_number}`\n"
        )
        f.write(f"- selected variables: `{predictor.variable_number}`\n")
        f.write(f"- selected-variable export: `{os.path.basename(csv_path)}`\n")
        f.write(f"- full Khiops report JSON: `{os.path.basename(json_path)}`\n\n")
        f.write(f"- predictor model: `{os.path.basename(model_kdic_path)}`\n")
        f.write(f"- interpretation model: `{os.path.basename(interpretor_kdic_path)}`\n")
        f.write(f"- case index for notebook drill-down: `{os.path.basename(cases_path)}`\n\n")
        if not selected_df.empty:
            f.write("## Top selected variables\n\n")
            f.write("```text\n")
            f.write(selected_df.head(25).to_string(index=False))
            f.write("\n```\n")

    return {
        "selected_variables_csv": csv_path,
        "global_explainability_md": md_path,
        "analysis_results_json": json_path,
        "predictor_model_kdic": model_kdic_path,
        "interpretation_model_kdic": interpretor_kdic_path,
        "explainability_cases_csv": cases_path,
    }


def save_horizon_curves(
    y_true,
    y_prob,
    prefix: str,
    title: str,
    horizon_h: int,
    auc_roc: float,
    auc_pr: float,
    *,
    save_png: bool,
    display_plot: bool,
):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    axes[0].plot(fpr, tpr, lw=1.5, label=f"h={horizon_h}h (AUC={auc_roc:.3f})")
    axes[0].plot([0, 1], [0, 1], "k--", lw=0.8)
    axes[0].set_xlabel("False Positive Rate")
    axes[0].set_ylabel("True Positive Rate")
    axes[0].set_title(f"ROC - {title} horizon {horizon_h}h")
    axes[0].legend()

    prec, rec, _ = precision_recall_curve(y_true, y_prob)
    axes[1].plot(rec, prec, lw=1.5, label=f"h={horizon_h}h (AUPRC={auc_pr:.3f})")
    axes[1].set_xlabel("Recall")
    axes[1].set_ylabel("Precision")
    axes[1].set_title(f"PR Curve - {title} horizon {horizon_h}h")
    axes[1].legend()

    plt.tight_layout()
    plot_path = None
    if save_png:
        plot_path = _artifact_path(RUN_OUTPUT_DIR, f"{prefix}_h{horizon_h}_curves.png")
        plt.savefig(plot_path, dpi=150)
    if display_plot:
        display(fig)
    plt.close()
    return plot_path


def save_summary_plot(results, prefix: str, title: str, *, save_png: bool, display_plot: bool):
    horizons = [r["h"] for r in results]
    aucs = [r["auc_roc"] for r in results]
    auprcs = [r["auprc"] for r in results]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(horizons, aucs, "o-", lw=1.5, label="AUC-ROC")
    ax.plot(horizons, auprcs, "s--", lw=1.5, label="AUPRC")
    ax.set_xlabel("Prediction horizon (hours before onset)")
    ax.set_ylabel("Score")
    ax.set_title(title)
    ax.set_xticks(horizons)
    ax.set_xticklabels([f"{h}h" for h in horizons])
    ax.set_ylim(0.5, 1.0)
    ax.grid(alpha=0.3)
    ax.legend()
    ax.invert_xaxis()
    plt.tight_layout()
    plot_path = None
    if save_png:
        plot_path = _artifact_path(RUN_OUTPUT_DIR, f"{prefix}_auc_by_horizon.png")
        plt.savefig(plot_path, dpi=150)
    if display_plot:
        display(fig)
    plt.close()
    return plot_path


def append_experiment_summary(
    rows: list[dict],
    *,
    summary_table_name: str,
):
    summary_path = _artifact_path(SUMMARY_OUTPUT_DIR, summary_table_name)
    summary_df = pd.DataFrame(rows)
    if os.path.exists(summary_path):
        prior_df = pd.read_csv(summary_path)
        summary_df = pd.concat([prior_df, summary_df], ignore_index=True)
        summary_df = summary_df.drop_duplicates(
            subset=[
                "task_prefix",
                "cohort_before",
                "cohort_after",
                "horizon_h",
                "split_mode",
                "negative_alignment_mode",
                "include_static_in_root",
            ],
            keep="last",
        )
    summary_df.to_csv(summary_path, index=False)
    return summary_path


def run_split(root, hourly, y, *, n_splits: int):
    splitter = StratifiedKFold(
        n_splits=n_splits, shuffle=True, random_state=RANDOM_SEED
    )
    oof = pd.DataFrame({"y_true": y.reset_index(drop=True), "y_prob": 0.0})
    fold_rows = []

    for fold_idx, (train_idx, test_idx) in enumerate(splitter.split(root, y), start=1):
        print(f"    Fold {fold_idx}/{n_splits}...")
        root_tr = root.iloc[train_idx].reset_index(drop=True)
        root_te = root.iloc[test_idx].reset_index(drop=True)
        y_tr = y.iloc[train_idx].reset_index(drop=True)
        y_te = y.iloc[test_idx].reset_index(drop=True)

        ids_tr = set(root_tr["ICUSTAY_ID"])
        ids_te = set(root_te["ICUSTAY_ID"])
        hrly_tr = hourly[hourly["ICUSTAY_ID"].isin(ids_tr)].reset_index(drop=True)
        hrly_te = hourly[hourly["ICUSTAY_ID"].isin(ids_te)].reset_index(drop=True)

        x_tr = make_dataset(root_tr, hrly_tr)
        x_te = make_dataset(root_te, hrly_te)

        t0 = time.time()
        clf = KhiopsClassifier(n_features=N_FEATURES, n_trees=0, verbose=False)
        clf.fit(x_tr, y_tr)
        train_sec = time.time() - t0

        y_prob = clf.predict_proba(x_te)[:, 1]
        y_pred = (y_prob >= 0.5).astype(int)
        auc_roc = roc_auc_score(y_te, y_prob)
        auc_pr = average_precision_score(y_te, y_prob)

        oof.loc[test_idx, "y_prob"] = y_prob
        fold_rows.append(
            {
                "fold": fold_idx,
                "auc_roc": auc_roc,
                "auprc": auc_pr,
                "n_test": len(test_idx),
                "n_pos_test": int(y_te.sum()),
                "n_neg_test": int((y_te == 0).sum()),
                "train_sec": train_sec,
                "report": classification_report(
                    y_te, y_pred, target_names=["No Sepsis", "Sepsis"]
                ),
            }
        )

    return oof, fold_rows


def run_experiment(
    *,
    task_name: str,
    task_prefix: str,
    cohort_before: int,
    cohort_after: int,
    split_mode: str = "cv10",
    include_static_in_root: bool = INCLUDE_STATIC_IN_ROOT,
    export_explainability: bool = EXPORT_GLOBAL_EXPLAINABILITY,
    negative_alignment_mode: str = NEGATIVE_ALIGNMENT_MODE,
    save_relational_tables: bool = False,
    save_curve_png: bool = False,
    display_plots: bool = True,
    append_summary: bool = True,
    summary_table_name: str = "mimiciii_experiment_summary.csv",
):
    if split_mode not in {"cv10", "cv5_80_20"}:
        raise ValueError(f"Unsupported split_mode: {split_mode}")
    if negative_alignment_mode not in {"paper", "tail"}:
        raise ValueError(
            f"Unsupported negative_alignment_mode: {negative_alignment_mode}"
        )
    if split_mode == "cv10":
        n_splits = N_SPLITS
        split_desc = f"{N_SPLITS}-fold CV"
    else:
        n_splits = CV5_SPLITS
        split_desc = f"{CV5_SPLITS}-fold CV ({int((1 - 1 / CV5_SPLITS) * 100)}/{int(1 / CV5_SPLITS * 100)} per fold)"

    print(task_name)
    print(
        f"  cohort_before={cohort_before}h  cohort_after={cohort_after}h  "
        f"horizons={HORIZONS}  n_features={N_FEATURES}  split={split_desc}"
    )
    active_static_cols = STATIC_COLS if include_static_in_root else []
    print(
        f"  static_cols={len(active_static_cols)}  dynamic_cols={len(DYNAMIC_COLS)}  "
        f"include_static_in_root={include_static_in_root}  "
        f"negative_alignment_mode={negative_alignment_mode}"
    )
    print("=" * 84)
    print("\n[Pre-load] Loading data from parquet...")
    t0 = time.time()
    pos_static, pos_hourly, neg_static, neg_hourly = preload_data(
        cohort_before,
        cohort_after,
        include_static_in_root=include_static_in_root,
    )
    print(f"  Positive stays loaded: {len(pos_static):,}")
    print(f"  Negative stays loaded: {len(neg_static):,}")
    print(f"  Pre-load done in {time.time() - t0:.1f}s")

    results = []
    summary_rows = []
    for horizon_h in HORIZONS:
        print(f"\n{'=' * 84}")
        print(f"Horizon h = {horizon_h}h before onset")

        root, hourly, y = extract_horizon_windows(
            pos_static,
            pos_hourly,
            neg_static,
            neg_hourly,
            cohort_before,
            cohort_after,
            horizon_h,
            negative_alignment_mode=negative_alignment_mode,
        )
        n_pos = int(y.sum())
        n_neg = int((y == 0).sum())
        avg_rows = len(hourly) / max(len(root), 1)
        visible_rows_per_stay = int(cohort_before - horizon_h + 1)

        print(f"  stays: {len(root):,}  ({n_pos:,} sepsis / {n_neg:,} non-sepsis)")
        print(
            f"  hourly rows: {len(hourly):,}  "
            f"(avg {avg_rows:.1f}/stay, expected visible rows/stay={visible_rows_per_stay})"
        )
        debug_artifacts = {}
        if save_relational_tables:
            debug_artifacts = export_relational_tables(root, hourly, task_prefix, horizon_h)

        eval_df, fold_rows = run_split(root, hourly, y, n_splits=n_splits)
        auc_roc = roc_auc_score(eval_df["y_true"], eval_df["y_prob"])
        auc_pr = average_precision_score(eval_df["y_true"], eval_df["y_prob"])
        y_pred = (eval_df["y_prob"] >= 0.5).astype(int)
        report = classification_report(
            eval_df["y_true"], y_pred, target_names=["No Sepsis", "Sepsis"]
        )
        mean_auc = sum(row["auc_roc"] for row in fold_rows) / len(fold_rows)
        mean_auprc = sum(row["auprc"] for row in fold_rows) / len(fold_rows)
        mean_time = sum(row["train_sec"] for row in fold_rows) / len(fold_rows)

        print(f"  OOF AUC-ROC : {auc_roc:.4f}")
        print(f"  OOF AUPRC   : {auc_pr:.4f}")
        print(f"  Mean fold AUC-ROC : {mean_auc:.4f}")
        print(f"  Mean fold AUPRC   : {mean_auprc:.4f}")

        explainability_artifacts = {}
        if export_explainability:
            print("  Training full-data explainability model...")
            explain_clf = KhiopsClassifier(
                n_features=N_FEATURES,
                n_trees=0,
                verbose=False,
            )
            explain_clf.fit(make_dataset(root, hourly), y.reset_index(drop=True))
            explainability_artifacts = export_global_explainability(
                explain_clf,
                prefix=task_prefix,
                task_name=task_name,
                horizon_h=horizon_h,
                n_pos=n_pos,
                n_neg=n_neg,
                eval_df=eval_df,
                stay_ids=root["ICUSTAY_ID"],
            )

        curve_path = save_horizon_curves(
            eval_df["y_true"],
            eval_df["y_prob"],
            task_prefix,
            task_name,
            horizon_h,
            auc_roc,
            auc_pr,
            save_png=save_curve_png,
            display_plot=display_plots,
        )

        results.append(
            {
                "h": horizon_h,
                "split_mode": split_mode,
                "n_pos": n_pos,
                "n_neg": n_neg,
                "n_rows": len(hourly),
                "visible_rows_per_stay": visible_rows_per_stay,
                "auc_roc": auc_roc,
                "auprc": auc_pr,
                "mean_fold_auc_roc": mean_auc,
                "mean_fold_auprc": mean_auprc,
                "mean_train_sec": mean_time,
                "report": report,
                "fold_rows": fold_rows,
                "curve_path": curve_path,
                "debug_artifacts": debug_artifacts,
                "explainability_artifacts": explainability_artifacts,
            }
        )
        summary_rows.append(
            {
                "task_name": task_name,
                "task_prefix": task_prefix,
                "cohort_before": cohort_before,
                "cohort_after": cohort_after,
                "horizon_h": horizon_h,
                "split_mode": split_mode,
                "include_static_in_root": include_static_in_root,
                "negative_alignment_mode": negative_alignment_mode,
                "export_explainability": export_explainability,
                "n_pos": n_pos,
                "n_neg": n_neg,
                "visible_rows_per_stay": visible_rows_per_stay,
                "hourly_rows": len(hourly),
                "oof_auc_roc": auc_roc,
                "oof_auprc": auc_pr,
                "mean_fold_auc_roc": mean_auc,
                "mean_fold_auprc": mean_auprc,
                "mean_train_sec": mean_time,
            }
        )

    metrics_path = _artifact_path(RUN_OUTPUT_DIR, f"{task_prefix}_metrics.txt")
    with open(metrics_path, "w", encoding="utf-8") as f:
        f.write(task_name + "\n")
        f.write(
            f"cohort_before={cohort_before}h  cohort_after={cohort_after}h  "
            f"horizons={HORIZONS}  n_features={N_FEATURES}  split={split_desc}\n"
        )
        f.write(
            f"static_cols={len(active_static_cols)}  dynamic_cols={len(DYNAMIC_COLS)}  "
            f"include_static_in_root={include_static_in_root}  "
            f"negative_alignment_mode={negative_alignment_mode}\n"
        )
        f.write("=" * 84 + "\n\n")
        for result in results:
            f.write(
                f"h={result['h']}h | stays={result['n_pos'] + result['n_neg']} | "
                f"pos={result['n_pos']} | neg={result['n_neg']} | "
                f"visible_rows_per_stay={result['visible_rows_per_stay']} | "
                f"hourly_rows={result['n_rows']} | "
                f"OOF_AUC={result['auc_roc']:.4f} | "
                f"OOF_AUPRC={result['auprc']:.4f} | "
                f"mean_fold_AUC={result['mean_fold_auc_roc']:.4f} | "
                f"mean_fold_AUPRC={result['mean_fold_auprc']:.4f} | "
                f"mean_train_sec={result['mean_train_sec']:.1f}\n"
            )
            for fold_row in result["fold_rows"]:
                f.write(
                    f"  fold={fold_row['fold']} | n={fold_row['n_test']} | "
                    f"pos={fold_row['n_pos_test']} | neg={fold_row['n_neg_test']} | "
                    f"AUC={fold_row['auc_roc']:.4f} | "
                    f"AUPRC={fold_row['auprc']:.4f} | "
                    f"time={fold_row['train_sec']:.1f}s\n"
                )
            f.write(result["report"] + "\n")

    summary_plot_path = save_summary_plot(
        results,
        task_prefix,
        f"{task_name} - AUC/AUPRC by horizon",
        save_png=save_curve_png,
        display_plot=display_plots,
    )
    summary_csv_path = None
    if append_summary:
        summary_csv_path = append_experiment_summary(
            summary_rows,
            summary_table_name=summary_table_name,
        )
    print(f"\n  Metrics -> {metrics_path}")
    if summary_plot_path:
        print(f"  Summary plot -> {summary_plot_path}")
    if summary_csv_path:
        print(f"  Summary table -> {summary_csv_path}")
    return {
        "task_name": task_name,
        "task_prefix": task_prefix,
        "metrics_path": metrics_path,
        "summary_plot_path": summary_plot_path,
        "summary_csv_path": summary_csv_path,
        "results": results,
    }
