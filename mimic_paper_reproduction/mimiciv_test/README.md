## MIMIC-IV Paper Reproduction Workspace

This folder mirrors the MIMIC-III experiment layout for MIMIC-IV.

It uses the project-wide hourly wide table:

- `build_mimic/mimiciv/output/mimic4_wide.parquet`

### Structure

- `baselines/`
  - reserved for simpler MIMIC-IV baseline experiments
- `paper_method/`
  - paper-oriented Khiops relational experiments
- `output/`
  - saved metrics, plots, and exported relational tables
- `docs/`
  - short notes and mismatch checklists

### Current Main Scripts

- `paper_method/window_experiment_common.py`
  - shared relational runner for the current notebook/grid experiment pipeline
- `paper_method/variable_sets.py`
  - current leakage-reduced feature specification for the MIMIC-IV wide table
- `paper_method/mimiciv_grid_runner.ipynb`
  - notebook runner for the 12 requested window/split configurations
- `paper_method/archive/`
  - archived single-task scripts kept for reference only
