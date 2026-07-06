## MIMIC-III Relational Task Workspace

This folder is the active MIMIC-III experiment workspace for relational
Khiops-style sepsis prediction runs derived from the paper direction in:

`Temporal Sepsis Modeling: a Relational and Explainable-by-Design Framework`

It currently uses the rebuilt MIMIC-III hourly wide table:

- `build_mimic/mimiciii/output/mimic3_wide.parquet`

### Structure

- `paper_method/`
  - current main experiment scripts
- `output/`
  - metrics, plots, relational exports, and comparison summaries
- `docs/`
  - planning notes and historical audit material

### Current Main Scripts

- `paper_method/window_experiment_common.py`
  - shared relational runner for the current notebook/grid experiment pipeline
- `paper_method/variable_sets.py`
  - current leakage-reduced feature specification for the MIMIC-III wide table
- `paper_method/mimiciii_grid_runner.ipynb`
  - notebook runner for the 12 requested window/split configurations
- `paper_method/archive/`
  - archived single-task scripts kept for reference only

### Current Status

This workspace has been consolidated to a single no-leak mainline with a
notebook-first runner. Older task scripts were moved under `paper_method/archive/`.
