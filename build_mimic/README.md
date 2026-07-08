# Build Outputs

This folder contains the build pipelines and output locations for the MIMIC-III and MIMIC-IV wide tables used in this repository.

## Shared Wide Tables

The generated wide-table files are available through the OneDrive link below:

- <https://devinci-my.sharepoint.com/personal/yijia_zeng_edu_devinci_fr/Documents/Mimic%20docs?csf=1&web=1&e=lDineh>

Access may require approval from the owner.

That shared folder contains:

- `mimic3_wide.parquet`: MIMIC-III wide table in parquet format
- `mimic4_wide.parquet`: MIMIC-IV wide table in parquet format
- `mimic3_wide.csv`: MIMIC-III wide table exported to CSV
- `mimic4_wide.csv`: MIMIC-IV wide table exported to CSV

## Expected Local Output Paths

If you place the downloaded files back into this repository, the default output locations are:

- `build_mimic/mimiciii/output/mimic3_wide.parquet`
- `build_mimic/mimiciv/output/mimic4_wide.parquet`
- `build_mimic/mimiciii/output/mimic3_wide.csv`
- `build_mimic/mimiciv/output/mimic4_wide.csv`

The smaller DuckDB build files remain in the corresponding `output/` folders inside the repository.
