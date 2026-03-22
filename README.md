# CalR Guard

CalR Guard is a companion tool for indirect calorimetry workflows. It does **not** try to replace CalR. It handles the ugly front end of the process:

- preflight validation of manufacturer exports
- standardized metadata scaffolding and optional metadata-sheet merge
- suspicious feeding spike flagging
- suggested feed exclusion windows derived from QC flags
- variable-specific and time-window-specific manual exclusions
- export of an auditable bundle for review and downstream analysis
- human-readable review report and QC plots

That is the whole point. Labs do not lose time on a beautiful final figure. They lose it on bad headers, wrong TSE channels, inconsistent subject sheets, and one feeder developing delusions.

## Why this exists

CalR already covers plotting, QC, power analysis, reporting, and repository preparation. The practical pain points are earlier in the workflow:

1. import failures caused by format drift or malformed files
2. manual re-entry of project and subject metadata
3. coarse subject-level exclusions when only one variable or time slice is broken
4. weak traceability for what was excluded and why
5. poor handoff to another human who just wants one reviewable report instead of six CSVs

CalR Guard is designed to sit **before** CalR and reduce those failure points.

## What it does

### Input

Accepted input formats:
- `.csv`
- `.tsv`
- `.txt`
- `.xlsx`
- `.xls`

Optional metadata sheet merge:
- `.csv`
- `.tsv`
- `.txt`
- `.xlsx`
- `.xls`

### Vendor heuristics

It heuristically detects likely vendor families:
- TSE
- Sable / Promethion
- Columbus / Oxymax
- Unknown

### Column mapping

It infers likely columns for:
- subject
- timestamp
- VO2
- VCO2
- EE
- RER
- feed
- drink
- wheel
- activity

Optional JSON/YAML mapping overrides are supported.

### Validation checks

Current checks include:
- fragile filenames and subject IDs (`/`, `\`, `;`, `,`)
- duplicate headers
- blank / unnamed columns
- missing subject or timestamp column
- likely TSE normalized `VO2(1)` / `VCO2(1)` exports without `VO2(3)` / `VCO2(3)`
- timestamp parse failures
- irregular interval spacing
- non-numeric mapped values
- negative feed / drink / EE / VO2 / VCO2 values

### QC

It flags suspicious feeding spikes using a robust threshold per subject:
- `median + multiplier * 1.4826 * MAD`
- fallback to `Q3 + multiplier * IQR` when MAD is zero

It **flags** them. It does not silently delete them.

### Suggested exclusions

Feed spike QC flags are grouped into candidate exclusion windows per subject and file.

Examples:
- one isolated flagged point becomes a narrow suggested exclusion around that point
- adjacent flagged points are merged into one suggested interval

These are suggestions for review, not auto-applied deletions.

### Manual exclusions

It supports exclusions at the level of:
- subject
- variable
- time window (`start_hour` to `end_hour`)

Examples:
- remove `feed` for `Mouse_01` from hour `0.4` to `0.6`
- remove `all` variables for `Mouse_07` from hour `18` to `20`

### Metadata merge

The tool generates a subject metadata scaffold and can merge an existing subject sheet onto it.

This matters because most labs already have a half-broken spreadsheet somewhere, and pretending they do not is not a serious engineering plan.

### Output bundle

The export bundle contains:
- `cleaned_tidy.csv`
- `vendor_preserved/*`
- `metadata_sheet.csv`
- `metadata_sheet.xlsx`
- `qc_flags.csv`
- `suggested_exclusions.csv`
- `suggested_exclusions.json`
- `exclusion_log.csv`
- `validation_report.csv`
- `validation_report.json`
- `mapping_review.csv`
- `mapping.json`
- `project_metadata.json`
- `bundle_summary.json`
- `review_report.html`
- `qc_plots/*`

## Quick start

### 1. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows:

```bat
python -m venv .venv
.venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the local app

```bash
python run_app.py
```

Then open the local address Gradio prints, usually `http://127.0.0.1:7860`.

### 4. Or run the CLI

```bash
python run_cli.py \
  --input examples/demo_tse_like.csv \
  --subject-sheet examples/subject_sheet.example.csv \
  --outdir output_bundle
```

## CLI usage

```bash
python run_cli.py \
  --input path/to/file1.csv path/to/file2.xlsx \
  --outdir output_bundle \
  --config examples/config.example.yml
```

### Optional inputs

- `--mapping examples/mapping.example.json`
- `--project examples/project.example.yml`
- `--subject-sheet examples/subject_sheet.example.csv`
- `--exclusions examples/exclusions.example.json`
- `--qc-multiplier 6.0`
- `--no-zip`

## What changed versus the original browser prototype

The first HTML prototype was good enough to show the concept and not much else. This release fixes the obvious weaknesses:

- moved the logic into reusable Python modules instead of one giant browser file
- added a real CLI
- added a real local app
- added CSV / TSV / TXT / XLSX support
- added config files and mapping overrides
- preserved vendor-column exports per source file
- added auditable bundle export instead of one-off downloads
- added merged metadata sheet support
- added suggested exclusions from QC flags
- added a human-readable review report and QC plots
- added smoke tests
- stopped over-claiming direct CalR compatibility


## GitHub packaging notes

This repository is prepared for GitHub review, but a few things are intentionally left for the real repository owner to finalize:

- `docs/CITATION.cff.template` should be filled with real author metadata before a public release
- the recommended first push is to a **private** GitHub repository

See:
- `docs/PUBLISH_TO_GITHUB.md`
- `docs/PUBLIC_RELEASE_CHECKLIST.md`
- `docs/GITHUB_REPO_SETUP.md`

## Limitations

This is intentionally a **companion layer**, not a CalR fork.

Known limitations:
- mapping is heuristic and will still need manual override on messy exports
- `vendor_preserved` outputs preserve original columns, but direct CalR import is not guaranteed for every manufacturer export style
- suggested exclusions are triage, not automatic truth
- metadata merge assumes a `subject_id` column and does not yet enforce a strict lab-wide schema
- no cloud / multi-user deployment setup yet
- no direct round-trip from CalR session files yet

## What I would build next

1. CalR session file import / export
2. stricter manufacturer-specific parsers for Columbus, Sable, and TSE
3. interactive approval workflow for suggested exclusions
4. repository-schema validation for metadata sheets
