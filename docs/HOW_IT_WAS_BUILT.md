# How CalR Guard was built

## Core idea

I started from the browser prototype logic and split it into a real pipeline with six layers:

1. **file reading**
2. **vendor / column inference**
3. **validation**
4. **standardization + QC**
5. **suggestion / exclusion handling**
6. **bundle export + review report**

That separation matters because once the logic is modular, the same pipeline can drive both a CLI and a local app.

## Modules

### `io_utils.py`
Reads `.csv`, `.tsv`, `.txt`, `.xlsx`, `.xls`.

Key details:
- delimiter sniffing for text files
- header preservation
- numeric coercion helper
- timestamp coercion helper
- output writing that preserves tabular file type where reasonable

### `vendor.py`
Handles:
- vendor heuristics
- standard variable names
- regex-based column matching
- mapping overrides

### `pipeline.py`
This is the main engine.

It does the following for each file:
- read file
- infer vendor
- infer column mapping
- apply optional mapping override
- run validation checks
- build a normalized standardized table

Then across all files it:
- computes `hours_since_start`
- generates a subject metadata sheet
- optionally merges an existing metadata sheet
- flags suspicious feeding spikes
- derives suggested exclusion windows from QC spikes
- applies manual variable/time-window exclusions
- produces summary counts and mapping review tables

### `exporter.py`
Writes the output bundle:
- tidy export
- vendor-preserved exports
- metadata sheet CSV/XLSX
- QC flags
- suggested exclusions
- exclusion log
- validation reports
- mapping review CSV and JSON mapping
- review report HTML
- subject-level QC plot PNGs

### `cli.py`
Lets the tool run in a reproducible batch mode.

### `app.py`
Provides a local Gradio interface for interactive use.

## Why Python

I moved the real implementation into Python rather than leaving it as one static HTML file because Python is better for:
- batch execution
- file IO
- testability
- writing multiple output formats
- future integration with lab pipelines and notebooks

## Why Gradio for the local app

Because it is fast to stand up, easy to run locally, and good enough for a working beta.

I did not waste time pretending the first serious release needed a custom front-end. That is how people spend three weeks designing buttons instead of solving import failures.

## Why the QC rule uses robust statistics

The feed spike detector uses a robust threshold per subject:
- `median + multiplier * 1.4826 * MAD`
- fallback to `Q3 + multiplier * IQR`

Reason:
- a single extreme feeding event should not define the threshold
- simple robust rules are transparent and easy to inspect
- this is triage, not an attempt to classify every artifact perfectly

## Why suggested exclusions are separate from manual exclusions

Suggested exclusions are derived from QC flags and exported for review.
They are not automatically applied.

Reason:
- keeps the workflow auditable
- makes the recommendation useful without pretending it is always correct
- lets a lab decide whether to accept, reject, or edit the interval

## Why exclusions blank source columns in vendor-preserved exports

In the standardized table, excluded values become `NaN`.
In vendor-preserved exports, the corresponding original source cells are blanked.

Reason:
- the action is traceable
- the output stays close to the original file format
- it avoids inventing new columns that may confuse downstream tools

## Deliberate non-features

I intentionally did **not** add:
- new statistical models for energy balance
- auto-removal of QC outliers
- CalR replacement plotting
- complex database plumbing

Reason:
- those are higher-risk and less clearly justified as a first contribution
- the first version should solve the most practical bottlenecks first

## Hardest part

The hard part is not coding. It is refusing to build the wrong thing.

The temptation is to keep piling on analysis features. The better move is to build the layer that removes friction from the existing workflow and leaves a clean audit trail.
