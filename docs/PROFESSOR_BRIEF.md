# Professor brief: what this tool is and why it is worth looking at

## One-sentence summary

CalR Guard is a **companion preflight layer** for calorimetry analysis that reduces file import failures, standardizes metadata collection, adds variable/time-window exclusions, and produces an auditable review bundle before data enters CalR.

## Why this was the right first target

Rewriting CalR would have been the wrong move.

CalR already addresses the back half of the workflow well: visualization, QC, power analysis, reporting, and repository preparation. The more practical gap is earlier in the pipeline, where labs lose time to:

- manufacturer export quirks
- malformed timestamps or extra columns
- wrong TSE channels
- repeated manual metadata entry
- coarse whole-subject exclusion when only one variable or time block is corrupted
- messy handoff when a collaborator wants a quick reviewable summary rather than raw JSON/CSV files

A front-end validation and curation layer is lower risk, easier to adopt, and easier to test incrementally.

## Main design decisions

### 1. Companion architecture instead of core rewrite

This is a thin layer that sits *before* CalR rather than an attempt to replace its analysis engine.

Reason:
- faster to build
- easier to explain
- easier to integrate into an existing lab workflow
- avoids changing downstream statistical behavior prematurely

### 2. Vendor-preserved outputs plus standardized tidy output

The tool writes two flavors of output:
- a normalized `cleaned_tidy.csv` for transparent review and downstream analysis
- `vendor_preserved/*` files that keep original manufacturer columns while blanking only the explicitly excluded source values

Reason:
- tidy output is easier to inspect and audit
- vendor-preserved output stays closer to the original import surface
- traceability is preserved without pretending every export can be perfectly normalized into one universal format

### 3. Variable/time-window exclusion instead of subject-only exclusion

The tool supports exclusions such as:
- `Mouse_01`, `feed`, `0.4` to `0.6`
- `Mouse_07`, `all`, `18` to `20`

Reason:
- in real runs, failures are often local to one sensor or one interval
- forcing an entire subject out of all variables is often too destructive
- this gives a better middle ground between “keep everything” and “throw out the whole animal”

### 4. QC flagging plus suggested exclusions, not automatic deletion

Feeding spikes are **flagged** and converted into **candidate exclusion windows**, not silently removed.

Reason:
- it keeps the user in the loop
- it avoids hiding data treatment decisions
- it produces something immediately actionable for review without claiming too much certainty

### 5. Metadata scaffold generation with optional merge

The tool creates a subject sheet and can merge a pre-existing metadata sheet into it.

Reason:
- metadata entry is repeated too often by hand
- labs already keep partial spreadsheets
- merging them is more realistic than asking users to start over from a blank scaffold every time

### 6. Human-readable review report

The bundle now includes `review_report.html` and subject-level QC plots.

Reason:
- a collaborator can review one file instead of opening six separate outputs
- it makes the beta easier to evaluate on real data
- it raises the floor from “code demo” to “reviewable workflow artifact”

## Current status

Working beta.

Strong enough to discuss seriously. Still not something that should be marketed as finished infrastructure.

## Most important next step

Test it on real lab exports from at least:
- one TSE dataset
- one Sable / Promethion dataset
- one Columbus / Oxymax dataset

That is where the real value is. Synthetic demos are fine for a meeting. Real vendor edge cases are what decide whether this becomes useful or just another polished side project.
