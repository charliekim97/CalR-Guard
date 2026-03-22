# Contributing

CalR Guard is a companion tool for the messy front end of calorimetry analysis. Contributions are welcome, but they need to be reproducible and boring in the best possible way.

## Development setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .[dev]
```

On Windows:

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install -e .[dev]
```

## Run tests

```bash
pytest -q
```

## Run the local app

```bash
python run_app.py
```

## Run the CLI

```bash
python run_cli.py --input examples/demo_tse_like.csv --outdir output_bundle
```

## Contribution guidelines

- Keep changes scoped to a clear workflow problem.
- Do not silently delete data. Flag, annotate, or suggest instead.
- Prefer explicit audit trails over clever hidden behavior.
- Add or update tests when behavior changes.
- Avoid breaking the current bundle structure unless there is a strong reason.

## Good first contributions

- tighter vendor-specific column mapping
- better validation messages for malformed timestamps
- richer metadata schema checks
- review/approve/reject flows for suggested exclusions
- more realistic example files and tests
