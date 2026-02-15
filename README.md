# Healthcare Forensics Website

Static website shell for publishing Medicaid data-generation forensics findings.

## Files
- `index.html`: report layout and sections
- `styles.css`: visual design and responsive behavior
- `app.js`: loads state bundles and re-renders all cards/charts/verdicts per selected state

## Data Contract
Primary runtime inputs:
- `outputs/json/report_by_state.json`
- `outputs/json/signal_score_by_state.json`
- `outputs/json/provider_peer_outliers_by_state.json`

Fallback inputs (legacy/single-state mode):
- `outputs/json/report.json`
- `outputs/json/signal_score.json`

Expected keys (from the pipeline discussed in chat):
- `data_health`
- `unit_price`
- `digits`
- `correlations`
- `temporal`

The by-state bundle is expected to include:
- `default_state`
- `available_states`
- `reports` / `scores` keyed by state code (`ALL`, `CA`, `NY`, etc.)
- `outliers` keyed by state code for provider peer-group ranking

If files are missing, the site falls back to built-in demo values.

## Run locally
From this folder:

```bash
# Heavy build (recompute report artifacts from parquet):
./.venv/bin/python -u src/report.py

# Fast rebuild (recompute signal verdicts from existing report JSON only):
./.venv/bin/python -u src/signal_score.py

# Serve frontend:
python3 -m http.server 8080
```

Then open:
- `http://localhost:8080`

## Notes
- Scientific scope: this tool detects dataset-level transformation/aggregation artifacts, not row-level fraud or legal wrongdoing.
- Last Digit + Entropy are treated as one structural family to avoid double-counting related discretization effects.
- Signal 6 uses reimbursement-grid heaping detection (5-cent/25-cent spacing) instead of Benford.
- State Lens includes a Peer Group Outliers tab that ranks provider NPIs by peer-relative z-score screening metrics.
- Thresholds are calibrated from a null-model baseline written to `outputs/json/null_model_baseline.json`.
- Preferred execution order remains: Data Health -> Unit Price -> Digits -> Temporal -> Relationships -> Heaping.
- The U.S. map supports hover and selected-state highlighting. Territories and `UNK` remain selectable via dropdown.
- Null-model calibration uses realistic bootstrap samples and artifacted synthetic contrast samples.
