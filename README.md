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

If files are missing, the site falls back to built-in demo values.

## Run locally
From this folder:

```bash
python3 -m http.server 8080
```

Then open:
- `http://localhost:8080`

## Notes
- Benford is presented as supporting evidence only, not a standalone fraud proof.
- Preferred execution order remains: Data Health -> Unit Price -> Digits -> Temporal -> Relationships -> Benford.
- The U.S. map supports hover and selected-state highlighting. Territories and `UNK` remain selectable via dropdown.
