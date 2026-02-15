from __future__ import annotations

import json
import math
from pathlib import Path

REPORT_PATH = Path("outputs/json/report.json")
REPORT_BY_STATE_PATH = Path("outputs/json/report_by_state.json")
OUT_PATH = Path("outputs/json/signal_score.json")
OUT_BY_STATE_PATH = Path("outputs/json/signal_score_by_state.json")


def pct(v: float | None) -> float:
    return float(v or 0.0)


def entropy_from_distribution(dist: dict) -> float:
    ps = [float(v) for v in dist.values() if v and float(v) > 0]
    if not ps:
        return 0.0
    h = -sum(p * math.log2(p) for p in ps)
    return float(h / math.log2(100))


def score_report(report: dict) -> dict:
    top_volume_summary = report.get("unit_price", {}).get("top_volume_cv_summary", {})
    cv_med = pct(top_volume_summary.get("median_cv"))
    cv_p90 = pct(top_volume_summary.get("p90_cv"))
    ratio_fail = (cv_med > 3.0) or (cv_p90 > 8.0)

    d = (
        report.get("digits", {}).get("unit_paid_cents_last1_dist")
        or report.get("digits", {}).get("cents_last1_dist")
        or {}
    )
    obs = [float(d.get(str(i), d.get(i, 0.0))) for i in range(10)]
    max_dev = max(abs(x - 0.1) for x in obs) if obs else 0.0
    chi_digit = sum(((x - 0.1) ** 2) / 0.1 for x in obs) if obs else 0.0
    digit_fail = (max_dev > 0.05) or (chi_digit > 0.25)

    corr = report.get("correlations", {})
    corr_scope = "global"
    corr_strat = corr.get("within_hcpcs_top200")
    if corr_strat:
        c_bc = pct(corr_strat.get("median_ben_claims"))
        c_bp = pct(corr_strat.get("median_ben_paid"))
        c_cp = pct(corr_strat.get("median_claims_paid"))
        corr_scope = "within_hcpcs_top200_median"
    else:
        c_bc = pct(corr.get("TOTAL_UNIQUE_BENEFICIARIES", {}).get("TOTAL_CLAIMS"))
        c_bp = pct(corr.get("TOTAL_UNIQUE_BENEFICIARIES", {}).get("TOTAL_PAID"))
        c_cp = pct(corr.get("TOTAL_CLAIMS", {}).get("TOTAL_PAID"))
    corr_fail = (c_cp < 0.6) or (c_bc < 0.4) or (c_bp < 0.2)

    temporal = report.get("temporal", {}).get("noise_features", {})
    acf1 = pct(temporal.get("acf1_total_paid"))
    smooth_ratio = pct(temporal.get("smooth_ratio"))
    temporal_fail = (acf1 > 0.97 and smooth_ratio < 0.03)

    entropy = report.get("digits", {}).get("normalized_entropy_last2")
    if entropy is None:
        entropy = entropy_from_distribution(
            report.get("digits", {}).get("unit_paid_cents_last2_dist")
            or report.get("digits", {}).get("cents_last2_dist")
            or {}
        )
    entropy = pct(entropy)
    entropy_fail = entropy < 0.92

    chi_ben = pct(report.get("benford", {}).get("chi_like"))
    benford_fail = chi_ben > 0.08

    signals = [
        {
            "name": "Reimbursement ratio clustering",
            "failed": ratio_fail,
            "metrics": {"basis": "top_volume_hcpcs", "median_cv": cv_med, "p90_cv": cv_p90},
        },
        {
            "name": "Last digit analysis",
            "failed": digit_fail,
            "metrics": {"basis": "unit_paid", "max_abs_dev": max_dev, "chi_like": chi_digit},
        },
        {
            "name": "Correlation structure",
            "failed": corr_fail,
            "metrics": {"scope": corr_scope, "ben_claims": c_bc, "ben_paid": c_bp, "claims_paid": c_cp},
        },
        {
            "name": "Temporal noise",
            "failed": temporal_fail,
            "metrics": {"acf1_total_paid": acf1, "smooth_ratio": smooth_ratio},
        },
        {
            "name": "Entropy",
            "failed": entropy_fail,
            "metrics": {"basis": "unit_paid", "normalized_entropy_last2": entropy},
        },
        {
            "name": "Benford (provider-year)",
            "failed": benford_fail,
            "metrics": {"chi_like": chi_ben},
        },
    ]

    family_failures = {
        "reimbursement_ratio_clustering": ratio_fail,
        "digit_structure_family": digit_fail or entropy_fail,
        "correlation_structure": corr_fail,
        "temporal_noise": temporal_fail,
        "benford_supporting": benford_fail,
    }
    fail_count = sum(1 for v in family_failures.values() if v)
    raw_fail_count = sum(1 for s in signals if s["failed"])
    verdict = "LIKELY_SYNTHETIC_OR_ALTERED" if fail_count >= 3 else "NOT_FLAGGED_BY_3PLUS_RULE"

    return {
        "rule": "If 3+ independent signal families fail -> dataset likely synthetic or altered",
        "fail_count": fail_count,
        "family_total": len(family_failures),
        "raw_fail_count": raw_fail_count,
        "family_failures": family_failures,
        "signals": signals,
        "verdict": verdict,
    }


def main() -> None:
    if REPORT_BY_STATE_PATH.exists():
        bundle = json.loads(REPORT_BY_STATE_PATH.read_text())
        reports = bundle.get("reports", {})
        scores = {state: score_report(rep) for state, rep in reports.items()}

        by_state_out = {
            "default_state": bundle.get("default_state", "ALL"),
            "available_states": bundle.get("available_states", ["ALL"]),
            "scores": scores,
        }
        OUT_BY_STATE_PATH.write_text(json.dumps(by_state_out, indent=2), encoding="utf-8")

        all_score = scores.get("ALL") or (score_report(reports["ALL"]) if "ALL" in reports else score_report({}))
        OUT_PATH.write_text(json.dumps(all_score, indent=2), encoding="utf-8")

        print(f"Wrote {OUT_PATH}")
        print(f"Wrote {OUT_BY_STATE_PATH}")
        return

    report = json.loads(REPORT_PATH.read_text())
    result = score_report(report)
    OUT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
