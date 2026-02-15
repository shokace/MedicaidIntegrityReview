from __future__ import annotations

import json
import math
from pathlib import Path
import random
import statistics

REPORT_PATH = Path("outputs/json/report.json")
REPORT_BY_STATE_PATH = Path("outputs/json/report_by_state.json")
OUT_PATH = Path("outputs/json/signal_score.json")
OUT_BY_STATE_PATH = Path("outputs/json/signal_score_by_state.json")
NULL_BASELINE_PATH = Path("outputs/json/null_model_baseline.json")


def pct(v: float | None) -> float:
    return float(v or 0.0)


def entropy_from_distribution(dist: dict) -> float:
    ps = [float(v) for v in dist.values() if v and float(v) > 0]
    if not ps:
        return 0.0
    h = -sum(p * math.log2(p) for p in ps)
    return float(h / math.log2(100))


def heaping_from_distribution(dist: dict) -> dict[str, float]:
    if not dist:
        return {"share_on_5c_grid": 0.0, "share_on_25c_grid": 0.0, "max_cent_bucket_share": 0.0}
    norm = {int(k): float(v) for k, v in dist.items() if v is not None}
    if not norm:
        return {"share_on_5c_grid": 0.0, "share_on_25c_grid": 0.0, "max_cent_bucket_share": 0.0}
    return {
        "share_on_5c_grid": float(sum(v for k, v in norm.items() if k % 5 == 0)),
        "share_on_25c_grid": float(sum(v for k, v in norm.items() if k % 25 == 0)),
        "max_cent_bucket_share": float(max(norm.values())),
    }


def quantile(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    arr = sorted(float(x) for x in xs)
    if len(arr) == 1:
        return arr[0]
    idx = (len(arr) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return arr[lo]
    w = idx - lo
    return arr[lo] * (1.0 - w) + arr[hi] * w


def clamp(v: float, lo: float, hi: float) -> float:
    return min(hi, max(lo, float(v)))


def state_features(report: dict) -> dict[str, float]:
    top_volume_summary = report.get("unit_price", {}).get("top_volume_cv_summary", {})
    cv_med = pct(top_volume_summary.get("median_cv"))
    cv_p90 = pct(top_volume_summary.get("p90_cv"))

    d1 = (
        report.get("digits", {}).get("unit_paid_cents_last1_dist")
        or report.get("digits", {}).get("cents_last1_dist")
        or {}
    )
    obs = [float(d1.get(str(i), d1.get(i, 0.0))) for i in range(10)]
    max_dev = max(abs(x - 0.1) for x in obs) if obs else 0.0
    chi_digit = sum(((x - 0.1) ** 2) / 0.1 for x in obs) if obs else 0.0

    corr = report.get("correlations", {})
    corr_strat = corr.get("within_hcpcs_top200")
    if corr_strat:
        c_bc = pct(corr_strat.get("median_ben_claims"))
        c_bp = pct(corr_strat.get("median_ben_paid"))
        c_cp = pct(corr_strat.get("median_claims_paid"))
    else:
        c_bc = pct(corr.get("TOTAL_UNIQUE_BENEFICIARIES", {}).get("TOTAL_CLAIMS"))
        c_bp = pct(corr.get("TOTAL_UNIQUE_BENEFICIARIES", {}).get("TOTAL_PAID"))
        c_cp = pct(corr.get("TOTAL_CLAIMS", {}).get("TOTAL_PAID"))

    temporal = report.get("temporal", {}).get("noise_features", {})
    acf1 = pct(temporal.get("acf1_total_paid"))
    smooth_ratio = pct(temporal.get("smooth_ratio"))

    entropy = report.get("digits", {}).get("normalized_entropy_last2")
    if entropy is None:
        entropy = entropy_from_distribution(
            report.get("digits", {}).get("unit_paid_cents_last2_dist")
            or report.get("digits", {}).get("cents_last2_dist")
            or {}
        )
    entropy = pct(entropy)

    heaping = report.get("heaping", {}) or {}
    share_5c = heaping.get("share_on_5c_grid")
    share_25c = heaping.get("share_on_25c_grid")
    max_bucket = heaping.get("max_cent_bucket_share")
    if share_5c is None or share_25c is None or max_bucket is None:
        heaping_fallback = heaping_from_distribution(
            report.get("digits", {}).get("unit_paid_cents_last2_dist")
            or report.get("digits", {}).get("cents_last2_dist")
            or {}
        )
        share_5c = heaping_fallback["share_on_5c_grid"]
        share_25c = heaping_fallback["share_on_25c_grid"]
        max_bucket = heaping_fallback["max_cent_bucket_share"]

    return {
        "ratio_median_cv": cv_med,
        "ratio_p90_cv": cv_p90,
        "digit_max_dev": max_dev,
        "digit_chi": chi_digit,
        "corr_ben_claims": c_bc,
        "corr_ben_paid": c_bp,
        "corr_claims_paid": c_cp,
        "temporal_acf1": acf1,
        "temporal_smooth_ratio": smooth_ratio,
        "entropy": entropy,
        "heaping_share_5c": pct(share_5c),
        "heaping_share_25c": pct(share_25c),
        "heaping_max_bucket": pct(max_bucket),
    }


def robust_sigma(xs: list[float]) -> float:
    if len(xs) < 2:
        return max(abs(xs[0]) * 0.05, 1e-4) if xs else 1e-4
    med = statistics.median(xs)
    abs_dev = [abs(x - med) for x in xs]
    mad = statistics.median(abs_dev)
    sigma = mad * 1.4826
    if sigma <= 0:
        sigma = statistics.pstdev(xs)
    return max(float(sigma), 1e-4)


def synthesize_realistic(features: list[dict[str, float]], n: int, seed: int = 42) -> list[dict[str, float]]:
    rng = random.Random(seed)
    if not features:
        return []

    keys = list(features[0].keys())
    sigmas = {k: robust_sigma([f[k] for f in features]) for k in keys}

    out: list[dict[str, float]] = []
    for _ in range(n):
        base = dict(rng.choice(features))
        row: dict[str, float] = {}
        for k in keys:
            v = base[k] + rng.gauss(0.0, sigmas[k] * 0.25)
            row[k] = float(v)

        row["ratio_median_cv"] = max(0.0, row["ratio_median_cv"])
        row["ratio_p90_cv"] = max(0.0, row["ratio_p90_cv"])
        row["digit_max_dev"] = clamp(row["digit_max_dev"], 0.0, 1.0)
        row["digit_chi"] = max(0.0, row["digit_chi"])
        row["corr_ben_claims"] = clamp(row["corr_ben_claims"], -1.0, 1.0)
        row["corr_ben_paid"] = clamp(row["corr_ben_paid"], -1.0, 1.0)
        row["corr_claims_paid"] = clamp(row["corr_claims_paid"], -1.0, 1.0)
        row["temporal_acf1"] = clamp(row["temporal_acf1"], -1.0, 1.0)
        row["temporal_smooth_ratio"] = max(0.0, row["temporal_smooth_ratio"])
        row["entropy"] = clamp(row["entropy"], 0.0, 1.0)
        row["heaping_share_5c"] = clamp(row["heaping_share_5c"], 0.0, 1.0)
        row["heaping_share_25c"] = clamp(row["heaping_share_25c"], 0.0, 1.0)
        row["heaping_max_bucket"] = clamp(row["heaping_max_bucket"], 0.0, 1.0)
        out.append(row)
    return out


def synthesize_artifacted(realistic_samples: list[dict[str, float]], seed: int = 43) -> list[dict[str, float]]:
    rng = random.Random(seed)
    out: list[dict[str, float]] = []
    for base in realistic_samples:
        row = dict(base)
        row["ratio_median_cv"] = max(0.0, row["ratio_median_cv"] * rng.uniform(1.35, 2.4))
        row["ratio_p90_cv"] = max(0.0, row["ratio_p90_cv"] * rng.uniform(1.35, 2.8))

        row["digit_max_dev"] = clamp(row["digit_max_dev"] + rng.uniform(0.03, 0.12), 0.0, 1.0)
        row["digit_chi"] = max(0.0, row["digit_chi"] + rng.uniform(0.15, 0.9))

        row["corr_ben_claims"] = clamp(row["corr_ben_claims"] - rng.uniform(0.18, 0.55), -1.0, 1.0)
        row["corr_ben_paid"] = clamp(row["corr_ben_paid"] - rng.uniform(0.18, 0.55), -1.0, 1.0)
        row["corr_claims_paid"] = clamp(row["corr_claims_paid"] - rng.uniform(0.18, 0.55), -1.0, 1.0)

        row["temporal_acf1"] = clamp(max(row["temporal_acf1"], rng.uniform(0.96, 0.999)), -1.0, 1.0)
        row["temporal_smooth_ratio"] = max(0.0, row["temporal_smooth_ratio"] * rng.uniform(0.10, 0.55))

        row["entropy"] = clamp(row["entropy"] - rng.uniform(0.05, 0.22), 0.0, 1.0)

        row["heaping_share_5c"] = clamp(row["heaping_share_5c"] + rng.uniform(0.06, 0.28), 0.0, 1.0)
        row["heaping_share_25c"] = clamp(row["heaping_share_25c"] + rng.uniform(0.08, 0.30), 0.0, 1.0)
        row["heaping_max_bucket"] = clamp(row["heaping_max_bucket"] + rng.uniform(0.08, 0.30), 0.0, 1.0)
        out.append(row)
    return out


def derive_thresholds(realistic_samples: list[dict[str, float]]) -> dict[str, float]:
    def col(name: str) -> list[float]:
        return [r[name] for r in realistic_samples]

    return {
        "ratio_median_cv_hi": quantile(col("ratio_median_cv"), 0.975),
        "ratio_p90_cv_hi": quantile(col("ratio_p90_cv"), 0.975),
        "digit_max_dev_hi": quantile(col("digit_max_dev"), 0.975),
        "digit_chi_hi": quantile(col("digit_chi"), 0.975),
        "corr_ben_claims_lo": quantile(col("corr_ben_claims"), 0.025),
        "corr_ben_paid_lo": quantile(col("corr_ben_paid"), 0.025),
        "corr_claims_paid_lo": quantile(col("corr_claims_paid"), 0.025),
        "temporal_acf1_hi": quantile(col("temporal_acf1"), 0.975),
        "temporal_smooth_ratio_lo": quantile(col("temporal_smooth_ratio"), 0.025),
        "entropy_lo": quantile(col("entropy"), 0.025),
        "heaping_share_25c_hi": quantile(col("heaping_share_25c"), 0.975),
        "heaping_max_bucket_hi": quantile(col("heaping_max_bucket"), 0.975),
    }


def signal_failures(feat: dict[str, float], thr: dict[str, float]) -> dict[str, bool]:
    ratio_fail = (feat["ratio_median_cv"] > thr["ratio_median_cv_hi"]) or (feat["ratio_p90_cv"] > thr["ratio_p90_cv_hi"])
    digit_fail = (feat["digit_max_dev"] > thr["digit_max_dev_hi"]) or (feat["digit_chi"] > thr["digit_chi_hi"])
    corr_fail = (
        (feat["corr_claims_paid"] < thr["corr_claims_paid_lo"])
        or (feat["corr_ben_claims"] < thr["corr_ben_claims_lo"])
        or (feat["corr_ben_paid"] < thr["corr_ben_paid_lo"])
    )
    temporal_fail = (feat["temporal_acf1"] > thr["temporal_acf1_hi"]) and (
        feat["temporal_smooth_ratio"] < thr["temporal_smooth_ratio_lo"]
    )
    entropy_fail = feat["entropy"] < thr["entropy_lo"]
    heaping_fail = (feat["heaping_share_25c"] > thr["heaping_share_25c_hi"]) or (
        feat["heaping_max_bucket"] > thr["heaping_max_bucket_hi"]
    )
    return {
        "ratio_fail": ratio_fail,
        "digit_fail": digit_fail,
        "corr_fail": corr_fail,
        "temporal_fail": temporal_fail,
        "entropy_fail": entropy_fail,
        "heaping_fail": heaping_fail,
    }


def failure_counts(feat: dict[str, float], thr: dict[str, float]) -> tuple[int, int]:
    fs = signal_failures(feat, thr)
    family_failures = {
        "reimbursement_ratio_clustering": fs["ratio_fail"],
        "digit_structure_family": fs["digit_fail"] or fs["entropy_fail"],
        "correlation_structure": fs["corr_fail"],
        "temporal_noise": fs["temporal_fail"],
        "heaping_grid_spacing": fs["heaping_fail"],
    }
    return sum(1 for v in family_failures.values() if v), sum(1 for v in fs.values() if v)


def calibrate_null_baseline(reports: dict[str, dict]) -> dict:
    observed = [
        state_features(rep)
        for state, rep in reports.items()
        if state not in {"ALL", "UNK"} and isinstance(rep, dict)
    ]

    realistic = synthesize_realistic(observed, n=max(4000, len(observed) * 120), seed=42)
    synthetic = synthesize_artifacted(realistic, seed=43)
    thr = derive_thresholds(realistic)

    real_family_fails = []
    real_raw_fails = []
    for feat in realistic:
        ff, rf = failure_counts(feat, thr)
        real_family_fails.append(ff)
        real_raw_fails.append(rf)

    syn_family_fails = []
    syn_raw_fails = []
    for feat in synthetic:
        ff, rf = failure_counts(feat, thr)
        syn_family_fails.append(ff)
        syn_raw_fails.append(rf)

    def summary(xs: list[int]) -> dict:
        if not xs:
            return {"mean": 0.0, "p50": 0.0, "p90": 0.0}
        return {
            "mean": float(sum(xs) / len(xs)),
            "p50": float(quantile([float(x) for x in xs], 0.5)),
            "p90": float(quantile([float(x) for x in xs], 0.9)),
        }

    return {
        "method": "empirical_null_bootstrap_plus_artifacted_synthetic",
        "notes": "Thresholds are calibrated from a realistic null-model bootstrap of observed state metrics. Synthetic artifacted samples are used as a contrast model.",
        "n_observed_states": len(observed),
        "n_realistic_samples": len(realistic),
        "n_synthetic_samples": len(synthetic),
        "thresholds": thr,
        "benchmark": {
            "realistic": {
                "family_fail_count": summary(real_family_fails),
                "raw_fail_count": summary(real_raw_fails),
            },
            "synthetic": {
                "family_fail_count": summary(syn_family_fails),
                "raw_fail_count": summary(syn_raw_fails),
            },
        },
    }


def fallback_thresholds() -> dict[str, float]:
    return {
        "ratio_median_cv_hi": 3.0,
        "ratio_p90_cv_hi": 8.0,
        "digit_max_dev_hi": 0.05,
        "digit_chi_hi": 0.25,
        "corr_ben_claims_lo": 0.4,
        "corr_ben_paid_lo": 0.2,
        "corr_claims_paid_lo": 0.6,
        "temporal_acf1_hi": 0.97,
        "temporal_smooth_ratio_lo": 0.03,
        "entropy_lo": 0.92,
        "heaping_share_25c_hi": 0.35,
        "heaping_max_bucket_hi": 0.35,
    }


def score_report(report: dict, thresholds: dict[str, float] | None = None) -> dict:
    thr = thresholds or fallback_thresholds()
    feat = state_features(report)
    fs = signal_failures(feat, thr)

    signals = [
        {
            "name": "Reimbursement ratio clustering",
            "failed": fs["ratio_fail"],
            "metrics": {
                "basis": "top_volume_hcpcs",
                "median_cv": feat["ratio_median_cv"],
                "p90_cv": feat["ratio_p90_cv"],
                "threshold_median_cv_hi": thr["ratio_median_cv_hi"],
                "threshold_p90_cv_hi": thr["ratio_p90_cv_hi"],
            },
        },
        {
            "name": "Last digit analysis",
            "failed": fs["digit_fail"],
            "metrics": {
                "basis": "unit_paid",
                "max_abs_dev": feat["digit_max_dev"],
                "chi_like": feat["digit_chi"],
                "threshold_max_abs_dev_hi": thr["digit_max_dev_hi"],
                "threshold_chi_like_hi": thr["digit_chi_hi"],
            },
        },
        {
            "name": "Correlation structure",
            "failed": fs["corr_fail"],
            "metrics": {
                "scope": "within_hcpcs_top200_median_or_proxy",
                "ben_claims": feat["corr_ben_claims"],
                "ben_paid": feat["corr_ben_paid"],
                "claims_paid": feat["corr_claims_paid"],
                "threshold_ben_claims_lo": thr["corr_ben_claims_lo"],
                "threshold_ben_paid_lo": thr["corr_ben_paid_lo"],
                "threshold_claims_paid_lo": thr["corr_claims_paid_lo"],
            },
        },
        {
            "name": "Temporal noise",
            "failed": fs["temporal_fail"],
            "metrics": {
                "acf1_total_paid": feat["temporal_acf1"],
                "smooth_ratio": feat["temporal_smooth_ratio"],
                "threshold_acf1_hi": thr["temporal_acf1_hi"],
                "threshold_smooth_ratio_lo": thr["temporal_smooth_ratio_lo"],
            },
        },
        {
            "name": "Entropy",
            "failed": fs["entropy_fail"],
            "metrics": {
                "basis": "unit_paid",
                "normalized_entropy_last2": feat["entropy"],
                "threshold_entropy_lo": thr["entropy_lo"],
            },
        },
        {
            "name": "Heaping detection (grid spacing)",
            "failed": fs["heaping_fail"],
            "metrics": {
                "basis": "unit_paid_cents_last2",
                "share_on_5c_grid": feat["heaping_share_5c"],
                "share_on_25c_grid": feat["heaping_share_25c"],
                "max_cent_bucket_share": feat["heaping_max_bucket"],
                "threshold_share_on_25c_grid_hi": thr["heaping_share_25c_hi"],
                "threshold_max_cent_bucket_share_hi": thr["heaping_max_bucket_hi"],
            },
        },
    ]

    family_failures = {
        "reimbursement_ratio_clustering": fs["ratio_fail"],
        "digit_structure_family": fs["digit_fail"] or fs["entropy_fail"],
        "correlation_structure": fs["corr_fail"],
        "temporal_noise": fs["temporal_fail"],
        "heaping_grid_spacing": fs["heaping_fail"],
    }
    fail_count = sum(1 for v in family_failures.values() if v)
    raw_fail_count = sum(1 for s in signals if s["failed"])
    verdict = "LIKELY_SYNTHETIC_OR_ALTERED" if fail_count >= 3 else "NOT_FLAGGED_BY_3PLUS_RULE"

    return {
        "rule": "If 3+ independent signal families fail -> dataset likely synthetic or altered",
        "calibration": "null_model_baseline",
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

        baseline = calibrate_null_baseline(reports)
        NULL_BASELINE_PATH.write_text(json.dumps(baseline, indent=2), encoding="utf-8")
        thresholds = baseline.get("thresholds", fallback_thresholds())

        scores = {state: score_report(rep, thresholds) for state, rep in reports.items()}

        by_state_out = {
            "default_state": bundle.get("default_state", "ALL"),
            "available_states": bundle.get("available_states", ["ALL"]),
            "scores": scores,
            "calibration": {
                "source": str(NULL_BASELINE_PATH),
                "method": baseline.get("method"),
            },
        }
        OUT_BY_STATE_PATH.write_text(json.dumps(by_state_out, indent=2), encoding="utf-8")

        all_score = scores.get("ALL") or (score_report(reports["ALL"], thresholds) if "ALL" in reports else score_report({}, thresholds))
        OUT_PATH.write_text(json.dumps(all_score, indent=2), encoding="utf-8")

        print(f"Wrote {NULL_BASELINE_PATH}")
        print(f"Wrote {OUT_PATH}")
        print(f"Wrote {OUT_BY_STATE_PATH}")
        return

    thresholds = fallback_thresholds()
    report = json.loads(REPORT_PATH.read_text())
    result = score_report(report, thresholds)
    OUT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
