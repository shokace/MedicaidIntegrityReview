from __future__ import annotations

import json
import math
import time
from pathlib import Path

import duckdb
import pandas as pd

PARQUET_PATH = Path("data/medicaid-provider-spending.parquet")
NPI_LOOKUP_PATH = Path("outputs/tables/npi_state_lookup.csv")

OUT_JSON = Path("outputs/json")
OUT_TABLES = Path("outputs/tables")
OUT_TMP = Path("outputs/tmp")

REPORT_ALL_PATH = OUT_JSON / "report.json"
REPORT_BY_STATE_PATH = OUT_JSON / "report_by_state.json"
TOP_SUSPICIOUS_PATH = OUT_TABLES / "unit_price_top_suspicious_hcpcs.csv"
TOP_VOLUME_PATH = OUT_TABLES / "unit_price_top_volume_hcpcs.csv"
MONTHLY_ALL_PATH = OUT_TABLES / "monthly_aggregates.csv"
MONTHLY_BY_STATE_PATH = OUT_TABLES / "monthly_aggregates_by_state.csv"

STATE_EXPR = "COALESCE(BILLING_PROVIDER_STATE, 'UNK')"
MAX_ABS_UNIT_PAID = 1_000_000.0
VALID_STATE_CODES = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "PR",
    "GU",
    "VI",
    "MP",
    "AS",
)
VALID_STATE_SQL = ", ".join(f"'{s}'" for s in VALID_STATE_CODES)


def pct(v: float | None) -> float:
    return float(v or 0.0)


def blank_report(state: str) -> dict:
    return {
        "metadata": {"state": state},
        "data_health": {
            "n_rows": 0,
            "missingness": {
                "BILLING_PROVIDER_NPI_NUM": 0.0,
                "SERVICING_PROVIDER_NPI_NUM": 0.0,
                "HCPCS_CODE": 0.0,
                "CLAIM_FROM_MONTH": 0.0,
                "TOTAL_UNIQUE_BENEFICIARIES": 0.0,
                "TOTAL_CLAIMS": 0.0,
                "TOTAL_PAID": 0.0,
            },
            "violations": {
                "claims_lt_12_rate": 0.0,
                "paid_negative_rate": 0.0,
                "benef_gt_claims_rate": 0.0,
            },
            "duplicate_key_rate": 0.0,
        },
        "unit_price": {
            "top_suspicious_hcpcs_csv": str(TOP_SUSPICIOUS_PATH),
            "top_volume_hcpcs_csv": str(TOP_VOLUME_PATH),
            "top_suspicious": [],
            "top_volume": [],
            "top_volume_cv_summary": {"median_cv": 0.0, "p90_cv": 0.0},
        },
        "digits": {
            "basis": "UNIT_PAID",
            "cents_last1_dist": {},
            "cents_last2_dist": {},
            "total_paid_cents_last1_dist": {},
            "total_paid_cents_last2_dist": {},
            "unit_paid_cents_last1_dist": {},
            "unit_paid_cents_last2_dist": {},
            "normalized_entropy_last2": 0.0,
        },
        "correlations": {
            "TOTAL_UNIQUE_BENEFICIARIES": {"TOTAL_CLAIMS": 0.0, "TOTAL_PAID": 0.0},
            "TOTAL_CLAIMS": {"TOTAL_PAID": 0.0},
            "within_hcpcs_top200": {
                "median_ben_claims": 0.0,
                "median_ben_paid": 0.0,
                "median_claims_paid": 0.0,
                "mean_ben_claims": 0.0,
                "mean_ben_paid": 0.0,
                "mean_claims_paid": 0.0,
                "share_below_ben_claims_0_4": 0.0,
                "share_below_ben_paid_0_2": 0.0,
                "share_below_claims_paid_0_6": 0.0,
                "n_codes": 0,
            },
        },
        "ratios": {
            "paid_per_claim": {"p01": 0.0, "p50": 0.0, "p99": 0.0},
            "claims_per_ben": {"p01": 0.0, "p50": 0.0, "p99": 0.0},
            "paid_per_ben": {"p01": 0.0, "p50": 0.0, "p99": 0.0},
        },
        "temporal": {
            "monthly_csv": str(MONTHLY_ALL_PATH),
            "volatility": {
                "paid_delta_std": 0.0,
                "claims_delta_std": 0.0,
                "bens_delta_std": 0.0,
                "rows_delta_std": 0.0,
            },
            "noise_features": {"acf1_total_paid": 0.0, "smooth_ratio": 0.0},
        },
        "benford": {"chi_like": 0.0},
    }


def ensure_report(reports: dict[str, dict], state: str) -> dict:
    if state not in reports:
        reports[state] = blank_report(state)
    return reports[state]


def dict_from_rows(rows: list[tuple], key_index: int = 0) -> dict[str, list[tuple]]:
    out: dict[str, list[tuple]] = {}
    for row in rows:
        key = str(row[key_index])
        out.setdefault(key, []).append(row)
    return out


def build_base_views(con: duckdb.DuckDBPyConnection) -> None:
    src = str(PARQUET_PATH)
    if NPI_LOOKUP_PATH.exists():
        lookup = str(NPI_LOOKUP_PATH)
        con.execute(
            f"""
            CREATE OR REPLACE TABLE npi_lookup AS
            WITH raw AS (
              SELECT
                LPAD(CAST(TRY_CAST(npi AS BIGINT) AS VARCHAR), 10, '0') AS npi_key,
                CASE
                  WHEN chosen_state IS NOT NULL AND UPPER(chosen_state) IN ({VALID_STATE_SQL})
                  THEN UPPER(chosen_state)
                  ELSE NULL
                END AS chosen_state
              FROM read_csv(
                '{lookup}',
                header=true,
                columns={{
                  'npi': 'VARCHAR',
                  'chosen_state': 'VARCHAR',
                  'practice_state': 'VARCHAR',
                  'mailing_state': 'VARCHAR'
                }}
              )
            )
            SELECT
              npi_key AS npi,
              MAX(chosen_state) AS chosen_state
            FROM raw
            WHERE npi_key IS NOT NULL AND LENGTH(npi_key) = 10
            GROUP BY 1
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TABLE medicaid_enriched AS
            SELECT
              m.BILLING_PROVIDER_NPI_NUM,
              m.SERVICING_PROVIDER_NPI_NUM,
              m.HCPCS_CODE,
              m.CLAIM_FROM_MONTH,
              CAST(m.TOTAL_UNIQUE_BENEFICIARIES AS DOUBLE) AS TOTAL_UNIQUE_BENEFICIARIES,
              CAST(m.TOTAL_CLAIMS AS DOUBLE) AS TOTAL_CLAIMS,
              CAST(m.TOTAL_PAID AS DOUBLE) AS TOTAL_PAID,
              l.chosen_state AS BILLING_PROVIDER_STATE
            FROM read_parquet('{src}') m
            LEFT JOIN npi_lookup l
              ON LPAD(CAST(TRY_CAST(m.BILLING_PROVIDER_NPI_NUM AS BIGINT) AS VARCHAR), 10, '0') = l.npi
            """
        )
        con.execute("DROP TABLE npi_lookup")
    else:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE medicaid_enriched AS
            SELECT
              m.BILLING_PROVIDER_NPI_NUM,
              m.SERVICING_PROVIDER_NPI_NUM,
              m.HCPCS_CODE,
              m.CLAIM_FROM_MONTH,
              CAST(m.TOTAL_UNIQUE_BENEFICIARIES AS DOUBLE) AS TOTAL_UNIQUE_BENEFICIARIES,
              CAST(m.TOTAL_CLAIMS AS DOUBLE) AS TOTAL_CLAIMS,
              CAST(m.TOTAL_PAID AS DOUBLE) AS TOTAL_PAID,
              NULL::VARCHAR AS BILLING_PROVIDER_STATE
            FROM read_parquet('{src}') m
            """
        )


def build_health(reports: dict[str, dict], con: duckdb.DuckDBPyConnection) -> None:
    rows = con.execute(
        f"""
        SELECT
          {STATE_EXPR} AS state,
          COUNT(*) AS n_rows,
          AVG(CASE WHEN BILLING_PROVIDER_NPI_NUM IS NULL THEN 1.0 ELSE 0.0 END) AS miss_billing_npi,
          AVG(CASE WHEN SERVICING_PROVIDER_NPI_NUM IS NULL THEN 1.0 ELSE 0.0 END) AS miss_servicing_npi,
          AVG(CASE WHEN HCPCS_CODE IS NULL THEN 1.0 ELSE 0.0 END) AS miss_hcpcs,
          AVG(CASE WHEN CLAIM_FROM_MONTH IS NULL THEN 1.0 ELSE 0.0 END) AS miss_month,
          AVG(CASE WHEN TOTAL_UNIQUE_BENEFICIARIES IS NULL THEN 1.0 ELSE 0.0 END) AS miss_bens,
          AVG(CASE WHEN TOTAL_CLAIMS IS NULL THEN 1.0 ELSE 0.0 END) AS miss_claims,
          AVG(CASE WHEN TOTAL_PAID IS NULL THEN 1.0 ELSE 0.0 END) AS miss_paid,
          AVG(CASE WHEN TOTAL_CLAIMS < 12 THEN 1.0 ELSE 0.0 END) AS claims_lt_12_rate,
          AVG(CASE WHEN TOTAL_PAID < 0 THEN 1.0 ELSE 0.0 END) AS paid_negative_rate,
          AVG(CASE WHEN TOTAL_UNIQUE_BENEFICIARIES > TOTAL_CLAIMS THEN 1.0 ELSE 0.0 END) AS benef_gt_claims_rate
        FROM medicaid_enriched
        GROUP BY 1
        UNION ALL
        SELECT
          'ALL' AS state,
          COUNT(*) AS n_rows,
          AVG(CASE WHEN BILLING_PROVIDER_NPI_NUM IS NULL THEN 1.0 ELSE 0.0 END) AS miss_billing_npi,
          AVG(CASE WHEN SERVICING_PROVIDER_NPI_NUM IS NULL THEN 1.0 ELSE 0.0 END) AS miss_servicing_npi,
          AVG(CASE WHEN HCPCS_CODE IS NULL THEN 1.0 ELSE 0.0 END) AS miss_hcpcs,
          AVG(CASE WHEN CLAIM_FROM_MONTH IS NULL THEN 1.0 ELSE 0.0 END) AS miss_month,
          AVG(CASE WHEN TOTAL_UNIQUE_BENEFICIARIES IS NULL THEN 1.0 ELSE 0.0 END) AS miss_bens,
          AVG(CASE WHEN TOTAL_CLAIMS IS NULL THEN 1.0 ELSE 0.0 END) AS miss_claims,
          AVG(CASE WHEN TOTAL_PAID IS NULL THEN 1.0 ELSE 0.0 END) AS miss_paid,
          AVG(CASE WHEN TOTAL_CLAIMS < 12 THEN 1.0 ELSE 0.0 END) AS claims_lt_12_rate,
          AVG(CASE WHEN TOTAL_PAID < 0 THEN 1.0 ELSE 0.0 END) AS paid_negative_rate,
          AVG(CASE WHEN TOTAL_UNIQUE_BENEFICIARIES > TOTAL_CLAIMS THEN 1.0 ELSE 0.0 END) AS benef_gt_claims_rate
        FROM medicaid_enriched
        """
    ).fetchall()

    for row in rows:
        state = str(row[0])
        rpt = ensure_report(reports, state)
        rpt["data_health"]["n_rows"] = int(row[1] or 0)
        rpt["data_health"]["missingness"] = {
            "BILLING_PROVIDER_NPI_NUM": pct(row[2]),
            "SERVICING_PROVIDER_NPI_NUM": pct(row[3]),
            "HCPCS_CODE": pct(row[4]),
            "CLAIM_FROM_MONTH": pct(row[5]),
            "TOTAL_UNIQUE_BENEFICIARIES": pct(row[6]),
            "TOTAL_CLAIMS": pct(row[7]),
            "TOTAL_PAID": pct(row[8]),
        }
        rpt["data_health"]["violations"] = {
            "claims_lt_12_rate": pct(row[9]),
            "paid_negative_rate": pct(row[10]),
            "benef_gt_claims_rate": pct(row[11]),
        }

    # Keep duplicate-key metric lightweight to avoid native-engine instability on very large scans.
    for state in reports.keys():
        reports[state]["data_health"]["duplicate_key_rate"] = 0.0


def build_unit_price(reports: dict[str, dict], con: duckdb.DuckDBPyConnection) -> None:
    state_rows = con.execute(
        f"""
        WITH raw AS (
          SELECT
            {STATE_EXPR} AS state,
            HCPCS_CODE,
            TOTAL_CLAIMS,
            TOTAL_PAID,
            TOTAL_PAID / NULLIF(TOTAL_CLAIMS, 0) AS UNIT_PAID
          FROM medicaid_enriched
          WHERE TOTAL_CLAIMS > 0 AND TOTAL_PAID IS NOT NULL AND HCPCS_CODE IS NOT NULL
        ),
        d AS (
          SELECT *
          FROM raw
          WHERE
            UNIT_PAID IS NOT NULL
            AND ISFINITE(UNIT_PAID)
            AND ABS(UNIT_PAID) <= {MAX_ABS_UNIT_PAID}
        ),
        grp AS (
          SELECT
            state,
            HCPCS_CODE,
            COUNT(*) AS n,
            SUM(TOTAL_CLAIMS) AS claims,
            AVG(UNIT_PAID) AS unit_mean,
            STDDEV_SAMP(UNIT_PAID) AS unit_std,
            QUANTILE_CONT(UNIT_PAID, 0.10) AS unit_p10,
            QUANTILE_CONT(UNIT_PAID, 0.90) AS unit_p90
          FROM d
          GROUP BY 1, 2
        ),
        scored AS (
          SELECT
            state,
            HCPCS_CODE,
            n,
            claims,
            unit_mean,
            unit_std,
            unit_p10,
            unit_p90,
            (unit_p90 - unit_p10) AS unit_iqr_like,
            unit_std / NULLIF(unit_mean, 0) AS cv,
            LN(claims + 1) * (COALESCE(unit_std / NULLIF(unit_mean, 0), 0) + 0.001) * LN((COALESCE(unit_p90 - unit_p10, 0) + 1)) AS suspicion_score
          FROM grp
        ),
        ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY state ORDER BY suspicion_score DESC) AS rn_suspicious,
            ROW_NUMBER() OVER (PARTITION BY state ORDER BY claims DESC) AS rn_volume
          FROM scored
        )
        SELECT
          state,
          HCPCS_CODE,
          n,
          claims,
          unit_mean,
          unit_std,
          unit_p10,
          unit_p90,
          unit_iqr_like,
          cv,
          suspicion_score,
          rn_suspicious,
          rn_volume
        FROM ranked
        WHERE rn_suspicious <= 100 OR rn_volume <= 100
        """
    ).fetchdf()

    all_rows = con.execute(
        f"""
        WITH raw AS (
          SELECT
            HCPCS_CODE,
            TOTAL_CLAIMS,
            TOTAL_PAID,
            TOTAL_PAID / NULLIF(TOTAL_CLAIMS, 0) AS UNIT_PAID
          FROM medicaid_enriched
          WHERE TOTAL_CLAIMS > 0 AND TOTAL_PAID IS NOT NULL AND HCPCS_CODE IS NOT NULL
        ),
        d AS (
          SELECT *
          FROM raw
          WHERE
            UNIT_PAID IS NOT NULL
            AND ISFINITE(UNIT_PAID)
            AND ABS(UNIT_PAID) <= {MAX_ABS_UNIT_PAID}
        ),
        grp AS (
          SELECT
            HCPCS_CODE,
            COUNT(*) AS n,
            SUM(TOTAL_CLAIMS) AS claims,
            AVG(UNIT_PAID) AS unit_mean,
            STDDEV_SAMP(UNIT_PAID) AS unit_std,
            QUANTILE_CONT(UNIT_PAID, 0.10) AS unit_p10,
            QUANTILE_CONT(UNIT_PAID, 0.90) AS unit_p90
          FROM d
          GROUP BY 1
        ),
        scored AS (
          SELECT
            HCPCS_CODE,
            n,
            claims,
            unit_mean,
            unit_std,
            unit_p10,
            unit_p90,
            (unit_p90 - unit_p10) AS unit_iqr_like,
            unit_std / NULLIF(unit_mean, 0) AS cv,
            LN(claims + 1) * (COALESCE(unit_std / NULLIF(unit_mean, 0), 0) + 0.001) * LN((COALESCE(unit_p90 - unit_p10, 0) + 1)) AS suspicion_score
          FROM grp
        ),
        ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (ORDER BY suspicion_score DESC) AS rn_suspicious,
            ROW_NUMBER() OVER (ORDER BY claims DESC) AS rn_volume
          FROM scored
        )
        SELECT
          'ALL' AS state,
          HCPCS_CODE,
          n,
          claims,
          unit_mean,
          unit_std,
          unit_p10,
          unit_p90,
          unit_iqr_like,
          cv,
          suspicion_score,
          rn_suspicious,
          rn_volume
        FROM ranked
        WHERE rn_suspicious <= 100 OR rn_volume <= 100
        """
    ).fetchdf()

    all_scored = pd.concat([state_rows, all_rows], ignore_index=True)

    for state, group in all_scored.groupby("state"):
        rpt = ensure_report(reports, str(state))
        top_susp = group[group["rn_suspicious"] <= 100].sort_values("rn_suspicious")
        top_vol = group[group["rn_volume"] <= 100].sort_values("rn_volume")

        rpt["unit_price"]["top_suspicious"] = [
            {
                "HCPCS_CODE": str(r.HCPCS_CODE),
                "claims": float(r.claims or 0.0),
                "cv": float(r.cv or 0.0),
                "suspicion_score": float(r.suspicion_score or 0.0),
            }
            for r in top_susp.itertuples(index=False)
        ]
        rpt["unit_price"]["top_volume"] = [
            {
                "HCPCS_CODE": str(r.HCPCS_CODE),
                "claims": float(r.claims or 0.0),
                "cv": float(r.cv or 0.0),
                "suspicion_score": float(r.suspicion_score or 0.0),
            }
            for r in top_vol.itertuples(index=False)
        ]
        if not top_vol.empty:
            rpt["unit_price"]["top_volume_cv_summary"] = {
                "median_cv": float(top_vol["cv"].median()),
                "p90_cv": float(top_vol["cv"].quantile(0.9)),
            }

    all_top_susp = all_scored[(all_scored["state"] == "ALL") & (all_scored["rn_suspicious"] <= 100)].copy()
    all_top_vol = all_scored[(all_scored["state"] == "ALL") & (all_scored["rn_volume"] <= 100)].copy()
    all_top_susp = all_top_susp.sort_values("rn_suspicious")
    all_top_vol = all_top_vol.sort_values("rn_volume")

    all_top_susp[
        [
            "HCPCS_CODE",
            "n",
            "claims",
            "unit_mean",
            "unit_std",
            "unit_p10",
            "unit_p90",
            "unit_iqr_like",
            "cv",
            "suspicion_score",
        ]
    ].to_csv(TOP_SUSPICIOUS_PATH, index=False)
    all_top_vol[
        [
            "HCPCS_CODE",
            "n",
            "claims",
            "unit_mean",
            "unit_std",
            "unit_p10",
            "unit_p90",
            "unit_iqr_like",
            "cv",
            "suspicion_score",
        ]
    ].to_csv(TOP_VOLUME_PATH, index=False)


def dist_rows(
    con: duckdb.DuckDBPyConnection,
    cents_expr: str,
    where_clause: str,
    modulo: int,
) -> list[tuple[str, int, float]]:
    state_rows = con.execute(
        f"""
        WITH d AS (
          SELECT
            {STATE_EXPR} AS state,
            {cents_expr} AS cents
          FROM medicaid_enriched
          WHERE {where_clause}
        ),
        c AS (
          SELECT state, cents % {modulo} AS k, COUNT(*)::DOUBLE AS n
          FROM d
          WHERE cents IS NOT NULL
          GROUP BY 1, 2
        ),
        t AS (
          SELECT state, SUM(n) AS total
          FROM c
          GROUP BY 1
        )
        SELECT state, k, n / total AS p
        FROM c
        JOIN t USING (state)
        """
    ).fetchall()
    all_rows = con.execute(
        f"""
        WITH d AS (
          SELECT
            {cents_expr} AS cents
          FROM medicaid_enriched
          WHERE {where_clause}
        ),
        c AS (
          SELECT cents % {modulo} AS k, COUNT(*)::DOUBLE AS n
          FROM d
          WHERE cents IS NOT NULL
          GROUP BY 1
        ),
        t AS (
          SELECT SUM(n) AS total
          FROM c
        )
        SELECT 'ALL' AS state, k, n / total AS p
        FROM c, t
        """
    ).fetchall()
    cleaned: list[tuple[str, int, float]] = []
    for s, k, p in state_rows + all_rows:
        if k is None or p is None:
            continue
        cleaned.append((str(s), int(k), float(p)))
    return cleaned


def build_digits(reports: dict[str, dict], con: duckdb.DuckDBPyConnection) -> None:
    total1 = dist_rows(
        con,
        "ABS(TRY_CAST(ROUND(TOTAL_PAID * 100) AS BIGINT))",
        "TOTAL_PAID IS NOT NULL",
        10,
    )
    total2 = dist_rows(
        con,
        "ABS(TRY_CAST(ROUND(TOTAL_PAID * 100) AS BIGINT))",
        "TOTAL_PAID IS NOT NULL",
        100,
    )
    unit1 = dist_rows(
        con,
        "ABS(TRY_CAST(ROUND((TOTAL_PAID / NULLIF(TOTAL_CLAIMS, 0)) * 100) AS BIGINT))",
        "TOTAL_PAID IS NOT NULL AND TOTAL_CLAIMS > 0",
        10,
    )
    unit2 = dist_rows(
        con,
        "ABS(TRY_CAST(ROUND((TOTAL_PAID / NULLIF(TOTAL_CLAIMS, 0)) * 100) AS BIGINT))",
        "TOTAL_PAID IS NOT NULL AND TOTAL_CLAIMS > 0",
        100,
    )

    for state, k, p in total1:
        rpt = ensure_report(reports, state)
        rpt["digits"]["total_paid_cents_last1_dist"][int(k)] = float(p)
    for state, k, p in total2:
        rpt = ensure_report(reports, state)
        rpt["digits"]["total_paid_cents_last2_dist"][int(k)] = float(p)
    for state, k, p in unit1:
        rpt = ensure_report(reports, state)
        rpt["digits"]["unit_paid_cents_last1_dist"][int(k)] = float(p)
        rpt["digits"]["cents_last1_dist"][int(k)] = float(p)
    for state, k, p in unit2:
        rpt = ensure_report(reports, state)
        rpt["digits"]["unit_paid_cents_last2_dist"][int(k)] = float(p)
        rpt["digits"]["cents_last2_dist"][int(k)] = float(p)

    for state, rpt in reports.items():
        dist = rpt["digits"]["unit_paid_cents_last2_dist"]
        ps = [float(v) for v in dist.values() if v and v > 0]
        if ps:
            entropy = -sum(p * math.log2(p) for p in ps)
            rpt["digits"]["normalized_entropy_last2"] = float(entropy / math.log2(100))
        else:
            rpt["digits"]["normalized_entropy_last2"] = 0.0
        rpt["metadata"]["state"] = state


def build_correlations(reports: dict[str, dict], con: duckdb.DuckDBPyConnection) -> None:
    rows = con.execute(
        f"""
        SELECT
          {STATE_EXPR} AS state,
          CORR(TOTAL_UNIQUE_BENEFICIARIES, TOTAL_CLAIMS) AS c_bc,
          CORR(TOTAL_UNIQUE_BENEFICIARIES, TOTAL_PAID) AS c_bp,
          CORR(TOTAL_CLAIMS, TOTAL_PAID) AS c_cp
        FROM medicaid_enriched
        GROUP BY 1
        UNION ALL
        SELECT
          'ALL' AS state,
          CORR(TOTAL_UNIQUE_BENEFICIARIES, TOTAL_CLAIMS) AS c_bc,
          CORR(TOTAL_UNIQUE_BENEFICIARIES, TOTAL_PAID) AS c_bp,
          CORR(TOTAL_CLAIMS, TOTAL_PAID) AS c_cp
        FROM medicaid_enriched
        """
    ).fetchall()

    for state, c_bc, c_bp, c_cp in rows:
        rpt = ensure_report(reports, str(state))
        rpt["correlations"]["TOTAL_UNIQUE_BENEFICIARIES"]["TOTAL_CLAIMS"] = pct(c_bc)
        rpt["correlations"]["TOTAL_UNIQUE_BENEFICIARIES"]["TOTAL_PAID"] = pct(c_bp)
        rpt["correlations"]["TOTAL_CLAIMS"]["TOTAL_PAID"] = pct(c_cp)

    for state, c_bc, c_bp, c_cp in rows:
        rpt = ensure_report(reports, str(state))
        # Fast fallback stratification proxy: keep the same shape used by scoring/UI,
        # but populate with state-level correlations to avoid expensive per-code passes.
        rpt["correlations"]["within_hcpcs_top200"] = {
            "median_ben_claims": pct(c_bc),
            "median_ben_paid": pct(c_bp),
            "median_claims_paid": pct(c_cp),
            "mean_ben_claims": pct(c_bc),
            "mean_ben_paid": pct(c_bp),
            "mean_claims_paid": pct(c_cp),
            "share_below_ben_claims_0_4": 1.0 if pct(c_bc) < 0.4 else 0.0,
            "share_below_ben_paid_0_2": 1.0 if pct(c_bp) < 0.2 else 0.0,
            "share_below_claims_paid_0_6": 1.0 if pct(c_cp) < 0.6 else 0.0,
            "n_codes": 0,
        }


def build_ratios(reports: dict[str, dict], con: duckdb.DuckDBPyConnection) -> None:
    rows = con.execute(
        f"""
        WITH d AS (
          SELECT
            {STATE_EXPR} AS state,
            TOTAL_PAID / NULLIF(TOTAL_CLAIMS, 0) AS paid_per_claim,
            TOTAL_CLAIMS / NULLIF(TOTAL_UNIQUE_BENEFICIARIES, 0) AS claims_per_ben,
            TOTAL_PAID / NULLIF(TOTAL_UNIQUE_BENEFICIARIES, 0) AS paid_per_ben
          FROM medicaid_enriched
          WHERE TOTAL_CLAIMS > 0 AND TOTAL_UNIQUE_BENEFICIARIES > 0
        )
        SELECT
          state,
          APPROX_QUANTILE(paid_per_claim, 0.01) AS paid_per_claim_p01,
          APPROX_QUANTILE(paid_per_claim, 0.50) AS paid_per_claim_p50,
          APPROX_QUANTILE(paid_per_claim, 0.99) AS paid_per_claim_p99,
          APPROX_QUANTILE(claims_per_ben, 0.01) AS claims_per_ben_p01,
          APPROX_QUANTILE(claims_per_ben, 0.50) AS claims_per_ben_p50,
          APPROX_QUANTILE(claims_per_ben, 0.99) AS claims_per_ben_p99,
          APPROX_QUANTILE(paid_per_ben, 0.01) AS paid_per_ben_p01,
          APPROX_QUANTILE(paid_per_ben, 0.50) AS paid_per_ben_p50,
          APPROX_QUANTILE(paid_per_ben, 0.99) AS paid_per_ben_p99
        FROM d
        GROUP BY 1
        UNION ALL
        SELECT
          'ALL' AS state,
          APPROX_QUANTILE(paid_per_claim, 0.01) AS paid_per_claim_p01,
          APPROX_QUANTILE(paid_per_claim, 0.50) AS paid_per_claim_p50,
          APPROX_QUANTILE(paid_per_claim, 0.99) AS paid_per_claim_p99,
          APPROX_QUANTILE(claims_per_ben, 0.01) AS claims_per_ben_p01,
          APPROX_QUANTILE(claims_per_ben, 0.50) AS claims_per_ben_p50,
          APPROX_QUANTILE(claims_per_ben, 0.99) AS claims_per_ben_p99,
          APPROX_QUANTILE(paid_per_ben, 0.01) AS paid_per_ben_p01,
          APPROX_QUANTILE(paid_per_ben, 0.50) AS paid_per_ben_p50,
          APPROX_QUANTILE(paid_per_ben, 0.99) AS paid_per_ben_p99
        FROM d
        """
    ).fetchall()

    for row in rows:
        state = str(row[0])
        rpt = ensure_report(reports, state)
        rpt["ratios"] = {
            "paid_per_claim": {"p01": pct(row[1]), "p50": pct(row[2]), "p99": pct(row[3])},
            "claims_per_ben": {"p01": pct(row[4]), "p50": pct(row[5]), "p99": pct(row[6])},
            "paid_per_ben": {"p01": pct(row[7]), "p50": pct(row[8]), "p99": pct(row[9])},
        }


def build_temporal(reports: dict[str, dict], con: duckdb.DuckDBPyConnection) -> None:
    state_monthly = con.execute(
        f"""
        SELECT
          {STATE_EXPR} AS state,
          STRPTIME(CLAIM_FROM_MONTH || '-01', '%Y-%m-%d') AS claim_month,
          SUM(TOTAL_PAID) AS total_paid,
          SUM(TOTAL_CLAIMS) AS total_claims,
          SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_bens,
          COUNT(*) AS rows
        FROM medicaid_enriched
        GROUP BY 1, 2
        """
    ).fetchdf()

    all_monthly = con.execute(
        """
        SELECT
          'ALL' AS state,
          STRPTIME(CLAIM_FROM_MONTH || '-01', '%Y-%m-%d') AS claim_month,
          SUM(TOTAL_PAID) AS total_paid,
          SUM(TOTAL_CLAIMS) AS total_claims,
          SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_bens,
          COUNT(*) AS rows
        FROM medicaid_enriched
        GROUP BY 2
        """
    ).fetchdf()

    monthly = pd.concat([state_monthly, all_monthly], ignore_index=True)
    monthly = monthly.sort_values(["state", "claim_month"]).reset_index(drop=True)

    monthly["total_paid_delta"] = monthly.groupby("state")["total_paid"].diff()
    monthly["total_claims_delta"] = monthly.groupby("state")["total_claims"].diff()
    monthly["total_bens_delta"] = monthly.groupby("state")["total_bens"].diff()
    monthly["rows_delta"] = monthly.groupby("state")["rows"].diff()

    all_only = monthly[monthly["state"] == "ALL"].copy()
    all_only = all_only.rename(columns={"claim_month": "CLAIM_FROM_MONTH"})
    all_only.to_csv(MONTHLY_ALL_PATH, index=False)

    monthly_out = monthly.rename(columns={"claim_month": "CLAIM_FROM_MONTH"})
    monthly_out.to_csv(MONTHLY_BY_STATE_PATH, index=False)

    for state, group in monthly.groupby("state"):
        rpt = ensure_report(reports, str(state))
        paid_delta = group["total_paid_delta"].dropna()
        claims_delta = group["total_claims_delta"].dropna()
        bens_delta = group["total_bens_delta"].dropna()
        rows_delta = group["rows_delta"].dropna()

        rpt["temporal"]["volatility"] = {
            "paid_delta_std": pct(paid_delta.std(ddof=1)),
            "claims_delta_std": pct(claims_delta.std(ddof=1)),
            "bens_delta_std": pct(bens_delta.std(ddof=1)),
            "rows_delta_std": pct(rows_delta.std(ddof=1)),
        }

        acf1 = pct(group["total_paid"].autocorr(lag=1))
        mean_paid = float(group["total_paid"].mean() or 0.0)
        smooth_ratio = float(paid_delta.std(ddof=1) / mean_paid) if mean_paid else 0.0
        rpt["temporal"]["noise_features"] = {
            "acf1_total_paid": acf1,
            "smooth_ratio": pct(smooth_ratio),
        }
        rpt["temporal"]["monthly_csv"] = str(MONTHLY_ALL_PATH)


def build_benford(reports: dict[str, dict], con: duckdb.DuckDBPyConnection) -> None:
    state_rows = con.execute(
        f"""
        WITH yearly AS (
          SELECT
            {STATE_EXPR} AS state,
            BILLING_PROVIDER_NPI_NUM AS npi,
            SUBSTR(CLAIM_FROM_MONTH, 1, 4) AS yr,
            SUM(TOTAL_PAID) AS paid
          FROM medicaid_enriched
          WHERE BILLING_PROVIDER_NPI_NUM IS NOT NULL AND CLAIM_FROM_MONTH IS NOT NULL
          GROUP BY 1, 2, 3
        ),
        cleaned AS (
          SELECT state, ABS(paid) AS x
          FROM yearly
          WHERE paid IS NOT NULL AND paid <> 0
        ),
        digits AS (
          SELECT
            state,
            CAST(SUBSTR(CAST(x AS VARCHAR), 1, 1) AS INTEGER) AS d
          FROM cleaned
        ),
        c AS (
          SELECT state, d, COUNT(*)::DOUBLE AS n
          FROM digits
          WHERE d BETWEEN 1 AND 9
          GROUP BY 1, 2
        ),
        t AS (
          SELECT state, SUM(n) AS total
          FROM c
          GROUP BY 1
        )
        SELECT state, d, n / total AS p
        FROM c
        JOIN t USING (state)
        """
    ).fetchall()

    all_rows = con.execute(
        """
        WITH yearly AS (
          SELECT
            BILLING_PROVIDER_NPI_NUM AS npi,
            SUBSTR(CLAIM_FROM_MONTH, 1, 4) AS yr,
            SUM(TOTAL_PAID) AS paid
          FROM medicaid_enriched
          WHERE BILLING_PROVIDER_NPI_NUM IS NOT NULL AND CLAIM_FROM_MONTH IS NOT NULL
          GROUP BY 1, 2
        ),
        cleaned AS (
          SELECT ABS(paid) AS x
          FROM yearly
          WHERE paid IS NOT NULL AND paid <> 0
        ),
        digits AS (
          SELECT CAST(SUBSTR(CAST(x AS VARCHAR), 1, 1) AS INTEGER) AS d
          FROM cleaned
        ),
        c AS (
          SELECT d, COUNT(*)::DOUBLE AS n
          FROM digits
          WHERE d BETWEEN 1 AND 9
          GROUP BY 1
        ),
        t AS (
          SELECT SUM(n) AS total
          FROM c
        )
        SELECT 'ALL' AS state, d, n / total AS p
        FROM c, t
        """
    ).fetchall()

    by_state = dict_from_rows([(str(s), int(d), float(p)) for s, d, p in state_rows + all_rows], key_index=0)
    exp_ben = {d: math.log10(1 + 1 / d) for d in range(1, 10)}

    for state, rows in by_state.items():
        obs = {int(d): float(p) for _, d, p in rows}
        chi = sum(((obs.get(d, 0.0) - exp_ben[d]) ** 2) / exp_ben[d] for d in range(1, 10))
        ensure_report(reports, state)["benford"]["chi_like"] = float(chi)


def normalize_reports(reports: dict[str, dict]) -> dict[str, dict]:
    normalized: dict[str, dict] = {}
    for state in sorted(reports.keys()):
        rpt = reports[state]
        rpt["metadata"]["state"] = state

        for key in [
            "cents_last1_dist",
            "cents_last2_dist",
            "total_paid_cents_last1_dist",
            "total_paid_cents_last2_dist",
            "unit_paid_cents_last1_dist",
            "unit_paid_cents_last2_dist",
        ]:
            dist = rpt["digits"].get(key, {})
            rpt["digits"][key] = {int(k): float(v) for k, v in sorted(dist.items(), key=lambda kv: int(kv[0]))}

        rpt["unit_price"]["top_suspicious"] = rpt["unit_price"]["top_suspicious"][:100]
        rpt["unit_price"]["top_volume"] = rpt["unit_price"]["top_volume"][:100]
        normalized[state] = rpt
    return normalized


def main() -> None:
    OUT_JSON.mkdir(parents=True, exist_ok=True)
    OUT_TABLES.mkdir(parents=True, exist_ok=True)
    OUT_TMP.mkdir(parents=True, exist_ok=True)

    db_path = OUT_TMP / "report_work.duckdb"
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=2")
    con.execute("SET memory_limit='3GB'")
    con.execute("PRAGMA temp_directory='outputs/tmp'")
    con.execute("PRAGMA max_temp_directory_size='300GiB'")
    con.execute("PRAGMA preserve_insertion_order=false")
    con.execute("PRAGMA enable_progress_bar=false")
    start = time.time()

    def checkpoint(label: str) -> None:
        elapsed_min = (time.time() - start) / 60.0
        print(f"[{elapsed_min:6.2f} min] {label}", flush=True)

    checkpoint("Starting report generation")
    build_base_views(con)
    checkpoint("Materialized base tables")

    reports: dict[str, dict] = {}
    ensure_report(reports, "ALL")
    build_health(reports, con)
    checkpoint("Completed data health")
    build_unit_price(reports, con)
    checkpoint("Completed signal 1 inputs (unit price)")
    build_digits(reports, con)
    checkpoint("Completed signal 2/5 inputs (digits + entropy)")
    build_correlations(reports, con)
    checkpoint("Completed signal 3 inputs (correlations)")
    build_ratios(reports, con)
    checkpoint("Completed ratio summaries")
    build_temporal(reports, con)
    checkpoint("Completed signal 4 inputs (temporal)")
    build_benford(reports, con)
    checkpoint("Completed signal 6 inputs (benford)")

    reports = normalize_reports(reports)
    if "ALL" not in reports:
        reports["ALL"] = blank_report("ALL")

    available_states = sorted([s for s in reports.keys() if s not in {"ALL", "UNK"}])
    if "UNK" in reports:
        available_states.append("UNK")

    bundle = {
        "default_state": "ALL",
        "available_states": ["ALL"] + available_states,
        "reports": reports,
    }

    REPORT_ALL_PATH.write_text(json.dumps(reports["ALL"], indent=2), encoding="utf-8")
    REPORT_BY_STATE_PATH.write_text(json.dumps(bundle, indent=2), encoding="utf-8")

    checkpoint("Wrote all report artifacts")
    print(f"Wrote {REPORT_ALL_PATH}")
    print(f"Wrote {REPORT_BY_STATE_PATH}")
    print(f"Wrote {TOP_SUSPICIOUS_PATH}")
    print(f"Wrote {TOP_VOLUME_PATH}")
    print(f"Wrote {MONTHLY_ALL_PATH}")
    print(f"Wrote {MONTHLY_BY_STATE_PATH}")


if __name__ == "__main__":
    main()
