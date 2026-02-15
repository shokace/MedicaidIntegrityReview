from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import duckdb

PARQUET_PATH = Path("data/medicaid-provider-spending.parquet")
NPPES_ZIP_PATH = Path("data/nppes/NPPES_Data_Dissemination_February_2026.zip")
OUT_LOOKUP_PATH = Path("outputs/tables/npi_state_lookup.csv")
OUT_SUMMARY_PATH = Path("outputs/json/state_enrichment.json")
OUT_STATE_ROLLUP_PATH = Path("outputs/tables/state_rollup_by_billing_state.csv")
OUT_PREVIEW_PATH = Path("outputs/tables/medicaid_with_state_preview.csv")


def find_main_nppes_csv(zf: zipfile.ZipFile) -> str:
    names = [n for n in zf.namelist() if n.startswith("npidata_pfile_") and n.endswith(".csv")]
    if not names:
        raise RuntimeError("Could not find npidata_pfile_*.csv inside NPPES zip")
    if len(names) > 1:
        names.sort()
    return names[0]


def get_target_npis(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        f"""
        WITH npis AS (
          SELECT BILLING_PROVIDER_NPI_NUM AS npi
          FROM read_parquet('{PARQUET_PATH}')
          WHERE BILLING_PROVIDER_NPI_NUM IS NOT NULL
          UNION
          SELECT SERVICING_PROVIDER_NPI_NUM AS npi
          FROM read_parquet('{PARQUET_PATH}')
          WHERE SERVICING_PROVIDER_NPI_NUM IS NOT NULL
        )
        SELECT npi FROM npis
        """
    ).fetchall()
    return {str(r[0]).strip() for r in rows if r and r[0]}


def normalize_state(state_value: str) -> str:
    s = (state_value or "").strip().upper()
    return s if len(s) == 2 and s.isalpha() else ""


def build_lookup(npi_targets: set[str]) -> tuple[int, int]:
    if not NPPES_ZIP_PATH.exists():
        raise FileNotFoundError(f"Missing NPPES zip: {NPPES_ZIP_PATH}")

    NPPES_ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_LOOKUP_PATH.parent.mkdir(parents=True, exist_ok=True)

    matched_rows = 0
    lookup: dict[str, tuple[str, str, str]] = {}

    with zipfile.ZipFile(NPPES_ZIP_PATH) as zf:
        main_csv_name = find_main_nppes_csv(zf)
        with zf.open(main_csv_name, "r") as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8", newline=""))
            header = next(reader)
            idx = {name: i for i, name in enumerate(header)}
            npi_i = idx["NPI"]
            practice_state_i = idx["Provider Business Practice Location Address State Name"]
            mailing_state_i = idx["Provider Business Mailing Address State Name"]

            for row in reader:
                npi = row[npi_i].strip()
                if not npi or npi not in npi_targets:
                    continue

                practice_state = normalize_state(row[practice_state_i])
                mailing_state = normalize_state(row[mailing_state_i])
                chosen_state = practice_state or mailing_state
                lookup[npi] = (chosen_state, practice_state, mailing_state)
                matched_rows += 1

    with OUT_LOOKUP_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["npi", "chosen_state", "practice_state", "mailing_state"])
        for npi in sorted(lookup):
            chosen_state, practice_state, mailing_state = lookup[npi]
            writer.writerow([npi, chosen_state, practice_state, mailing_state])

    return len(lookup), matched_rows


def build_state_rollups(con: duckdb.DuckDBPyConnection) -> dict:
    lookup = str(OUT_LOOKUP_PATH)
    parquet = str(PARQUET_PATH)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW npi_lookup AS
        SELECT
          CAST(npi AS VARCHAR) AS npi,
          CAST(chosen_state AS VARCHAR) AS chosen_state,
          CAST(practice_state AS VARCHAR) AS practice_state,
          CAST(mailing_state AS VARCHAR) AS mailing_state
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
        """
    )

    billing_cov = con.execute(
        f"""
        WITH by_npi AS (
          SELECT BILLING_PROVIDER_NPI_NUM AS npi, COUNT(*) AS rows
          FROM read_parquet('{parquet}')
          WHERE BILLING_PROVIDER_NPI_NUM IS NOT NULL
          GROUP BY 1
        ), joined AS (
          SELECT b.rows, l.chosen_state
          FROM by_npi b
          LEFT JOIN npi_lookup l
          ON b.npi = l.npi
        )
        SELECT
          SUM(rows) AS total_rows,
          SUM(CASE WHEN chosen_state IS NOT NULL AND chosen_state <> '' THEN rows ELSE 0 END) AS mapped_rows
        FROM joined
        """
    ).fetchone()

    servicing_cov = con.execute(
        f"""
        WITH by_npi AS (
          SELECT SERVICING_PROVIDER_NPI_NUM AS npi, COUNT(*) AS rows
          FROM read_parquet('{parquet}')
          WHERE SERVICING_PROVIDER_NPI_NUM IS NOT NULL
          GROUP BY 1
        ), joined AS (
          SELECT b.rows, l.chosen_state
          FROM by_npi b
          LEFT JOIN npi_lookup l
          ON b.npi = l.npi
        )
        SELECT
          SUM(rows) AS total_rows,
          SUM(CASE WHEN chosen_state IS NOT NULL AND chosen_state <> '' THEN rows ELSE 0 END) AS mapped_rows
        FROM joined
        """
    ).fetchone()

    con.execute(
        f"""
        COPY (
          WITH by_billing_npi AS (
            SELECT
              BILLING_PROVIDER_NPI_NUM AS npi,
              COUNT(*) AS rows,
              SUM(TOTAL_CLAIMS) AS total_claims,
              SUM(TOTAL_PAID) AS total_paid,
              SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_bens
            FROM read_parquet('{parquet}')
            WHERE BILLING_PROVIDER_NPI_NUM IS NOT NULL
            GROUP BY 1
          )
          SELECT
            l.chosen_state AS state,
            SUM(b.rows) AS rows,
            SUM(b.total_claims) AS total_claims,
            SUM(b.total_paid) AS total_paid,
            SUM(b.total_bens) AS total_bens
          FROM by_billing_npi b
          JOIN npi_lookup l
          ON b.npi = l.npi
          WHERE l.chosen_state IS NOT NULL AND l.chosen_state <> ''
          GROUP BY 1
          ORDER BY rows DESC
        ) TO '{OUT_STATE_ROLLUP_PATH}' (HEADER, DELIMITER ',')
        """
    )

    con.execute(
        f"""
        COPY (
          SELECT
            m.BILLING_PROVIDER_NPI_NUM,
            m.SERVICING_PROVIDER_NPI_NUM,
            m.HCPCS_CODE,
            m.CLAIM_FROM_MONTH,
            m.TOTAL_UNIQUE_BENEFICIARIES,
            m.TOTAL_CLAIMS,
            m.TOTAL_PAID,
            l.chosen_state AS BILLING_PROVIDER_STATE
          FROM read_parquet('{parquet}') m
          LEFT JOIN npi_lookup l
          ON m.BILLING_PROVIDER_NPI_NUM = l.npi
          LIMIT 1000
        ) TO '{OUT_PREVIEW_PATH}' (HEADER, DELIMITER ',')
        """
    )

    rollup_top10 = con.execute(
        f"""
        SELECT state, rows, total_claims, total_paid
        FROM read_csv_auto('{OUT_STATE_ROLLUP_PATH}', header=true)
        ORDER BY rows DESC
        LIMIT 10
        """
    ).fetchall()

    billing_total = int(billing_cov[0] or 0)
    billing_mapped = int(billing_cov[1] or 0)
    servicing_total = int(servicing_cov[0] or 0)
    servicing_mapped = int(servicing_cov[1] or 0)

    return {
        "billing_row_state_coverage_rate": (billing_mapped / billing_total) if billing_total else 0.0,
        "servicing_row_state_coverage_rate": (servicing_mapped / servicing_total) if servicing_total else 0.0,
        "billing_rows_total": billing_total,
        "billing_rows_with_state": billing_mapped,
        "servicing_rows_total": servicing_total,
        "servicing_rows_with_state": servicing_mapped,
        "state_rollup_csv": str(OUT_STATE_ROLLUP_PATH),
        "preview_csv": str(OUT_PREVIEW_PATH),
        "top_10_states_by_rows": [
            {
                "state": str(r[0]),
                "rows": int(r[1] or 0),
                "total_claims": float(r[2] or 0.0),
                "total_paid": float(r[3] or 0.0),
            }
            for r in rollup_top10
        ],
    }


def main() -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA preserve_insertion_order=false")

    targets = get_target_npis(con)
    lookup_count, matched_rows = build_lookup(targets)

    summary = {
        "nppes_zip": str(NPPES_ZIP_PATH),
        "parquet": str(PARQUET_PATH),
        "target_npi_count": len(targets),
        "lookup_count": lookup_count,
        "matched_nppes_rows": matched_rows,
        "lookup_csv": str(OUT_LOOKUP_PATH),
    }
    summary.update(build_state_rollups(con))

    OUT_SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Wrote {OUT_LOOKUP_PATH}")
    print(f"Wrote {OUT_STATE_ROLLUP_PATH}")
    print(f"Wrote {OUT_PREVIEW_PATH}")
    print(f"Wrote {OUT_SUMMARY_PATH}")


if __name__ == "__main__":
    main()
