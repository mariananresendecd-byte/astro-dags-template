# dags/openfda_covid_vaccine_stage_pipeline.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import pendulum
import pandas as pd
import pandas_gbq
import requests
import time
from datetime import date
from typing import Any, Dict, List

# ========================= Config =========================
GCP_PROJECT    = "learned-cosine-471123-r4"
BQ_DATASET     = "dataset_fda"
BQ_TABLE_STAGE = "covid_vaccine_events_stage"   # stage (flat)
BQ_TABLE_COUNT = "openfda_covid_vaccine"        # contagem diária
BQ_LOCATION    = "US"
GCP_CONN_ID    = "google_cloud_default"

# Jan -> Jun/2025 (exemplo de janela fixa)
TEST_START = date(2025, 1, 1)
TEST_END   = date(2025, 6, 30)
DRUG_QUERY = "COVID-19 vaccine"

TIMEOUT_S   = 30
MAX_RETRIES = 3

# HTTP session
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "didactic-openfda-etl/1.0 (contato: exemplo@dominio.com)"})


# ========================= Helpers =========================
def _search_expr_by_day(day: date, drug_query: str) -> str:
    d = day.strftime("%Y%m%d")
    return f'patient.drug.medicinalproduct:"{drug_query}" AND receivedate:{d}'

def _openfda_get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=TIMEOUT_S)
        if r.status_code == 404:
            return {"results": []}
        if 200 <= r.status_code < 300:
            return r.json()
        if attempt < MAX_RETRIES and r.status_code in (429, 500, 502, 503, 504):
            time.sleep(attempt)
            continue
        r.raise_for_status()

def _to_flat(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    flat: List[Dict[str, Any]] = []
    for ev in rows:
        patient   = (ev or {}).get("patient", {}) or {}
        reactions = patient.get("reaction", []) or []
        drugs     = patient.get("drug", []) or []
        flat.append({
            "safetyreportid":        ev.get("safetyreportid"),
            "receivedate":           ev.get("receivedate"),
            "patientsex":            patient.get("patientsex"),
            "primarysourcecountry":  ev.get("primarysourcecountry"),
            "serious":               ev.get("serious"),
            "reaction_pt":           (reactions[0].get("reactionmeddrapt") if reactions else None),
            "drug_product":          (drugs[0].get("medicinalproduct") if drugs else None),
        })
    df = pd.DataFrame(flat)
    if df.empty:
        return df
    df["safetyreportid"] = df["safetyreportid"].astype(str)
    df["receivedate"] = pd.to_datetime(df["receivedate"], format="%Y%m%d", errors="coerce").dt.date
    for col in ["patientsex", "serious"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df = df.drop_duplicates(subset=["safetyreportid"], keep="first")
    return df


# ========================= DAG =========================
@dag(
    dag_id="openfda_covid_vaccine_stage_pipeline",
    description="Consulta openFDA (COVID-19 vaccine) -> flat -> BQ stage -> agrega diário",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "faers", "covid19", "vaccine", "bigquery", "etl"],
)
def openfda_covid_vaccine_stage_pipeline():

    @task(retries=0)
    def extract_transform_load() -> Dict[str, str]:
        base_url = "https://api.fda.gov/drug/event.json"
        all_rows: List[Dict[str, Any]] = []

        day = TEST_START
        while day <= TEST_END:
            limit = 1000
            skip = 0
            while True:
                params = {
                    "search": _search_expr_by_day(day, DRUG_QUERY),
                    "limit": str(limit),
                    "skip": str(skip),
                }
                payload = _openfda_get(base_url, params)
                rows = payload.get("results", []) or []
                all_rows.extend(rows)
                if len(rows) < limit:
                    break
                skip += limit
                time.sleep(0.25)
            print(f"[fetch] {day}: {len(rows)} registros.")
            day = date.fromordinal(day.toordinal() + 1)

        df = _to_flat(all_rows)
        print(f"[normalize] {len(df)} linhas após normalização")

        schema = [
            {"name": "safetyreportid",       "type": "STRING"},
            {"name": "receivedate",          "type": "DATE"},
            {"name": "patientsex",           "type": "INTEGER"},
            {"name": "primarysourcecountry", "type": "STRING"},
            {"name": "serious",              "type": "INTEGER"},
            {"name": "reaction_pt",          "type": "STRING"},
            {"name": "drug_product",         "type": "STRING"},
        ]
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        creds = bq.get_credentials()
        df.to_gbq(
            destination_table=f"{BQ_DATASET}.{BQ_TABLE_STAGE}",
            project_id=GCP_PROJECT,
            if_exists="replace",
            credentials=creds,
            table_schema=schema,
            location=BQ_LOCATION,
            progress_bar=False,
        )
        return {"start": TEST_START.strftime("%Y-%m-%d"),
                "end":   TEST_END.strftime("%Y-%m-%d"),
                "drug":  DRUG_QUERY}

    @task(retries=0)
    def build_daily_counts(meta: Dict[str, str]) -> None:
        start, end, drug = meta["start"], meta["end"], meta["drug"]

        sql = f"""
        SELECT
          receivedate AS day,
          COUNT(*)    AS events,
          '{drug}'    AS drug
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}`
        WHERE receivedate BETWEEN DATE('{start}') AND DATE('{end}')
        GROUP BY day
        ORDER BY day
        """
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        creds = bq.get_credentials()
        df_counts = pandas_gbq.read_gbq(
            sql,
            project_id=GCP_PROJECT,
            credentials=creds,
            dialect="standard",
            progress_bar_type=None,
            location=BQ_LOCATION,
        )
        schema_counts = [
            {"name": "day",    "type": "DATE"},
            {"name": "events", "type": "INTEGER"},
            {"name": "drug",   "type": "STRING"},
        ]
        df_counts.to_gbq(
            destination_table=f"{BQ_DATASET}.{BQ_TABLE_COUNT}",
            project_id=GCP_PROJECT,
            if_exists="replace",
            credentials=creds,
            table_schema=schema_counts,
            location=BQ_LOCATION,
            progress_bar=False,
        )
        print(f"[counts] {len(df_counts)} linhas gravadas em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_COUNT}.")

    build_daily_counts(extract_transform_load())

openfda_covid_vaccine_stage_pipeline()
















