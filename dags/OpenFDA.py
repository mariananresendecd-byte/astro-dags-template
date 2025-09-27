# dags/OpenFDA.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from datetime import timedelta
import pendulum
import requests
import pandas as pd
import time
import logging
import os

# ====== CONFIG ======
ASTRONOMER_DEPLOYMENT_ID = "cmfv4iy9a2hi301qf0hlm0xdv"  # só para logging
GCP_PROJECT  = "learned-cosine-471123-r4"
BQ_DATASET   = os.getenv("BQ_DATASET", "openfda")
BQ_TABLE     = os.getenv("BQ_TABLE", "drug_event_daily_counts")
BQ_LOCATION  = os.getenv("BQ_LOCATION", "US")
GCP_CONN_ID  = os.getenv("GCP_CONN_ID", "google_cloud_default")

MEDICINAL_PRODUCT = os.getenv("OPENFDA_MEDICINAL_PRODUCT", "sildenafil citrate")
OPENFDA_BASE = "https://api.fda.gov/drug/event.json"
MAX_RETRIES = 5
INITIAL_BACKOFF_SEC = 2
# ====================

DEFAULT_ARGS = {
    "email_on_failure": True,
    "owner": "airflow",
}

def _fetch_openfda_for_date(date_str: str, medicinal_product: str) -> pd.DataFrame:
    """
    Busca contagem por receivedate para um único dia YYYYMMDD.
    Retorna DataFrame com colunas: date (DATE), count (INT64), search_term (STRING).
    """
    params = {
        "search": f'patient.drug.medicinalproduct:"{medicinal_product}" AND receivedate:[{date_str}+TO+{date_str}]',
        "count": "receivedate",
        "limit": "10000",
    }

    backoff = INITIAL_BACKOFF_SEC
    for attempt in range(1, MAX_RETRIES + 1):
        r = requests.get(OPENFDA_BASE, params=params, timeout=60)
        if r.status_code == 200:
            data = r.json()
            results = data.get("results", [])
            if not results:
                df = pd.DataFrame([{
                    "date": pd.to_datetime(date_str, format="%Y%m%d").date(),
                    "count": 0
                }])
            else:
                df = pd.DataFrame(results)
                df["date"] = pd.to_datetime(df["time"], format="%Y%m%d").dt.date
                df = df[["date", "count"]]
            df["search_term"] = medicinal_product
            return df

        if r.status_code in (429, 500, 502, 503, 504):
            logging.warning(
                f"[{ASTRONOMER_DEPLOYMENT_ID}] OpenFDA HTTP {r.status_code} "
                f"(tentativa {attempt}/{MAX_RETRIES}) — aguardando {backoff}s"
            )
            time.sleep(backoff)
            backoff *= 2
            continue

        logging.error(f"[{ASTRONOMER_DEPLOYMENT_ID}] Falha OpenFDA: {r.status_code} - {r.text}")
        break

    # Se falhou todas as tentativas, retorna DF vazio com schema
    return pd.DataFrame(columns=["date", "count", "search_term"])

@task
def fetch_and_to_gbq():
    """
    Usa o intervalo de dados do Airflow. Para execução diária, gravamos o dia
    exatamente igual a data_interval_start (UTC) no formato YYYYMMDD.
    """
    ctx = get_current_context()
    start = ctx["data_interval_start"]              # pendulum DateTime (UTC)
    date_str = start.format("YYYYMMDD")

    logging.info(
        f"[{ASTRONOMER_DEPLOYMENT_ID}] Coletando OpenFDA para {date_str} | "
        f"termo='{MEDICINAL_PRODUCT}' → {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    )

    df = _fetch_openfda_for_date(date_str, MEDICINAL_PRODUCT)
    if df.empty:
        logging.warning("Nada a gravar no BigQuery (DF vazio).")
        return

    # Tipagem e ordenação
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["search_term"] = df["search_term"].astype(str)
    df = df.sort_values("date")

    # Credenciais do Airflow connection
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"
    table_schema = [
        {"name": "date",        "type": "DATE"},
        {"name": "count",       "type": "INTEGER"},
        {"name": "search_term", "type": "STRING"},
    ]

    # Grava via pandas-gbq
    df.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=credentials,
        table_schema=table_schema,  # usado na criação inicial
        location=BQ_LOCATION,
        progress_bar=False,
    )
    logging.info(f"Loaded {len(df)} rows to {GCP_PROJECT}.{destination_table} (location={BQ_LOCATION}).")

@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",  # todo dia 00:00 UTC
    start_date=pendulum.datetime(2020, 11, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["openfda", "bigquery", "lookerstudio", "etl"],
)
def openfda_drug_event_daily_to_bq():
    fetch_and_to_gbq()

dag = openfda_drug_event_daily_to_bq()








