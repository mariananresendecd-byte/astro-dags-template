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
from google.cloud import bigquery

# ====== CONFIG ======
ASTRONOMER_DEPLOYMENT_ID = "cmfv4iy9a2hi301qf0hlm0xdv"
GCP_PROJECT  = "learned-cosine-471123-r4"
BQ_DATASET   = os.getenv("BQ_DATASET", "openfda")
BQ_TABLE     = os.getenv("BQ_TABLE", "drug_event_daily_counts")
BQ_LOCATION  = os.getenv("BQ_LOCATION", "US")
GCP_CONN_ID  = os.getenv("GCP_CONN_ID", "google_cloud_default")

OPENFDA_BASE = "https://api.fda.gov/drug/event.json"
MAX_RETRIES = 5
INITIAL_BACKOFF_SEC = 2
# ====================

DEFAULT_ARGS = {
    "email_on_failure": True,
    "owner": "airflow",
}

def _fetch_openfda_for_date(date_str: str, medicinal_product: str) -> pd.DataFrame:
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
                # Sem eventos para o dia → grava 0 para garantir linha e criar tabela
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

    # Falha geral → DF vazio (não grava)
    return pd.DataFrame(columns=["date", "count", "search_term"])

@task
def ensure_dataset_and_table():
    """Garante dataset e tabela (particionada) existentes antes de escrever."""
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    client = hook.get_client(project_id=GCP_PROJECT)

    # Dataset
    ds_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    ds_ref.location = BQ_LOCATION
    client.create_dataset(ds_ref, exists_ok=True)

    # Tabela particionada por date e cluster em search_term
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        client.get_table(table_id)
    except Exception:
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("count", "INTEGER"),
            bigquery.SchemaField("search_term", "STRING"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date"
        )
        table.clustering_fields = ["search_term"]
        client.create_table(table)
        logging.info(f"✅ Tabela criada: {table_id} (partitioned/clustering)")

@task
def fetch_and_to_gbq():
    """
    Busca os dados do OpenFDA e grava no BigQuery.
    Params aceitos no Trigger:
      - medicinal_product (str)  ex.: "ibuprofen"
      - create_table_only (bool) se true, só garante a tabela e sai
    """
    ctx = get_current_context()
    start = ctx["data_interval_start"]
    date_str = start.format("YYYYMMDD")

    medicinal_product = ctx["params"].get("medicinal_product", "sildenafil citrate")
    create_table_only = bool(ctx["params"].get("create_table_only", False))

    logging.info(f"[DBG] project={GCP_PROJECT} dataset={BQ_DATASET} table={BQ_TABLE} location={BQ_LOCATION}")
    logging.info(f"[DBG] params: medicinal_product='{medicinal_product}', create_table_only={create_table_only}")

    if create_table_only:
        logging.info("create_table_only=True → não fará fetch nem write.")
        return

    df = _fetch_openfda_for_date(date_str, medicinal_product)
    logging.info(f"[DBG] df shape={df.shape} head3={df.head(3).to_dict(orient='records')}")

    if df.empty:
        logging.warning("DF vazio — não gravará no BigQuery.")
        return

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["search_term"] = df["search_term"].astype(str)
    df = df.drop_duplicates(subset=["date", "search_term"]).sort_values("date")

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

    try:
        df.to_gbq(
            destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
            project_id=GCP_PROJECT,
            if_exists="append",
            credentials=credentials,
            table_schema=[
                {"name": "date", "type": "DATE"},
                {"name": "count", "type": "INTEGER"},
                {"name": "search_term", "type": "STRING"},
            ],
            location=BQ_LOCATION,
            progress_bar=False,
        )
        logging.info(f"✅ Loaded {len(df)} rows to {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    except Exception as e:
        import traceback
        logging.error("❌ to_gbq FAILED")
        logging.error(repr(e))
        logging.error(traceback.format_exc())
        raise

@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",      # diária
    start_date=pendulum.datetime(2025, 9, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"medicinal_product": "sildenafil citrate", "create_table_only": False},
    tags=["openfda", "bigquery", "lookerstudio", "etl"],
)
def openfda_drug_event_daily_to_bq3():
    ensure_dataset_and_table() >> fetch_and_to_gbq()

dag = openfda_drug_event_daily_to_bq3()












