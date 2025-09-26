"""
OpenFDA → BigQuery (diário)
DAG para coletar contagem diária de eventos do OpenFDA e gravar no BigQuery.
Compatível com Astronomer/Airflow 2.x
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import requests
import pandas as pd
import logging
import os

# ==== IDENTIFICAÇÃO DO DEPLOYMENT (pedido do usuário) ====
# Este ID é usado no processo de deploy (astro deploy --deployment <ID>).
# Mantido aqui para referência e logging operacional.
ASTRONOMER_DEPLOYMENT_ID = "cmfv4iy9a2hi301qf0hlm0xdv"

# ==== CONFIGURAÇÕES (edite aqui OU use Variáveis de Ambiente) ====
PROJECT_ID  = "learned-cosine-471123-r4"                    # seu projeto BQ
BQ_DATASET  = os.getenv("BQ_DATASET", "openfda")            # dataset BQ
BQ_TABLE    = os.getenv("BQ_TABLE", "drug_event_daily_counts")  # tabela BQ
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")                # ex.: US, EU, southamerica-east1
MEDICINAL_PRODUCT = os.getenv("OPENFDA_MEDICINAL_PRODUCT", "sildenafil citrate")

# Endpoint/OpenFDA
OPENFDA_BASE = "https://api.fda.gov/drug/event.json"
MAX_RETRIES = 5
INITIAL_BACKOFF_SEC = 2


def _openfda_count_by_date(date_str: str, medicinal_product: str) -> pd.DataFrame:
    """
    Busca contagem diária por 'receivedate' para o medicamento informado.
    Retorna DF com colunas: date (DATE), count (INT64), search_term (STRING).
    """
    params = {
        "search": f'patient.drug.medicinalproduct:"{medicinal_product}" AND receivedate:[{date_str}+TO+{date_str}]',
        "count": "receivedate",
        "limit": "10000"
    }

    backoff = INITIAL_BACKOFF_SEC
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(OPENFDA_BASE, params=params, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results", [])
            if not results:
                df = pd.DataFrame([{"date": pd.to_datetime(date_str, format="%Y%m%d").date(), "count": 0}])
            else:
                df = pd.DataFrame(results)
                df["date"] = pd.to_datetime(df["time"], format="%Y%m%d").dt.date
                df = df[["date", "count"]]
            df["search_term"] = medicinal_product
            return df

        if resp.status_code in (429, 500, 502, 503, 504):
            logging.warning(
                f"[{ASTRONOMER_DEPLOYMENT_ID}] OpenFDA HTTP {resp.status_code} "
                f"(tentativa {attempt}/{MAX_RETRIES}). Backoff {backoff}s..."
            )
            time.sleep(backoff)
            backoff *= 2
            continue

        logging.error(f"[{ASTRONOMER_DEPLOYMENT_ID}] Falha OpenFDA: HTTP {resp.status_code} - {resp.text}")
        return pd.DataFrame(columns=["date", "count", "search_term"])

    logging.error(f"[{ASTRONOMER_DEPLOYMENT_ID}] Excedeu tentativas no OpenFDA (backoff).")
    return pd.DataFrame(columns=["date", "count", "search_term"])


def fetch_openfda_daily(**context):
    """
    Usa a execution_date (UTC) como dia de referência (YYYYMMDD).
    """
    date_str = context["ds_nodash"]  # YYYYMMDD
    logging.info(
        f"[{ASTRONOMER_DEPLOYMENT_ID}] Coletando OpenFDA para {date_str} | "
        f"termo='{MEDICINAL_PRODUCT}' → {PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    )
    df = _openfda_count_by_date(date_str, MEDICINAL_PRODUCT)
    context["ti"].xcom_push(key="openfda_daily_df", value=df.to_dict(orient="list"))


def save_to_bigquery(**context):
    """
    Reconstrói DF do XCom e grava no BigQuery via pandas_gbq.
    Requer credenciais GCP válidas (ADC) no worker.
    """
    data = context["ti"].xcom_pull(key="openfda_daily_df", task_ids="fetch_openfda_daily")
    if not data:
        logging.warning(f"[{ASTRONOMER_DEPLOYMENT_ID}] Nada a gravar (DF vazio).")
        return

    df = pd.DataFrame(data)
    if df.empty:
        logging.info(f"[{ASTRONOMER_DEPLOYMENT_ID}] DF vazio – nenhuma linha para gravar.")
        return

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["search_term"] = df["search_term"].astype(str)

    table_id = f"{BQ_DATASET}.{BQ_TABLE}"

    from pandas_gbq import to_gbq
    table_schema = [
        {"name": "date", "type": "DATE"},
        {"name": "count", "type": "INTEGER"},
        {"name": "search_term", "type": "STRING"},
    ]

    to_gbq(
        df,
        table_id,
        project_id=PROJECT_ID,
        if_exists="append",
        table_schema=table_schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )
    logging.info(
        f"[{ASTRONOMER_DEPLOYMENT_ID}] {len(df)} linha(s) gravadas em {PROJECT_ID}.{table_id}."
    )


# ==== Definição da DAG ====
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="openfda_drug_event_daily_to_bq",
    default_args=default_args,
    description="OpenFDA drug/event diário → BigQuery (para Looker Studio)",
    schedule_interval="@daily",
    start_date=datetime(2020, 11, 1),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["openfda", "bigquery", "lookerstudio"],
)

fetch_data = PythonOperator(
    task_id="fetch_openfda_daily",
    python_callable=fetch_openfda_daily,
    provide_context=True,
    dag=dag,
)

write_bq = PythonOperator(
    task_id="save_to_bigquery",
    python_callable=save_to_bigquery,
    provide_context=True,
    dag=dag,
)

fetch_data >> write_bq






