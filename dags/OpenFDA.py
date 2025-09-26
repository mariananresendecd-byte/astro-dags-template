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
import pytz
import math
import logging
import os

# ==== CONFIGURAÇÕES (edite aqui OU use Variables/Env) ====
PROJECT_ID = "learned-cosine-471123-r4"   # já informado por você
BQ_DATASET = os.getenv("BQ_DATASET", "openfda")  # informe seu dataset
BQ_TABLE   = os.getenv("BQ_TABLE",   "drug_event_daily_counts")  # informe sua tabela
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")  # local do BigQuery (US, EU, southamerica-east1, etc.)
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")  # se preferir usar pandas_gbq com ADC, pode ignorar

# Termo do medicamento (altere se quiser outro):
MEDICINAL_PRODUCT = os.getenv("OPENFDA_MEDICINAL_PRODUCT", "sildenafil citrate")

# Limites/Backoff
OPENFDA_BASE = "https://api.fda.gov/drug/event.json"
MAX_RETRIES = 5
INITIAL_BACKOFF_SEC = 2

# ==== Funções auxiliares ====

def _openfda_count_by_date(date_str, medicinal_product):
    """
    Faz uma chamada ao OpenFDA para retornar a contagem por 'receivedate' em uma janela de 1 dia.
    Retorna um DataFrame com colunas: date (DATE), count (INT64), search_term (STRING).
    """
    # Janela diária no formato YYYYMMDD
    start_date = date_str
    end_date = date_str

    # count por receivedate, limit alto para garantir retorno (count tem bucket limit)
    params = {
        "search": f'patient.drug.medicinalproduct:"{medicinal_product}" AND receivedate:[{start_date}+TO+{end_date}]',
        "count": "receivedate",
        "limit": "10000"
    }

    backoff = INITIAL_BACKOFF_SEC
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(OPENFDA_BASE, params=params, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results", [])
            # Se a API não retornar nada para o dia, criar linha com 0
            if not results:
                df = pd.DataFrame([{"date": pd.to_datetime(date_str, format="%Y%m%d").date(), "count": 0}])
            else:
                df = pd.DataFrame(results)
                # 'time' vem como 'YYYYMMDD'
                df["date"] = pd.to_datetime(df["time"], format="%Y%m%d").dt.date
                df = df[["date", "count"]]
            df["search_term"] = medicinal_product
            return df

        # 429/5xx → backoff exponencial
        if resp.status_code in (429, 500, 502, 503, 504):
            logging.warning(f"OpenFDA HTTP {resp.status_code} (tentativa {attempt}/{MAX_RETRIES}). Aguardando {backoff}s...")
            time.sleep(backoff)
            backoff *= 2
            continue

        # Outros erros → log e retorna DF vazio
        logging.error(f"Falha OpenFDA: HTTP {resp.status_code} - {resp.text}")
        return pd.DataFrame(columns=["date", "count", "search_term"])

    logging.error("Excedeu tentativas no OpenFDA (backoff).")
    return pd.DataFrame(columns=["date", "count", "search_term"])


def fetch_openfda_daily(**context):
    """
    Tarefa diária: usa a execution_date da DAG (UTC) como dia de referência.
    """
    # Airflow usa UTC para execution_date
    execution_date = context["ds_nodash"]  # 'YYYYMMDD'
    df = _openfda_count_by_date(execution_date, MEDICINAL_PRODUCT)

    # Envia para XCom (como dict) – pequeno e suficiente
    context["ti"].xcom_push(key="openfda_daily_df", value=df.to_dict(orient="list"))


def save_to_bigquery(**context):
    """
    Recupera o DF do XCom e grava no BigQuery usando pandas_gbq.
    Necessário: credenciais GCP válidas (ADC) no ambiente do worker.
    """
    # Reconstrói DF
    data = context["ti"].xcom_pull(key="openfda_daily_df", task_ids="fetch_openfda_daily")
    if not data:
        logging.warning("Nada a gravar no BigQuery (DF vazio).")
        return

    df = pd.DataFrame(data)
    if df.empty:
        logging.info("DF vazio – nenhuma linha para gravar.")
        return

    # Tipagem
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["search_term"] = df["search_term"].astype(str)

    # Esquema BigQuery
    table_id = f"{BQ_DATASET}.{BQ_TABLE}"

    # Gravação via pandas_gbq
    from pandas_gbq import to_gbq
    table_schema = [
        {"name": "date", "type": "DATE"},
        {"name": "count", "type": "INTEGER"},
        {"name": "search_term", "type": "STRING"},
    ]

    # Cria/append (sem particionamento). Se quiser particionar por date, crie a tabela antes com partitioning.
    to_gbq(
        df,
        table_id,
        project_id=PROJECT_ID,
        if_exists="append",
        table_schema=table_schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )

    logging.info(f"{len(df)} linha(s) gravadas em {PROJECT_ID}.{table_id}.")


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
    description="Coleta diária de eventos de drug/event (OpenFDA) e carrega contagem por dia no BigQuery",
    schedule_interval="@daily",
    start_date=datetime(2020, 11, 1),  # habilita catchup desde 2020-11-01
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




