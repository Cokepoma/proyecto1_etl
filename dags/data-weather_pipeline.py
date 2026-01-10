#Importaciones Airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

#importaciones Funciones
from datetime import datetime,timedelta
from src.extract.extract_weather import extract_weather
from src.extract.extract_weather import extract_and_load_weather

#mover entre contenedores
import sys
from pathlib import Path


AIRFLOW_HOME = Path(__file__).resolve().parents[1]
sys.path.append(str(AIRFLOW_HOME))

default_args = {
    "owner" : "Jorge Pons",
    "depends_on_past" : False,
    "retries" : 1 ,
    "retry_delay" : timedelta(minutes = 1),
    "email_on_failure" : False,
    "email_on_retry" : False,
    "execution_timeout" : timedelta(minutes = 10)
}
with DAG (
    dag_id = "Etl_Tiempo",
    description = "Etl diseñado para practicar nueva versión de Airflow",
    default_args = default_args,
    schedule = "@daily",
    start_date =  datetime(2025,6,29),
    catchup = False,
    tags = ["Etl","Eltiempo", "Practica"]

) as dag:
    inicio = EmptyOperator(task_id="inicio")

    extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather
    )

    fin = EmptyOperator(task_id="fin")

    inicio >> extract >> fin