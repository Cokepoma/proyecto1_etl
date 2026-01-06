from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime,timedelta

default_args = {
    "owner" : "Jorge Pons",
    "start_date" : datetime(2025,6,29),
    "retries" : 1 ,
    "retry_delay" : timedelta(minutes = 1)
}
with DAG (
    default_args = default_args,
    schedule = "@daily",
    dag_id = "a"

) as dag:
    inicio = EmptyOperator(task_id = "inicio")
    fin = EmptyOperator(task_id = "fin")
    inicio >> fin
