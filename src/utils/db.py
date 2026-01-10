import pyodbc
from airflow.hooks.base import BaseHook

def get_connection():
    conn = BaseHook.get_connection("sqlserver_weather")

    connection_string = (
        f"DRIVER={{{conn.extra_dejson['driver']}}};"
        f"SERVER={conn.host},{conn.port};"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};"
        f"PWD={conn.password}"
    )

    return pyodbc.connect(connection_string)

