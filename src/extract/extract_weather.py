# src/extract/extract_weather.py

# src/extract/extract_weather.py

import requests
import polars as pl
from datetime import datetime

from airflow.hooks.base import BaseHook

from src.sources.open_meteo import (
    get_valencia_coordinates,
    build_url
)


# ============================================================
# CONEXIÓN A SQL SERVER DESDE AIRFLOW
# ============================================================

def get_sqlserver_connection():
    """
    Obtiene conexión a SQL Server usando Airflow Connection
    """
    conn = BaseHook.get_connection("sqlserver_weather_pipeline")

    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={conn.host},{conn.port};"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};"
        f"PWD={conn.password};"
        "TrustServerCertificate=yes;"
    )

    import pyodbc
    return pyodbc.connect(connection_string)


# ============================================================
# EXTRACCIÓN DE DATOS
# ============================================================

def extract_weather() -> pl.DataFrame:
    """
    Extrae datos horarios de Open-Meteo
    y los devuelve como Polars DataFrame
    """

    # 1️⃣ Coordenadas y URL
    coords = get_valencia_coordinates()
    url = build_url(coords)

    # 2️⃣ Llamada API
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    # 3️⃣ Normalizar datos
    hourly = data["hourly"]

    records = [
        {
            "city": coords["city"],
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "timestamp": ts,
            "temperature": hourly["temperature_2m"][i],
            "precipitation": hourly["precipitation"][i],
        }
        for i, ts in enumerate(hourly["time"])
    ]

    df = pl.DataFrame(records)
    return df


# ============================================================
# CARGA A SQL SERVER
# ============================================================

def load_weather_to_sqlserver(df: pl.DataFrame) -> None:
    """
    Inserta los datos en raw.raw_weather
    """

    conn = get_sqlserver_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO raw.raw_weather (
            raw_weather_city_nv,
            raw_weather_latitude_d,
            raw_weather_longitude_d,
            raw_weather_time_dt,
            raw_weather_temperature_d,
            raw_weather_precipitation_d,
            raw_weather_ingestion_date_dt
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    ingestion_date = datetime.now()

    for row in df.iter_rows(named=True):
        cursor.execute(
            insert_sql,
            row["city"],
            row["latitude"],
            row["longitude"],
            datetime.fromisoformat(row["timestamp"]),
            row["temperature"],
            row["precipitation"],
            ingestion_date
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"{df.height} registros insertados en raw.raw_weather")


# ============================================================
# FUNCIÓN PRINCIPAL PARA AIRFLOW
# ============================================================

def extract_and_load_weather():
    """
    Función única pensada para PythonOperator
    """
    df = extract_weather()
    load_weather_to_sqlserver(df)


