# src/extract/extract_weather.py

import requests
import polars as pl
from src.utils.db import get_connection
from src.sources.open_meteo import get_valencia_coordinates, build_url
from datetime import datetime

def extract_weather():
    # 1️⃣ Obtener coordenadas y URL
    coords = get_valencia_coordinates()
    url = build_url(coords)

    # 2️⃣ Llamada a la API
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # 3️⃣ Normalizar datos por hora en lista de diccionarios
    hourly = data["hourly"]
    records = [
        {
            "city": coords["city"],
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "timestamp": timestamp,
            "temperature": hourly["temperature_2m"][i],
            "precipitation": hourly["precipitation"][i]
        }
        for i, timestamp in enumerate(hourly["time"])
    ]

    # 4️⃣ Convertir a Polars DataFrame
    df = pl.DataFrame(records)
    return df

def load_to_sqlserver(df: pl.DataFrame):
    """
    Inserta los datos en raw_weather usando pyodbc
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Iterar sobre Polars DataFrame
    for row in df.iter_rows(named=True):
        ingestion_date = datetime.now()
        timestamp = datetime.fromisoformat(row["timestamp"])

        cursor.execute("""
            INSERT INTO raw.raw_weather (
                        raw_weather_city_nv
                       , raw_weather_latitude_d
                       , raw_weather_longitude_d
                       , raw_weather_time_dt
                       , raw_weather_temperature_d
                       , raw_weather_precipitation_d
                       ,raw_weather_ingestion_date_dt
            )
            VALUES (?, ?, ?, ?, ?, ?,?)
        """, row["city"]
            , row["latitude"]
            , row["longitude"]
            , timestamp
            , row["temperature"]
            , row["precipitation"]
            , ingestion_date)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"{df.height} registros insertados en raw_weather")

# Ejecutar si se corre directamente
if __name__ == "__main__":
    df_weather = extract_weather()
    load_to_sqlserver(df_weather)
