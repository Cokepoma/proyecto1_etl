# src/transform/transform_weather.py

import polars as pl
from datetime import datetime
from src.utils.db import get_connection

# ==============================
# Extract
# ==============================
def extract_raw_data():
    """
    Lee todos los datos de raw_weather desde SQL Server
    y devuelve un DataFrame de Polars
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM raw.raw_weather")
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    # Convertir a lista de diccionarios
    data = [dict(zip(columns, row)) for row in rows]

    # Convertir a Polars DataFrame
    df = pl.DataFrame(data)

    cursor.close()
    conn.close()

    return df

# ==============================
# Transform
# ==============================
def transform_daily_metrics(df: pl.DataFrame):
    """
    Hace agregaciones diarias por ciudad usando Polars
    """
    # Convertir timestamp string a datetime
    df = df.with_columns(
        pl.col("raw_weather_time_dt").dt.date().alias("date")

    )


    # Agregar métricas por ciudad y fecha
    df_agg = df.group_by(["raw_weather_city_nv", "date"]).agg([
        pl.col("raw_weather_temperature_d").mean().alias("avg_temperature"),
        pl.col("raw_weather_temperature_d").max().alias("max_temperature"),
        pl.col("raw_weather_precipitation_d").sum().alias("total_precipitation")
    ])

    # Agregar fecha de ingestión
    df_agg = df_agg.with_columns(
        pl.lit(datetime.now()).alias("ingestion_date")
    )

    return df_agg

# ==============================
# Load
# ==============================
def load_to_daily_metrics(df_agg: pl.DataFrame):
    """
    Inserta los datos transformados en daily_weather_metrics
    """
    conn = get_connection()
    cursor = conn.cursor()

    for row in df_agg.iter_rows(named=True):
        cursor.execute("""
            INSERT INTO analytics.daily_weather_metrics
            (   daily_weather_city_nv
                , daily_weather_date_dt
                , daily_weather_avg_temperature_d
                , daily_weather_max_temperature_d
                , daily_weather_total_precipitation_d
                , daily_weather_ingestion_date_dt)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
        row["raw_weather_city_nv"], 
        row["date"], 
        row["avg_temperature"], 
        row["max_temperature"], 
        row["total_precipitation"], 
        row["ingestion_date"]
        )


    conn.commit()
    cursor.close()
    conn.close()
    print(f"{df_agg.height} registros insertados en daily_weather_metrics")

# ==============================
# Main
# ==============================
if __name__ == "__main__":
    print("Extrayendo datos crudos...")
    df_raw = extract_raw_data()

    print("Transformando métricas diarias...")
    df_daily = transform_daily_metrics(df_raw)

    print("Cargando datos en daily_weather_metrics...")
    load_to_daily_metrics(df_daily)

    print("Transformación completa ✅")
