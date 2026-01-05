# src/sources/open_meteo.py

def get_valencia_coordinates():
    """
    Devuelve las coordenadas de Valencia para la API Open-Meteo
    """
    return {
        "city": "Valencia",
        "latitude": 39.4699,
        "longitude": -0.3763,
        "timezone": "Europe/Madrid"
    }

def build_url(coords, variables=["temperature_2m", "precipitation"]):
    """
    Construye la URL de la API Open-Meteo con los parámetros deseados
    """
    base_url = "https://api.open-meteo.com/v1/forecast"
    hourly = ",".join(variables)
    url = (
        f"{base_url}?latitude={coords['latitude']}"
        f"&longitude={coords['longitude']}"
        f"&hourly={hourly}"
        f"&timezone={coords['timezone']}"
    )
    return url
