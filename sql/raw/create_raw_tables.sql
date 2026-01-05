
Drop table if exists raw.raw_weather

create table raw.raw_weather(
    raw_weather_id_i INT IDENTITY(1,1) PRIMARY KEY,
    raw_weather_city_nv NVARCHAR(100) NOT NULL,
    raw_weather_latitude_d DECIMAL(8,5) NOT NULL,
    raw_weather_longitude_d DECIMAL(8,5) NOT NULL,
    raw_weather_time_dt DATETIME NOT NULL,
    raw_weather_temperature_d DECIMAL(5,2) NULL,
    raw_weather_precipitation_d DECIMAL(5,2) NULL,
    raw_weather_ingestion_date_dt DATETIME DEFAULT GETDATE()
)