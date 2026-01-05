drop table if exists analytics.daily_weather_metrics
CREATE TABLE analytics.daily_weather_metrics (
    daily_weather_id_i INT IDENTITY(1,1) PRIMARY KEY,
    daily_weather_city_nv NVARCHAR(100) NOT NULL,
    daily_weather_date_dt DATE NOT NULL,
    daily_weather_avg_temperature_d DECIMAL(5,2) NULL,
    daily_weather_max_temperature_d DECIMAL(5,2) NULL,
    daily_weather_total_precipitation_d DECIMAL(7,2) NULL,
    daily_weather_ingestion_date_dt DATETIME DEFAULT GETDATE()
);