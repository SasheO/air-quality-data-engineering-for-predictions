select 

    {{ dbt_utils.generate_surrogate_key(['time_of_measurement', 'city_coordinates_id']) }} as air_pollution_features_row_id, -- air_pollution_features_row_id integer [primary key, note: "surrogate key for time_of_measurement + city_coordinates_id composite key"]
    
    time_of_measurement,
    city_coordinates_id, 
    co_concentration,
    no_concentration,
    no2_concentration,
    o3_concentration,
    so2_concentration,
    pm2_5_concentration,
    pm10_concentration,
    nh3_concentration,
    aqi,

--   // ============================================================
--   // AQI — lags
--   // ============================================================
    lagInFrame(aqi, 1) OVER (
            partition by city_coordinates_id
            ORDER BY time_of_measurement 
            RANGE BETWEEN INTERVAL 3600 PRECEDING AND CURRENT ROW
        ) AS aqi_1_hour_past,
--   aqi_1_hour_past integer [note: "AQI 1 hour before time_of_measurement"]
--   aqi_3_hour_past integer [note: "AQI 3 hours before time_of_measurement"]
--   aqi_24_hour_past integer [note: "AQI 24 hours before time_of_measurement"]
--   aqi_168_hour_past integer [note: "AQI 168 hours (7 days) before time_of_measurement"]

--   // ============================================================
--   // AQI — rolling statistics (6h / 24h / 72h trailing)
--   // ============================================================
    avg(aqi) OVER (
        partition by city_coordinates_id
        ORDER BY time_of_measurement 
        RANGE BETWEEN INTERVAL 21600 PRECEDING AND CURRENT ROW
    ) AS aqi_6_hour_rolling_mean,
--   aqi_6_hour_rolling_mean decimal(6, 3) [note: "Trailing 6-hour mean of AQI"]
    stddevSamp(aqi) OVER (
        partition by city_coordinates_id
        ORDER BY time_of_measurement 
        RANGE BETWEEN INTERVAL 21600 PRECEDING AND CURRENT ROW
    ) AS aqi_6_hour_rolling_stddev,
--   aqi_6_hour_rolling_stddev decimal(6, 3) [note: "Trailing 6-hour standard deviation of AQI"]
    max(aqi) OVER (
        partition by city_coordinates_id
        ORDER BY time_of_measurement 
        RANGE BETWEEN INTERVAL 21600 PRECEDING AND CURRENT ROW
    ) AS aqi_6_hour_rolling_max,
--   aqi_6_hour_rolling_max integer [note: "Trailing 6-hour max of AQI"]

--   aqi_24_hour_rolling_mean decimal(6, 3) [note: "Trailing 24-hour mean of AQI"]
--   aqi_24_hour_rolling_stddev decimal(6, 3) [note: "Trailing 24-hour standard deviation of AQI"]
--   aqi_24_hour_rolling_max integer [note: "Trailing 24-hour max of AQI"]

--   aqi_72_hour_rolling_mean decimal(6, 3) [note: "Trailing 72-hour mean of AQI"]
--   aqi_72_hour_rolling_stddev decimal(6, 3) [note: "Trailing 72-hour standard deviation of AQI"]
--   aqi_72_hour_rolling_max integer [note: "Trailing 72-hour max of AQI"]

--   // ============================================================
--   // AQI — rate of change
--   // ============================================================
--   aqi_1_hour_delta integer [note: "AQI(t) − AQI(t−1h)"]
--   aqi_3_hour_delta integer [note: "AQI(t) − AQI(t−3h)"]

--   // ============================================================
--   // PM2.5 — lags
--   // ============================================================
--   pm2_5_1_hour_past decimal(6, 3) [note: "PM₂.₅ concentration 1 hour before time_of_measurement, µg/m³"]
--   pm2_5_3_hour_past decimal(6, 3) [note: "PM₂.₅ concentration 3 hours before time_of_measurement, µg/m³"]
--   pm2_5_24_hour_past decimal(6, 3) [note: "PM₂.₅ concentration 24 hours before time_of_measurement, µg/m³"]
--   pm2_5_168_hour_past decimal(6, 3) [note: "PM₂.₅ concentration 168 hours before time_of_measurement, µg/m³"]

--   // ============================================================
--   // PM2.5 — rolling statistics
--   // ============================================================
--   pm2_5_6_hour_rolling_mean decimal(6, 3) [note: "Trailing 6-hour mean of PM₂.₅, µg/m³"]
--   pm2_5_6_hour_rolling_stddev decimal(6, 3) [note: "Trailing 6-hour standard deviation of PM₂.₅"]
--   pm2_5_6_hour_rolling_max decimal(6, 3) [note: "Trailing 6-hour max of PM₂.₅"]

--   pm2_5_24_hour_rolling_mean decimal(6, 3) [note: "Trailing 24-hour mean of PM₂.₅, µg/m³"]
--   pm2_5_24_hour_rolling_stddev decimal(6, 3) [note: "Trailing 24-hour standard deviation of PM₂.₅"]
--   pm2_5_24_hour_rolling_max decimal(6, 3) [note: "Trailing 24-hour max of PM₂.₅"]

--   pm2_5_72_hour_rolling_mean decimal(6, 3) [note: "Trailing 72-hour mean of PM₂.₅, µg/m³"]
--   pm2_5_72_hour_rolling_stddev decimal(6, 3) [note: "Trailing 72-hour standard deviation of PM₂.₅"]
--   pm2_5_72_hour_rolling_max decimal(6, 3) [note: "Trailing 72-hour max of PM₂.₅"]

--   // ============================================================
--   // PM2.5 — rate of change
--   // ============================================================
--   pm2_5_1_hour_delta decimal(6, 3) [note: "PM₂.₅(t) − PM₂.₅(t−1h), µg/m³"]
--   pm2_5_3_hour_delta decimal(6, 3) [note: "PM₂.₅(t) − PM₂.₅(t−3h), µg/m³"]

--   // ============================================================
--   // PM10 — lags
--   // ============================================================
--   pm10_1_hour_past decimal(6, 3) [note: "PM₁₀ concentration 1 hour before time_of_measurement, µg/m³"]
--   pm10_3_hour_past decimal(6, 3) [note: "PM₁₀ concentration 3 hours before time_of_measurement, µg/m³"]
--   pm10_24_hour_past decimal(6, 3) [note: "PM₁₀ concentration 24 hours before time_of_measurement, µg/m³"]
--   pm10_168_hour_past decimal(6, 3) [note: "PM₁₀ concentration 168 hours before time_of_measurement, µg/m³"]

--   // ============================================================
--   // PM10 — rolling statistics
--   // ============================================================
--   pm10_6_hour_rolling_mean decimal(6, 3) [note: "Trailing 6-hour mean of PM₁₀, µg/m³"]
--   pm10_6_hour_rolling_stddev decimal(6, 3) [note: "Trailing 6-hour standard deviation of PM₁₀"]
--   pm10_6_hour_rolling_max decimal(6, 3) [note: "Trailing 6-hour max of PM₁₀"]

--   pm10_24_hour_rolling_mean decimal(6, 3) [note: "Trailing 24-hour mean of PM₁₀, µg/m³"]
--   pm10_24_hour_rolling_stddev decimal(6, 3) [note: "Trailing 24-hour standard deviation of PM₁₀"]
--   pm10_24_hour_rolling_max decimal(6, 3) [note: "Trailing 24-hour max of PM₁₀"]

--   pm10_72_hour_rolling_mean decimal(6, 3) [note: "Trailing 72-hour mean of PM₁₀, µg/m³"]
--   pm10_72_hour_rolling_stddev decimal(6, 3) [note: "Trailing 72-hour standard deviation of PM₁₀"]
--   pm10_72_hour_rolling_max decimal(6, 3) [note: "Trailing 72-hour max of PM₁₀"]

--   // ============================================================
--   // PM10 — rate of change
--   // ============================================================
--   pm10_1_hour_delta decimal(6, 3) [note: "PM₁₀(t) − PM₁₀(t−1h), µg/m³"]
--   pm10_3_hour_delta decimal(6, 3) [note: "PM₁₀(t) − PM₁₀(t−3h), µg/m³"]

--   // ============================================================
--   // Calendar features
--   // ============================================================
--   hour_of_day tinyint [not null, note: "UTC hour of time_of_measurement, 0–23"]
--   day_of_week tinyint [not null, note: "UTC day of week, 1 = Monday … 7 = Sunday (ISO)"]
--   is_weekend boolean [not null, note: "True when day_of_week ∈ {6, 7}"]

--   // ============================================================
--   // Target
--   // ============================================================
--   aqi_6_hour_ahead integer [note: "AQI 6 hours after time_of_measurement; NULL where future value is unavailable (latest rows)"]

--   target_aqi_6_hour_ahead integer [note: "AQI 6 hours after time_of_measurement; NULL where future value is unavailable (latest rows)"]
from {{ source('weather_raw', 'city_coordinates') }}
