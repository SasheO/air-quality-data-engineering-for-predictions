select

    {{ dbt_utils.generate_surrogate_key(['time_of_measurement', 'city_coordinates_id']) }} as air_pollution_features_row_id,

    time_of_measurement,
    city_coordinates_id, -- TODO: convert this to hot enconding of cities
    co_concentration,
    no_concentration,
    no2_concentration,
    o3_concentration,
    so2_concentration,
    pm2_5_concentration,
    pm10_concentration,
    nh3_concentration,
    aqi,

-- ============================================================
-- AQI — lags (NULL when no row exactly N hours back)
-- ============================================================
    any(aqi) OVER w_1h   AS aqi_1_hour_past,
    any(aqi) OVER w_3h   AS aqi_3_hour_past,
    any(aqi) OVER w_24h  AS aqi_24_hour_past,
    any(aqi) OVER w_168h AS aqi_168_hour_past,

-- ============================================================
-- AQI — rolling statistics (trailing 6h / 24h / 72h, inclusive of current row)
-- ============================================================
    avg(aqi)        OVER w_roll_6h  AS aqi_6_hour_rolling_mean,
    stddevSamp(aqi) OVER w_roll_6h  AS aqi_6_hour_rolling_stddev,
    max(aqi)        OVER w_roll_6h  AS aqi_6_hour_rolling_max,

    avg(aqi)        OVER w_roll_24h AS aqi_24_hour_rolling_mean,
    stddevSamp(aqi) OVER w_roll_24h AS aqi_24_hour_rolling_stddev,
    max(aqi)        OVER w_roll_24h AS aqi_24_hour_rolling_max,

    avg(aqi)        OVER w_roll_72h AS aqi_72_hour_rolling_mean,
    stddevSamp(aqi) OVER w_roll_72h AS aqi_72_hour_rolling_stddev,
    max(aqi)        OVER w_roll_72h AS aqi_72_hour_rolling_max,

-- ============================================================
-- AQI — rate of change
-- ============================================================
    aqi - any(aqi) OVER w_1h AS aqi_1_hour_delta,
    aqi - any(aqi) OVER w_3h AS aqi_3_hour_delta,

-- ============================================================
-- PM2.5 — lags
-- ============================================================
    any(pm2_5_concentration) OVER w_1h   AS pm2_5_1_hour_past,
    any(pm2_5_concentration) OVER w_3h   AS pm2_5_3_hour_past,
    any(pm2_5_concentration) OVER w_24h  AS pm2_5_24_hour_past,
    any(pm2_5_concentration) OVER w_168h AS pm2_5_168_hour_past,

-- ============================================================
-- PM2.5 — rolling statistics
-- ============================================================
    avg(pm2_5_concentration)        OVER w_roll_6h  AS pm2_5_6_hour_rolling_mean,
    stddevSamp(pm2_5_concentration) OVER w_roll_6h  AS pm2_5_6_hour_rolling_stddev,
    max(pm2_5_concentration)        OVER w_roll_6h  AS pm2_5_6_hour_rolling_max,

    avg(pm2_5_concentration)        OVER w_roll_24h AS pm2_5_24_hour_rolling_mean,
    stddevSamp(pm2_5_concentration) OVER w_roll_24h AS pm2_5_24_hour_rolling_stddev,
    max(pm2_5_concentration)        OVER w_roll_24h AS pm2_5_24_hour_rolling_max,

    avg(pm2_5_concentration)        OVER w_roll_72h AS pm2_5_72_hour_rolling_mean,
    stddevSamp(pm2_5_concentration) OVER w_roll_72h AS pm2_5_72_hour_rolling_stddev,
    max(pm2_5_concentration)        OVER w_roll_72h AS pm2_5_72_hour_rolling_max,

-- ============================================================
-- PM2.5 — rate of change
-- ============================================================
    pm2_5_concentration - any(pm2_5_concentration) OVER w_1h AS pm2_5_1_hour_delta,
    pm2_5_concentration - any(pm2_5_concentration) OVER w_3h AS pm2_5_3_hour_delta,

-- ============================================================
-- PM10 — lags
-- ============================================================
    any(pm10_concentration) OVER w_1h   AS pm10_1_hour_past,
    any(pm10_concentration) OVER w_3h   AS pm10_3_hour_past,
    any(pm10_concentration) OVER w_24h  AS pm10_24_hour_past,
    any(pm10_concentration) OVER w_168h AS pm10_168_hour_past,

-- ============================================================
-- PM10 — rolling statistics
-- ============================================================
    avg(pm10_concentration)        OVER w_roll_6h  AS pm10_6_hour_rolling_mean,
    stddevSamp(pm10_concentration) OVER w_roll_6h  AS pm10_6_hour_rolling_stddev,
    max(pm10_concentration)        OVER w_roll_6h  AS pm10_6_hour_rolling_max,

    avg(pm10_concentration)        OVER w_roll_24h AS pm10_24_hour_rolling_mean,
    stddevSamp(pm10_concentration) OVER w_roll_24h AS pm10_24_hour_rolling_stddev,
    max(pm10_concentration)        OVER w_roll_24h AS pm10_24_hour_rolling_max,

    avg(pm10_concentration)        OVER w_roll_72h AS pm10_72_hour_rolling_mean,
    stddevSamp(pm10_concentration) OVER w_roll_72h AS pm10_72_hour_rolling_stddev,
    max(pm10_concentration)        OVER w_roll_72h AS pm10_72_hour_rolling_max,

-- ============================================================
-- PM10 — rate of change
-- ============================================================
    pm10_concentration - any(pm10_concentration) OVER w_1h AS pm10_1_hour_delta,
    pm10_concentration - any(pm10_concentration) OVER w_3h AS pm10_3_hour_delta,

-- ============================================================
-- Calendar features (UTC)
-- ============================================================
    toHour(time_of_measurement)                        AS hour_of_day,
    toDayOfWeek(time_of_measurement)                   AS day_of_week,   -- ISO: 1=Mon … 7=Sun
    toDayOfWeek(time_of_measurement) IN (6, 7)         AS is_weekend,

-- ============================================================
-- Target — AQI 6 hours ahead (gap-aware; NULL when future row missing)
-- ============================================================
    any(aqi) OVER (
        partition by city_coordinates_id
        ORDER BY time_of_measurement
        RANGE BETWEEN INTERVAL 21600 FOLLOWING AND INTERVAL 21600 FOLLOWING
    ) AS aqi_6_hour_ahead

from {{ ref('stg_weather_raw__air_pollution') }}

window
    w_1h   as (partition by city_coordinates_id order by time_of_measurement range between interval 3600  preceding and interval 3600  preceding),
    w_3h   as (partition by city_coordinates_id order by time_of_measurement range between interval 10800 preceding and interval 10800 preceding),
    w_24h  as (partition by city_coordinates_id order by time_of_measurement range between interval 86400 preceding and interval 86400 preceding),
    w_168h as (partition by city_coordinates_id order by time_of_measurement range between interval 604800 preceding and interval 604800 preceding),
    w_roll_6h  as (partition by city_coordinates_id order by time_of_measurement range between interval 21600 preceding and current row),
    w_roll_24h as (partition by city_coordinates_id order by time_of_measurement range between interval 86400 preceding and current row),
    w_roll_72h as (partition by city_coordinates_id order by time_of_measurement range between interval 259200 preceding and current row)