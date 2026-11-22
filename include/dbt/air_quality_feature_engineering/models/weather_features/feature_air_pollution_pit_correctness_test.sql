{{
    config(
        materialized='incremental',
        tags=["pit-correctness-test"]
    )
}}


select

    {{ dbt_utils.generate_surrogate_key(['time_of_measurement', 'city_coordinates_scd_id']) }} as air_pollution_features_row_id,

    time_of_measurement,
    time_of_measurement_utc,
    lat_and_long_encoding_id, 
    co_concentration,
    no_concentration,
    no2_concentration,
    o3_concentration,
    so2_concentration,
    pm2_5_concentration,
    pm10_concentration,
    nh3_concentration,
    aqi,

    any(aqi) OVER w_1h   AS aqi_1_hour_past,
    any(aqi) OVER w_3h   AS aqi_3_hour_past,
    any(aqi) OVER w_24h  AS aqi_24_hour_past,
    any(aqi) OVER w_168h AS aqi_168_hour_past,

    avg(aqi)        OVER w_roll_6h  AS aqi_6_hour_rolling_mean,
    stddevSamp(aqi) OVER w_roll_6h  AS aqi_6_hour_rolling_stddev,
    max(aqi)        OVER w_roll_6h  AS aqi_6_hour_rolling_max,

    avg(aqi)        OVER w_roll_24h AS aqi_24_hour_rolling_mean,
    stddevSamp(aqi) OVER w_roll_24h AS aqi_24_hour_rolling_stddev,
    max(aqi)        OVER w_roll_24h AS aqi_24_hour_rolling_max,

    avg(aqi)        OVER w_roll_72h AS aqi_72_hour_rolling_mean,
    stddevSamp(aqi) OVER w_roll_72h AS aqi_72_hour_rolling_stddev,
    max(aqi)        OVER w_roll_72h AS aqi_72_hour_rolling_max,

    aqi - any(aqi) OVER w_1h AS aqi_1_hour_delta,
    aqi - any(aqi) OVER w_3h AS aqi_3_hour_delta,

    any(pm2_5_concentration) OVER w_1h   AS pm2_5_1_hour_past,
    any(pm2_5_concentration) OVER w_3h   AS pm2_5_3_hour_past,
    any(pm2_5_concentration) OVER w_24h  AS pm2_5_24_hour_past,
    any(pm2_5_concentration) OVER w_168h AS pm2_5_168_hour_past,

    avg(pm2_5_concentration)        OVER w_roll_6h  AS pm2_5_6_hour_rolling_mean,
    stddevSamp(pm2_5_concentration) OVER w_roll_6h  AS pm2_5_6_hour_rolling_stddev,
    max(pm2_5_concentration)        OVER w_roll_6h  AS pm2_5_6_hour_rolling_max,

    avg(pm2_5_concentration)        OVER w_roll_24h AS pm2_5_24_hour_rolling_mean,
    stddevSamp(pm2_5_concentration) OVER w_roll_24h AS pm2_5_24_hour_rolling_stddev,
    max(pm2_5_concentration)        OVER w_roll_24h AS pm2_5_24_hour_rolling_max,

    avg(pm2_5_concentration)        OVER w_roll_72h AS pm2_5_72_hour_rolling_mean,
    stddevSamp(pm2_5_concentration) OVER w_roll_72h AS pm2_5_72_hour_rolling_stddev,
    max(pm2_5_concentration)        OVER w_roll_72h AS pm2_5_72_hour_rolling_max,

    pm2_5_concentration - any(pm2_5_concentration) OVER w_1h AS pm2_5_1_hour_delta,
    pm2_5_concentration - any(pm2_5_concentration) OVER w_3h AS pm2_5_3_hour_delta,

    any(pm10_concentration) OVER w_1h   AS pm10_1_hour_past,
    any(pm10_concentration) OVER w_3h   AS pm10_3_hour_past,
    any(pm10_concentration) OVER w_24h  AS pm10_24_hour_past,
    any(pm10_concentration) OVER w_168h AS pm10_168_hour_past,

    avg(pm10_concentration)        OVER w_roll_6h  AS pm10_6_hour_rolling_mean,
    stddevSamp(pm10_concentration) OVER w_roll_6h  AS pm10_6_hour_rolling_stddev,
    max(pm10_concentration)        OVER w_roll_6h  AS pm10_6_hour_rolling_max,

    avg(pm10_concentration)        OVER w_roll_24h AS pm10_24_hour_rolling_mean,
    stddevSamp(pm10_concentration) OVER w_roll_24h AS pm10_24_hour_rolling_stddev,
    max(pm10_concentration)        OVER w_roll_24h AS pm10_24_hour_rolling_max,

    avg(pm10_concentration)        OVER w_roll_72h AS pm10_72_hour_rolling_mean,
    stddevSamp(pm10_concentration) OVER w_roll_72h AS pm10_72_hour_rolling_stddev,
    max(pm10_concentration)        OVER w_roll_72h AS pm10_72_hour_rolling_max,

    pm10_concentration - any(pm10_concentration) OVER w_1h AS pm10_1_hour_delta,
    pm10_concentration - any(pm10_concentration) OVER w_3h AS pm10_3_hour_delta,

    toHour(time_of_measurement)                        AS hour_of_day,
    toDayOfWeek(time_of_measurement)                   AS day_of_week,   -- ISO: 1=Mon … 7=Sun
    toDayOfWeek(time_of_measurement) IN (6, 7)         AS is_weekend,
    toMonth(time_of_measurement)                        AS month_of_the_year,
    multiIf(
        -- Tropical / Equatorial Zone (Wet vs. Dry)
        abs(city_coordinates.latitude_coordinate) <= 23.5, 
        multiIf(
            month_of_the_year IN (5, 6, 7, 8, 9, 10), 'Rainy Season',
            'Dry Season'
        ),
        
        -- Northern Hemisphere (Conventional Seasons)
        city_coordinates.latitude_coordinate > 23.5,
        multiIf(
            month_of_the_year IN (12, 1, 2), 'Winter',
            month_of_the_year IN (3, 4, 5),  'Spring',
            month_of_the_year IN (6, 7, 8),  'Summer',
            'Autumn'
        ),
        
        -- Southern Hemisphere (Conventional Seasons - Inverted)
        city_coordinates.latitude_coordinate < -23.5,
        multiIf(
            month_of_the_year IN (12, 1, 2), 'Summer',
            month_of_the_year IN (3, 4, 5),  'Autumn',
            month_of_the_year IN (6, 7, 8),  'Winter',
            'Spring'
        ),
        'Unknown'
    ) AS season,

    any(aqi) OVER (
        partition by lat_and_long_encoding_id
        ORDER BY time_of_measurement
        RANGE between 21300 FOLLOWING and 21900 FOLLOWING -- approximate value with 5 minute grace period
    ) AS aqi_6_hour_ahead

from {{ ref('stg_weather_raw__air_pollution') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  -- however, a greater than means retrospective backfills will be missed. this is something to consider 
where time_of_measurement > (select coalesce(max(time_of_measurement_utc),'1970-01-01') from {{ this }} )

{% endif %}

window
    window
    w_1h   as (partition by lat_and_long_encoding_id order by time_of_measurement range between 3300   preceding and 3900   preceding), -- approximate hour with 5 minute grace window
    w_3h   as (partition by lat_and_long_encoding_id order by time_of_measurement range between 10500  preceding and 11100  preceding),
    w_24h  as (partition by lat_and_long_encoding_id order by time_of_measurement range between 86100  preceding and 86700  preceding),
    w_168h as (partition by lat_and_long_encoding_id order by time_of_measurement range between 604500 preceding and 605100 preceding),
    w_roll_6h  as (partition by lat_and_long_encoding_id order by time_of_measurement range between 11100  preceding and 11100  following),
    w_roll_24h as (partition by lat_and_long_encoding_id order by time_of_measurement range between 43500  preceding and 43500  following),
    w_roll_72h as (partition by lat_and_long_encoding_id order by time_of_measurement range between 129900 preceding and 129900 following)