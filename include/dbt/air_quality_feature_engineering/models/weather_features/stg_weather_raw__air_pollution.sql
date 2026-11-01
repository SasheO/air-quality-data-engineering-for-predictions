with air_pollution_raw as 
(
    select 
        longitude_coordinate,
        latitude_coordinate,
        co_concentration,
        no_concentration,
        no2_concentration,
        o3_concentration,
        so2_concentration,
        pm2_5_concentration,
        pm10_concentration,
        nh3_concentration,
        aqi,
        time_of_measurement
    from {{ source('weather_raw', 'air_pollution') }}
),
city_coordinates as (
    select 
        longitude_coordinate,
        latitude_coordinate,
        dbt_scd_id as city_coordinates_scd_id,
        lat_and_long_encoding_id
    from {{ ref('city_coordinates') }} -- the snapshot table showing the most recent names of the places
    where (
        (dbt_valid_to is null or {{ dbt.current_timestamp() }} <= dbt_valid_to) 
        and {{ dbt.current_timestamp() }} >= dbt_valid_from
    )

)
select 
    city_coordinates.longitude_coordinate,
    city_coordinates.latitude_coordinate,
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
    time_of_measurement,
    city_coordinates_scd_id
from air_pollution_raw
join city_coordinates
using(longitude_coordinate, latitude_coordinate)
