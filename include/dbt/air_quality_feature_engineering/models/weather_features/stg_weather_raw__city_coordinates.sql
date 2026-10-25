select 
    {{ dbt_utils.generate_surrogate_key(['latitude_coordinate', 'longitude_coordinate']) }} as city_coordinates_id,
    latitude_coordinate,
    longitude_coordinate,
    city,
    country
from {{ source('weather_raw', 'city_coordinates') }}
