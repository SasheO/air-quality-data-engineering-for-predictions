{% snapshot city_coordinates %}

{{
    config(
      target_schema='weather_features',
      unique_key = 'city_coordinates_id',
      strategy='check',
      check_cols=['city', 'country'],
    )
}}

select 
    city_coordinates_id,
    latitude_coordinate,
    longitude_coordinate,
    city,
    country
from {{ ref('stg_weather_raw__city_coordinates') }}

{% endsnapshot %}
