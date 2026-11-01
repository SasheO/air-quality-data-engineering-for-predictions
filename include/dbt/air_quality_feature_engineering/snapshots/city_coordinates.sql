{% snapshot city_coordinates %}

{{
    config(
      target_schema='weather_features',
      unique_key = 'lat_and_long_encoding_id',
      strategy='check',
      check_cols=['city', 'country'],
    )
}}

select 
    *
from {{ ref('stg_weather_raw__city_coordinates') }}

{% endsnapshot %}
