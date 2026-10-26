{% snapshot city_coordinates %}

{{
    config(
      target_schema='weather_features',
      unique_key = 'temp_key',
      strategy='check',
      check_cols=['city', 'country'],
      snapshot_meta_column_names={
        'dbt_valid_from': 'valid_from',
        'dbt_valid_to': 'valid_to',
        'dbt_scd_id': 'city_coordinates_id',
        'dbt_updated_at': 'snapshot_updated_at',
        'dbt_is_deleted': 'is_deleted'
      }
    )
}}

select 
    latitude_coordinate,
    longitude_coordinate,
    city,
    country
from {{ ref('stg_weather_raw__city_coordinates') }}

{% endsnapshot %}
