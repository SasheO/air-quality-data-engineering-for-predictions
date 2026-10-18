with deduplicated as (
    select 
        city,
        country,
        latitude_coordinate,
        longitude_coordinate,
        row_number() over (partition by (city, country) ORDER by created_at desc) as rn
    FROM {{params.schema_name}}.{{params.city_coordinates_table_name}}
)
select 
    city,
    country,
    latitude_coordinate,
    longitude_coordinate
from deduplicated
where rn = 1