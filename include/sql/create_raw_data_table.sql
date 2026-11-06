create table if not exists {{ params.schema_name }}.{{ params.air_quality_table_name }} (
    longitude_coordinate Decimal(10, 7), 
    latitude_coordinate Decimal(10, 7),
    co_concentration Nullable(Float32),
    no_concentration Nullable(Float32), 
    no2_concentration Nullable(Float32), 
    o3_concentration Nullable(Float32), 
    so2_concentration Nullable(Float32), 
    pm2_5_concentration Nullable(Float32),
    pm10_concentration Nullable(Float32), 
    nh3_concentration Nullable(Float32), 
    aqi Nullable(UInt8), 
    time_of_measurement DateTime,
    created_at DateTime DEFAULT now(),
    inserted_by Nullable(String),
    row_content_hash String
)
ENGINE = MergeTree()
ORDER BY (time_of_measurement, longitude_coordinate, latitude_coordinate);

create table if not exists {{ params.schema_name }}.{{ params.city_coordinates_table_name }} (
    city String,
    country FixedString(2), 
    longitude_coordinate Decimal(10, 7), 
    latitude_coordinate Decimal(10, 7),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (longitude_coordinate, latitude_coordinate);