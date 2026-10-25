create table if not exists {{ params.schema_name }}.{{ params.raw_table_name }} (
    longitude_coordinate Decimal(10, 7), 
    latitude_coordinate Decimal(10, 7),
    city_coordinates_id UInt64,
    co_concentration Nullable(Decimal(8, 3)),
    no_concentration Nullable(Decimal(6, 3)), 
    no2_concentration Nullable(Decimal(6, 3)), 
    o3_concentration Nullable(Decimal(6, 3)), 
    so2_concentration Nullable(Decimal(6, 3)), 
    pm2_5_concentration Nullable(Decimal(6, 3)),
    pm10_concentration Nullable(Decimal(6, 3)), 
    nh3_concentration Nullable(Decimal(6, 3)), 
    aqi Nullable(UInt8), 
    time_of_measurement DateTime,
    created_at DateTime DEFAULT now(),
    inserted_by Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (longitude_coordinate, latitude_coordinate, time_of_measurement);


create table if not exists {{ params.schema_name }}.{{ params.city_coordinates_table_name }} (
    city String,
    country FixedString(2), 
    longitude_coordinate Decimal(10, 7), 
    latitude_coordinate Decimal(10, 7),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (longitude_coordinate, latitude_coordinate);