create table if not exists {{ params.schema_name }}.{{ params.raw_table_name }} (
    longitude_coordinate Decimal(10, 7), 
    latitude_coordinate Decimal(10, 7),
    city_coordinates_id UInt64,
    co_concentration Decimal(8, 3),
    no_concentration Decimal(6, 3), 
    no2_concentration Decimal(6, 3), 
    o3_concentration Decimal(6, 3), 
    so2_concentration Decimal(6, 3), 
    pm2_5_concentration Decimal(6, 3),
    pm10_concentration Decimal(6, 3), 
    nh3_concentration Decimal(6, 3), 
    aqi UInt8, 
    `datetime` DateTime,
    created_at DateTime DEFAULT now(),
    inserted_by Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (longitude_coordinate, latitude_coordinate, `datetime`);

create table if not exists {{ params.schema_name }}.{{ params.city_coordinates_table_name }} (
    id UInt64 DEFAULT generateSerialID('my_database.orders_autonumber.id'),
    city String,
    country FixedString(2), 
    longitude_coordinate Decimal(10, 7), 
    latitude_coordinate Decimal(10, 7),
    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (country, city);