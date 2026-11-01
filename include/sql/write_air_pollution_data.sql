insert into {{ params.schema_name }}.{{ params.air_quality_table_name }}
with
    (
        select max(time_of_measurement) from {{ params.schema_name }}.{{ params.air_quality_staging_table_name }}
    ) as max_measurement_time,
    (
        select min(time_of_measurement) from {{ params.schema_name }}.{{ params.air_quality_staging_table_name }}
    ) as min_measurement_time
select * from {{ params.schema_name }}.{{ params.air_quality_staging_table_name }}
where row_content_hash not in (
        select 
            row_content_hash
        from {{ params.schema_name }}.{{ params.air_quality_table_name }}
        where time_of_measurement between min_measurement_time and max_measurement_time
    )