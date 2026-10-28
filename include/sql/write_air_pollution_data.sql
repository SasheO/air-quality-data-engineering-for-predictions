insert into {{ params.schema_name }}.{{ params.air_quality_table_name }}
select * from {{ params.schema_name }}.{{ params.air_quality_staging_table_name }}
where row_content_hash not in (select row_content_hash from {{ params.schema_name }}.{{ params.air_quality_table_name }})