/* 
lightweight update since only about five rows out of millions updated per run

update the rows between 5:30 min and 6:30 ago which are null on target

*/

update  weather_features.feature_air_pollution
set aqi_6_hour_ahead = (
    CASE 
        {% for lat_and_long_encoding_id, tup in location_to_time_and_aqi_mapping.items() -%}
        WHEN (lat_and_long_encoding_id = '{{ lat_and_long_encoding_id }}' and time_of_measurement >= '{{ tup[0].strftime("%Y-%m-%d %H:%M:%S") }}' - (INTERVAL 6 HOUR + INTERVAL 30 MINUTE) and time_of_measurement < '{{ tup[0].strftime("%Y-%m-%d %H:%M:%S") }}' - (INTERVAL 5 HOUR + INTERVAL 30 MINUTE))
        then {{ tup[1] }}
        {% endfor %}
    else null
    end
)
WHERE aqi_6_hour_ahead is null and time_of_measurement >= '{{min_time_of_measurement}}' - (INTERVAL 6 HOUR + INTERVAL 30 MINUTE)
