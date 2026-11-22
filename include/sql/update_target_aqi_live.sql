/*
    Lightweight back-fill of aqi_6_hour_ahead for rows whose future observation
    arrived after the row was first written (num_city rows per run).

    Matches rows on the same location whose time_of_measurement falls in the
    ±5-minute grace band around (anchor_time - 6 hours)

    Only NULL targets within the last (6h + 5m) slice are touched.
*/
update weather_features.feature_air_pollution
set aqi_6_hour_ahead = (
    CASE 
        {% for lat_and_long_encoding_id, tup in location_to_time_and_aqi_mapping.items() -%}
        WHEN (lat_and_long_encoding_id = '{{ lat_and_long_encoding_id }}' and time_of_measurement >= parseDateTimeBestEffort('{{ tup[0] }}') - (INTERVAL 6 HOUR + INTERVAL 5 MINUTE) and time_of_measurement < parseDateTimeBestEffort('{{ tup[0] }}') - (INTERVAL 5 HOUR + INTERVAL 5 MINUTE))
        then {{ tup[1] }}
        {% endfor %}
    else null
    end
)
WHERE aqi_6_hour_ahead is null and time_of_measurement >= parseDateTimeBestEffort('{{min_time_of_measurement}}') - (INTERVAL 6 HOUR + INTERVAL 5 MINUTE)