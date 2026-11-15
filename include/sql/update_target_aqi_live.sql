/* 
lightweight update since only about five rows out of millions updated per run

update the rows between 5:30 min and 6:30 ago which are null on target

*/

update  weather_features.feature_air_pollution
set aqi_6_hour_ahead = {{}}

WHERE aqi_6_hour_ahead is null
and time_of_measurement >= (now() -  (INTERVAL 6 HOUR + INTERVAL 30 MINUTE)) 
and time_of_measurement < (now() -  (INTERVAL 5 HOUR + INTERVAL 30 MINUTE))
and lat_and_long_encoding_id = {{ ?? }}