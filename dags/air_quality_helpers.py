import logging
logger = logging.getLogger(__name__)

from timezonefinder import TimezoneFinder
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd

# Initialize once and reuse across requests for better performance
tf = TimezoneFinder()

RAW_TABLE_COLUMNS = [
    "longitude_coordinate",
    "latitude_coordinate",
    "co_concentration",
    "no_concentration",
    "no2_concentration",
    "o3_concentration",
    "so2_concentration",
    "pm2_5_concentration",
    "pm10_concentration",
    "nh3_concentration",
    "aqi",
    "time_of_measurement",
    "time_of_measurement_utc",
    "inserted_by",
    # created_at not included
    "row_content_hash",
]

def utc_to_local(utc_dt: datetime, lat: float, lng: float) -> int:
    '''
    converts a utc date time to a local time based on a given latitude and longitude coordinates
    
    returns a timestamp which when converted naively will give the local timezone (since the)
    '''
    # Look up the IANA time zone string from coordinates
    tz_name = tf.timezone_at(lat=lat, lng=lng)
    if not tz_name:
        raise ValueError("Time zone could not be determined for these coordinates.")
    
    # convert time to local timezone
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(ZoneInfo(tz_name))

def build_rows(city_dict: dict[str, str], payload: dict, dag_run_id: str, output_rows: list[tuple]) -> None:
    '''
    modifies output_rows in place to contain insert ready rows with data from city_dict and json payload
    based on fields in RAW_TABLE_COLUMNS
    '''
    import hashlib, json
    lat = float(city_dict["latitude_coordinate"])
    lon = float(city_dict["longitude_coordinate"])

    row_count = 0

    for entry in payload.get("list", []):
        comp = entry.get("components", {}) or {}

        aqi = (entry.get("main") or {}).get("aqi")
        try:
            aqi = int(aqi)
        except Exception as e:
            logger.warning(f"aqi {aqi} is not a number, failing safe to aqi=0: {e}")
            aqi = 0 # 0 is an error row because aqi goes from 1-5

        if comp:
            utc_time = datetime.fromtimestamp(entry["dt"], tz=timezone.utc)
            local_time = utc_to_local(utc_time, lat, lon)
            output_row = [lon,
                lat,
                comp.get("co"),
                comp.get("no"),
                comp.get("no2"),
                comp.get("o3"),
                comp.get("so2"),
                comp.get("pm2_5"),
                comp.get("pm10"),
                comp.get("nh3"),
                aqi,
                int(local_time.replace(tzinfo=None).timestamp()), # unix timestamp format but not in UTC i.e. when converted timezone unawre, it will return the right local time
                entry["dt"], # utc time as a unix time stamp
                ]
            row_hash = hashlib.md5(json.dumps(output_row).encode('utf-8')).hexdigest()
            output_row.append(dag_run_id)
            output_row.append(row_hash)
            output_rows.append(output_row)
            row_count += 1
    logger.info(f"{row_count} queued for insertion")

def transform_feature_rows_for_model_readiness(air_pollution_features_df: pd.DataFrame) -> tuple:
    '''
    applies one hot encoding to lat_and_long_encoding_id and season
    drops air_pollution_features_row_id, time_of_measurement_utc, time_of_measurement

    returns (X, Y):
        X: pd.DataFrame of X features
        Y: pd.Series of y features
    '''
    # Appy one-hot encoding per city and season. remove air_pollution_features_row_id. remove time of measurement. 
    air_pollution_features_df = pd.get_dummies(air_pollution_features_df, columns=['lat_and_long_encoding_id'], dtype=int)
    air_pollution_features_df = pd.get_dummies(air_pollution_features_df, columns=['season'], dtype=int)
    air_pollution_features_df=air_pollution_features_df.drop(columns="air_pollution_features_row_id")
    air_pollution_features_df=air_pollution_features_df.drop(columns="time_of_measurement_utc")
    air_pollution_features_df=air_pollution_features_df.drop(columns="time_of_measurement") # relevant time feature

    ## get training and test features and targets
    Y=air_pollution_features_df["aqi_6_hour_ahead"]
    X=air_pollution_features_df.drop(columns="aqi_6_hour_ahead")

    return X, Y