import logging
logger = logging.getLogger(__name__)

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
                entry["dt"],
                ]
            row_hash = hashlib.md5(json.dumps(output_row).encode('utf-8')).hexdigest()
            output_row.append(dag_run_id)
            output_row.append(row_hash)
            output_rows.append(output_row)
            row_count += 1
    logger.info(f"{row_count} queued for insertion")
