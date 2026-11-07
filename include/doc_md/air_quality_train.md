# Train Air Quality Model

## Overview
Trains an Light GBM model with all data in `weather_features.feature_air_quality` and saves it to minio bucket in a `train-<datestamp>` folder with a metadata description file. Models are hmac signed for extra security since the python pickle library is used.

## Schedule
- None — (ony when triggered)

## DAG Task Dependencies
TODO


## Tasks

### `task-1`
TODO

## Required Variables/Connections
| Connection | Description |
|---|---|
| `minio_conn` | Minio connection used for writing pickled model to object storage. |