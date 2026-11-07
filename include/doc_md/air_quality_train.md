# Train Air Quality Model

## Overview
Trains an Light GBM model with all data in `weather_features.feature_air_quality` and saves it to minio bucket in a `train-<datestamp>` folder with a metadata description file. It saves models in a dictionary with HMAC singature in "signature" key and the actual model in "payload" key. The model can be reloaded into a python object using the pickle library, but the HMAC signature should always be checked for security purposes.

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