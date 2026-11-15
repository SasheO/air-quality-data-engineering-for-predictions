CREATE TABLE IF NOT EXISTS ml_model_stats
(
    -- Identity / classification
    model_name       LowCardinality(String)   COMMENT 'Human-readable model name',
    model_type       LowCardinality(String)   COMMENT 'Algorithm family: xgboost, regressor, etc.',
    model_class      LowCardinality(String)   COMMENT 'What the model is used for e.g air_quality for air quality prediction models,',
    key              String                   COMMENT 'Unique storage key (s3 path, registry id, etc.)',
    loss_value       Float64                  COMMENT 'Primary loss/metric value',
    loss_function    LowCardinality(String)   COMMENT 'e.g. mse, logloss, mae',
    entry_timestamp  DateTime64(3, 'UTC')     COMMENT 'When this row was recorded (ms precision)',
    details_json     String                   COMMENT 'Raw JSON blob (use JSONExtract* to query)',
    first_entry      Bool DEFAULT false                     COMMENT 'true only for the initial training entry per key'
)
ENGINE = MergeTree
ORDER BY (key, entry_timestamp)
SETTINGS index_granularity = 8192;