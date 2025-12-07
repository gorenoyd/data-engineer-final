-- Trip facts
CREATE TABLE IF NOT EXISTS fact_trip (
    trip_hashkey String NOT NULL,
    pickup_datetime_id BIGINT NOT NULL,
    dropoff_datetime_id BIGINT NOT NULL,
    PULocationID UInt16 NOT NULL,
    DOLocationID UInt16 NOT NULL,
    payment_type Nullable(UInt16),
    distance_bucket_id Nullable(UInt16),
    duration_bucket_id Nullable(UInt16),
    pickup_datetime Datetime NOT NULL,
    dropoff_datetime Datetime NOT NULL,
    passenger_count Nullable(UInt8),
    trip_distance Nullable(Float32),
    trip_duration Nullable(UInt16),
    fare_amount Nullable(Float32),
    total_amount Nullable(Float32),
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY trip_hashkey
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (trip_hashkey);

-- Taxi zone dimension
CREATE TABLE IF NOT EXISTS dim_taxi_zone (
    taxi_zone_id UInt16 NOT NULL,
    borough String NOT NULL,
    zone String NOT NULL,
    is_airport BOOLEAN NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Payment type dimension
CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_id UInt16 NOT NULL,
    payment_type_name String NOT NULL,
    is_cash BOOLEAN,
    is_digital BOOLEAN,
    is_dispute BOOLEAN,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Date and time dimension
CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_id BIGINT NOT NULL,
    date Date NOT NULL,
    year UInt16 NOT NULL,
    month UInt8 NOT NULL,
    day_of_month UInt8 NOT NULL,
    day_of_week UInt8 NOT NULL,
    hour UInt8 NOT NULL,
    minute UInt8 NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY datetime_id
PARTITION BY toYYYYMM(date)
ORDER BY (datetime_id);

-- Distance buckets
CREATE TABLE IF NOT EXISTS dim_distance (
    distance_bucket_id UInt16 NOT NULL,
    distance_bucket_name String NOT NULL,
    min_distance Float32 NOT NULL,
    max_distance Float32 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Duration buckets
CREATE TABLE IF NOT EXISTS dim_duration (
    duration_bucket_id UInt16 NOT NULL,
    duration_bucket_name String NOT NULL,
    min_duration UInt16 NOT NULL,
    max_duration UInt16 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;
