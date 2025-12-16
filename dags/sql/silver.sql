-- Trip main details
CREATE TABLE IF NOT EXISTS trip_main_details (
    trip_hashkey String NOT NULL,
    VendorID Nullable(UInt16),
    pickup_datetime DateTime NOT NULL,
    dropoff_datetime DateTime NOT NULL,
    passenger_count Nullable(UInt8),
    trip_distance Nullable(Float32),
    RatecodeID Nullable(UInt16),
    store_and_fwd_flag  Nullable(BOOLEAN),   
    PULocationID UInt16 NOT NULL,
    DOLocationID UInt16 NOT NULL,
    payment_type Nullable(UInt16),
    trip_type_id Nullable(UInt16),
    taxi_type_id UInt8 NOT NULL,
    fare_amount Nullable(Float32),
    total_amount Nullable(Float32),
    trip_duration Nullable(UInt16),
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY trip_hashkey
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (trip_hashkey);

-- Trip financial details
CREATE TABLE IF NOT EXISTS trip_fin_details (
    trip_hashkey String NOT NULL,
    pickup_datetime DateTime NOT NULL,
    extra Nullable(Float32),
    mta_tax Nullable(Float32),
    tip_amount Nullable(Float32),
    tolls_amount Nullable(Float32),
    improvement_surcharge Nullable(Float32),
    congestion_surcharge Nullable(Float32),
    airport_fee Nullable(Float32),
    cbd_congestion_fee Nullable(Float32),
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY trip_hashkey
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (trip_hashkey);

-- Taxi zone details
CREATE TABLE IF NOT EXISTS taxi_zone_details (
    taxi_zone_id UInt16 NOT NULL,
    borough String NOT NULL,
    zone String NOT NULL,
    service_zone String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Payment type details
CREATE TABLE IF NOT EXISTS payment_type_details (
    payment_type_id UInt16 NOT NULL,
    payment_type_name String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Rate details
CREATE TABLE IF NOT EXISTS rate_details (
    rate_id UInt16 NOT NULL,
    rate_name String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Vendor details
CREATE TABLE IF NOT EXISTS vendor_details (
    vendor_id UInt16 NOT NULL,
    vendor_name String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Trip type details
CREATE TABLE IF NOT EXISTS trip_type_details (
    trip_type_id UInt16 NOT NULL,
    trip_type_name String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Taxi type details
CREATE TABLE IF NOT EXISTS taxi_type_details (
    taxi_type_id UInt16 NOT NULL,
    taxi_type_name String NOT NULL,
    file_prefix String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;
