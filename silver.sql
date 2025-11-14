-- HUBS

-- Trip
CREATE TABLE IF NOT EXISTS hub_trip (
    trip_hashkey String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY trip_hashkey
ORDER BY trip_hashkey;

-- Taxi Zone
CREATE TABLE IF NOT EXISTS hub_taxi_zone (
    zone_id UInt16 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Payment type
CREATE TABLE IF NOT EXISTS hub_payment_type (
    payment_type_id UInt16 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Rate
CREATE TABLE IF NOT EXISTS hub_rate (
    rate_id UInt16 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Vendor
CREATE TABLE IF NOT EXISTS hub_vendor (
    vendor_id UInt8 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;


-- Satellites

-- Trip details
CREATE TABLE IF NOT EXISTS sat_trip_details (
    trip_hashkey String NOT NULL,
    tpep_pickup_datetime DateTime NOT NULL,
    tpep_dropoff_datetime DateTime NOT NULL,
    passenger_count UInt8,
    trip_distance Float32,
    store_and_fwd_flag  BOOLEAN,   
    fare_amount Float32,
    extra Float32,
    mta_tax Float32,
    tip_amount Float32,
    tolls_amount Float32,
    improvement_surcharge Float32,
    total_amount Float32,
    congestion_surcharge Float32,
    airport_fee Float32,
    cbd_congestion_fee Float32,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(tpep_pickup_datetime)
ORDER BY (trip_hashkey);

-- Taxi zone details
CREATE TABLE IF NOT EXISTS sat_taxi_zone_details (
    taxi_zone_id UInt16 NOT NULL,
    borough String NOT NULL,
    zone String NOT NULL,
    service_zone String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Payment type details
CREATE TABLE IF NOT EXISTS sat_payment_type_details (
    payment_type_id UInt16 NOT NULL,
    payment_type_name String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Rate details
CREATE TABLE IF NOT EXISTS sat_rate_details (
    rate_id UInt16 NOT NULL,
    rate_name String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;

-- Vendor details
CREATE TABLE IF NOT EXISTS sat_vendor_details (
    vendor_id UInt16 NOT NULL,
    vendor_name String NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = TinyLog;


-- Links

-- Trip to taxi zones
CREATE TABLE IF NOT EXISTS link_trip_taxi_zones (
    trip_hashkey String NOT NULL,
    pickup_zone_id UInt16 NOT NULL,
    dropoff_zone_id UInt16 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree()
ORDER BY (trip_hashkey, pickup_zone_id, dropoff_zone_id);

-- Trip to payment type
CREATE TABLE IF NOT EXISTS link_trip_payment (
    trip_hashkey String NOT NULL,
    payment_type_id UInt16 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree()
ORDER BY (trip_hashkey, payment_type_id);

-- Trip to rate
CREATE TABLE IF NOT EXISTS link_trip_rate (
    trip_hashkey String NOT NULL,
    rate_id UInt16 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (trip_hashkey, rate_id);

-- Trip to vendor
CREATE TABLE IF NOT EXISTS link_trip_vendor (
    trip_hashkey String NOT NULL,
    vendor_id UInt8 NOT NULL,
    load_date Datetime DEFAULT now(),
    record_source LowCardinality(String) NOT NULL
)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (trip_hashkey, vendor_id);