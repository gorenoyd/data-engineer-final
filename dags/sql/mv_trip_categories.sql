CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trip_categories
ENGINE = ReplacingMergeTree()
ORDER BY (trip_hashkey)
POPULATE AS

SELECT ft.trip_hashkey,
    -- Trips to airports and back
    (tz1.is_airport OR tz2.is_airport) AS is_airport,
    -- Trips during peak hours
    ((dt.`hour` BETWEEN 8 AND 9) OR (dt.`hour` BETWEEN 18 AND 19)) AS is_peak_hour,
    -- Trips on weekends
    (dt.is_weekend OR dt.is_holiday) AS is_weekend,
    -- Trips inside one taxi zone
    (ft.PULocationID = ft.DOLocationID) AS is_intra_zone,
    -- Trips between boroughs
    (tz1.borough != tz2.borough) AS is_inter_borough
    FROM fact_trip ft
        INNER JOIN dim_datetime dt
            ON ft.pickup_datetime_id = dt.datetime_id
        INNER JOIN dim_taxi_zone tz1
            ON ft.PULocationID = tz1.taxi_zone_id
        INNER JOIN dim_taxi_zone tz2
            ON ft.DOLocationID = tz2.taxi_zone_id;
