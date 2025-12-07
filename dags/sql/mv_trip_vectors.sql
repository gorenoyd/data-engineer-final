CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trip_vectors
ENGINE = SummingMergeTree()
ORDER BY (vector, `hour`, `minute`)
POPULATE AS

SELECT CAST(concat(tz1.borough, '.', tz1.`zone`, '->', tz2.borough, '.', tz2.`zone`) AS LowCardinality(String)) AS vector, dt.`hour`, dt.`minute`, count(*) AS rides_count
    FROM fact_trip ft
    INNER JOIN dim_taxi_zone tz1
        ON ft.PULocationID = tz1.taxi_zone_id
    INNER JOIN dim_taxi_zone tz2
        ON ft.DOLocationID = tz2.taxi_zone_id
    INNER JOIN dim_datetime dt
        ON ft.pickup_datetime_id = dt.datetime_id
    WHERE passenger_count <=2
        AND distance_bucket_id IN (1, 2)
    GROUP BY vector, `hour`, `minute`
