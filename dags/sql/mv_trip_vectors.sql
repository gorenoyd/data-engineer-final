CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trip_vectors
ENGINE = SummingMergeTree()
ORDER BY (vector, `hour`, `minute`)
POPULATE AS

WITH tmp AS (
	SELECT max_distance FROM dim_distance WHERE LOWER(distance_bucket_name) = 'normal'
	ORDER BY load_date DESC
	LIMIT 1
)
SELECT CAST(concat(tz1.borough, '.', tz1.`zone`, '->', tz2.borough, '.', tz2.`zone`) AS LowCardinality(String)) AS vector,
        dt.`hour`,
        dt.`minute`,
        count(*) AS rides_count,
        AVG(fare_amount) AS avg_fare,
        AVG(trip_distance) AS avg_distance,
        ROUND(AVG(passenger_count)) AS avg_passengers_count
FROM fact_trip ft
    CROSS JOIN tmp
    INNER JOIN dim_taxi_zone tz1
        ON ft.PULocationID = tz1.taxi_zone_id
    INNER JOIN dim_taxi_zone tz2
        ON ft.DOLocationID = tz2.taxi_zone_id
    INNER JOIN dim_datetime dt
        ON ft.pickup_datetime_id = dt.datetime_id
    WHERE ft.passenger_count <= 2
        AND ft.trip_distance < tmp.max_distance
    GROUP BY vector, `hour`, `minute`
