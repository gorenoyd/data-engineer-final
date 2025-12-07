CREATE MATERIALIZED VIEW IF NOT EXISTS mv_zones_counts
ENGINE = SummingMergeTree()
ORDER BY (zone_id)
POPULATE AS
SELECT
    zs.zone_id,
    tz.borough,
    tz.`zone`,
    SUM(zs.pickup_count) AS pickup_count,
    SUM(zs.dropoff_count) AS dropoff_count
FROM
(
    SELECT PULocationID AS zone_id, COUNT(*) AS pickup_count, 0 AS dropoff_count
        FROM fact_trip GROUP BY PULocationID
    UNION ALL
    SELECT DOLocationID AS zone_id, 0 AS pickup_count, COUNT(*) AS dropoff_count
        FROM fact_trip GROUP BY DOLocationID
) AS zs
INNER JOIN dim_taxi_zone tz
    ON zs.zone_id = tz.taxi_zone_id
GROUP BY zs.zone_id, tz.borough, tz.`zone`;
