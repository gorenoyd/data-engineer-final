CREATE MATERIALIZED VIEW IF NOT EXISTS mv_rides_by_hour
ENGINE = SummingMergeTree()
ORDER BY (`hour`)
POPULATE AS
SELECT dt.`hour`, count(ft.pickup_datetime_id) AS rides_number
    FROM fact_trip ft
INNER JOIN dim_datetime dt
    ON ft.pickup_datetime_id = dt.datetime_id
GROUP BY dt.`hour`;
