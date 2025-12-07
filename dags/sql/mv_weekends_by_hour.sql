CREATE MATERIALIZED VIEW IF NOT EXISTS mv_weekends_by_hour
ENGINE = SummingMergeTree()
ORDER BY (`hour`)
POPULATE AS

SELECT dt.hour, count(ft.pickup_datetime_id) AS rides_number
    FROM fact_trip AS ft
    INNER JOIN dim_datetime AS dt
        ON ft.pickup_datetime_id = dt.datetime_id
    WHERE ft.trip_hashkey NOT IN
    (
        -- Exclude airports
        SELECT ft.trip_hashkey
            FROM fact_trip ft, dim_taxi_zone tz
            WHERE (ft.PULocationID = tz.taxi_zone_id
                OR ft.DOLocationID = tz.taxi_zone_id)
                AND tz.is_airport = True
    )
    AND dt.is_weekend = TRUE
    OR dt.is_holiday = TRUE
GROUP BY dt.hour;