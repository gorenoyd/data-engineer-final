INSERT INTO dim_duration(duration_bucket_id, duration_bucket_name, min_duration, max_duration, record_source)
SELECT duration_tuple.1, duration_tuple.2, duration_tuple.3, duration_tuple.4, record_source
    FROM (
        SELECT arrayJoin([
        	(0, '1: Short', 0, 4),
        	(1, '2: Normal', 4, 12),
        	(2, '3: Long', 12, 20),
        	(3, '4: Extra Long', 20, 1440)]) AS duration_tuple,
            'Manual' AS record_source
    )
WHERE duration_tuple.1 NOT IN (SELECT duration_bucket_id FROM dim_duration);
