INSERT INTO dim_distance(distance_bucket_id, distance_bucket_name, min_distance, max_distance, record_source)
SELECT distance_tuple.1, distance_tuple.2, distance_tuple.3, distance_tuple.4, record_source
    FROM (
        SELECT arrayJoin([
        	(0, 'Short', 0, 0.5),
        	(1, 'Normal', 0.5, 1.5),
        	(2, 'Long', 1.5, 3.0),
        	(3, 'Extra Long', 3.0, 999)]) AS distance_tuple,
            'Manual' AS record_source
    )
WHERE distance_tuple.1 NOT IN (SELECT distance_bucket_id FROM dim_distance);
