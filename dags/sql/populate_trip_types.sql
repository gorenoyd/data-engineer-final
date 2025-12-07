INSERT INTO trip_type_details(trip_type_id, trip_type_name, record_source)
SELECT trip_tuple.1, trip_tuple.2, record_source
    FROM (
        SELECT arrayJoin([
        	(1, 'Street-hail'),
        	(2, 'Dispatch')]) AS trip_tuple,
            'Data Dictionary - LPEP Trip Records March 18, 2025' AS record_source
    )
WHERE trip_tuple.1 NOT IN (SELECT trip_type_id FROM trip_type_details);
