INSERT INTO taxi_type_details(taxi_type_id, taxi_type_name, file_prefix, record_source)
SELECT taxi_tuple.1, taxi_tuple.2, taxi_tuple.3, record_source
    FROM (
        SELECT arrayJoin([
        	(0, 'Yellow taxi', 'yellow'),
        	(1, 'Green taxi', 'green')]) AS taxi_tuple,
            'Manual' AS record_source
    )
WHERE taxi_tuple.1 NOT IN (SELECT taxi_type_id FROM taxi_type_details);
