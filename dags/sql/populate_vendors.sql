INSERT INTO vendor_details(vendor_id, vendor_name, record_source)
SELECT vendor_tuple.1, vendor_tuple.2, record_source
    FROM (
        SELECT arrayJoin([
        	(1, 'Creative Mobile Technologies, LLC'),
        	(2, 'Curb Mobility, LLC'),
        	(6, 'Myle Technologies Inc'),
        	(7, 'Helix')]) AS vendor_tuple,
            'Data Dictionary – Yellow Taxi Trip Records - March 18, 2025' AS record_source
    )
WHERE vendor_tuple.1 NOT IN (SELECT vendor_id FROM vendor_details);
