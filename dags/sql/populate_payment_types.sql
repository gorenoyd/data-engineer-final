INSERT INTO payment_type_details(payment_type_id, payment_type_name, record_source)
SELECT payment_tuple.1, payment_tuple.2, record_source
    FROM (
        SELECT arrayJoin([
        	(0, 'Flex Fare trip'),
        	(1, 'Credit card'),
        	(2, 'Cash'),
        	(3, 'No charge'),
        	(4, 'Dispute'),
        	(5, 'Unknown'),
        	(6, 'Voided trip')]) AS payment_tuple,
            'Data Dictionary – Yellow Taxi Trip Records - March 18, 2025' AS record_source
    )
WHERE payment_tuple.1 NOT IN (SELECT payment_type_id FROM payment_type_details);
