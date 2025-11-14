INSERT INTO hub_payment_type(payment_type_id, record_source)
SELECT *
    FROM (
        SELECT arrayJoin([0, 1, 2, 3, 4, 5, 6]) AS payment_type_id,
            'Data Dictionary – Yellow Taxi Trip Records - March 18, 2025' AS record_source
    )
WHERE payment_type_id NOT IN (SELECT payment_type_id FROM hub_payment_type);

INSERT INTO sat_payment_type_details(payment_type_id, payment_type_name, record_source)
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
WHERE payment_tuple.1 NOT IN (SELECT payment_type_id FROM sat_payment_type_details);
