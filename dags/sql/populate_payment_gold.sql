INSERT INTO dim_payment_type(payment_type_id, payment_type_name, is_cash, is_digital, is_dispute, record_source)
SELECT payment_tuple.1, payment_tuple.2, payment_tuple.3, payment_tuple.4, payment_tuple.5, record_source
    FROM (
        SELECT arrayJoin([
        	(0, 'Flex Fare trip', NULL, NULL, NULL),
        	(1, 'Credit card', 0, 1, NULL),
        	(2, 'Cash', 1, 0, NULL),
        	(3, 'No charge', NULL, NULL, NULL),
        	(4, 'Dispute', NULL, NULL, 1),
        	(5, 'Unknown', NULL, NULL, NULL),
        	(6, 'Voided trip', NULL, NULL, NULL)]) AS payment_tuple,
            'Data Dictionary – Yellow Taxi Trip Records - March 18, 2025' AS record_source
    )
WHERE payment_tuple.1 NOT IN (SELECT payment_type_id FROM dim_payment_type);
