INSERT INTO hub_rate(rate_id, record_source)
SELECT *
    FROM (
        SELECT arrayJoin([1, 2, 3, 4, 5, 6, 99]) AS rate_id,
            'Data Dictionary – Yellow Taxi Trip Records - March 18, 2025' AS record_source
    )
WHERE rate_id NOT IN (SELECT rate_id FROM hub_rate);

INSERT INTO sat_rate_details(rate_id, rate_name, record_source)
SELECT rate_tuple.1, rate_tuple.2, record_source
    FROM (
        SELECT arrayJoin([
        	(1, 'Standard rate'),
        	(2, 'JFK'),
        	(3, 'Newark'),
        	(4, 'Nassau or Westchester'),
        	(5, 'Negotiated fare'),
        	(6, 'Group ride'),
        	(99, 'Null/unknown')]) AS rate_tuple,
            'Data Dictionary – Yellow Taxi Trip Records - March 18, 2025' AS record_source
    )
WHERE rate_tuple.1 NOT IN (SELECT rate_id FROM sat_rate_details);
