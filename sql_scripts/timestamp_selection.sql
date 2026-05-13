/*
SELECT *
FROM odd_folder_local
WHERE timestamp > (
    SELECT timestamp
    FROM run_events
    WHERE node_name = 'TestNode'
        AND run_id = 0
        AND event_type = 'start'
)
AND timestamp <= (
    SELECT timestamp
    FROM run_events
    WHERE node_name = 'TestNode'
        AND run_id = 1
        AND event_type = 'start'
);
*/

SELECT *
FROM even_folder_local
WHERE timestamp <= (
    SELECT timestamp
    FROM run_events
    WHERE node_name = 'TestNode'
        AND run_id = 0
        AND event_type = 'start'
);