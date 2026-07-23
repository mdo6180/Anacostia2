SELECT artifact_hash FROM 'artifact_usage_events'
WHERE node_name = 'stream_consumer_odd' AND state = 'primed' AND artifact_hash NOT IN (
    SELECT artifact_hash FROM 'artifact_usage_events'
    WHERE node_name = 'TestNode' AND state = 'using'
)
ORDER BY timestamp ASC;

SELECT
    l.artifact_hash,
    l.artifact_location
FROM artifact_usage_events AS a
JOIN odd_folder_local AS l
    ON a.artifact_hash = l.artifact_hash
WHERE
    a.node_name = 'stream_consumer_odd'
    AND a.state = 'primed'
    AND a.artifact_hash NOT IN (
        SELECT artifact_hash
        FROM artifact_usage_events
        WHERE node_name = 'TestNode' AND state = 'using'
    )
ORDER BY a.timestamp ASC;