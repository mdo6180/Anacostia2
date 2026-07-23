-- Run this script when debugging the mid_stream_stop.py test. 
-- It will show the artifacts the pipeline is using on restart in the Consumer.get_using_artifacts() method.
SELECT
    a.artifact_hash,
    l.artifact_location
FROM artifact_usage_events AS a
JOIN odd_folder_local AS l
    ON a.artifact_hash = l.artifact_hash
WHERE
    a.node_name = 'TestNode'
    AND a.run_id = 1
    AND a.state = 'using'
ORDER BY
    a.timestamp ASC;