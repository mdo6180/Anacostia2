/*
instructions on how to run this script:
# cd into anacostia2/
$ sqlite3 testing_artifacts/.anacostia/anacostia.db < sql_scripts/producer_local.sql
*/

-- SELECT node_name, state, details, timestamp from artifact_usage_events WHERE node_name IN ('file_transport', 'combined_folder');

SELECT
    e.node_name,
    e.state,
    p.artifact_src_path AS filepath,
    e.timestamp AS timestamp
FROM artifact_usage_events e
JOIN file_transport_local p
    ON e.artifact_hash = p.artifact_hash
WHERE e.node_name = 'file_transport'

UNION ALL

SELECT
    e.node_name,
    e.state,
    f.artifact_path AS filepath,
    e.timestamp AS timestamp
FROM artifact_usage_events e
JOIN combined_folder_local f
    ON e.artifact_hash = f.artifact_hash
WHERE e.node_name = 'combined_folder'

ORDER BY timestamp;