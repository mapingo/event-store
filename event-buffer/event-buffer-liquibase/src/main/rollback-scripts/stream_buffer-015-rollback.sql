-- Roll back liquibase script '015-add-index-to-stream-to-improve-metrics-queries.changelog.xml'
DROP INDEX IF EXISTS stream_status_src_comp_idx;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-015';
