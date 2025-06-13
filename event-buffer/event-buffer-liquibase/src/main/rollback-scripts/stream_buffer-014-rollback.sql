-- Roll back liquibase script '014-add-latest-known-position-column-to-stream-status-table.changelog.xml'
ALTER TABLE stream_status DROP COLUMN latest_known_position;
ALTER TABLE stream_status DROP COLUMN is_up_to_date;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-014';
