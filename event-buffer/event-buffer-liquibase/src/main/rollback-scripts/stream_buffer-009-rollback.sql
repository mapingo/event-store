-- Roll back liquibase script '009-add-stream-error-id-and-position-to-stream-status-table.changelog.xml'
ALTER TABLE stream_status DROP CONSTRAINT stream_status_to_stream_error_fk;
ALTER TABLE stream_status DROP COLUMN stream_error_id;
ALTER TABLE stream_status DROP COLUMN stream_error_position;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-009';
