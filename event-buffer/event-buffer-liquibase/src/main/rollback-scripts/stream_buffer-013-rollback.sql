-- Roll back liquibase script '013-add-updated-at-column-to-stream-status-table.changelog.xml'
ALTER TABLE stream_status DROP COLUMN updated_at;
ALTER TABLE stream_error RENAME COLUMN component TO component_name;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-013';
