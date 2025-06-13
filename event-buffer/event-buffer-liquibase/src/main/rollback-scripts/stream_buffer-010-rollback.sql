-- Roll back liquibase script '010-add-component-name-and-source-to-stream-error-table.changelog.xml'
ALTER TABLE stream_error DROP COLUMN component_name;
ALTER TABLE stream_error DROP COLUMN source;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-010';
