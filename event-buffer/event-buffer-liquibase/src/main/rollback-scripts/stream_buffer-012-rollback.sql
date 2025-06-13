-- Roll back liquibase script '012-add-unique-constraint-on-stream-error-hash-columns.changelog.xml'
ALTER TABLE stream_error DROP CONSTRAINT stream_error_stream_id_source_component_name_key;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-012';
