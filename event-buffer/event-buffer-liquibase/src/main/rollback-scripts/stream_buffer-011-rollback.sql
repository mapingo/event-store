-- Roll back liquibase script '011-add-stream-error-hash-table.changelog.xml'
ALTER TABLE stream_error ADD exception_classname text;
ALTER TABLE stream_error ADD cause_classname text;
ALTER TABLE stream_error ADD java_classname text;
ALTER TABLE stream_error ADD java_method text;
ALTER TABLE stream_error ADD java_line_number bigint;
ALTER TABLE stream_error DROP CONSTRAINT stream_error_to_stream_error_hash_fk;
DROP TABLE stream_error_hash;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-011';
