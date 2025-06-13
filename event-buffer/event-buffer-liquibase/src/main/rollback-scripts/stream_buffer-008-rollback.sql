-- Roll back liquibase script '008-create-stream-error-table.changelog.xml'
DROP TABLE stream_error;
DELETE FROM databasechangelog WHERE id = 'stream_buffer-008';