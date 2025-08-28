-- Roll back liquibase script '017-add-index-on-error-hash-in-stream-status.changelog.xml'
DROP INDEX IF EXISTS "stream_error.hash.idx";
DELETE FROM databasechangelog WHERE id = 'stream_buffer-017';
