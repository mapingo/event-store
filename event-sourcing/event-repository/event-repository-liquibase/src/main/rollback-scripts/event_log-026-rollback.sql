-- Roll back liquibase script '026-add-self-healing-columns-to-event-log-table.changelog.xml'

DROP INDEX IF EXISTS idx_event_log_global_sequence;
DROP INDEX IF EXISTS idx_event_log_not_published;
DROP INDEX IF EXISTS idx_event_log_not_sequenced;
ALTER TABLE event_log DROP COLUMN is_published;
ALTER TABLE event_log DROP COLUMN previous_event_number;
DELETE FROM databasechangelog WHERE id = 'event-store-026';

