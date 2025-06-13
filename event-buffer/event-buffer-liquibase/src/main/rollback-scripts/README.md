# Postgres SLQ scripts for Liquibase roll back

These scripts are for rolling back individual liquibase update scripts. 

The scripts must be run in reverse order. 

The name of the script is the id of the script both in the liquibase scripts themselves 
and as the `id` column in the `databasechangelog` table in event-store database. 