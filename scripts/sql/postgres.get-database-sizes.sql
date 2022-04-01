SELECT pg_database.datname as "databasename",
pg_database_size(pg_database.datname)/1024/1024 AS sizemb
FROM pg_database ORDER by pg_database_size(pg_database.datname) DESC;
