SELECT schemaname                                    AS table_schema,
       relname                                       AS table_name,
       PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE(relid)) AS total_size,
       PG_SIZE_PRETTY(PG_RELATION_SIZE(relid))       AS data_size,
       PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE(relid) - PG_RELATION_SIZE(relid))
                                                     AS external_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY PG_TOTAL_RELATION_SIZE(relid) DESC,
         PG_RELATION_SIZE(relid) DESC;
