-- This renames the auto-generated constraint name to something more meaningful

-- Original constraint name obtained using this query:
-- SELECT con.conname
--        FROM pg_catalog.pg_constraint con
--             INNER JOIN pg_catalog.pg_class rel
--                        ON rel.oid = con.conrelid
--             INNER JOIN pg_catalog.pg_namespace nsp
--                        ON nsp.oid = connamespace
--        WHERE nsp.nspname = 'public'
--              AND rel.relname = 'energymixture';

ALTER TABLE energymixture
    RENAME CONSTRAINT energymixture_datetime_category_region_key
        TO energymixture_unique_datetime_category_region;
