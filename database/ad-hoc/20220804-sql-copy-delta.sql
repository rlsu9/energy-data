-- This script copies the delta table collected during maintenance of the main SQL database.

BEGIN TRANSACTION;

WITH t AS (
    INSERT INTO energymixture (datetime, category, power_mw, region)
    SELECT datetime, category, power_mw, region FROM energymixture_delta_20220804
    ON CONFLICT DO NOTHING RETURNING xmax
)
SELECT COUNT(*) AS count_all,
       SUM(CASE WHEN xmax = 0 THEN 1 ELSE 0 END) AS count_insert,
       SUM(CASE WHEN xmax::text::int > 0 THEN 1 ELSE 0 END) AS count_update
FROM t;

-- Use this or commit.
ROLLBACK TRANSACTION;

-- COMMIT TRANSACTION;

-- Optionally drops the temp table.
-- DROP TABLE energymixture_delta_20220804;
