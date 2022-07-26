CREATE VIEW UpdateInterval
AS
    SELECT region, update_interval, count(*) AS count
    FROM
        (SELECT AGE(datetime, LAG(datetime, 1) OVER (PARTITION BY region ORDER BY datetime)) AS update_interval, region
        FROM
            (SELECT DISTINCT datetime, region
                FROM energymixture
--                 WHERE region LIKE 'US-%'
                GROUP BY region, datetime
                ORDER BY datetime, region) AS disinct_dt_per_region
        ) AS intervals_per_region
        WHERE update_interval IS NOT NULL
        GROUP BY region, update_interval
        ORDER BY region, count DESC, update_interval;
