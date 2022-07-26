CREATE VIEW AverageUpdateInterval
AS
    SELECT region, AVG(update_interval) AS avg_update_interval
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
        GROUP BY region
        ORDER BY avg_update_interval;
