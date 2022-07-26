CREATE VIEW AvailableTimeRange
AS
    SELECT region, MIN(datetime) AS min_timestamp, MAX(datetime)  AS max_timestamp
        FROM energymixture
        GROUP BY region
        ORDER BY region;
