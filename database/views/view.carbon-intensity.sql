CREATE VIEW CarbonIntensity
AS
    SELECT DateTime,
            Region,
            SUM(Power_MW * CASE WHEN carbonintensity is NULL THEN 700 ELSE greatest(0, carbonintensity) END) / SUM(Power_MW) AS CarbonIntensity
        FROM EnergyMixture LEFT OUTER JOIN CarbonIntensityByFuelType
            ON EnergyMixture.Category = CarbonIntensityByFuelType.FuelType
        GROUP BY DateTime, Region
        ORDER BY DateTime DESC, Region;
