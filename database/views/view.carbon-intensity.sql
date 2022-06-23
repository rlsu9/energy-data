CREATE VIEW CarbonIntensity
AS
    SELECT DateTime, Region, SUM(Power_MW * CarbonIntensity) / SUM(Power_MW) AS CarbonIntensity
        FROM EnergyMixture INNER JOIN CarbonIntensityByFuelType
            ON EnergyMixture.Category = CarbonIntensityByFuelType.FuelType
        GROUP BY DateTime, Region
        ORDER BY DateTime DESC, Region;
