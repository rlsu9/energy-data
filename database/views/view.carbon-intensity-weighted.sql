CREATE VIEW CarbonIntensityByRenewable
AS
    SELECT DateTime,
            Region,
            COALESCE(SUM(CASE WHEN ci.group = 'Renewable' THEN Power_MW * (CASE WHEN carbonintensity is NULL THEN 700 ELSE greatest(0, carbonintensity) END) ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN ci.group = 'Renewable' THEN Power_MW ELSE 0 END), 0),
                0)
                AS Renewable_CarbonIntensity,
            COALESCE(SUM(CASE WHEN ci.group != 'Renewable' THEN Power_MW * (CASE WHEN carbonintensity is NULL THEN 700 ELSE greatest(0, carbonintensity) END) ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN ci.group != 'Renewable' THEN Power_MW ELSE 0 END), 0),
                0)
                AS NonRenewable_CarbonIntensity,
            SUM(CASE WHEN ci.group = 'Renewable' THEN Power_MW ELSE 0 END)
                / NULLIF(SUM(Power_MW), 0) AS Renewable_Ratio
        FROM EnergyMixture INNER JOIN CarbonIntensityByFuelType ci
            ON EnergyMixture.Category = ci.FuelType
        GROUP BY DateTime, Region
        ORDER BY DateTime DESC, Region;
