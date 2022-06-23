-- This script sets the carbon intensity constants.
-- Source: electricityMap

INSERT INTO CarbonIntensityByFuelType(FuelType, CarbonIntensity)
VALUES
    ('biomass', 230),
    ('coal', 820),
    ('gas', 490),
    ('geothermal', 38),
    ('hydro', 24),
    ('nuclear', 12),
    ('solar', 45),
    ('unknown', 700),
    ('wind', 11)
