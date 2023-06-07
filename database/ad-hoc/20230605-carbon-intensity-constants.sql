-- This script sets the carbon intensity constants.
-- Source: electricityMap

INSERT INTO CarbonIntensityByFuelType(FuelType, CarbonIntensity, "Group")
VALUES
    ('battery', 251, 'NonRenewable'),   -- only when discharging (positive amount of power)
    ('biomass', 230, 'NonRenewable'),
    ('coal', 820, 'NonRenewable'),
    ('gas', 490, 'NonRenewable'),
    ('geothermal', 38, 'Renewable'),
    ('hydro', 24, 'Renewable'),
    ('import', 460, 'NonRenewable'),    -- average across the US for now.
    ('nuclear', 12, 'Renewable'),
    ('oil', 650, 'NonRenewable'),
    ('other', 700, 'NonRenewable'),
    ('solar', 45, 'Renewable'),
    ('unknown', 700, 'NonRenewable'),
    ('unknown-renewables', 230 , 'NonRenewable'),
    ('wind', 11, 'Renewable'),
    ('wind/solar', 28, 'Renewable')

