-- This script checks for irregular negative values in certain fuel types

select distinct on (category) datetime, region, category
    from energymixture
    where power_mw < -10;


select * from energymixture
    where region = 'US-MISO' and category = 'unknown' and power_mw < -10;


-- Results:
-- US-CAISO,battery		normal (charging)
-- US-CAISO,biomass		one off (1)
-- US-CAISO,geothermal		one off (1)
-- US-CAISO,hydro			normal (charging)
-- US-CAISO,import			normal (export)
-- US-MISO,solar			one off (3)
-- US-MISO,unknown			quite a lot from 2022/
