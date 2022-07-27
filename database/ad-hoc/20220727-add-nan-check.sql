-- This script adds a new constraint to avoid NaN values in energymixture table's Power_MW column.

ALTER TABLE energymixture
    ADD CONSTRAINT check_power_mw_not_nan
        CHECK (Power_MW <> 'NaN');
