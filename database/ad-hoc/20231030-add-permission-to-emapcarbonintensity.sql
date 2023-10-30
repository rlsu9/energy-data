-- This updates the permission for crawler users on EMapCarbonIntensity table.
-- Run with user postgres in the same database, e.g. `sudo su postgres` and then `psql -d electricity-data`.

GRANT SELECT, INSERT, UPDATE ON TABLE EMapCarbonIntensity to crawler_rw;
