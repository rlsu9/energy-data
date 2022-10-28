-- This updates the permission for crawler user.
-- Run with user postgres in the same database, e.g. `sudo su postgres` and then `psql -d electricity-data`.

GRANT SELECT, INSERT ON TABLE AzureCarbonEmissions, azurecarbonemissionsforecast to crawler_rw;
