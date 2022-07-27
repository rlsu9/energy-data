-- This updates the permission for crawler user.
-- Run with user postgres in the same database, e.g. `sudo su postgres` and then `psql -d electricity-data`.

GRANT SELECT, INSERT, UPDATE ON TABLE EnergyMixture to crawler_rw;
