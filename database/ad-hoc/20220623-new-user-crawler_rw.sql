-- This creates a read-only user for crawler.
-- Run with user postgres in the same database, e.g. `sudo su postgres` and then `psql -d electricity-data`.

CREATE USER crawler_rw;
GRANT CONNECT ON DATABASE "electricity-data" TO crawler_rw;
GRANT USAGE ON SCHEMA public TO crawler_rw;
GRANT SELECT, INSERT, UPDATE ON TABLE LastUpdated to crawler_rw;
GRANT INSERT ON TABLE EnergyMixture to crawler_rw;
