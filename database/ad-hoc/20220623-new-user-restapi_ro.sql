-- This creates a read-only user for REST API.
-- Run with user postgres in the same database, e.g. `sudo su postgres` and then `psql -d electricity-data`.

CREATE USER restapi_ro;
GRANT CONNECT ON DATABASE "electricity-data" TO restapi_ro;
GRANT USAGE ON SCHEMA public TO restapi_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public to restapi_ro;
