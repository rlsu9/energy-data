-- Note: this update changes region "US-CA" to "US-CAISO" to avoid confusion betwen CA ISO and all of California.
--  See history of crawler/parsers/US_CAISO.py for details.

BEGIN;

DELETE FROM energymixture
    WHERE region = 'US-CAISO';

UPDATE energymixture
    SET region = 'US-CAISO'
    WHERE region = 'US-CA';

-- Change this to COMMIT, or run without begin to perform actual update
ROLLBACK;
