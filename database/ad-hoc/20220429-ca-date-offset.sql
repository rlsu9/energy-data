-- Note: this update adds an offset of -1 day to all California data prior to 4/29,
--  as the data collected incorrect uses the current day's date whereas the quried data is for the prior day.
--  See history of crawler/parsers/US_CAISO.py for details.

BEGIN;

UPDATE energymixture
    SET datetime = datetime - INTERVAL '1 DAY'
    WHERE region = 'US-CAISO';

SELECT DISTINCT datetime, COUNT(*)
    FROM energymixture
    WHERE region = 'US-CAISO'
    GROUP BY datetime
    ORDER BY datetime DESC;

-- Change this to COMMIT, or run without begin to perform actual update
ROLLBACK;
