BEGIN TRANSACTION;

SET timezone TO 'UTC';

CREATE TEMPORARY TABLE EMapImportTempTable (
    DateTime TIMESTAMP WITH TIME ZONE NOT NULL,
    Country VARCHAR(255) NOT NULL,
    "Zone Name" VARCHAR(255),
    "Zone Id" VARCHAR(255),
    "Carbon Intensity gCO₂eq/kWh (direct)" DOUBLE PRECISION,
    "Carbon Intensity gCO₂eq/kWh (LCA)" DOUBLE PRECISION,
    "Low Carbon Percentage" NUMERIC(5, 2),
    "Renewable Percentage" NUMERIC(5, 2),
    "Data Source" VARCHAR(255),
    "Data Estimated" BOOLEAN,
    "Data Estimation Method" VARCHAR(255)
);

\set copy_command '\\copy EMapImportTempTable FROM ' :file ' WITH CSV HEADER;'
-- \echo Running :"copy_command" ...
:copy_command

INSERT INTO EMapCarbonIntensity(
    DateTime,
    ZoneId,
    CarbonIntensity,
    LowCarbonPercentage,
    RenewablePercentage
)
SELECT
    DateTime,
    "Zone Id",
    "Carbon Intensity gCO₂eq/kWh (LCA)",
    "Low Carbon Percentage",
    "Renewable Percentage"
FROM EMapImportTempTable
WHERE "Carbon Intensity gCO₂eq/kWh (LCA)" IS NOT NULL
    AND "Low Carbon Percentage" IS NOT NULL
    AND "Renewable Percentage" IS NOT NULL;

DROP TABLE EMapImportTempTable;

COMMIT TRANSACTION;
