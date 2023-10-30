CREATE TABLE EMapCarbonIntensity(
    DateTime TIMESTAMP WITH TIME ZONE NOT NULL,
    ZoneId VARCHAR(32) NOT NULL,
    CarbonIntensity DOUBLE PRECISION NOT NULL,
    LowCarbonPercentage NUMERIC(5, 2) NOT NULL,
    RenewablePercentage NUMERIC(5, 2) NOT NULL
)
