CREATE TABLE CarbonIntensityByFuelType (
    FuelType VARCHAR(32) NOT NULL,
    CarbonIntensity DOUBLE PRECISION NOT NULL,
    UNIQUE (FuelType)
)
