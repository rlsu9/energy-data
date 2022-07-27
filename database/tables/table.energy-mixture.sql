CREATE TABLE EnergyMixture(
    DateTime TIMESTAMP WITH TIME ZONE NOT NULL,
    Category VARCHAR(32) NOT NULL,
    Power_MW DOUBLE PRECISION NOT NULL,
    Region VARCHAR(32) NOT NULL,
    UNIQUE (DateTime, Category, Region),
    CHECK (Power_MW <> 'NaN')
)
