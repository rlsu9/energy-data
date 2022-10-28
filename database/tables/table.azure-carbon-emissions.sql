CREATE TABLE AzureCarbonEmissions (
    region VARCHAR(32) NOT NULL,
    iso VARCHAR(32) NOT NULL,
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    rating DOUBLE PRECISION NOT NULL,
    duration INTERVAL NOT NULL,
    UNIQUE (region, iso, time, rating, duration)
)