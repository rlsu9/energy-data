CREATE TABLE AzureCarbonEmissionsForecast (
    region VARCHAR(32) NOT NULL,
    iso VARCHAR(32) NOT NULL,
    generatedAt TIMESTAMP WITH TIME ZONE NOT NULL,
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    rating DOUBLE PRECISION NOT NULL,
    duration INTERVAL NOT NULL,
    UNIQUE (region, iso, generatedAt, time, rating, duration)
)