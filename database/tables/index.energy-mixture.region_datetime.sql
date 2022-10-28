CREATE INDEX index_energymixture_region_datetime ON "electricity-data".public.energymixture
(
    region,
    datetime
);
