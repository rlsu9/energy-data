-- Find the differences in the conflicted rows and make sure there's no missing rows.

-- match rows with different values, within +/- 1
select * from
    energymixture as main
    inner join energymixture_yeti_02 as alt
    on main.datetime = alt.datetime
        and main.region = alt.region
        and main.category = alt.category
        and main.power_mw != alt.power_mw;

-- unmatched rows in new table: 0 rows
select * from
    energymixture as main
    right outer join energymixture_yeti_02 as alt
    on main.datetime = alt.datetime
        and main.region = alt.region
        and main.category = alt.category
    where main.datetime is null;