INSERT INTO gold.Dim_Race (raceid, year, round, circuitid, name_x, date, time_races, url_x)
SELECT 
    hr.raceid, 
    sr.year, 
    sr.round, 
    lrc.circuitid,
    sr.name_x, 
    sr.date, 
    sr.time_races, 
    sr.url_x
FROM silver.hub_race hr
JOIN silver.sat_race sr ON hr.raceid = sr.raceid
LEFT JOIN silver.link_race_circuits lrc ON hr.raceid = lrc.raceid;

--lrc because logistics of the data vault-> link-> relationships between business keys
--a race happens at a specific circuit 
--there is no direct relationship between hub_race and hub_circuit 
--the only way to link raceid to circuitid would be through link_race_circuits
