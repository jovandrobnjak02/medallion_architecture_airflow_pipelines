
INSERT INTO star_schema.Fact_Race_Results (
    resultid, raceid, driverid, constructorid, grid, positionorder, points, 
    laps, time, milliseconds, fastestlap, fastestlaptime, fastestlapspeed, 
    rank, positiontext, wins, number_drivers, statusid, driverstandingsid, constructorstandingsid
)
SELECT 
    lr.resultid, 
    lr.raceid, 
    lr.driverid, 
    lr.constructorid, 
    sr.grid, 
    sr.positionorder, 
    sr.points, 
    sr.laps, 
    sr.time, 
    sr.milliseconds, 
    sfl.fastestlap, 
    sfl.fastestlaptime, 
    sfl.fastestlapspeed, 
    sr.rank, 
    sr.positiontext, 
    sr.wins, 
    sr.number_drivers, 
    lr.statusid, 
    lsd.driverstandingsid, 
    lsc.constructorstandingsid 
FROM f1_data_vault.link_results lr
JOIN f1_data_vault.sat_results sr ON lr.resultid = sr.resultid
LEFT JOIN f1_data_vault.link_fastestlap lfl 
    ON lr.raceid = lfl.raceid  
    AND lr.driverid = lfl.driverid  
LEFT JOIN f1_data_vault.sat_fastestlap sfl 
    ON lfl.fastestlapid = sfl.fastestlapid  
LEFT JOIN f1_data_vault.link_result_status lrs 
    ON lr.resultid = lrs.resultid  
LEFT JOIN f1_data_vault.link_standings_driver lsd 
    ON lr.driverid = lsd.driverid
LEFT JOIN f1_data_vault.sat_driverstandings sds 
    ON lsd.standingsdriverid = sds.standingsdriverid  
LEFT JOIN f1_data_vault.link_standings_constructor lsc 
    ON lr.constructorid = lsc.constructorid
LEFT JOIN f1_data_vault.sat_constructorstandings scs 
    ON lsc.standingsconstructorid = scs.standingsconstructorid
-- LEFT JOIN f1_data_vault.sat_status ss ON ss.statusid=lr.statusid
ON CONFLICT (resultid) DO NOTHING;



