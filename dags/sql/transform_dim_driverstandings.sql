INSERT INTO gold.Dim_DriverStandings (driverstandingsid, driverid, points_driverstandings, positionText_driverstandings, wins)
SELECT 
    lsd.driverstandingsid,
    lsd.driverid,
    sds.points_driverstandings,
    sds.positionText_driverstandings,
    sds.wins
FROM silver.link_standings_driver lsd 
JOIN silver.sat_driverstandings sds ON lsd.standingsdriverid = sds.standingsdriverid

