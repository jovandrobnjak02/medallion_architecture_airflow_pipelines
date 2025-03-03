INSERT INTO star_schema.Dim_DriverStandings (driverstandingsid, driverid, points_driverstandings, positionText_driverstandings, wins)
SELECT 
    lsd.driverstandingsid,
    lsd.driverid,
    sds.points_driverstandings,
    sds.positionText_driverstandings,
    sds.wins
FROM f1_data_vault.link_standings_driver lsd 
JOIN f1_data_vault.sat_driverstandings sds ON lsd.standingsdriverid = sds.standingsdriverid

