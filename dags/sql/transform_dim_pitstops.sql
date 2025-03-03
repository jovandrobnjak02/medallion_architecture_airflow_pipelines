INSERT INTO gold.Dim_Pit_Stops(raceid, driverid, stop, lap_pitstops, time_pitstops, duration, milliseconds_pitstops)
SELECT
    lps.raceid,
    lps.driverid,
    sps.stop::INTEGER,
    sps.lap_pitstops::INTEGER,
    sps.time_pitstops,
    sps.duration,
    sps.milliseconds_pitstops::INTEGER
FROM silver.link_pitstops lps
JOIN silver.sat_pitstops sps ON lps.pitstopid=sps.pitstopid