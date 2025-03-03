INSERT INTO star_schema.Dim_Pit_Stops(raceid, driverid, stop, lap_pitstops, time_pitstops, duration, milliseconds_pitstops)
SELECT
    lps.raceid,
    lps.driverid,
    sps.stop::INTEGER,
    sps.lap_pitstops::INTEGER,
    sps.time_pitstops,
    sps.duration,
    sps.milliseconds_pitstops::INTEGER
FROM f1_data_vault.link_pitstops lps
JOIN f1_data_vault.sat_pitstops sps ON lps.pitstopid=sps.pitstopid