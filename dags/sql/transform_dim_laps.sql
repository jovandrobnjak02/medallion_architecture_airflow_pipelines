INSERT INTO star_schema.Dim_Laps(raceid, driverid, lap, position_laptimes, time_laptimes, milliseconds_laptimes)
SELECT 
    ll.raceid,
    ll.driverid,
    sl.lap,
    sl.position_laptimes,
    sl.time_laptimes,
    sl.milliseconds_laptimes
FROM f1_data_vault.link_laps ll
JOIN f1_data_vault.sat_laps sl ON ll.lapid=sl.lapid