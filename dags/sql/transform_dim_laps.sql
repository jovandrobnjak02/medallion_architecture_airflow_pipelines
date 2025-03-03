INSERT INTO gold.Dim_Laps(raceid, driverid, lap, position_laptimes, time_laptimes, milliseconds_laptimes)
SELECT 
    ll.raceid,
    ll.driverid,
    sl.lap,
    sl.position_laptimes,
    sl.time_laptimes,
    sl.milliseconds_laptimes
FROM silver.link_laps ll
JOIN silver.sat_laps sl ON ll.lapid=sl.lapid