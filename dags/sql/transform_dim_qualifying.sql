INSERT INTO gold.Dim_Qualifying (raceid, driverid, quali_date, quali_time)
SELECT 
    lq.raceid, 
    lq.driverid,
    sq.quali_date, 
    sq.quali_time
FROM silver.link_qualifying lq
JOIN silver.sat_qualifying sq ON lq.qualiid = sq.qualiid
WHERE sq.quali_date IS NOT NULL;

