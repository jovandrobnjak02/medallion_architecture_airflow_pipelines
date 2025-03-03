INSERT INTO star_schema.Dim_Qualifying (raceid, driverid, quali_date, quali_time)
SELECT 
    lq.raceid, 
    lq.driverid,
    sq.quali_date, 
    sq.quali_time
FROM f1_data_vault.link_qualifying lq
JOIN f1_data_vault.sat_qualifying sq ON lq.qualiid = sq.qualiid
WHERE sq.quali_date IS NOT NULL;

