INSERT INTO gold.Dim_Status (statusid, status)
SELECT
    lrs.statusid, 
    ss.status 
FROM silver.link_result_status lrs
JOIN silver.sat_status ss ON lrs.statusid= ss.statusid
