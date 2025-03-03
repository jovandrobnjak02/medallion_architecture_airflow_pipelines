INSERT INTO star_schema.Dim_Status (statusid, status)
SELECT
    lrs.statusid, 
    ss.status 
FROM f1_data_vault.link_result_status lrs
JOIN f1_data_vault.sat_status ss ON lrs.statusid= ss.statusid
