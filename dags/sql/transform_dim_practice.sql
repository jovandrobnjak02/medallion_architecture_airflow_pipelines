INSERT INTO gold.Dim_Practice(raceid, fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time)
SELECT 
    lp.raceid,
    sp.fp1_date::DATE,
    sp.fp1_time::TIME,
    sp.fp2_date::DATE,
    sp.fp2_time::TIME,
    sp.fp3_date::DATE,
    sp.fp3_time::TIME
FROM silver.link_practices lp 
JOIN silver.sat_practices sp ON lp.practiceid=sp.practiceid