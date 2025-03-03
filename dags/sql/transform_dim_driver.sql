INSERT INTO gold.Dim_Driver (driverid, driverref, number, code, forename, surname, dob, nationality, url)
SELECT 
    hd.driverid, 
    sd.driverref, 
    sd.number, 
    sd.code, 
    sd.forename, 
    sd.surname, 
    sd.dob, 
    sd.nationality, 
    sd.url
FROM silver.hub_driver hd
JOIN silver.sat_driver sd ON hd.driverid = sd.driverid;
