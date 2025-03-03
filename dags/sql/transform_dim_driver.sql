INSERT INTO star_schema.Dim_Driver (driverid, driverref, number, code, forename, surname, dob, nationality, url)
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
FROM f1_data_vault.hub_driver hd
JOIN f1_data_vault.sat_driver sd ON hd.driverid = sd.driverid;
