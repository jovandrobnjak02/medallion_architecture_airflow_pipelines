INSERT INTO star_schema.Dim_Constructor (constructorid, constructorref, name, nationality_constructor, url_constructors)
SELECT 
    hc.constructorid, 
    sc.constructorref, 
    sc.name, 
    sc.nationality_constructors, 
    sc.url_constructors
FROM f1_data_vault.hub_constructor hc
JOIN f1_data_vault.sat_constructor sc ON hc.constructorid = sc.constructorid;
