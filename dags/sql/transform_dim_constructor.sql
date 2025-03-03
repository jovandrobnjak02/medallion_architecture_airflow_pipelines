INSERT INTO gold.Dim_Constructor (constructorid, constructorref, name, nationality_constructor, url_constructors)
SELECT 
    hc.constructorid, 
    sc.constructorref, 
    sc.name, 
    sc.nationality_constructors, 
    sc.url_constructors
FROM silver.hub_constructor hc
JOIN silver.sat_constructor sc ON hc.constructorid = sc.constructorid;
