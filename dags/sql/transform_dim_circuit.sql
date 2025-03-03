INSERT INTO gold.Dim_Circuit (circuitid, circuitref, name_y, location, country, lat, lng, alt, url_y)
SELECT 
    hc.circuitid, 
    sc.circuitref, 
    sc.name_y, 
    sc.location, 
    sc.country, 
    sc.lat, 
    sc.lng, 
    sc.alt, 
    sc.url_y
FROM silver.hub_circuit hc
JOIN silver.sat_circuit sc ON hc.circuitid = sc.circuitid;
