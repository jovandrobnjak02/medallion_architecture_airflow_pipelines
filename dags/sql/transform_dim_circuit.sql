INSERT INTO star_schema.Dim_Circuit (circuitid, circuitref, name_y, location, country, lat, lng, alt, url_y)
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
FROM f1_data_vault.hub_circuit hc
JOIN f1_data_vault.sat_circuit sc ON hc.circuitid = sc.circuitid;
