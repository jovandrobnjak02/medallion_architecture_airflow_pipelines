INSERT INTO star_schema.Dim_Sprint(raceid, driverid, sprint_date, sprint_time)
SELECT 
    ls.raceid,
    ls.driverid,
    ss.sprint_date,
    ss.sprint_time
FROM f1_data_vault.link_sprints ls
JOIN f1_data_vault.sat_sprints ss ON ls.sprintid= ss.sprintid
