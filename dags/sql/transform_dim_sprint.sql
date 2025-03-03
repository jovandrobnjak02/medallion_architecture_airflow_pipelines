INSERT INTO gold.Dim_Sprint(raceid, driverid, sprint_date, sprint_time)
SELECT 
    ls.raceid,
    ls.driverid,
    ss.sprint_date,
    ss.sprint_time
FROM silver.link_sprints ls
JOIN silver.sat_sprints ss ON ls.sprintid= ss.sprintid
