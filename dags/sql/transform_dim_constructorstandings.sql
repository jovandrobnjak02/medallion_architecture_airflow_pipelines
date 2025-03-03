INSERT INTO star_schema.Dim_ConstructorStandings(constructorstandingsid, constructorid, points_constructorstandings, positiontext_constructorstandings, wins_constructorstandings)
SELECT 
    lsc.constructorstandingsid,
    lsc.constructorid,
    scs.points_constructorstandings,
    scs.positiontext_constructorstandings,
    scs.wins_constructorstandings::INTEGER
FROM f1_data_vault.link_standings_constructor lsc
JOIN f1_data_vault.sat_constructorstandings scs ON lsc.standingsconstructorid= scs.standingsconstructorid