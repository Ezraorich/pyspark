select team_name,
  COUNT(1) as total_races, 
  SUM(calculted_points) AS total_points,
  AVG(calculated_points) AS avg_points
  from f1_presentation.calculated_race_results
  where race_year between 2001 and 2011
  group by team name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC
