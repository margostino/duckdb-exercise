SELECT vehicle_model_year, COUNT(*) AS count
FROM electric_vehicle_population
WHERE electric_vehicle_type IS NOT NULL
GROUP BY vehicle_model_year