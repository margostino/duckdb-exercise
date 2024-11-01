SELECT vehicle_model, COUNT(*) as popularity
FROM electric_vehicle_population
WHERE electric_vehicle_type IS NOT NULL
GROUP BY vehicle_model
ORDER BY popularity DESC
LIMIT 3