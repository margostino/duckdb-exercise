SELECT vehicle_model, COUNT(*) as count
FROM electric_vehicle_population
GROUP BY vehicle_model
ORDER BY count DESC LIMIT 3