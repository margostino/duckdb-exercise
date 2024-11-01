SELECT vehicle_model_year, COUNT(*) as count
FROM electric_vehicle_population
GROUP BY vehicle_model_year