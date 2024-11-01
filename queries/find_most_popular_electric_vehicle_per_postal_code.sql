SELECT postal_code, vehicle_model, COUNT(*) as count
FROM electric_vehicle_population
GROUP BY postal_code, vehicle_model
ORDER BY postal_code, count DESC