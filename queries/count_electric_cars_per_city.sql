SELECT city, COUNT(*) as count
FROM electric_vehicle_population
WHERE electric_vehicle_type IS NOT NULL
GROUP BY city