WITH vehicle_counts AS (
    SELECT
        postal_code,
        vehicle_model,
        COUNT(*) AS popularity
    FROM
        electric_vehicle_population
    WHERE
        electric_vehicle_type IS NOT NULL
    GROUP BY
        postal_code,
        vehicle_model
),
max_popularity AS (
    SELECT
        postal_code,
        MAX(popularity) AS max_popularity
    FROM
        vehicle_counts
    GROUP BY
        postal_code
)
SELECT
    vc.postal_code,
    vc.vehicle_model,
    vc.popularity
FROM
    vehicle_counts vc
JOIN
    max_popularity mp
ON
    vc.postal_code = mp.postal_code
    AND vc.popularity = mp.max_popularity;