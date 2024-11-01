CSV_FILE_PATH = "data/Electric_Vehicle_Population_Data.csv"
TABLE_NAME = "electric_vehicle_population"
DATA_BASE_PATH = "./data"
DB_PATH_FILE = f"{DATA_BASE_PATH}/electric_vehicle.duckdb"
DB_PATH_IN_MEMORY = ":memory:"
DB_PATH = DB_PATH_FILE
BATCH_SIZE = 30000

QUERIES_MAP = {
    "count_electric_cars_per_city": "SELECT city, COUNT(*) as count FROM electric_vehicle_population GROUP BY city",
    "find_top_3_most_popular_electric_vehicles": "SELECT vehicle_model, COUNT(*) as count FROM electric_vehicle_population GROUP BY vehicle_model ORDER BY count DESC LIMIT 3",
    "find_most_popular_electric_vehicle_per_postal_code": "SELECT postal_code, vehicle_model, COUNT(*) as count FROM electric_vehicle_population GROUP BY postal_code, vehicle_model ORDER BY postal_code, count DESC",
    "count_electric_cars_by_model_year": "SELECT vehicle_model_year, COUNT(*) as count FROM electric_vehicle_population GROUP BY vehicle_model_year",
}
