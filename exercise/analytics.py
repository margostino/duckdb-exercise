from exercise.utils import execute_query_for


def count_electric_cars_per_city(db_path):
    result_df = execute_query_for(db_path, "count_electric_cars_per_city")
    return dict(zip(result_df["city"], result_df["count"]))


def find_top_3_most_popular_electric_vehicles(db_path):
    result_df = execute_query_for(db_path, "find_top_3_most_popular_electric_vehicles")
    return result_df["vehicle_model"].tolist()


def find_most_popular_electric_vehicle_per_postal_code(db_path):
    result_df = execute_query_for(
        db_path, "find_most_popular_electric_vehicle_per_postal_code"
    )
    return dict(zip(result_df["postal_code"], result_df["vehicle_model"]))


def count_electric_cars_by_model_year(db_path):
    result_df = execute_query_for(db_path, "count_electric_cars_by_model_year")
    return dict(zip(result_df["vehicle_model_year"], result_df["count"]))
