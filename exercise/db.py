import time

import duckdb

from exercise.constants import DB_PATH, QUERIES_MAP, TABLE_NAME
from exercise.model import columns_types
from exercise.utils import create_table_statement


class DBClient:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.table_name = TABLE_NAME
        self.columns_types = columns_types
        self.conn = duckdb.connect(
            database=self.db_path, read_only=False
        )  # Single connection

    def __del__(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def create_table(self):
        create_table_start_time = time.perf_counter()
        self.conn.execute(create_table_statement(self.columns_types, self.table_name))
        create_table_end_time = time.perf_counter()
        print(
            f"Create table duration {create_table_end_time - create_table_start_time:0.2f} seconds"
        )

    def drop_table(self):
        self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        print(f"Table {self.table_name} dropped successfully.")

    def insert_batch(self, batch):
        insert_query = f"INSERT INTO {self.table_name} ({', '.join(self.columns_types.keys())}) VALUES ({', '.join(['?' for _ in self.columns_types.keys()])})"
        self.conn.executemany(insert_query, batch)
        print(f"Inserted {len(batch)} rows")

    def backfill_data(self, rows_to_insert):
        backfill_start_time = time.perf_counter()
        insert_query = f"INSERT INTO {self.table_name} ({', '.join(self.columns_types.keys())}) VALUES ({', '.join(['?' for _ in self.columns_types.keys()])})"
        self.conn.executemany(insert_query, rows_to_insert)
        print(f"Inserted {len(rows_to_insert)} rows")
        backfill_end_time = time.perf_counter()
        print(
            f"Backfill duration {backfill_end_time - backfill_start_time:0.2f} seconds"
        )

    def sanity_select(self):
        sanity_select_start_time = time.perf_counter()
        result = self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchall()
        print(f"Sanity check count result: {result}")
        sanity_select_end_time = time.perf_counter()
        print(
            f"Sanity Select duration {sanity_select_end_time - sanity_select_start_time:0.2f} seconds"
        )

    def count_electric_cars_per_city(self):
        start_time = time.perf_counter()
        result_df = self.conn.execute(
            QUERIES_MAP["count_electric_cars_per_city"]
        ).fetchdf()
        print("Electric cars per city:\n", result_df)
        end_time = time.perf_counter()
        print(
            f"Count electric cars per city duration {end_time - start_time:0.2f} seconds"
        )
        result = dict(zip(result_df["city"], result_df["count"]))
        return result

    def find_top_3_most_popular_electric_vehicles(self):
        start_time = time.perf_counter()
        result_df = self.conn.execute(
            QUERIES_MAP["find_top_3_most_popular_electric_vehicles"]
        ).fetchdf()
        print("Top 3 most popular electric vehicles:\n", result_df)
        end_time = time.perf_counter()
        print(
            f"Top 3 most popular electric vehicles duration {end_time - start_time:0.2f} seconds"
        )
        result = result_df["vehicle_model"].tolist()
        return result

    def find_most_popular_electric_vehicle_per_postal_code(self):
        start_time = time.perf_counter()
        result_df = self.conn.execute(
            QUERIES_MAP["find_most_popular_electric_vehicle_per_postal_code"]
        ).fetchdf()
        print(
            "Most popular electric vehicle per postal code:\n",
            result_df,
        )
        end_time = time.perf_counter()
        print(
            f"Most popular electric vehicle per postal code duration {end_time - start_time:0.2f} seconds"
        )
        result = dict(zip(result_df["postal_code"], result_df["vehicle_model"]))
        return result

    def count_electric_cars_by_model_year(self):
        start_time = time.perf_counter()
        result_df = self.conn.execute(
            QUERIES_MAP["count_electric_cars_by_model_year"]
        ).fetchdf()
        for year in result_df["vehicle_model_year"].unique():
            year_data = result_df[result_df["vehicle_model_year"] == year]
            year_data.to_parquet(f"./data/electric_cars_by_model_year={year}.parquet")
        print("Count electric cars by model year:\n", result_df)
        end_time = time.perf_counter()
        print(
            f"Count electric cars by model year duration {end_time - start_time:0.2f} seconds"
        )
        result = dict(zip(result_df["vehicle_model_year"], result_df["count"]))
        return result

    def calculate_analytics(self):
        analytics_start_time = time.perf_counter()
        results = {}
        # Count the number of electric cars per city.
        results["count_electric_cars_per_city"] = self.count_electric_cars_per_city()
        # Find the top 3 most popular electric vehicles.
        results["find_top_3_most_popular_electric_vehicles"] = (
            self.find_top_3_most_popular_electric_vehicles()
        )
        # Find the most popular electric vehicle in each postal code.
        results["find_most_popular_electric_vehicle_per_postal_code"] = (
            self.find_most_popular_electric_vehicle_per_postal_code()
        )
        # Count the number of electric cars by model year. Write out the answer as parquet files partitioned by year.
        results["count_electric_cars_by_model_year"] = (
            self.count_electric_cars_by_model_year()
        )

        analytics_end_time = time.perf_counter()
        print(
            f"Analytics duration {analytics_end_time - analytics_start_time:0.2f} seconds"
        )

        return results
