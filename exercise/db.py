import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor

import duckdb

from exercise.analytics import (
    count_electric_cars_by_model_year,
    count_electric_cars_per_city,
    find_most_popular_electric_vehicle_per_postal_code,
    find_top_3_most_popular_electric_vehicles,
)
from exercise.model import columns_types, index_columns
from exercise.utils import create_index_statement, create_table_statement

db_lock = multiprocessing.Lock()  # Lock to ensure only one process writes at a time


class DBClient:
    def __init__(self, db_path, table_name, queries):
        self.db_path = db_path
        self.queries = queries
        self.table_name = table_name
        self.columns_types = columns_types
        self.conn = duckdb.connect(database=self.db_path, read_only=False)

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def __del__(self):
        self.close_connection()

    def create_indexes(self):
        create_index_start_time = time.perf_counter()
        for index_column in index_columns:
            self.conn.execute(create_index_statement(self.table_name, index_column))
        create_index_end_time = time.perf_counter()
        print(
            f"Create indexes duration {create_index_end_time - create_index_start_time:0.2f} seconds"
        )

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

    def backfill_data_by_chunks(self, data_chunks):
        backfill_start_time = time.perf_counter()
        insert_query = f"INSERT INTO {self.table_name} ({', '.join(self.columns_types.keys())}) VALUES ({', '.join(['?' for _ in self.columns_types.keys()])})"
        batch_counter = 0
        for batch in data_chunks:
            batch_counter += 1
            self.conn.executemany(insert_query, batch)
            print(f"Inserted batch #{batch_counter} of {len(batch)} rows")

        backfill_end_time = time.perf_counter()
        print(
            f"Backfill duration {backfill_end_time - backfill_start_time:0.2f} seconds"
        )

    @staticmethod
    def insert_chunk(chunk, db_path, table_name):
        """
        Insert a single chunk of data into DuckDB, with retry logic on lock errors.
        """
        attempt = 0
        retry_attempts = 5
        retry_delay = 10
        while attempt < retry_attempts:
            try:
                conn = duckdb.connect(database=db_path, read_only=False)

                with db_lock:
                    insert_query = f"INSERT INTO {table_name} ({', '.join(chunk[0].keys())}) VALUES ({', '.join(['?' for _ in chunk[0]])})"
                    conn.executemany(insert_query, chunk)
                    print(f"Inserted chunk of {len(chunk)} rows")

                conn.close()
                return  # Success, exit the function

            except duckdb.IOException as e:
                if "Could not set lock on file" in str(e):
                    attempt += 1
                    print(
                        f"Lock error encountered. Retry {attempt}/{retry_attempts} in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                else:
                    print(f"Unexpected error: {e}")
                    raise e
        print(
            f"Failed to insert chunk after {retry_attempts} attempts due to locking issues."
        )

    def backfill_data_by_chunks_multiprocess(self, data_chunks, num_processes=6):
        backfill_start_time = time.perf_counter()

        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.starmap(
                self.insert_chunk,
                [(chunk, self.db_path, self.table_name) for chunk in data_chunks],
            )

        backfill_end_time = time.perf_counter()
        print(
            f"Backfill duration {backfill_end_time - backfill_start_time:0.2f} seconds"
        )

    def sanity_select_count(self):
        sanity_select_start_time = time.perf_counter()
        result = self.conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchall()
        sanity_select_end_time = time.perf_counter()
        print(
            f"Sanity Select duration {sanity_select_end_time - sanity_select_start_time:0.2f} seconds"
        )
        count = result[0][0] if result else 0
        print(f"Sanity check count result: {count}")
        return count

    def count_electric_cars_per_city(self):
        start_time = time.perf_counter()
        result_df = self.conn.execute(
            self.queries["count_electric_cars_per_city"]
        ).fetchdf()
        end_time = time.perf_counter()
        print(
            f"Count electric cars per city duration {end_time - start_time:0.2f} seconds"
        )
        result = dict(zip(result_df["city"], result_df["count"]))
        return result

    def find_top_3_most_popular_electric_vehicles(self):
        start_time = time.perf_counter()
        result_df = self.conn.execute(
            self.queries["find_top_3_most_popular_electric_vehicles"]
        ).fetchdf()
        end_time = time.perf_counter()
        print(
            f"Top 3 most popular electric vehicles duration {end_time - start_time:0.2f} seconds"
        )
        result = result_df["vehicle_model"].tolist()
        return result

    def find_most_popular_electric_vehicle_per_postal_code(self):
        start_time = time.perf_counter()
        result_df = self.conn.execute(
            self.queries["find_most_popular_electric_vehicle_per_postal_code"]
        ).fetchdf()
        end_time = time.perf_counter()
        print(
            f"Most popular electric vehicle per postal code duration {end_time - start_time:0.2f} seconds"
        )
        result = dict(zip(result_df["postal_code"], result_df["vehicle_model"]))
        return result

    def count_electric_cars_by_model_year(self):
        start_time = time.perf_counter()
        result_df = self.conn.execute(
            self.queries["count_electric_cars_by_model_year"]
        ).fetchdf()
        for year in result_df["vehicle_model_year"].unique():
            year_data = result_df[result_df["vehicle_model_year"] == year]
            year_data.to_parquet(f"./data/electric_cars_by_model_year={year}.parquet")
        end_time = time.perf_counter()
        print(
            f"Count electric cars by model year duration {end_time - start_time:0.2f} seconds"
        )
        result = dict(zip(result_df["vehicle_model_year"], result_df["count"]))
        return result

    def calculate_analytics(self):
        analytics_start_time = time.perf_counter()
        results = {
            "count_electric_cars_per_city": self.count_electric_cars_per_city(),
            "find_top_3_most_popular_electric_vehicles": self.find_top_3_most_popular_electric_vehicles(),
            "find_most_popular_electric_vehicle_per_postal_code": self.find_most_popular_electric_vehicle_per_postal_code(),
            "count_electric_cars_by_model_year": self.count_electric_cars_by_model_year(),
        }
        analytics_end_time = time.perf_counter()
        print(
            f"Analytics duration {analytics_end_time - analytics_start_time:0.2f} seconds"
        )

        return results

    def calculate_analytics_parallel(self):
        self.close_connection()
        analytics_start_time = time.perf_counter()

        tasks = {
            "count_electric_cars_per_city": count_electric_cars_per_city,
            "find_top_3_most_popular_electric_vehicles": find_top_3_most_popular_electric_vehicles,
            "find_most_popular_electric_vehicle_per_postal_code": find_most_popular_electric_vehicle_per_postal_code,
            "count_electric_cars_by_model_year": count_electric_cars_by_model_year,
        }

        with ProcessPoolExecutor() as executor:
            future_to_task_name = {
                executor.submit(task, self.db_path): name
                for name, task in tasks.items()
            }

            # Gather results
            results = {}
            for future in future_to_task_name:
                task_name = future_to_task_name[future]
                try:
                    results[task_name] = future.result()
                except Exception as e:
                    print(f"Error in task {task_name}: {e}")

        analytics_end_time = time.perf_counter()
        print(
            f"Analytics duration {analytics_end_time - analytics_start_time:0.2f} seconds"
        )

        return results
