import glob
import os

import duckdb


def delete_parquet_files(data_base_path):
    parquet_files = glob.glob(os.path.join(data_base_path, "*.parquet"))

    for file_path in parquet_files:
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")

    print("Deleted parquet files")


def create_table_statement(columns_types, table_name):
    columns = ",\n    ".join(
        [f"{col_name} {data_type}" for col_name, data_type in columns_types.items()]
    )
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    )
    """


def create_index_statement(table_name, column_name):
    return (
        f"CREATE INDEX IF NOT EXISTS idx_{column_name} ON {table_name} ({column_name})"
    )


def print_results(results):
    print("Results:")
    for key, value in results.items():
        print(f"\n{key}:")
        print(value)


def execute_query_for(db_path, query):
    with duckdb.connect(database=db_path, read_only=True) as conn:
        result_df = conn.execute(query).fetchdf()
    return result_df


def load_queries(queries_path):
    queries_map = {}

    for filename in os.listdir(queries_path):
        if filename.endswith(".sql"):
            query_name = os.path.splitext(filename)[0]

            with open(
                os.path.join(queries_path, filename), "r", encoding="utf-8"
            ) as file:
                query = file.read().strip()

            queries_map[query_name] = query

    return queries_map
