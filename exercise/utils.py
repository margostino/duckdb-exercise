import glob
import os

from exercise.constants import DATA_BASE_PATH


def delete_parquet_files():
    parquet_files = glob.glob(os.path.join(DATA_BASE_PATH, "*.parquet"))

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
