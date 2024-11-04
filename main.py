import argparse
import time

import toml

from exercise.db import DBClient
from exercise.transformation import validate_and_prepare_data_chunks
from exercise.utils import delete_parquet_files, load_queries, print_results


# See notes witu assumptions, considerations and potential improvements here:
# https://github.com/margostino/duckdb-exercise/blob/master/notes.txt
def main(config):
    total_rows_sanity_check = config["ingestion"]["total_rows_sanity_check"]
    queries_path = config["analytics"]["queries_path"]
    db_path = config["database"]["db_path"]
    table_name = config["database"]["table_name"]
    chunk_size = config["database"]["chunk_size"]
    csv_file_path = config["ingestion"]["csv_file_path"]
    data_base_path = config["database"]["data_base_path"]
    reload_enabled = config["ingestion"]["reload_enabled"]
    verbose = config["analytics"]["verbose"]

    queries = load_queries(queries_path)

    db = DBClient(db_path, table_name, queries)

    delete_parquet_files(data_base_path)

    if reload_enabled:
        db.drop_table()
        db.create_table()
        db.create_indexes()
        chunks = validate_and_prepare_data_chunks(chunk_size, csv_file_path)
        db.backfill_data_by_chunks(chunks)  # DuckDB supports single-writer process only
        # Potential solution for multi-writer process using multiprocessing and retries due locking but still one writer at a time
        # db.backfill_data_by_chunks_multiprocess(chunks, db_path, table_name)
        # Current insert is including some CPU bound (i.e. in memory transformation) which makes this operation not fully I/O bound (hence GIL bottleneck)
        # await db.backfill_data_by_chunks_multiprocess(chunks, db_path, table_name)

    total_rows = db.sanity_select_count()

    if total_rows != total_rows_sanity_check:
        raise ValueError(
            f"Total rows inserted {total_rows} does not match expected {total_rows_sanity_check}"
        )
    else:
        print("Sanity check passed")

    # Sequential execution. Although DuckDB support multiple readers processes with read-only mode. But for this data size is not needed.
    results = db.calculate_analytics()
    # Wether CPU or I/O bound depends on the data size and how complex the queries.
    # If needed, parallel execution, multiprocess in case of CPU bound (i.e. in-memory aggregations).
    # If I/O bound, consider asyncio or threads
    # results = db.calculate_analytics_parallel()

    if verbose:
        print_results(results)


if __name__ == "__main__":
    global_start_time = time.perf_counter()

    parser = argparse.ArgumentParser(description="Exercise 8: DuckDB.")
    parser.add_argument(
        "--config_file",
        type=str,
        help="Path to the configuration file.",
        default="config.toml",
    )

    args = parser.parse_args()

    config_file = toml.load(args.config_file)

    main(config_file)

    global_end_time = time.perf_counter()
    print(f"Total duration {global_end_time - global_start_time:0.2f} seconds")
