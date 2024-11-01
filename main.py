import time

from exercise.constants import TOTAL_ROWS_SANITY_CHECK
from exercise.db import DBClient
from exercise.transformation import validate_and_prepare_data_chunks
from exercise.utils import delete_parquet_files, print_results


def main():
    delete_parquet_files()
    db = DBClient()

    db.drop_table()
    db.create_table()
    db.create_indexes()
    chunks = validate_and_prepare_data_chunks()
    db.backfill_data_by_chunks(chunks)
    total_rows = db.sanity_select_count()

    if total_rows != TOTAL_ROWS_SANITY_CHECK:
        raise ValueError(
            f"Total rows inserted {total_rows} does not match expected {TOTAL_ROWS_SANITY_CHECK}"
        )
    else:
        print("Sanity check passed")

    results = db.calculate_analytics()
    print_results(results)


if __name__ == "__main__":
    global_start_time = time.perf_counter()
    main()
    global_end_time = time.perf_counter()
    print(f"Total duration {global_end_time - global_start_time:0.2f} seconds")
