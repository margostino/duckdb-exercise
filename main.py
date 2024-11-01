import time

from exercise.constants import TOTAL_ROWS_SANITY_CHECK
from exercise.db import DBClient
from exercise.transformation import validate_and_prepare_data
from exercise.utils import delete_parquet_files


def main():
    delete_parquet_files()
    db = DBClient()

    db.drop_table()
    db.create_table()
    rows_to_insert = validate_and_prepare_data()
    db.backfill_data(rows_to_insert)
    total_rows = db.sanity_select_count()

    if total_rows != TOTAL_ROWS_SANITY_CHECK:
        raise ValueError(
            f"Total rows inserted {total_rows} does not match expected {TOTAL_ROWS_SANITY_CHECK}"
        )
    else:
        print("Sanity check passed")

    db.calculate_analytics()


if __name__ == "__main__":
    global_start_time = time.perf_counter()
    main()
    global_end_time = time.perf_counter()
    print(f"Total duration {global_end_time - global_start_time:0.2f} seconds")
