import logging
import time

from exercise.db import DBClient
from exercise.transformation import validate_and_prepare_data
from exercise.utils import delete_parquet_files

logger = logging.getLogger(__name__)


def main():
    logger.info("Starting the main process")
    delete_parquet_files()
    db = DBClient()

    db.drop_table()
    db.create_table()
    rows_to_insert = validate_and_prepare_data()
    db.backfill_data(rows_to_insert)
    db.calculate_analytics()


if __name__ == "__main__":
    global_start_time = time.perf_counter()
    main()
    global_end_time = time.perf_counter()
    print(f"Total duration {global_end_time - global_start_time:0.2f} seconds")
