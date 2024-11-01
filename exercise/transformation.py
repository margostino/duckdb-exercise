import csv
import time

from exercise.model import VehicleData, columns_types, header_mapping


def validate_and_prepare_data_chunks(chunk_size, csv_file_path):
    rows = []
    chunks = []

    start_time = time.perf_counter()
    with open(csv_file_path, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            transformed_row = {
                header_mapping[key]: value
                for key, value in row.items()
                if key in header_mapping
            }

            data_row = VehicleData(**transformed_row)
            values = tuple(getattr(data_row, col) for col in columns_types.keys())
            rows.append(values)

            if len(rows) == chunk_size:
                chunks.append(rows)
                rows = []

        if rows:
            chunks.append(rows)

    end_time = time.perf_counter()
    print(
        f"Process CSV, validate and create data to insert duration {end_time - start_time:0.2f} seconds"
    )
    return chunks
