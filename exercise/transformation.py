import concurrent.futures
import csv

from exercise.constants import CSV_FILE_PATH
from exercise.model import VehicleData, columns_types, header_mapping

# def validate_and_prepare_data():
#     rows = []

#     start_time = time.perf_counter()
#     with open(CSV_FILE_PATH, "r", encoding="utf-8") as csvfile:
#         reader = csv.DictReader(csvfile)

#         for row in reader:
#             transformed_row = {
#                 header_mapping[key]: value
#                 for key, value in row.items()
#                 if key in header_mapping
#             }

#             data_row = VehicleData(**transformed_row)
#             values = tuple(getattr(data_row, col) for col in columns_types.keys())
#             rows.append(values)

#     end_time = time.perf_counter()
#     print(
#         f"Process CSV, validate and create data to insert duration {end_time - start_time:0.2f} seconds"
#     )
#     return rows


def process_chunk(chunk):
    processed_rows = []
    for row in chunk:
        transformed_row = {
            header_mapping[key]: value
            for key, value in row.items()
            if key in header_mapping
        }
        data_row = VehicleData(**transformed_row)
        values = tuple(getattr(data_row, col) for col in columns_types.keys())
        processed_rows.append(values)
    return processed_rows


def validate_and_prepare_data(chunk_size=1000):
    rows = []
    with open(CSV_FILE_PATH, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        data_chunks = []
        chunk = []

        # Split data into chunks
        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                data_chunks.append(chunk)
                chunk = []

        # Add the last chunk if it contains any remaining rows
        if chunk:
            data_chunks.append(chunk)

    # Process each chunk in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(process_chunk, data_chunks)
        for result in results:
            rows.extend(result)

    return rows


# def process_chunk(chunk):
#     processed_rows = []
#     for row in chunk:
#         transformed_row = {
#             header_mapping[key]: value
#             for key, value in row.items()
#             if key in header_mapping
#         }
#         data_row = VehicleData(**transformed_row)
#         values = tuple(getattr(data_row, col) for col in columns_types.keys())
#         processed_rows.append(values)
#     return processed_rows
