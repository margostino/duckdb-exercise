from unittest.mock import patch

import pytest

from exercise.db import DBClient
from exercise.utils import load_queries
from fixtures.mocks import prepare_data_mocks


@pytest.fixture
def mock_duckdb():
    queries = load_queries("./queries")
    db = DBClient(":memory:", "electric_vehicle_population", queries)
    db.create_table()
    rows_to_insert = prepare_data_mocks()
    db.backfill_data(rows_to_insert)
    yield db
    db.drop_table()


def test_count_electric_cars_per_city(mock_duckdb):
    result = mock_duckdb.count_electric_cars_per_city()
    expected_result = {"CityA": 1, "CityB": 1}
    assert result == expected_result


def test_find_top_3_most_popular_electric_vehicles(mock_duckdb):
    result = mock_duckdb.find_top_3_most_popular_electric_vehicles()
    expected_result = ["ModelB", "ModelA"]
    assert sorted(result) == sorted(expected_result)


def test_find_most_popular_electric_vehicle_per_postal_code(mock_duckdb):
    result = mock_duckdb.find_most_popular_electric_vehicle_per_postal_code()
    expected_result = {
        12345: "ModelA",
        67890: "ModelB",
    }
    assert result == expected_result


@patch("pandas.DataFrame.to_parquet")
def test_count_electric_cars_by_model_year(mock_to_parquet, mock_duckdb):
    result = mock_duckdb.count_electric_cars_by_model_year()
    expected_result = {2020: 1, 2021: 1}
    assert result == expected_result
    assert mock_to_parquet.call_count == len(
        expected_result
    ), "to_parquet was not called the expected number of times"


def test_calculate_analytics(mock_duckdb):
    result = mock_duckdb.calculate_analytics()

    expected_result = {
        "count_electric_cars_per_city": {"CityA": 1, "CityB": 1},
        "find_top_3_most_popular_electric_vehicles": ["ModelB", "ModelA"],
        "find_most_popular_electric_vehicle_per_postal_code": {
            12345: "ModelA",
            67890: "ModelB",
        },
        "count_electric_cars_by_model_year": {2020: 1, 2021: 1},
    }

    assert (
        result["count_electric_cars_per_city"]
        == expected_result["count_electric_cars_per_city"]
    )
    assert sorted(result["find_top_3_most_popular_electric_vehicles"]) == sorted(
        expected_result["find_top_3_most_popular_electric_vehicles"]
    )
    assert (
        result["find_most_popular_electric_vehicle_per_postal_code"]
        == expected_result["find_most_popular_electric_vehicle_per_postal_code"]
    )
    assert (
        result["count_electric_cars_by_model_year"]
        == expected_result["count_electric_cars_by_model_year"]
    )
