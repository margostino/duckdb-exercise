from typing import Optional

from pydantic import BaseModel, Field, field_validator


class VehicleData(BaseModel):
    vin: str = Field(
        ..., json_schema_extra={"csv_column": "VIN (1-10)", "sql_type": "TEXT(10)"}
    )
    county: Optional[str] = Field(
        None, json_schema_extra={"csv_column": "County", "sql_type": "TEXT"}
    )
    city: Optional[str] = Field(
        None, json_schema_extra={"csv_column": "City", "sql_type": "TEXT"}
    )
    state: Optional[str] = Field(
        None, json_schema_extra={"csv_column": "State", "sql_type": "TEXT"}
    )
    postal_code: Optional[int] = Field(
        None, json_schema_extra={"csv_column": "Postal Code", "sql_type": "INTEGER"}
    )
    vehicle_model_year: Optional[int] = Field(
        None, json_schema_extra={"csv_column": "Model Year", "sql_type": "INTEGER"}
    )
    make: Optional[str] = Field(
        None, json_schema_extra={"csv_column": "Make", "sql_type": "TEXT"}
    )
    vehicle_model: Optional[str] = Field(
        None, json_schema_extra={"csv_column": "Model", "sql_type": "TEXT"}
    )
    electric_vehicle_type: Optional[str] = Field(
        None,
        json_schema_extra={"csv_column": "Electric Vehicle Type", "sql_type": "TEXT"},
    )
    clean_alternative_fuel_vehicle_eligibility: Optional[str] = Field(
        None,
        json_schema_extra={
            "csv_column": "Clean Alternative Fuel Vehicle (CAFV) Eligibility",
            "sql_type": "TEXT",
        },
    )
    electric_range: Optional[int] = Field(
        None, json_schema_extra={"csv_column": "Electric Range", "sql_type": "INTEGER"}
    )
    base_msrp: Optional[int] = Field(
        None, json_schema_extra={"csv_column": "Base MSRP", "sql_type": "INTEGER"}
    )
    legislative_district: Optional[int] = Field(
        None,
        json_schema_extra={"csv_column": "Legislative District", "sql_type": "INTEGER"},
    )
    dol_vehicle_id: Optional[int] = Field(
        None, json_schema_extra={"csv_column": "DOL Vehicle ID", "sql_type": "BIGINT"}
    )
    vehicle_location: Optional[str] = Field(
        None, json_schema_extra={"csv_column": "Vehicle Location", "sql_type": "TEXT"}
    )
    electric_utility: Optional[str] = Field(
        None, json_schema_extra={"csv_column": "Electric Utility", "sql_type": "TEXT"}
    )
    census_tract: Optional[int] = Field(
        None,
        json_schema_extra={"csv_column": "2020 Census Tract", "sql_type": "BIGINT"},
    )

    @field_validator("legislative_district", mode="before")
    def empty_string_to_none(cls, value):  # pylint: disable=no-self-argument
        if value == "":
            return None
        return value


header_mapping = {
    field.json_schema_extra["csv_column"]: field_name
    for field_name, field in VehicleData.model_fields.items()
}

columns_types = {
    field_name: field.json_schema_extra["sql_type"]
    for field_name, field in VehicleData.model_fields.items()
}
