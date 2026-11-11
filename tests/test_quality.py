import pytest
from pyspark.sql import SparkSession

from src.quality.checks import check_schema, check_nulls, check_duplicates, check_referencial_integrity, check_mobility_rules

@pytest.fixture(scope = "session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("UrbanFlow-Quality-Tests")
        .getOrCreate()
    )

    yield session

    session.stop()

def test_check_schema_passed(spark):
    df = spark.createDataFrame(
        [(1, "Manhattan"), (2, "Queens")],
        ["id", "borough"]
    )

    result = check_schema(df, ["id", "borough"])

    assert result["status"] == "PASSED"
    assert result["missing_columns"] == []

def test_check_schema_failed(spark):
    df = spark.createDataFrame(
        [(1, "Manhattan"), (2, "Queens")],
        ["id", "borough"]
    )

    result = check_schema(df, ["id", "borough", "service_zone"])

    assert result["status"] == "FAILED"
    assert result["missing_columns"] == ["service_zone"]

def test_check_nulls_passed(spark):
    df = spark.createDataFrame(
        [(1,"Queens"), (2, "Manhattan")],
        ["id", "borough"]
    )

    result = check_nulls(df, ["id", "borough"])

    assert result["status"] == "PASSED"
    assert result["null_counts"] == {}

def test_check_nulls_failed(spark):
    df = spark.createDataFrame(
        [(1,"Queens"), (2, None)],
        ["id", "borough"]
    )

    result = check_nulls(df, ["id", "borough"])

    assert result["status"] == "FAILED"
    assert result["null_counts"] == {"borough": 1}

def test_check_duplicates_passed(spark):
    df = spark.createDataFrame(
        [(1, "Queens"), (2, "Manhattan")],
        ["id", "borough"]
    )

    result = check_duplicates(df, "id")

    assert result["status"] == "PASSED"
    assert result["duplicates_count"] == 0

def test_check_duplicates_failed(spark):
    df = spark.createDataFrame(
        [(1, "Queens"), (1, "Manhattan")],
        ["id", "borough"]
    )

    result = check_duplicates(df, "id")

    assert result["status"] == "FAILED"
    assert result["duplicates_count"] == 1

def test_check_referencial_integrity_passed(spark):
    df = spark.createDataFrame(
        [(161,), (237,), (236,)],
        ["PULocationID"]
    )

    reference_df = spark.createDataFrame(
        [(161,), (237,), (236,), (259,)],
        ["LocationID"]
    )

    result = check_referencial_integrity(
        df, reference_df, "PULocationID", "LocationID"
    )

    assert result["status"] == "PASSED"
    assert result["invalid_count"] == 0

def test_check_referencial_integrity_failed(spark):
    df = spark.createDataFrame(
        [(161,), (237,), (236,), (999,)],
        ["PULocationID"]
    )

    reference_df = spark.createDataFrame(
        [(161,), (237,), (236,), (259,)],
        ["LocationID"]
    )

    result = check_referencial_integrity(
        df, reference_df, "PULocationID", "LocationID"
    )

    assert result["status"] == "FAILED"
    assert result["invalid_count"] == 1

def test_check_mobility_rules_passed(spark):
    df = spark.createDataFrame(
        [(2.5, 1200, 1, True),
        (3.0, 1500, 2, True)],

        ["trip_distance",
        "duracao_segundos",
        "passenger_count",
        "is_mobility_valid"]
    )
    result = check_mobility_rules(df)

    assert result["status"] == "PASSED"
    assert result["inconsistent_count"] == 0

def test_check_mobility_rules_failed(spark):
    df = spark.createDataFrame(
        [(2.5, 1200, 1, True),
        (3.0, 1500, 0, True)],

        ["trip_distance",
        "duracao_segundos",
        "passenger_count",
        "is_mobility_valid"]
    )
    result = check_mobility_rules(df)

    assert result["status"] == "FAILED"
    assert result["inconsistent_count"] == 1