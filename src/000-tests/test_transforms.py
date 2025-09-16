import pytest
from pyspark.sql import SparkSession
from src.etl_pipeline import transform_data

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest")
        .getOrCreate()
    )

def test_transform_data_preserves_required_columns(spark):
    data = [
        ("2023-01-01 10:00:00", "2023-01-01 10:30:00", 1, 20.5),
        ("2023-01-02 15:00:00", "2023-01-02 15:45:00", 2, 35.0),
    ]
    cols = ["pickup_datetime", "dropoff_datetime", "passenger_count", "total_amount"]

    df_input = spark.createDataFrame(data, cols)

    df_transformed = transform_data(df_input)

    required_cols = [
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "total_amount",
    ]

    for col in required_cols:
        assert col in df_transformed.columns, f"Coluna {col} não encontrada!"

def test_no_null_in_required_columns(spark):
    data = [
        ("2023-01-01 10:00:00", "2023-01-01 10:30:00", 1, 20.5),
        ("2023-01-02 15:00:00", "2023-01-02 15:45:00", 2, 35.0),
    ]
    cols = ["pickup_datetime", "dropoff_datetime", "passenger_count", "total_amount"]

    df_input = spark.createDataFrame(data, cols)
    df_transformed = transform_data(df_input)

    for col in ["pickup_datetime", "dropoff_datetime", "total_amount"]:
        assert df_transformed.filter(f"{col} IS NULL").count() == 0
