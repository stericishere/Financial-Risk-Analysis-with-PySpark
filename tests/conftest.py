"""
PyTest configuration and shared fixtures for PySpark testing.
"""

import pytest
import tempfile
import shutil
from typing import Generator
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
)


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """
    Create a SparkSession for testing.

    Yields:
        SparkSession configured for testing.
    """
    spark = (
        SparkSession.builder.appName("FinancialRiskETL-Test")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    # Set log level to reduce noise during testing
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    spark.stop()


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """
    Create a temporary directory for test files.

    Yields:
        Path to temporary directory.
    """
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_financial_data(spark_session):
    """
    Create sample financial risk data for testing.

    Args:
        spark_session: SparkSession fixture.

    Returns:
        DataFrame with sample financial data.
    """
    schema = StructType(
        [
            StructField("Age", IntegerType(), True),
            StructField("Income", DoubleType(), True),
            StructField("LoanAmount", DoubleType(), True),
            StructField("CreditScore", IntegerType(), True),
            StructField("MonthsEmployed", IntegerType(), True),
            StructField("NumCreditLines", IntegerType(), True),
            StructField("InterestRate", DoubleType(), True),
            StructField("LoanTerm", IntegerType(), True),
            StructField("DTIRatio", DoubleType(), True),
            StructField("Default", IntegerType(), True),
        ]
    )

    # Sample data representing various risk profiles
    data = [
        (25, 50000.0, 200000.0, 650, 24, 3, 6.5, 30, 0.35, 0),  # Young, decent credit
        (
            35,
            75000.0,
            150000.0,
            720,
            60,
            5,
            4.5,
            15,
            0.25,
            0,
        ),  # Good credit, lower risk
        (45, 40000.0, 250000.0, 580, 12, 2, 8.5, 30, 0.55, 1),  # High DTI, high risk
        (30, 60000.0, 180000.0, 700, 36, 4, 5.5, 20, 0.30, 0),  # Moderate risk
        (55, 90000.0, 300000.0, 750, 120, 6, 4.0, 15, 0.20, 0),  # Low risk, high income
        (28, 30000.0, 100000.0, 500, 6, 1, 12.0, 30, 0.60, 1),  # High risk, low credit
        (40, 80000.0, 220000.0, 680, 84, 5, 6.0, 25, 0.33, 0),  # Moderate risk
        (22, 35000.0, 120000.0, 620, 18, 2, 7.5, 30, 0.45, 1),  # Young, higher risk
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_config():
    """
    Create sample configuration for testing.

    Returns:
        Dictionary with test configuration.
    """
    return {
        "input": {
            "data_path": "test_data.csv",
            "format": "csv",
            "options": {"header": True, "inferSchema": True, "sep": ","},
        },
        "output": {
            "path": "test_output",
            "format": "parquet",
            "mode": "overwrite",
            "partitionBy": ["risk_category"],
        },
        "processing": {
            "risk_thresholds": {"low_risk": 0.3, "medium_risk": 0.6, "high_risk": 0.8},
            "feature_columns": [
                "Age",
                "Income",
                "LoanAmount",
                "CreditScore",
                "MonthsEmployed",
                "NumCreditLines",
                "InterestRate",
                "LoanTerm",
                "DTIRatio",
            ],
            "target_column": "Default",
            "aggregation_levels": ["risk_category", "age_group", "income_bracket"],
        },
        "spark": {
            "app_name": "FinancialRiskETL-Test",
            "master": "local[*]",
            "config": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        },
    }
