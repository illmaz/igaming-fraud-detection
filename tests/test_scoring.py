import pytest
from pyspark.sql import SparkSession
from batch.scoring import score

@pytest.fixture(scope="session")
def spark():
    return(
        SparkSession.builder
        .master("local[*]")
        .appName("tests")
        .getOrCreate()
    )

def test_score_counts_tp_fp_fn(spark):
    rows = [
        ("suspicious_flow", True),
        ("suspicious_flow", True),
        ("normal", True),
        ("suspicious_flow", False),
        ("normal", False),
    ]
    df = spark.createDataFrame(rows, ["fraud_type", "flagged"])

    result = score(df, "flagged", "suspicious_flow")
    assert result == (2, 1, 1)

def test_other_fraud_type_counts_as_false_positive(spark):
    rows = [
        ("suspicious_flow", True),
        ("suspicious_flow", True),
        ("normal", True),
        ("suspicious_flow", False),
        ("normal", False),
        ("bonus_abuse", True),
    ]

    df = spark.createDataFrame(rows, ["fraud_type", "flagged"])

    result = score(df, "flagged", "suspicious_flow")
    assert result == (2, 2, 1)