# tests/append_test_row.py
# Throwaway script to append hand-picked rows to silver for testing M6's
# stateful dedup. Never run against real pipeline data you care about
# reproducing without a backup, since this permanently adds rows to silver.

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, LongType, BooleanType
)
from datetime import datetime, timezone

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("append_test_row")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("player_id", StringType()),
    StructField("device_id", StringType()),
    StructField("payment_method_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_ts", TimestampType()),
    StructField("kafka_ts", TimestampType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("is_valid", BooleanType()),
    StructField("quarantine_reason", StringType()),
])

now = datetime.now(timezone.utc)

# EDIT THIS ROW before each run, per the step you're testing.
row = [(
    "test-event-0004",           # event_id, must be unique
    "login",                     # event_type
    "test-player-new-0002",      # player_id
    "d5a2c8c0-7da2-440b-a9b5-26c2b8b34a0f",  # device_id, the flagged device
    "test-payment-0001",         # payment_method_id
    None,                        # amount, null for login
    now,                         # event_ts
    now,                         # kafka_ts
    0,                           # kafka_partition
    1000002,                      # kafka_offset
    True,                        # is_valid
    None,                        # quarantine_reason
)]

df = spark.createDataFrame(row, schema)
df.write.format("delta").mode("append").save("data/delta/silver")
print("Appended 1 row to silver.")