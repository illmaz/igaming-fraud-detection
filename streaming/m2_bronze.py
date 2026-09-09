from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col

builder = SparkSession.builder \
    .master("local[*]") \
    .appName("betstream-m2")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(
    builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
).getOrCreate()

raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "betstream.events") \
    .option("startingOffsets", "earliest") \
    .load()

events_str = raw_stream.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS json_value", "timestamp AS kafka_ts", "partition AS kafka_partition", "offset AS kafka_offset")

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("payment_method_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_ts", StringType(), True)

])

parsed = events_str.select(
    from_json(col("json_value"), event_schema).alias("data"), col("kafka_ts"), col("kafka_partition"), col("kafka_offset")
).select("data.*", "kafka_ts", "kafka_partition", "kafka_offset")

parsed.printSchema()

query = parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoints/bronze") \
    .option("path", "data/delta/bronze") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()