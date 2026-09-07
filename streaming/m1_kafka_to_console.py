from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col

spark = (
    SparkSession.builder \
    .appName("betstream-m1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .getOrCreate()
    
)

raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "betstream.events") \
    .option("startingOffsets", "earliest") \
    .load()

raw_stream.printSchema()

events_str = raw_stream.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS json_value")

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
    from_json(col("json_value"), event_schema).alias("data")

).select("data.*")

parsed.printSchema()

parsed = parsed.filter(col("event_type") == "bet_placed")

query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()