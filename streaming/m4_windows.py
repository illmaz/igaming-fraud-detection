from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
from datetime import timedelta

builder = SparkSession.builder \
    .master("local[*]") \
    .appName("betstream-m4")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(
    builder
).getOrCreate()
spark.conf.set("spark.sql.streaming.multipleWatermarkPolicy", "max")

silver = spark.readStream \
    .format("delta") \
    .load("data/delta/silver")

silver_watermarked = silver.withWatermark("event_ts", "15 minutes")

# --- flush event setup ---
max_ts = spark.read.format("delta").load("data/delta/silver") \
    .agg(F.max("event_ts")).collect()[0][0]

flush_ts = max_ts + timedelta(days=1)

flush_row = spark.createDataFrame(
    [("__flush__", "login", flush_ts, 0.0)],
    ["player_id", "event_type", "event_ts", "amount"]
)

flush_row.write.format("delta").mode("overwrite").save("data/delta/_flush_source")

flush_stream = spark.readStream.format("delta").load("data/delta/_flush_source") \
    .withWatermark("event_ts", "15 minutes")
silver_with_flush = silver_watermarked.select("player_id", "event_type", "event_ts", "amount") \
    .union(flush_stream)
# --- end flush event setup ---

windowed = (
    silver_with_flush
        .groupBy(
            F.window("event_ts", "24 hours", "1 hour"),
            F.col("player_id"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "deposit", F.col("amount"))
                  .otherwise(0.0)).alias("total_deposit"),
            F.sum(F.when(F.col("event_type") == "bet_placed", F.col("amount"))
                  .otherwise(0.0)).alias("total_bets"),
            F.sum(F.when(F.col("event_type") == "withdrawal", F.col("amount"))
                  .otherwise(0.0)).alias("total_withdrawals"),
            F.count(F.when(F.col("event_type") == "deposit", True)).alias("deposit_count"),
            F.min("event_ts").alias("first_event_ts"),
            F.max("event_ts").alias("last_event_ts"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "*",
        )
        .drop("window")
)

windowed.printSchema()

query = windowed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoints/features_windowed") \
    .trigger(availableNow=True) \
    .start("data/delta/features_windowed")

query.awaitTermination()

print(spark.read.format("delta").load("data/delta/features_windowed").count())

spark.read.format("delta").load("data/delta/features_windowed") \
    .filter(F.col("player_id") == "f20b64da-8a5b-4775-8bc1-2d748cebe0db") \
    .orderBy("window_start") \
    .show(50, truncate=False)