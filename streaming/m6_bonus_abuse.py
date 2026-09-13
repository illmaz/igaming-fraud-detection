from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("m6_bonus_abuse")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = (
    spark.readStream
    .format("delta")
    .load("data/delta/silver")
)

pairs = df.select(
    "player_id",
    "device_id"
)

unique_pairs = pairs.dropDuplicates(
    ["device_id", "player_id"]
)

device_count = unique_pairs.groupBy("device_id").agg(F.count("player_id").alias("player_count"),
                                                     F.collect_set("player_id").alias("linked_players"))
                                                     

suspicious_devices = device_count.filter(F.col("player_count") >= 5).withColumn("detected_at", F.current_timestamp())

def process_batch(batch_df, batch_id):
    count = batch_df.count()
    print(f"batch {batch_id}: {count} flagged devices")
    if count > 0:
        batch_df.write.format("delta").mode("append").save("data/delta/flagged_devices")

query = suspicious_devices.writeStream \
    .foreachBatch(process_batch)\
    .outputMode("update") \
    .option("checkpointLocation", "data/checkpoints/m6_bonus_abuse")\
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()