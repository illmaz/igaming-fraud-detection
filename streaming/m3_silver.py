from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, when

builder = SparkSession.builder \
    .master("local[*]") \
    .appName("betstream-m3")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(
    builder
).getOrCreate()

bronze = (
    spark.readStream
        .format("delta")
        .load("data/delta/bronze")
)

bronze = bronze.withColumn("event_ts", col("event_ts").cast("timestamp"))
bronze = bronze.withWatermark("event_ts", "15 minutes")
bronze = bronze.dropDuplicatesWithinWatermark(["event_id"])

is_valid = (
   col("event_id").isNotNull()
   & col("player_id").isNotNull()
   & col("device_id").isNotNull()
   & col("payment_method_id").isNotNull()
   & col("event_ts").isNotNull()
)

amount_valid = (
    when(col("event_type").isin("login", "player_created"), col("amount").isNull())
    .when(col("event_type").isin("bet_placed", "withdrawal", "deposit"), col("amount") > 0)
    .otherwise(False)
)
quarantine_reason = (
    when(col("event_id").isNull(), "event_id is missing")
    .when(col("player_id").isNull(), "player_id is missing")
    .when(col("device_id").isNull(), "device_id is missing")
    .when(col("payment_method_id").isNull(), "payment_method_id is missing")
    .when(col("event_ts").isNull(), "event_ts is missing")
    .when(~amount_valid, "amount is invalid for this event_type")
)

final_valid = is_valid & amount_valid

def process_batch(batch_df, batch_id):
    print(f"batch {batch_id}: {batch_df.count()} rows, "
          f"event_ts range {batch_df.agg({'event_ts': 'min'}).collect()} to "
          f"{batch_df.agg({'event_ts': 'max'}).collect()}")
        
    flagged = batch_df.withColumn("is_valid", final_valid).withColumn("quarantine_reason", quarantine_reason)
    
    valid = flagged.filter(col("is_valid") == True)
    invalid = flagged.filter(col("is_valid") == False)

    valid.write.format("delta").mode("append").save("data/delta/silver")
    invalid.write.format("delta").mode("append").save("data/delta/quarantine")

query = bronze.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "data/checkpoints/silver") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

