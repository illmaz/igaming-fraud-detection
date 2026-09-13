# Ad hoc data profiling, read only. Findings move into milestone scripts.
# Never add writes here: no .save(), no writeStream, no checkpoints.

# %%
import os
ROOT = os.path.expanduser("~/betstream")
os.chdir(ROOT)

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

builder = SparkSession.builder \
    .master("local[*]") \
    .appName("betstream-explore") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(os.getcwd())

# %% silver profiling
silver = spark.read.format("delta").load(f"{ROOT}/data/delta/silver")
silver.printSchema()

# %%
silver.groupBy("event_type").agg(
    F.count("device_id").alias("with_device"),
    F.count("*").alias("total")
).show()

# %%
silver.groupBy("player_id").agg(
    F.countDistinct("device_id").alias("n_devices")
).groupBy("n_devices").count().orderBy("n_devices").show()

# %%
silver.groupBy("device_id").agg(
    F.countDistinct("player_id").alias("n_players")
).groupBy("n_players").count().orderBy("n_players").show()

# %% flagged_devices inspection (M6 output)
flagged = spark.read.format("delta").load("data/delta/flagged_devices").orderBy("detected_at")

flagged.select("device_id", "player_count", "detected_at").show(truncate=False)

flagged.select(
    "detected_at",
    F.explode("linked_players").alias("player_id")
).orderBy("detected_at").show(100, truncate=False)
