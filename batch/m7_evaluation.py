from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("m7_evaluation")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)
def score(df, flag_col, target):
    tp = df.filter((F.col(flag_col) == True) & (F.col("fraud_type") == target)).count()
    fp = df.filter((F.col(flag_col) == True) & (F.col("fraud_type") != target)).count()
    fn = df.filter((F.col(flag_col) == False) & (F.col("fraud_type") == target)).count()
    return tp, fp, fn

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = (
    spark.read
    .format("delta")
    .load("data/delta/silver")
)

players = df.select("player_id").distinct()

print(players.count())

df1 = spark.read.csv("data/raw/ground_truth.csv", header=True)

result = (
    players.join(df1, on="player_id", how="left")
.fillna({"fraud_type" : "normal"})
)

result.groupBy("fraud_type").count().show()

features = spark.read.format("delta").load("data/delta/features_windowed")

positive_deposits = features.filter(F.col("total_deposit") > 0)

quantiles = positive_deposits.approxQuantile("total_deposit", [0.75], 0.01)
deposit_floor = quantiles[0]

print(deposit_floor)

scored = (
    features
    .withColumn(
         "wager_ratio",
         F.when(F.col("total_deposit") == 0, F.lit(999.9))
         .otherwise(F.col("total_bets") / F.col("total_deposit"))
)
    .withColumn(
        "withdrawal_ratio",
        F.when(F.col("total_deposit") == 0, F.lit(0.00))
        .otherwise(F.col("total_withdrawals") / F.col("total_deposit"))
    )
)

scored.select("player_id", "total_deposit", "total_withdrawals", "withdrawal_ratio").show(5)

suspicious = scored.filter(
    (F.col("wager_ratio") < 1.0)
    & (F.col("withdrawal_ratio") > 0.70)
    & (F.col("total_deposit") >= deposit_floor)
)

print(suspicious.count())

suspicious_players = suspicious.select("player_id").distinct()
print(suspicious_players.count())

m5_flagged_ids = [row.player_id for row in suspicious_players.collect()]

result_m5 = result.withColumn(
    "m5_flagged",
    F.col("player_id").isin(m5_flagged_ids)
)

result_m5.filter(F.col("m5_flagged") == True).show(20)

suspicious_wager_only = scored.filter(F.col("wager_ratio") < 1.0)

wager_only_ids = [row.player_id for row in suspicious_wager_only.select("player_id").distinct().collect()]

result_wager = result.withColumn(
    "flagged",
    F.col("player_id").isin(wager_only_ids)
)

suspicious_withdrawal_only = scored.filter(F.col("withdrawal_ratio") > 0.70)

withdrawal_only_ids = [row.player_id for row in suspicious_withdrawal_only.select("player_id").distinct().collect()]

result_withdrawal = result.withColumn(
    "flagged",
    F.col("player_id").isin(withdrawal_only_ids)
)

flagged_devices = spark.read.format("delta").load("data/delta/flagged_devices")
flagged_devices.show(truncate=False)

bonus_abuse_flagged = flagged_devices.select(
    F.explode("linked_players").alias("player_id")
)

bonus_abuse_flagged.show()

bonus_abuse_ids = [row.player_id for row in bonus_abuse_flagged.collect()]

result_m6 = result.withColumn(
    "m6_flagged",
    F.col("player_id").isin(bonus_abuse_ids)
)

combined = result_m5.join(
    result_m6.select("player_id", "m6_flagged"),
    on="player_id",
    how="left"
)

combined = combined.withColumn(
    "any_flagged",
    F.col("m5_flagged") | F.col("m6_flagged")
)

combined.show(5)

results = [
    ("wager_ratio_alone", *score(result_wager, "flagged", "suspicious_flow")),
    ("withdrawal_ratio_alone", *score(result_withdrawal, "flagged", "suspicious_flow")),
    ("m5_combined", *score(result_m5, "m5_flagged", "suspicious_flow")),
    ("m6_bonus_abuse", *score(result_m6, "m6_flagged", "bonus_abuse")),
]

# pipeline_combined bypasses score(): its positive class is "not normal" rather than one specific fraud_type, which score() cannot express
tp_c = combined.filter((F.col("any_flagged") == True) & (F.col("fraud_type") != "normal")).count()
fp_c = combined.filter((F.col("any_flagged") == True) & (F.col("fraud_type") == "normal")).count()
fn_c = combined.filter((F.col("any_flagged") == False) & (F.col("fraud_type") != "normal")).count()

results.append(("pipeline_combined", tp_c, fp_c, fn_c))

metrics = spark.createDataFrame(results, ["rule", "tp", "fp", "fn"])

metrics = (
    metrics
    .withColumn("precision", F.col("tp") / (F.col("tp") + F.col("fp")))
    .withColumn("recall", F.col("tp") / (F.col("tp") + F.col("fn")))
    .withColumn("evaluated_at", F.current_timestamp())
)

metrics.show(truncate=False)

(
    metrics.write
    .format("delta")
    .mode("overwrite")
    .save("data/delta/evaluation_metrics")
)

spark.read.format("delta").load("data/delta/evaluation_metrics").show(truncate=False)