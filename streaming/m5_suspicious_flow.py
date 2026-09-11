from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("m5_suspicious_flow")
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

features = (
    spark.read
    .format("delta")
    .load("data/delta/features_windowed")
)

positive_deposits = features.filter(
    F.col("total_deposit") > 0
)

quantiles = positive_deposits.approxQuantile(
    "total_deposit",
    [0.25, 0.50, 0.75, 0.90],
    0.01
)

print("\n--- DEPOSIT DISTRIBUTION ---")
print(f"P25: {quantiles[0]:.2f}")
print(f"P50: {quantiles[1]:.2f}")
print(f"P75: {quantiles[2]:.2f}")
print(f"P90: {quantiles[3]:.2f}")

positive_deposits \
    .select("total_deposit") \
    .describe() \
    .show()

wager_ratio_threshold = 1.0
withdrawal_ratio_threshold = 0.70

# 75th precentile as the large deposit floor.

deposit_floor = quantiles[2]

print("\n--- SUSPICIOUS FLOW DETECTION ---")
print(f"Wager ratio:       < {wager_ratio_threshold}")
print(f"Withdrawal ratio:  > {withdrawal_ratio_threshold}")
print(f"Deposit floor:     >= {deposit_floor:.2f}")

df = (
    spark.readStream
    .format("delta")
    .load("data/delta/features_windowed")
)


df2 = df.withColumn(
    "wager_ratio",
    F.when(F.col("total_deposit") == 0, F.lit(999.9))
    .otherwise(F.col("total_bets") / F.col("total_deposit"))
)

df3 = df2.withColumn(
    "withdrawal_ratio",
    F.when(
        F.col("total_deposit") == 0,
        F.lit(0.0)
    ).otherwise(
        F.col("total_withdrawals") / F.col("total_deposit")
    )
)

suspicious = df3.filter(
    (F.col("wager_ratio") < wager_ratio_threshold)
    & (F.col("withdrawal_ratio") > withdrawal_ratio_threshold)
    & (F.col("total_deposit") >= deposit_floor)
)

suspicious_output = suspicious.select(
    "window_start",
    "window_end",
    "player_id",
    "total_deposit",
    "total_bets",
    "total_withdrawals",
    "deposit_count",
    "wager_ratio",
    "withdrawal_ratio",
    "first_event_ts",
    "last_event_ts"
)

query = (
    suspicious_output.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

static_features = (
    spark.read
    .format("delta")
    .load("data/delta/features_windowed")
)

static_scored = (
    static_features
    .withColumn(
        "wager_ratio",
        F.when(
            F.col("total_deposit") == 0,
            F.lit(999.9)
        ).otherwise(
            F.col("total_bets") / F.col("total_deposit")
        )
    )
    .withColumn(
        "withdrawal_ratio",
        F.when(
            F.col("total_deposit") == 0,
            F.lit(0.0)
        ).otherwise(
            F.col("total_withdrawals") / F.col("total_deposit")
        )
    )
)

static_suspicious = static_scored.filter(
    (F.col("wager_ratio") < wager_ratio_threshold)
    & (F.col("withdrawal_ratio") > withdrawal_ratio_threshold)
    & (F.col("total_deposit") >= deposit_floor)
)

total_suspicious_rows = static_suspicious.count()

distinct_suspicious_players = (
    static_suspicious
    .select("player_id")
    .distinct()
    .count()
)

print("\n--- SUSPICIOUS FLOW COUNTS ---")
print(f"Suspicious window rows: {total_suspicious_rows}")
print(f"Distinct suspicious players: {distinct_suspicious_players}")

player_summary = (
    static_suspicious
    .groupBy("player_id")
    .agg(
        F.count("*").alias("flagged_windows"),
        F.min("wager_ratio").alias("min_wager_ratio"),
        F.max("wager_ratio").alias("max_wager_ratio"),
        F.min("withdrawal_ratio").alias("min_withdrawal_ratio"),
        F.max("withdrawal_ratio").alias("max_withdrawal_ratio"),
        F.max("total_deposit").alias("max_deposit"),
        F.max("total_withdrawals").alias("max_withdrawal")
    )
    .orderBy(F.desc("max_withdrawal_ratio"))
)

player_summary.show(50, truncate=False)

total_players = (
    static_features
    .select("player_id")
    .distinct()
    .count()
)

flagged_players = (
    static_suspicious
    .select("player_id")
    .distinct()
    .count()
)

print(f"Total players: {total_players}")
print(f"Flagged players: {flagged_players}")
print(f"Flag rate: {flagged_players / total_players:.2%}")

spark.stop()

