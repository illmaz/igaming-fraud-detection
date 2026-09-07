from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("betstream-smoke")
    .master("local[*]")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "io.delta:delta-spark_2.12:3.2.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("rate").option("rowsPerSecond", 2).load()
q = df.writeStream.format("console").outputMode("append").start()
q.awaitTermination(15)
q.stop()
