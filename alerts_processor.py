from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import pandas as pd

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("AlertsProcessor") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Схема для зчитування Kafka
schema = StructType([
    StructField("id", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# Завантаження умов алертів
conditions = pd.read_csv("alerts_conditions.csv")
conditions_spark_df = spark.createDataFrame(conditions)

# Зчитування даних із Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Парсинг JSON із Kafka
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Перетворення timestamp на TIMESTAMP
value_df = value_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Агрегація з використанням Sliding Window
agg_df = value_df.withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )

# Обробка алертів
alerts_df = agg_df.crossJoin(conditions_spark_df).filter(
    ((col("t_avg") >= col("temperature_min")) | col("temperature_min").isNull()) &
    ((col("t_avg") <= col("temperature_max")) | col("temperature_max").isNull()) &
    ((col("h_avg") >= col("humidity_min")) | col("humidity_min").isNull()) &
    ((col("h_avg") <= col("humidity_max")) | col("humidity_max").isNull())
).select(
    col("window"),
    col("t_avg"),
    col("h_avg"),
    col("code"),
    col("message")
).withColumn("timestamp", col("window.start"))

# Запис у вихідний Kafka-топік
alerts_query = alerts_df.selectExpr(
    "CAST(NULL AS STRING) AS key",  # Kafka ключ може бути NULL
    "to_json(struct(*)) AS value"  # Перетворення у JSON
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts_topic") \
    .option("checkpointLocation", "./checkpoint") \
    .start()

alerts_query.awaitTermination()

