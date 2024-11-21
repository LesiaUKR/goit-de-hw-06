from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Визначаємо схему
schema = StructType([
    StructField("id", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", StringType(), True)  # Timestamp початково вказано як String
])

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("AlertsProcessor") \
    .config("spark.ui.port", "4041") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()


# Зчитування даних із Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Парсинг JSON з Kafka
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")  # Розпаковуємо JSON у колонки

# Перетворення timestamp на TIMESTAMP
value_df = value_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Агрегація з використанням Sliding Window
agg_df = value_df.withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )

# Виведення результатів у консоль для тестування
query = agg_df.writeStream.format("console").outputMode("update").start()
query.awaitTermination()

