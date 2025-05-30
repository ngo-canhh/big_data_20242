from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, year, month, dayofmonth, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
KAFKA_BROKER = "kafka-h2dn:29092"
KAFKA_TOPIC = "device-data"  
HDFS_RAW_DATA_PATH = "hdfs://namenode-h2dn:9000/user/data/device_raw_parquet"
CHECKPOINT_LOCATION_HDFS = "hdfs://namenode-h2dn:9000/user/spark/checkpoints/kafka_to_hdfs_device_checkpoint"

# Hàm kiểm tra thư mục HDFS
def ensure_hdfs_directories(spark, paths):
    try:
        conf = spark._jsc.hadoopConfiguration()
        for path_str in paths:
            uri = spark._jvm.java.net.URI(path_str)
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
            hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
            if not fs.exists(hdfs_path):
                fs.mkdirs(hdfs_path)
                logger.info(f"Created HDFS directory: {path_str}")
            else:
                logger.info(f"HDFS directory already exists: {path_str}")
    except Exception as e:
        logger.error(f"ERROR ensuring HDFS directories for path '{path_str if 'path_str' in locals() else 'unknown'}': {e}", exc_info=True)
        raise

def main():
    logger.info("Starting Spark Streaming job: Kafka to HDFS Raw Parquet for Device Data")

    spark = SparkSession \
        .builder \
        .appName("KafkaToHDFSDeviceRaw") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION_HDFS) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    try:
        logger.info("Ensuring HDFS directories exist...")
        ensure_hdfs_directories(spark, [CHECKPOINT_LOCATION_HDFS, HDFS_RAW_DATA_PATH])
        logger.info("HDFS directories checked/created.")
    except Exception as e:
        logger.error(f"FATAL: Could not ensure HDFS directories. Exiting. Error: {e}")
        spark.stop()
        exit()

    try:
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        logger.info("Kafka stream loaded.")
    except Exception as e:
        logger.error(f"ERROR loading Kafka stream: {e}", exc_info=True)
        spark.stop()
        exit()

    # Schema gốc từ Kafka topic `laptop-data`
    schema = StructType([
        StructField("crawl_timestamp", LongType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("category", StringType(), True),
        StructField("color", StringType(), True),
        StructField("price", StringType(), True),  # Sẽ được cast sang Float
        StructField("price_old", StringType(), True),  # Sẽ được cast sang Float
        StructField("RAM", StringType(), True),
        StructField("ROM", StringType(), True),
        StructField("percent", StringType(), True),  # Sẽ được cast sang Integer
        StructField("rating", StringType(), True),  # Sẽ được cast sang Float
        StructField("sold", StringType(), True),  # Sẽ được cast sang Integer
        StructField("source", StringType(), True)
    ])
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
                        .select(from_json(col("json_value"), schema).alias("data")) \
                        .select("data.*")

    # Thêm cột partition dựa trên crawl_timestamp
    final_df_to_write = parsed_df \
        .withColumn("timestamp", expr("CAST(crawl_timestamp / 1000 AS TIMESTAMP)")) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .withColumn("price", col("price").cast(FloatType())) \
        .withColumn("price_old", col("price_old").cast(FloatType())) \
        .withColumn("percent", col("percent").cast(IntegerType())) \
        .withColumn("rating", col("rating").cast(FloatType())) \
        .withColumn("sold", col("sold").cast(IntegerType())) \
        .drop("timestamp")  # Loại bỏ cột tạm thời sau khi tính partition

    logger.info("Schema to be written to HDFS:")
    final_df_to_write.printSchema()

    logger.info(f"Attempting to start writeStream to HDFS path '{HDFS_RAW_DATA_PATH}' with checkpoint '{CHECKPOINT_LOCATION_HDFS}'")
    try:
        query = final_df_to_write \
            .writeStream \
            .format("parquet") \
            .outputMode("append") \
            .partitionBy("year", "month", "day") \
            .option("path", HDFS_RAW_DATA_PATH) \
            .option("checkpointLocation", CHECKPOINT_LOCATION_HDFS) \
            .trigger(processingTime='1 minute') \
            .start()

        logger.info(f"Writing stream to HDFS path '{HDFS_RAW_DATA_PATH}' started successfully.")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"ERROR during writeStream operation to HDFS: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Spark Streaming job finished.")

if __name__ == "__main__":
    main()