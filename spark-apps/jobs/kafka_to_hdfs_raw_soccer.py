# --- START OF FILE kafka_to_hdfs_player.py ---

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import logging
import os
import time # Import time for potential retries

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
# Sử dụng biến môi trường cho các cấu hình
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-h2dn:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "player-data-topic")  # Topic chứa dữ liệu cầu thủ
HDFS_RAW_DATA_PATH = os.getenv("HDFS_RAW_DATA_PATH", "hdfs://namenode-h2dn:9000/user/data/player_raw_parquet") # Đường dẫn HDFS cho dữ liệu cầu thủ
CHECKPOINT_LOCATION_HDFS = os.getenv("CHECKPOINT_LOCATION_HDFS", "hdfs://namenode-h2dn:9000/user/spark/checkpoints/kafka_to_hdfs_player_checkpoint") # Checkpoint cho job cầu thủ

# Hàm kiểm tra thư mục HDFS
def ensure_hdfs_directories(spark, paths):
    """Checks if HDFS directories exist and creates them if they don't."""
    try:
        conf = spark._jsc.hadoopConfiguration()
        for path_str in paths:
            try:
                # Use org.apache.hadoop.fs.Path for HDFS path representation
                path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
                fs = path.getFileSystem(conf)
                if not fs.exists(path):
                    fs.mkdirs(path)
                    logger.info(f"Created HDFS directory: {path_str}")
                else:
                    logger.info(f"HDFS directory already exists: {path_str}")
            except Exception as path_e:
                 logger.error(f"Error processing HDFS path '{path_str}': {path_e}")
                 raise # Re-raise to fail the function if one path fails

    except Exception as e:
        logger.error(f"ERROR ensuring HDFS directories: {e}", exc_info=True)
        raise # Critical failure


def main():
    logger.info("Starting Spark Streaming job: Kafka to HDFS Raw Parquet for Player Data")

    # Khởi tạo Spark Session
    logger.info("Initializing SparkSession...")
    spark = SparkSession \
        .builder \
        .appName("KafkaToHDFSPlayerRaw") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION_HDFS) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") # Giảm bớt log từ Spark
    logger.info("SparkSession created.")

    # Đảm bảo các thư mục HDFS tồn tại
    try:
        logger.info("Ensuring HDFS directories exist...")
        ensure_hdfs_directories(spark, [CHECKPOINT_LOCATION_HDFS, HDFS_RAW_DATA_PATH])
        logger.info("HDFS directories checked/created.")
    except Exception as e:
        logger.critical(f"FATAL: Could not ensure HDFS directories. Exiting. Error: {e}")
        spark.stop()
        exit(1) # Thoát nếu không tạo/kiểm tra được thư mục HDFS

    # Schema dữ liệu cầu thủ từ Kafka topic `player-data-topic`
    # Schema này phải khớp với cấu trúc JSON mà script csv_to_kafka.py gửi đi
    schema = StructType([
        StructField("processing_timestamp", LongType(), True), # Thêm timestamp từ script CSV
        StructField("player_id", IntegerType(), True), # Dựa trên dtype from csv_to_kafka.py, có thể là Null
        StructField("name", StringType(), True),
        StructField("player_club", StringType(), True),
        StructField("age", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("position", StringType(), True),
        StructField("market_value", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("nationality", StringType(), True),
        StructField("player_height", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("player_agent", StringType(), True),
        StructField("strong_foot", StringType(), True),
        StructField("contract_value_time", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("appearances", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("PPG", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("goals", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("assists", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("own_goals", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("substitutions_on", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("substitutions_off", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("yellow_cards", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("second_yellow_cards", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("red_cards", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("penalty_goals", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("minutes_per_goal", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("minutes_played", FloatType(), True), # Kiểu dữ liệu float trong CSV
        StructField("source_file", StringType(), True) # Thêm trường source_file
    ])
    logger.info("Schema for player data defined.")

    # Đọc dữ liệu từ Kafka
    try:
        logger.info(f"Attempting to read stream from Kafka topic: {KAFKA_TOPIC} on {KAFKA_BROKER}")
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        logger.info("Kafka stream loaded successfully.")
    except Exception as e:
        logger.critical(f"CRITICAL ERROR loading Kafka stream from topic {KAFKA_TOPIC}: {e}", exc_info=True)
        spark.stop()
        exit(1) # Thoát nếu không đọc được stream từ Kafka

    # Select value và parse JSON
    # 'value' là cột chứa message từ Kafka (byte array)
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
                        .select(from_json(col("json_value"), schema).alias("data")) \
                        .select("data.*")
    logger.info("JSON value parsed using defined schema.")

    # Thêm cột partition dựa trên processing_timestamp
    # Sử dụng processing_timestamp (miliseconds) để tạo cột timestamp và sau đó là cột partition
    final_df_to_write = parsed_df \
        .withColumn("timestamp", expr("CAST(processing_timestamp / 1000 AS TIMESTAMP)")) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .drop("timestamp")  # Loại bỏ cột timestamp tạm thời sau khi tính partition

    logger.info("Schema to be written to HDFS:")
    final_df_to_write.printSchema()

    # Ghi stream vào HDFS dưới dạng Parquet, phân vùng theo ngày
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
        # Chờ cho đến khi luồng kết thúc (ví dụ: do lỗi hoặc tín hiệu dừng)
        query.awaitTermination()
    except Exception as e:
        logger.critical(f"CRITICAL ERROR during writeStream operation to HDFS: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Spark Streaming job finished.")

if __name__ == "__main__":
    main()
# --- END OF FILE kafka_to_hdfs_player.py ---