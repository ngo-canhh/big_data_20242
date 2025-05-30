# --- START OF FILE kafka_to_hdfs_raw.py ---

from pyspark.sql import SparkSession
# Đảm bảo import đủ các hàm cần thiết ở đầu file
from pyspark.sql.functions import col, from_json, to_date, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
KAFKA_BROKER = "kafka-h2dn:29092"
KAFKA_TOPIC = "gold-price-data"
HDFS_RAW_DATA_PATH = "hdfs://namenode-h2dn:9000/user/data/gold_raw_parquet"
CHECKPOINT_LOCATION_HDFS = "hdfs://namenode-h2dn:9000/user/spark/checkpoints/kafka_to_hdfs_checkpoint"

# Hàm kiểm tra thư mục HDFS (giữ nguyên hoặc sửa lại như phiên bản trước nếu cần)
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
    logger.info("Starting Spark Streaming job: Kafka to HDFS Raw Parquet")

    spark = SparkSession \
        .builder \
        .appName("KafkaToHDFSRaw") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION_HDFS) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    try:
        logger.info("Ensuring HDFS directories exist...")
        ensure_hdfs_directories(spark, [CHECKPOINT_LOCATION_HDFS, HDFS_RAW_DATA_PATH]) # Đảm bảo cả hai tồn tại
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

    # Schema gốc từ Kafka
    schema = StructType([
        StructField("crawl_timestamp", LongType(), True),
        StructField("price_date", StringType(), True), # Giữ là String
        StructField("gold_type", StringType(), True),
        StructField("buy_price", IntegerType(), True), # Giữ là Integer
        StructField("sell_price", IntegerType(), True), # Giữ là Integer
        StructField("source", StringType(), True)
    ])
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
                        .select(from_json(col("json_value"), schema).alias("data")) \
                        .select("data.*")

    # Thêm cột partition
    # Dữ liệu ghi vào HDFS sẽ có các cột gốc + các cột partition này
    final_df_to_write = parsed_df \
        .withColumn("year", year(to_date(col("price_date"), "yyyy-MM-dd"))) \
        .withColumn("month", month(to_date(col("price_date"), "yyyy-MM-dd"))) \
        .withColumn("day", dayofmonth(to_date(col("price_date"), "yyyy-MM-dd")))

    logger.info("Schema to be written to HDFS:")
    final_df_to_write.printSchema() # In schema cuối cùng trước khi ghi

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
# --- END OF FILE kafka_to_hdfs_raw.py ---