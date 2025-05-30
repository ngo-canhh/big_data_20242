# --- START OF FILE kafka_to_hdfs_raw_phone.py ---

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
KAFKA_BROKER = "kafka-h2dn:29092"
KAFKA_TOPIC = "phone-product-data"
HDFS_RAW_DATA_PATH = "hdfs://namenode-h2dn:9000/user/data/phone_raw_parquet/phone_data.parquet"
CHECKPOINT_LOCATION_HDFS = "hdfs://namenode-h2dn:9000/user/spark/checkpoints/kafka_to_hdfs_phone_checkpoint"

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
    logger.info("Starting Spark Streaming job: Kafka to HDFS Raw Parquet (Phone Data)")

    spark = SparkSession \
        .builder \
        .appName("KafkaToHDFSRawPhone") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION_HDFS) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    try:
        logger.info("Ensuring HDFS directories exist...")
        hdfs_parent_dir = "/".join(HDFS_RAW_DATA_PATH.split("/")[:-1])
        ensure_hdfs_directories(spark, [CHECKPOINT_LOCATION_HDFS, hdfs_parent_dir])
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

    # Schema dữ liệu điện thoại từ Kafka
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("capacity", StringType(), True),
        StructField("RAM", StringType(), True),
        StructField("ROM", StringType(), True),
        StructField("model", StringType(), True),
        StructField("screen_size", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("new_price", StringType(), True),
        StructField("old_price", StringType(), True),
        StructField("percent", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("sold", StringType(), True),
        StructField("crawl_timestamp", LongType(), True),
        StructField("source", StringType(), True)
    ])
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
                        .select(from_json(col("json_value"), schema).alias("data")) \
                        .select("data.*")

    # Thêm cột crawl_date (sửa lỗi chuyển đổi kiểu dữ liệu)
    final_df_to_write = parsed_df \
        .withColumn("crawl_date", to_date(from_unixtime(col("crawl_timestamp") / 1000)))

    logger.info("Schema to be written to HDFS:")
    final_df_to_write.printSchema()

    logger.info(f"Attempting to start writeStream to HDFS path '{HDFS_RAW_DATA_PATH}' with checkpoint '{CHECKPOINT_LOCATION_HDFS}'")
    try:
        query = final_df_to_write \
            .writeStream \
            .format("parquet") \
            .outputMode("append") \
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
# --- END OF FILE kafka_to_hdfs_raw_phone.py ---