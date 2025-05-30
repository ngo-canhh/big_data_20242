
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, expr, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
KAFKA_BROKER = "kafka-h2dn:29092"
KAFKA_TOPIC = "football_players"
HDFS_RAW_DATA_PATH = "hdfs://namenode-h2dn:9000/user/data/football_players_raw_parquet"
CHECKPOINT_LOCATION_HDFS = "hdfs://namenode-h2dn:9000/user/spark/checkpoints/kafka_to_hdfs_football_checkpoint"

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
    logger.info("Starting Spark Streaming job: Kafka to HDFS Raw Parquet for Football Players")

    spark = SparkSession \
        .builder \
        .appName("KafkaToHDFSFootballRaw") \
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

    # Schema cho dữ liệu bóng đá từ Kafka topic `football_players`
    schema = StructType([
        StructField("player_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("player_club", StringType(), True),
        StructField("age", FloatType(), True),
        StructField("position", StringType(), True),
        StructField("goalkeeper_or_not", StringType(), True),
        StructField("market_value", FloatType(), True),
        StructField("nationality", StringType(), True),
        StructField("player_height", FloatType(), True),
        # StructField("player_agent", StringType(), True),
        StructField("strong_foot", StringType(), True),
        StructField("contract_value_time", StringType(), True),
        StructField("appearances", FloatType(), True),
        StructField("PPG", FloatType(), True),
        StructField("goals", FloatType(), True),
        StructField("assists", FloatType(), True),
        StructField("own_goals", FloatType(), True),
        StructField("substitutions_on", FloatType(), True),
        StructField("substitutions_off", FloatType(), True),
        StructField("yellow_cards", FloatType(), True),
        StructField("second_yellow_cards", FloatType(), True),
        StructField("red_cards", FloatType(), True),
        StructField("penalty_goals", FloatType(), True),
        StructField("minutes_per_goal", FloatType(), True),
        StructField("minutes_played", FloatType(), True),
        StructField("goals_conceded", FloatType(), True),
        StructField("clean_sheet", FloatType(), True),
        StructField("crawl_timestamp", LongType(), True)
        # StructField("source", StringType(), True)
    ])

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
                        .select(from_json(col("json_value"), schema).alias("data")) \
                        .select("data.*")
    logger.info("JSON parsed.")

    # Chuẩn hóa và lọc dữ liệu
    # Thay thế null trong player_agent bằng "None"
    transformed_df = parsed_df \
        .withColumn("timestamp", expr("CAST(crawl_timestamp / 1000 AS TIMESTAMP)")) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp")))

    # Lọc các bản ghi có tất cả các trường chỉ số không null
    stat_columns = [
        "appearances", "PPG", "goals", "assists", "own_goals", "substitutions_on",
        "substitutions_off", "yellow_cards", "second_yellow_cards", "red_cards",
        "penalty_goals", "minutes_per_goal", "minutes_played", "goals_conceded", "clean_sheet"
    ]
    filter_condition = " AND ".join([f"{col_name} IS NOT NULL" for col_name in stat_columns])
    filtered_df = transformed_df.filter(filter_condition)
    logger.info("Applied filter to ensure all stat columns are not null.")

    # Loại bỏ cột tạm thời và chọn cột cuối cùng
    final_df_to_write = filtered_df.drop("timestamp").select(
        "player_id",
        "name",
        "player_club",
        "age",
        "position",
        "goalkeeper_or_not",
        "market_value",
        "nationality",
        "player_height",
        # "player_agent",
        "strong_foot",
        "contract_value_time",
        "appearances",
        "PPG",
        "goals",
        "assists",
        "own_goals",
        "substitutions_on",
        "substitutions_off",
        "yellow_cards",
        "second_yellow_cards",
        "red_cards",
        "penalty_goals",
        "minutes_per_goal",
        "minutes_played",
        "goals_conceded",
        "clean_sheet",
        "crawl_timestamp",
        # "source",
        "year",
        "month",
        "day"
    )

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
