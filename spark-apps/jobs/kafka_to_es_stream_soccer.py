# --- START OF FILE kafka_to_es_player.py ---

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import logging
import os
import time # Import time for potential retries

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
# Sử dụng biến môi trường cho các cấu hình nhạy cảm hoặc thay đổi
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-h2dn:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "player-data-topic") # Topic chứa dữ liệu cầu thủ
ES_NODES = os.getenv("ES_NODES", "elasticsearch-h2dn")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_INDEX = os.getenv("ES_INDEX", "players_prod")  # Index Elasticsearch cho dữ liệu cầu thủ
# Đảm bảo CHECKPOINT_LOCATION sử dụng scheme HDFS và đường dẫn phù hợp với cấu hình HDFS của bạn
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "hdfs://namenode-h2dn:9000/user/spark/checkpoints/player_stream_prod_checkpoint")

# Hàm kiểm tra thư mục HDFS checkpoint và tạo nếu không tồn tại
# Giữ nguyên hàm này vì vẫn sử dụng HDFS cho checkpointing
def ensure_hdfs_checkpoint(spark, checkpoint_path):
    try:
        conf = spark._jsc.hadoopConfiguration()
        # Use org.apache.hadoop.fs.Path for HDFS path
        path = spark._jvm.org.apache.hadoop.fs.Path(checkpoint_path)
        fs = path.getFileSystem(conf)
        if not fs.exists(path):
            fs.mkdirs(path)
            logger.info(f"Created checkpoint directory: {checkpoint_path}")
        else:
            logger.info(f"Checkpoint directory already exists: {checkpoint_path}")
    except Exception as e:
        logger.error(f"ERROR ensuring HDFS checkpoint directory: {e}", exc_info=True)
        # Depending on criticality, you might want to exit or raise here
        # raise # Uncomment to make this a critical failure
        logger.warning("Proceeding without guaranteed checkpoint directory creation. Stream might fail if directory doesn't exist.")


# Khởi tạo Spark Session
logger.info("Initializing SparkSession...")
spark = SparkSession.builder \
    .appName("PlayerDataKafkaToElasticsearch_Prod") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("SparkSession created.")

# Đảm bảo thư mục checkpoint tồn tại
# Thực hiện kiểm tra này sớm để phát hiện lỗi cấu hình HDFS
try:
    logger.info("Ensuring HDFS checkpoint directory exists...")
    ensure_hdfs_checkpoint(spark, CHECKPOINT_LOCATION)
    logger.info("HDFS checkpoint directory checked/created.")
except Exception as e:
    logger.critical(f"FATAL: Could not ensure HDFS checkpoint directory. Exiting. Error: {e}")
    spark.stop()
    exit(1) # Thoát nếu checkpoint không thể đảm bảo

# Schema dữ liệu cầu thủ từ Kafka topic `player-data-topic`
# Schema này phải khớp với cấu trúc JSON mà script csv_to_kafka.py gửi đi
schema = StructType([
    StructField("processing_timestamp", LongType(), True), # Thêm timestamp từ script CSV
    StructField("player_id", IntegerType(), True), # Dựa trên dtype from csv_to_kafka.py, có thể là Null
    StructField("name", StringType(), True),
    StructField("player_club", StringType(), True),
    StructField("age", FloatType(), True),
    StructField("position", StringType(), True),
    StructField("market_value", FloatType(), True),
    StructField("nationality", StringType(), True),
    StructField("player_height", FloatType(), True),
    StructField("player_agent", StringType(), True),
    StructField("strong_foot", StringType(), True),
    StructField("contract_value_time", FloatType(), True),
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
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
parsed_df = value_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")
logger.info("JSON value parsed using defined schema.")

# --- Chuẩn hóa Schema và thêm trường cho Elasticsearch ---
# Tạo trường @timestamp từ processing_timestamp (miliseconds)
# processing_timestamp / 1000 chuyển từ miliseconds sang seconds
# CAST(... AS TIMESTAMP) chuyển sang Spark Timestamp type
transformed_df = parsed_df \
    .withColumn("@timestamp", expr("CAST(processing_timestamp / 1000 AS TIMESTAMP)")) \
    .withColumn("view_type", lit("stream")) # Thêm trường phân biệt nguồn dữ liệu (stream/batch)

# Lựa chọn cột cuối cùng để ghi vào Elasticsearch
# Đảm bảo tất cả các trường từ schema gốc và các trường mới được thêm đều có mặt
final_df_to_write = transformed_df.select(
    "processing_timestamp", # Giữ lại nếu cần, không nhất thiết dùng làm ES ID
    "player_id",           # Sử dụng làm es.mapping.id
    "name",
    "player_club",
    "age",
    "position",
    "market_value",
    "nationality",
    "player_height",
    "player_agent",
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
    "source_file",
    "@timestamp",          # Trường timestamp chuẩn cho Elasticsearch
    "view_type"
)

logger.info("Schema to be written to Elasticsearch (Streaming Player Data):")
final_df_to_write.printSchema()

# Kiểm tra xem player_id có null không trước khi ghi
# Nếu player_id bị null, ES sẽ gặp lỗi khi sử dụng nó làm mapping ID
# Tùy chọn: lọc bỏ các bản ghi có player_id null, hoặc xử lý chúng khác
# filtered_df = final_df_to_write.filter(col("player_id").isNotNull())
# logger.info(f"Added filter to exclude rows with null player_id.")

# Ghi stream vào Elasticsearch
logger.info(f"Attempting to start writeStream to Elasticsearch index '{ES_INDEX}' with checkpoint '{CHECKPOINT_LOCATION}'")
try:
    query = final_df_to_write \
        .writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", ES_INDEX) \
        .option("es.mapping.id", "player_id") \
        .option("es.write.operation", "index") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.nodes.wan.only", "true") \
        .option("es.index.auto.create", "true") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .outputMode("append") \
        .trigger(processingTime='1 minute') \
        .start()

    logger.info(f"Writing stream to Elasticsearch index '{ES_INDEX}' started successfully.")
    # Chờ cho đến khi luồng kết thúc (ví dụ: do lỗi hoặc tín hiệu dừng)
    query.awaitTermination()
except Exception as e:
    logger.critical(f"CRITICAL ERROR during writeStream operation to Elasticsearch: {e}", exc_info=True)
finally:
    logger.info("Stopping SparkSession...")
    spark.stop()
    logger.info("Spark Streaming job finished.")

# --- END OF FILE kafka_to_es_player.py ---