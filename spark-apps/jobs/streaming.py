
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, lit, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
KAFKA_BROKER = "kafka-h2dn:29092"
KAFKA_TOPIC = "football_players"
ES_NODES = "elasticsearch-h2dn"
ES_PORT = "9200"
ES_INDEX = "football_players_prod"
CHECKPOINT_LOCATION = "hdfs://namenode-h2dn:9000/user/spark/checkpoints/football_stream_prod_checkpoint"

# Hàm kiểm tra thư mục HDFS checkpoint và tạo nếu không tồn tại
def ensure_hdfs_checkpoint(spark, checkpoint_path):
    try:
        conf = spark._jsc.hadoopConfiguration()
        uri = spark._jvm.java.net.URI(checkpoint_path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(checkpoint_path)
        if not fs.exists(path):
            fs.mkdirs(path)
            logger.info(f"Created checkpoint directory: {checkpoint_path}")
        else:
            logger.info(f"Checkpoint directory already exists: {checkpoint_path}")
    except Exception as e:
        logger.error(f"ERROR ensuring HDFS checkpoint directory: {e}", exc_info=True)
        raise

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("FootballPlayersKafkaToElasticsearch_Prod") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info("SparkSession created.")

try:
    logger.info("Ensuring HDFS checkpoint directory exists...")
    ensure_hdfs_checkpoint(spark, CHECKPOINT_LOCATION)
    logger.info("HDFS checkpoint directory checked/created.")
except Exception as e:
    logger.error(f"FATAL: Could not ensure HDFS checkpoint directory. Exiting. Error: {e}")
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
    StructField("player_agent", StringType(), True),
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
    StructField("crawl_timestamp", LongType(), True),
    StructField("source", StringType(), True)
])
logger.info("Schema defined.")

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

value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
parsed_df = value_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")
logger.info("JSON parsed.")

# --- Chuẩn hóa và lọc dữ liệu ---
# Thay thế null trong player_agent bằng "None"
transformed_df = parsed_df \
    .withColumn("player_agent", when(col("player_agent").isNull(), "None").otherwise(col("player_agent"))) \
    .withColumn("@timestamp", expr("CAST(crawl_timestamp / 1000 AS TIMESTAMP)")) \
    .withColumn("view_type", lit("stream"))

# Lọc các bản ghi có tất cả các trường chỉ số không null
stat_columns = [
    "appearances", "PPG", "goals", "assists", "own_goals", "substitutions_on",
    "substitutions_off", "yellow_cards", "second_yellow_cards", "red_cards",
    "penalty_goals", "minutes_per_goal", "minutes_played", "goals_conceded", "clean_sheet"
]
filter_condition = " AND ".join([f"{col_name} IS NOT NULL" for col_name in stat_columns])
filtered_df = transformed_df.filter(filter_condition)
logger.info("Applied filter to ensure all stat columns are not null.")

# Lựa chọn cột cuối cùng để ghi vào Elasticsearch
final_df_to_write = filtered_df.select(
    "player_id",
    "name",
    "player_club",
    "age",
    "position",
    "goalkeeper_or_not",
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
    "goals_conceded",
    "clean_sheet",
    "crawl_timestamp",  # Sẽ được dùng làm es.mapping.id
    "source",
    "@timestamp",  # Trường timestamp chuẩn cho Elasticsearch
    "view_type"
)

logger.info("Schema to be written to Elasticsearch (Streaming):")
final_df_to_write.printSchema()

logger.info(f"Attempting to start writeStream to Elasticsearch index '{ES_INDEX}' with checkpoint '{CHECKPOINT_LOCATION}'")
try:
    query = final_df_to_write \
        .writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.resource", ES_INDEX) \
        .option("es.mapping.id", "crawl_timestamp") \
        .option("es.write.operation", "index") \
        .option("es.nodes.wan.only", "true") \
        .option("es.index.auto.create", "true") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .outputMode("append") \
        .trigger(processingTime='1 minute') \
        .start()

    logger.info(f"Writing stream to Elasticsearch index '{ES_INDEX}' started successfully.")
    query.awaitTermination()
except Exception as e:
    logger.error(f"ERROR during writeStream operation to Elasticsearch: {e}", exc_info=True)
finally:
    logger.info("Stopping SparkSession...")
    spark.stop()
    logger.info("Spark Streaming job finished.")
