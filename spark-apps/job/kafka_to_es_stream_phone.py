# --- START OF FILE kafka_to_es_stream_phone.py ---

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, expr, lit, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
KAFKA_BROKER = "kafka-h2dn:29092"
KAFKA_TOPIC = "phone-product-data"
ES_NODES = "elasticsearch-h2dn"
ES_PORT = "9200"
ES_INDEX = "phone_products_prod"
CHECKPOINT_LOCATION = "hdfs://namenode-h2dn:9000/user/spark/checkpoints/phone_stream_prod_checkpoint"

# Hàm kiểm tra thư mục HDFS
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
    .appName("PhoneProductKafkaToElasticsearch_Prod") \
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

# Chuẩn hóa Schema và loại bỏ trùng lặp
transformed_df = parsed_df \
    .withColumn("@timestamp", expr("CAST(crawl_timestamp / 1000 AS TIMESTAMP)")) \
    .withColumn("crawl_date_dt", to_date(from_unixtime(col("crawl_timestamp") / 1000))) \
    .withColumn("view_type", lit("stream"))

# Loại bỏ trùng lặp: giữ bản ghi mới nhất dựa trên name
final_df_to_write = transformed_df.select(
    "name",  # Sẽ được dùng làm es.mapping.id
    "brand",
    "capacity",
    "RAM",
    "ROM",
    "model",
    "screen_size",
    "resolution",
    "new_price",
    "old_price",
    "percent",
    "rating",
    "sold",
    "source",
    "crawl_timestamp",
    "@timestamp",
    "crawl_date_dt",
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
        .option("es.mapping.id", "name") \
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
# --- END OF FILE kafka_to_es_stream_phone.py ---