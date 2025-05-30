from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, expr, lit, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
KAFKA_BROKER = "kafka-h2dn:29092"
KAFKA_TOPIC = "device-data"  # Có thể giữ nguyên hoặc thay bằng "device-data" nếu bạn crawl cả hai vào cùng topic
ES_NODES = "elasticsearch-h2dn"
ES_PORT = "9200"
ES_INDEX = "devices_prod"  # Đổi thành "devices" để phản ánh cả laptop và điện thoại
CHECKPOINT_LOCATION = "hdfs://namenode-h2dn:9000/user/spark/checkpoints/device_stream_prod_checkpoint"  # Đổi tên checkpoint

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
    .appName("DeviceDataKafkaToElasticsearch_Prod") \
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

# --- Chuẩn hóa Schema và thay thế "laptop" bằng "device" ---
# Thay thế "laptop" trong name và source bằng "device"
transformed_df = parsed_df \
    .withColumn("@timestamp", expr("CAST(crawl_timestamp / 1000 AS TIMESTAMP)")) \
    .withColumn("price", col("price").cast(FloatType())) \
    .withColumn("price_old", col("price_old").cast(FloatType())) \
    .withColumn("percent", col("percent").cast(IntegerType())) \
    .withColumn("rating", col("rating").cast(FloatType())) \
    .withColumn("sold", col("sold").cast(IntegerType())) \
    .withColumn("view_type", lit("stream"))

# Lựa chọn cột cuối cùng để ghi vào Elasticsearch
final_df_to_write = transformed_df.select(
    "crawl_timestamp",  # Sẽ được dùng làm es.mapping.id
    "product_id",
    "name",
    "brand",
    "category",
    "color",
    "price",  # Đã là Float
    "price_old",  # Đã là Float
    "RAM",
    "ROM",
    "percent",  # Đã là Integer
    "rating",  # Đã là Float
    "sold",  # Đã là Integer
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