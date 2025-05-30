from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, FloatType
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BROKERS = "kafka-h2dn:29092"
KAFKA_TOPIC_INPUT = "soccer_predictions_intermediate"
ES_NODES = "elasticsearch-h2dn"
ES_PORT = "9200"
ES_INDEX = "soccer_predictions"
CHECKPOINT_LOCATION = "hdfs://namenode-h2dn:9000/user/spark/checkpoints/elasticsearch_checkpoint"

input_data_schema = StructType([
    StructField("player_id", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("club", StringType(), True),
    StructField("age", FloatType(), True),
    StructField("position", StringType(), True),
    StructField("strong_foot", StringType(), True),
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
    StructField("player_height", FloatType(), True),
    StructField("nationality", StringType(), True),
    StructField("contract_value_time", StringType(), True),
    StructField("goalkeeper_or_not", StringType(), True),
    StructField("goals_conceded", FloatType(), True),
    StructField("clean_sheet", FloatType(), True),
    StructField("predicted_market_value", DoubleType(), True),
    StructField("kafka_ingest_timestamp", TimestampType(), True),
    StructField("ingest_timestamp_manual", LongType(), True),
    StructField("spark_processing_timestamp", TimestampType(), True),
    StructField("@timestamp", TimestampType(), True),
    StructField("source_type", StringType(), True),
    StructField("original_source", StringType(), True)
])
logger.info("Input data schema defined.")

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

def main():
    logger.info("Starting Spark Structured Streaming job for writing to Elasticsearch")

    spark = SparkSession \
        .builder \
        .appName("SoccerPredictionsToElasticsearch") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created for streaming.")

    try:
        ensure_hdfs_checkpoint(spark, CHECKPOINT_LOCATION)
        logger.info("HDFS checkpoint directory checked/created.")
    except Exception as e:
        logger.error(f"FATAL: Could not ensure HDFS checkpoint directory. Exiting. Error: {e}")
        spark.stop()
        exit(1)

    try:
        logger.info(f"Reading stream from Kafka topic: {KAFKA_TOPIC_INPUT}")
        kafka_stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("subscribe", KAFKA_TOPIC_INPUT) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        parsed_df = kafka_stream_df \
            .selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), input_data_schema).alias("data")) \
            .select("data.*")
        logger.info("JSON parsed from Kafka.")

        logger.info(f"Writing stream to Elasticsearch index: {ES_INDEX}")
        query = parsed_df \
            .writeStream \
            .outputMode("append") \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", ES_NODES) \
            .option("es.port", ES_PORT) \
            .option("es.resource", f"{ES_INDEX}/_doc") \
            .option("es.mapping.id", "player_id") \
            .option("es.write.operation", "index") \
            .option("es.nodes.wan.only", "true") \
            .option("es.net.ssl", "false") \
            .option("es.index.auto.create", "true") \
            .option("checkpointLocation", CHECKPOINT_LOCATION) \
            .trigger(processingTime='10 seconds') \
            .start()

        logger.info(f"Writing stream to Elasticsearch index '{ES_INDEX}' started successfully.")
        logger.info("Structured Streaming query started. Awaiting termination...")

        query.awaitTermination()

    except Exception as e:
        logger.error(f"ERROR during Spark Streaming job: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Spark Structured Streaming job finished.")

if __name__ == "__main__":
    main()