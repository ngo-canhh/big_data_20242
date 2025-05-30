from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, lit, current_timestamp, when, regexp_replace, expr, log1p
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, FloatType
from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml.regression import RandomForestRegressionModel
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
MODEL_PATH = "hdfs://namenode-h2dn:9000/user/spark/models/football_market_value_model"
SCALER_PATH = "hdfs://namenode-h2dn:9000/user/spark/models/scaler_model"
KAFKA_BROKERS = "kafka-h2dn:29092"  # Thay bằng hostname Kafka trong mạng Docker
KAFKA_TOPIC_PREDICT = "soccer_to_predict"
ES_NODES = "elasticsearch-h2dn"  # Thay bằng hostname Elasticsearch trong mạng Docker
ES_PORT = "9200"
ES_INDEX_PREDICT = "soccer_predictions"
CHECKPOINT_LOCATION_PREDICT = "hdfs://namenode-h2dn:9000/user/spark/checkpoints/soccer_predictions_stream_checkpoint"

input_data_schema = StructType([
    StructField("player_id", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("club", StringType(), True),
    StructField("position", StringType(), True),
    StructField("strong_foot", StringType(), True),
    StructField("age", FloatType(), True),
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
    StructField("market_value", FloatType(), True),
    StructField("ingest_timestamp_manual", LongType(), True),
    StructField("source", StringType(), True),
    StructField("source_type", StringType(), True)
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
    logger.info("Starting Spark Structured Streaming job for manual soccer player prediction")

    spark = SparkSession \
        .builder \
        .appName("SoccerPlayerManualPredictionStream") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created for streaming.")

    try:
        ensure_hdfs_checkpoint(spark, CHECKPOINT_LOCATION_PREDICT)
        logger.info("HDFS checkpoint directory checked/created.")
    except Exception as e:
        logger.error(f"FATAL: Could not ensure HDFS checkpoint directory. Exiting. Error: {e}")
        spark.stop()
        exit(1)

    try:
        # Kiểm tra và tải mô hình
        logger.info(f"Loading model from {MODEL_PATH}")
        model = RandomForestRegressionModel.load(MODEL_PATH)
        logger.info("Model loaded successfully.")

        # Tải scaler model
        logger.info(f"Loading scaler from {SCALER_PATH}")
        scaler_model = StandardScalerModel.load(SCALER_PATH)
        logger.info("Scaler loaded successfully.")

        logger.info(f"Reading stream from Kafka topic: {KAFKA_TOPIC_PREDICT}")
        kafka_stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("subscribe", KAFKA_TOPIC_PREDICT) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        parsed_df = kafka_stream_df \
            .selectExpr("CAST(value AS STRING) as json_string", "timestamp as kafka_timestamp") \
            .select(from_json(col("json_string"), input_data_schema).alias("data"), "kafka_timestamp")

        processed_df = parsed_df.select("kafka_timestamp", "data.*")
        logger.info("Raw data parsed from Kafka.")

        feature_columns = [
            "age", "appearances", "PPG", "goals", "assists", "own_goals",
            "substitutions_on", "substitutions_off", "yellow_cards",
            "second_yellow_cards", "red_cards", "penalty_goals",
            "minutes_per_goal", "minutes_played", "player_height"
        ]

        logger.info("Applying filters and null checks...")
        filtered_df = processed_df.filter(
            (col("position").isNull()) | (col("position") != "Goalkeeper")
        ).filter(
            (col("strong_foot").isNull()) | (col("strong_foot").isin("left", "right", "both"))
        )

        # Kiểm tra và sửa đơn vị (nếu cần)
        filtered_df = filtered_df.withColumn("player_height",
                                             when(col("player_height") > 10, col("player_height") / 100)
                                             .otherwise(col("player_height")))
        logger.info("Checked and corrected units (e.g., player_height from cm to m).")

        # Xử lý outlier bằng biến đổi log
        log_transform_columns = ["appearances", "goals", "assists", "minutes_played", "minutes_per_goal"]
        for col_name in log_transform_columns:
            filtered_df = filtered_df.withColumn(f"{col_name}_log", log1p(col(col_name)))
        feature_columns = [f"{col_name}_log" if col_name in log_transform_columns else col_name 
                           for col_name in feature_columns]
        logger.info("Applied log transformation to handle outliers.")

        for col_name in feature_columns:
            filtered_df = filtered_df.filter(col(col_name).isNotNull())

        filtered_df = filtered_df.filter(col("player_id").isNotNull())
        logger.info("Data filtered and required feature/ID nulls handled.")

        logger.info("Assembling features...")
        assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="features",
            handleInvalid="skip"
        )
        feature_vector_df = assembler.transform(filtered_df)

        # Chuẩn hóa dữ liệu
        logger.info("Scaling features...")
        scaled_df = scaler_model.transform(feature_vector_df)
        ready_to_predict_df = scaled_df.filter(col("scaled_features").isNotNull())
        logger.info("Data ready for prediction.")

        logger.info("Generating predictions...")
        predictions = model.transform(ready_to_predict_df)
        logger.info("Predictions generated.")

        logger.info(f"Preparing data for Elasticsearch index: {ES_INDEX_PREDICT}")
        output_df = predictions.select(
            col("player_id"),
            col("player_name"),
            col("club"),
            col("age"),
            col("position"),
            col("strong_foot"),
            col("appearances"),
            col("PPG"),
            col("goals"),
            col("assists"),
            col("own_goals"),
            col("substitutions_on"),
            col("substitutions_off"),
            col("yellow_cards"),
            col("second_yellow_cards"),
            col("red_cards"),
            col("penalty_goals"),
            col("minutes_per_goal"),
            col("minutes_played"),
            col("player_height"),
            col("nationality"),
            col("contract_value_time"),
            col("goalkeeper_or_not"),
            col("goals_conceded"),
            col("clean_sheet"),
            col("prediction").alias("predicted_market_value"),
            col("kafka_timestamp").alias("kafka_ingest_timestamp"),
            col("ingest_timestamp_manual"),
            current_timestamp().alias("spark_processing_timestamp"),
            expr("CAST(ingest_timestamp_manual / 1000 AS TIMESTAMP)").alias("@timestamp"),
            col("source_type"),
            col("source").alias("original_source")
        )

        logger.info(f"Schema to be written to Elasticsearch ({ES_INDEX_PREDICT}):")
        output_df.printSchema()

        logger.info(f"Attempting to start writeStream to Elasticsearch index '{ES_INDEX_PREDICT}' with checkpoint '{CHECKPOINT_LOCATION_PREDICT}'")
        query = output_df \
            .writeStream \
            .outputMode("append") \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", ES_NODES) \
            .option("es.port", ES_PORT) \
            .option("es.resource", ES_INDEX_PREDICT) \
            .option("es.mapping.id", "player_id") \
            .option("es.write.operation", "index") \
            .option("es.nodes.wan.only", "true") \
            .option("es.net.ssl", "false") \
            .option("es.index.auto.create", "true") \
            .option("checkpointLocation", CHECKPOINT_LOCATION_PREDICT) \
            .trigger(processingTime='30 seconds') \
            .start()

        logger.info(f"Writing stream to Elasticsearch index '{ES_INDEX_PREDICT}' started successfully.")
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