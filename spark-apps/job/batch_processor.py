# --- START OF FILE batch_processor.py ---

from pyspark.sql import SparkSession
# Import đầy đủ ở đây
from pyspark.sql.functions import col, avg, min as spark_min, max as spark_max, count, lit, date_format, to_date, current_date, date_sub, concat
from pyspark.sql.types import DoubleType
import logging
import sys

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
HDFS_RAW_DATA_PATH = "hdfs://namenode-h2dn:9000/user/data/gold_raw_parquet"
ES_NODES = "elasticsearch-h2dn"
ES_PORT = "9200"
ES_INDEX = "gold_prices_prod"

def main(days_to_process=30):
    try:
        days_to_process = int(days_to_process)
        logger.info(f"Starting Spark Batch job: HDFS to Elasticsearch Batch Views (Processing last {days_to_process} days)")
    except (ValueError, TypeError):
        days_to_process = None
        logger.info("Starting Spark Batch job: HDFS to Elasticsearch Batch Views (Processing all data - invalid or no day parameter provided)")

    spark = SparkSession \
        .builder \
        .appName("HDFSToESBatchViews") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    try:
        # Đọc dữ liệu từ HDFS, Spark sẽ tự động nhận diện schema Parquet
        # Schema đọc vào sẽ bao gồm: crawl_timestamp, price_date (string), gold_type, buy_price, sell_price, source
        # Và các cột partition year, month, day
        hdfs_df = spark.read.parquet(HDFS_RAW_DATA_PATH)
        logger.info(f"Successfully read data from HDFS path '{HDFS_RAW_DATA_PATH}'.")
        logger.info("Schema read from HDFS:")
        hdfs_df.printSchema()
        record_count = hdfs_df.count()
        logger.info(f"Total records read from HDFS: {record_count}")
        if record_count == 0:
             logger.warning("No data found in HDFS path. Exiting.")
             spark.stop()
             exit()
    except Exception as e:
        logger.error(f"ERROR reading data from HDFS: {e}", exc_info=True)
        spark.stop()
        exit()

    if days_to_process is not None:
        logger.info(f"Filtering data for the last {days_to_process} days...")
        # Dùng cột price_date (string) để lọc, Spark sẽ tự cast khi so sánh với date
        hdfs_df_filtered = hdfs_df.filter(
            col("price_date") >= date_format(date_sub(current_date(), days_to_process), "yyyy-MM-dd")
        )
        filtered_count = hdfs_df_filtered.count()
        logger.info(f"Records after filtering: {filtered_count}")
        if filtered_count == 0:
             logger.warning("No data to process after filtering. Exiting.")
             spark.stop()
             exit()
        hdfs_df_to_process = hdfs_df_filtered
    else:
        logger.info("Processing all available data from HDFS.")
        hdfs_df_to_process = hdfs_df

    logger.info("Calculating batch views (daily aggregations)...")
    # Group by cột price_date gốc (String) và gold_type
    daily_agg_df = hdfs_df_to_process \
        .groupBy("price_date", "gold_type") \
        .agg(
            avg("buy_price").alias("avg_buy_price"),
            avg("sell_price").alias("avg_sell_price"),
            spark_min("buy_price").alias("min_buy_price"),
            spark_max("buy_price").alias("max_buy_price"),
            spark_min("sell_price").alias("min_sell_price"),
            spark_max("sell_price").alias("max_sell_price"),
            count("*").alias("record_count")
        ) \
        .withColumn("view_type", lit("batch")) # Thêm trường nhận biết

    # Tạo ID duy nhất
    batch_view_df = daily_agg_df \
        .withColumn("doc_id", concat(col("price_date"), lit("_"), col("gold_type")).cast("string"))

    logger.info("Batch views calculated. Schema:")
    batch_view_df.printSchema()
    logger.info("Sample batch view data:")
    batch_view_df.show(5, truncate=False)

    logger.info(f"Writing batch views to Elasticsearch index '{ES_INDEX}'...")
    try:
        batch_view_df.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("append") \
            .option("es.nodes", ES_NODES) \
            .option("es.port", ES_PORT) \
            .option("es.resource", ES_INDEX) \
            .option("es.mapping.id", "doc_id") \
            .option("es.write.operation", "upsert") \
            .option("es.nodes.wan.only", "true") \
            .option("es.index.auto.create", "true") \
            .save()
        logger.info("Successfully wrote batch views to Elasticsearch.")
    except Exception as e:
        logger.error(f"ERROR writing batch views to Elasticsearch: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Spark Batch job finished.")

if __name__ == "__main__":
    num_days = 30
    if len(sys.argv) > 1:
        num_days_arg = sys.argv[1]
        if num_days_arg.lower() == 'all':
            num_days = None
        else:
            try:
                num_days = int(num_days_arg)
            except ValueError:
                 logger.warning(f"Invalid day argument '{num_days_arg}'. Processing all data.")
                 num_days = None
    else:
        logger.info("No day argument provided. Processing last 30 days by default.")

    main(num_days)

# --- END OF FILE batch_processor.py ---