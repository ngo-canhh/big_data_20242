# --- START OF FILE extract_to_csv.py (Phiên bản ghi HDFS) ---
from pyspark.sql import SparkSession
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_INPUT_PATH = "hdfs://namenode-h2dn:9000/user/data/gold_raw_parquet"
DEFAULT_OUTPUT_DIR_ON_HDFS = "hdfs://namenode-h2dn:9000/user/data/temp_gold_csv_parts"

def main(input_path, output_dir_on_hdfs):
    logger.info(f"Starting Spark job: Extract HDFS Parquet from '{input_path}' to HDFS CSV directory '{output_dir_on_hdfs}'")

    spark = SparkSession \
        .builder \
        .appName("HdfsParquetToCsv") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    try:
        logger.info(f"Reading Parquet data from HDFS path: {input_path}")
        hdfs_df = spark.read.parquet(input_path)

        columns_to_keep = ["crawl_timestamp", "price_date", "gold_type", "buy_price", "sell_price", "source"]
        final_df = hdfs_df.select(columns_to_keep)

        logger.info("Schema to be written to CSV:")
        final_df.printSchema()
        record_count = final_df.count()
        logger.info(f"Total records read and selected: {record_count}")

        if record_count == 0:
            logger.warning("No data found in the input path or after selection. Exiting.")
            spark.stop()
            return

        # --- Ghi song song ra nhiều file parts trên HDFS ---
        logger.info(f"Writing data to CSV directory on HDFS: {output_dir_on_hdfs}")
        final_df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("escape", "\"") \
            .csv(output_dir_on_hdfs) # <-- Hành động ghi vào HDFS

        logger.info(f"Successfully wrote CSV parts to HDFS directory: {output_dir_on_hdfs}")
        logger.info("Now you can merge these parts into a single local file.")
        logger.info(f"1. Run: docker exec namenode-h2dn hdfs dfs -getmerge {output_dir_on_hdfs} /tmp/merged_output.csv")
        logger.info(f"2. Run: docker cp namenode-h2dn:/tmp/merged_output.csv \"C:\\path\\to\\save\\gold_data_extracted.csv\"")
        logger.info(f"3. (Optional) Run: docker exec namenode-h2dn hdfs dfs -rm -r {output_dir_on_hdfs}")

    except Exception as e:
        logger.error(f"ERROR during Parquet to CSV conversion: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Spark job finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract HDFS Parquet data to a CSV file on HDFS.')
    parser.add_argument('--input', default=HDFS_INPUT_PATH, help=f'Input HDFS Parquet directory path (default: {HDFS_INPUT_PATH})')
    parser.add_argument('--output', default=DEFAULT_OUTPUT_DIR_ON_HDFS, help=f'Output HDFS directory path for CSV parts (default: {DEFAULT_OUTPUT_DIR_ON_HDFS})')
    args = parser.parse_args()
    main(args.input, args.output)
# --- END OF FILE extract_to_csv.py ---