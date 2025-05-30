from pyspark.sql import SparkSession
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
HDFS_RAW_DATA_PATH = "hdfs://namenode-h2dn:9000/user/data/football_players_raw_parquet"  # Thay bằng đường dẫn bạn muốn đọc (device hoặc gold)

def main():
    logger.info("Starting Spark job to read data from HDFS")

    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("ReadHDFSParquet") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    try:
        # Đọc dữ liệu Parquet từ HDFS
        logger.info(f"Reading Parquet data from {HDFS_RAW_DATA_PATH}")
        df = spark.read.parquet(HDFS_RAW_DATA_PATH)

        # Hiển thị schema
        logger.info("Schema of the DataFrame:")
        df.printSchema()
        # Hiển thị số lượng dòng
        logger.info(f"Number of rows in the DataFrame: {df.count()}")
        # Hiển thị 10 dòng đầu tiên
        logger.info("Showing first 10 rows of the DataFrame:")
        df.show(10, truncate=False)

        # (Tùy chọn) Lưu dữ liệu vào file local hoặc xử lý thêm
        # Ví dụ: df.write.csv("local_output.csv", header=True)

    except Exception as e:
        logger.error(f"ERROR reading data from HDFS: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Spark job finished.")

if __name__ == "__main__":
    main()