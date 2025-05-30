# --- START OF FILE view_parquet_phone.py ---

from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit view_parquet_phone.py <path_to_parquet_file>")
        exit(-1)

    file_path = sys.argv[1]

    print(f"Attempting to read Parquet file: {file_path}")

    spark = SparkSession \
        .builder \
        .appName("ViewParquetFilePhone") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(file_path)
        print(f"Successfully read file. Schema:")
        df.printSchema()
        print(f"Data (showing top rows):")
        df.show(truncate=False)
    except Exception as e:
        print(f"ERROR reading Parquet file {file_path}: {e}")
    finally:
        spark.stop()
        print("Spark job finished.")
# --- END OF FILE view_parquet_phone.py ---