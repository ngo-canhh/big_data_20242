from pyspark.sql import SparkSession
import sys # Để lấy đường dẫn file từ tham số

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit view_parquet.py <path_to_parquet_file>")
        exit(-1)

    file_path = sys.argv[1] # Lấy đường dẫn file từ tham số dòng lệnh

    print(f"Attempting to read Parquet file: {file_path}")

    spark = SparkSession \
        .builder \
        .appName("ViewParquetFile") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") # Giảm log thừa

    try:
        df = spark.read.parquet(file_path)
        print(f"Successfully read file. Schema:")
        df.printSchema()
        print(f"Data (showing top rows):")
        df.show(truncate=False) # Hiển thị dữ liệu, không cắt ngắn

        # Hoặc bạn có thể thu thập và in từng dòng nếu muốn
        # collected_data = df.collect()
        # print("Collected Data:")
        # for row in collected_data:
        #     print(row)

    except Exception as e:
        print(f"ERROR reading Parquet file {file_path}: {e}")
    finally:
        spark.stop()
        print("Spark job finished.")