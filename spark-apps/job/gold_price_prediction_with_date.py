from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, year, month, dayofmonth
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Gold Price Prediction with Spark MLlib using price_date")

    # Khởi tạo Spark Session
    spark = SparkSession \
        .builder \
        .appName("GoldPricePredictionWithDate") \
        .master("spark://spark-master-h2dn:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    try:
        # Đọc dữ liệu từ HDFS (tất cả file Parquet trong các thư mục con)
        logger.info("Loading data from HDFS...")
        data_df = spark.read.option("mergeSchema", "true").parquet("hdfs://namenode-h2dn:9000/user/data/gold_raw_parquet")
        logger.info("Data loaded successfully. Schema:")
        data_df.printSchema()

        # Kiểm tra dữ liệu
        data_df.show(5)

        # Chuẩn bị dữ liệu
        # Trích xuất các đặc trưng thời gian từ price_date
        data_with_features = data_df \
            .withColumn("year", year(col("price_date"))) \
            .withColumn("month", month(col("price_date"))) \
            .withColumn("day", dayofmonth(col("price_date")))

        # Mã hóa cột 'gold_type' thành số
        indexer = StringIndexer(inputCol="gold_type", outputCol="gold_type_indexed")
        indexed_df = indexer.fit(data_with_features).transform(data_with_features)

        # OneHotEncoder cho gold_type_indexed
        encoder = OneHotEncoder(inputCol="gold_type_indexed", outputCol="gold_type_encoded")
        encoded_df = encoder.fit(indexed_df).transform(indexed_df)

        # Tạo vector đặc trưng từ các cột: buy_price, year, month, day, gold_type_encoded
        assembler = VectorAssembler(
            inputCols=["buy_price", "year", "month", "day", "gold_type_encoded"],
            outputCol="features"
        )
        assembled_df = assembler.transform(encoded_df)

        # Chọn cột features và label (sell_price), giữ lại date và gold_type
        final_df = assembled_df.select("price_date", "gold_type", "features", col("sell_price").alias("label"))

        # Loại bỏ các hàng có giá trị null
        final_df = final_df.na.drop()

        # Chia dữ liệu thành tập huấn luyện (80%) và tập kiểm tra (20%)
        train_data, test_data = final_df.randomSplit([0.8, 0.2], seed=42)
        logger.info(f"Training data count: {train_data.count()}, Test data count: {test_data.count()}")

        # Tạo và huấn luyện mô hình Linear Regression
        logger.info("Training Linear Regression model...")
        lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
        model = lr.fit(train_data)

        # In thông tin mô hình
        logger.info(f"Coefficients: {model.coefficients}")
        logger.info(f"Intercept: {model.intercept}")

        # Đánh giá mô hình trên tập kiểm tra
        logger.info("Evaluating model...")
        predictions = model.transform(test_data)
        evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        logger.info(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

        # Hiển thị một số dự đoán với date và gold_type thay vì features
        predictions.select("price_date", "gold_type", "label", "prediction").show(10)

        # Lưu mô hình (sử dụng đường dẫn local để tránh lỗi HDFS)
        model_path_hdfs = "hdfs://namenode-h2dn:9000/user/spark/models/gold_price_model_with_date"
        model_path_local = "file:///tmp/spark_model"
        model.write().overwrite().save(model_path_hdfs)
        logger.info(f"Model saved to {model_path_hdfs}")

    except Exception as e:
        logger.error(f"ERROR during prediction process: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Prediction job finished.")

if __name__ == "__main__":
    main()