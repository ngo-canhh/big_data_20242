from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
HDFS_RAW_DATA_PATH = "hdfs://namenode-h2dn:9000/user/data/device_raw_parquet"
MODEL_PATH = "hdfs://namenode-h2dn:9000/user/spark/models/sales_prediction_model"

def main():
    logger.info("Starting Spark job to train sales prediction model")

    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("TrainSalesPredictionModel") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    try:
        # Đọc dữ liệu từ HDFS
        logger.info(f"Reading Parquet data from {HDFS_RAW_DATA_PATH}")
        df = spark.read.parquet(HDFS_RAW_DATA_PATH)

        # Hiển thị schema và một vài dòng dữ liệu
        logger.info("Schema of the DataFrame:")
        df.printSchema()
        logger.info("Sample data (5 rows):")
        df.show(5, truncate=False)

        # --- Tiền xử lý dữ liệu ---

        # 1. Chuyển đổi các cột phân loại thành dạng số
        categorical_columns = ["brand", "category", "color", "RAM", "ROM"]
        indexers = [
            StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
            for col in categorical_columns
        ]

        # 2. Tạo vector đặc trưng từ các cột số (không bao gồm rating) và cột phân loại đã được mã hóa
        numeric_columns = ["price", "price_old", "percent"]  # Loại bỏ rating
        indexed_columns = [f"{col}_index" for col in categorical_columns]
        feature_columns = numeric_columns + indexed_columns

        assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="raw_features",
            handleInvalid="skip"
        )

        # 3. Chuẩn hóa các đặc trưng số
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withStd=True,
            withMean=True
        )

        # 4. Pipeline tiền xử lý
        preprocessing_stages = indexers + [assembler, scaler]
        preprocessing_pipeline = Pipeline(stages=preprocessing_stages)

        # Áp dụng pipeline tiền xử lý
        logger.info("Applying preprocessing pipeline...")
        preprocessed_df = preprocessing_pipeline.fit(df).transform(df)

        # Loại bỏ các dòng có giá trị null trong cột mục tiêu (sold)
        preprocessed_df = preprocessed_df.filter(col("sold").isNotNull())

        # --- Chia dữ liệu thành tập huấn luyện, xác thực và kiểm tra theo category ---
        # Tách dữ liệu theo category (laptop và điện thoại)
        categories = preprocessed_df.select("category").distinct().collect()
        train_dfs = []
        val_dfs = []
        test_dfs = []

        for row in categories:
            category_value = row["category"]
            category_df = preprocessed_df.filter(col("category") == category_value)
            # Chia tỷ lệ 70% huấn luyện, 10% xác thực, 20% kiểm tra
            train_df_part, val_df_part, test_df_part = category_df.randomSplit([0.7, 0.1, 0.2], seed=42)
            train_dfs.append(train_df_part)
            val_dfs.append(val_df_part)
            test_dfs.append(test_df_part)

        # Gộp các tập con thành tập huấn luyện, xác thực và kiểm tra chung
        train_df = train_dfs[0]
        val_df = val_dfs[0]
        test_df = test_dfs[0]
        for i in range(1, len(train_dfs)):
            train_df = train_df.union(train_dfs[i])
            val_df = val_df.union(val_dfs[i])
            test_df = test_df.union(test_dfs[i])

        logger.info(f"Training data size: {train_df.count()}")
        logger.info(f"Validation data size: {val_df.count()}")
        logger.info(f"Testing data size: {test_df.count()}")

        # Kiểm tra phân bố category trong từng tập
        logger.info("Category distribution in training set:")
        train_df.groupBy("category").count().show()
        logger.info("Category distribution in validation set:")
        val_df.groupBy("category").count().show()
        logger.info("Category distribution in testing set:")
        test_df.groupBy("category").count().show()

        # --- Huấn luyện mô hình ---
        # Sử dụng GBTRegressor
        gbt = GBTRegressor(
            featuresCol="features",
            labelCol="sold",
            predictionCol="prediction",
            maxIter=200,
            maxDepth=15,
            stepSize=0.05,
            seed=42
        )
        logger.info("Training Gradient-Boosted Trees Regressor model...")
        gbt_model = gbt.fit(train_df)

        # --- Đánh giá mô hình trên tập xác thực ---
        val_predictions = gbt_model.transform(val_df)
        evaluator_rmse = RegressionEvaluator(labelCol="sold", predictionCol="prediction", metricName="rmse")
        evaluator_r2 = RegressionEvaluator(labelCol="sold", predictionCol="prediction", metricName="r2")

        val_rmse = evaluator_rmse.evaluate(val_predictions)
        val_r2 = evaluator_r2.evaluate(val_predictions)

        logger.info(f"Model evaluation on validation set:")
        logger.info(f"Validation RMSE: {val_rmse}")
        logger.info(f"Validation R²: {val_r2}")

        # Hiển thị một vài dự đoán trên tập xác thực
        logger.info("Sample predictions on validation set (5 rows) with original features:")
        val_predictions.select(
            "sold",
            "prediction",
            "brand",
            "category",
            "color",
            "RAM",
            "ROM",
            "price",
            "price_old",
            "percent",
            "rating"
        ).show(5, truncate=False)

        # --- Đánh giá mô hình trên tập kiểm tra ---
        test_predictions = gbt_model.transform(test_df)
        test_rmse = evaluator_rmse.evaluate(test_predictions)
        test_r2 = evaluator_r2.evaluate(test_predictions)

        logger.info(f"Model evaluation on test set:")
        logger.info(f"Test RMSE: {test_rmse}")
        logger.info(f"Test R²: {test_r2}")

        # Hiển thị một vài dự đoán trên tập kiểm tra
        logger.info("Sample predictions on test set (5 rows) with original features:")
        test_predictions.select(
            "sold",
            "prediction",
            "brand",
            "category",
            "color",
            "RAM",
            "ROM",
            "price",
            "price_old",
            "percent",
            "rating"
        ).show(5, truncate=False)

        # --- Lưu mô hình ---
        logger.info(f"Saving model to {MODEL_PATH}")
        gbt_model.write().overwrite().save(MODEL_PATH)

    except Exception as e:
        logger.error(f"ERROR during model training: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Spark job finished.")

if __name__ == "__main__":
    main()