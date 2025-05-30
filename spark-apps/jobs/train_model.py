# from pyspark.sql import SparkSession
# from pyspark.ml.feature import VectorAssembler, StandardScaler
# from pyspark.ml.regression import RandomForestRegressor
# from pyspark.ml.evaluation import RegressionEvaluator
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# from pyspark.sql.functions import col, log1p
# import logging

# # --- Cấu hình Logging ---
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # --- Cấu hình ---
# HDFS_DATA_PATH = "hdfs://namenode-h2dn:9000/user/data/football_players_raw_parquet"
# MODEL_PATH = "hdfs://namenode-h2dn:9000/user/spark/models/football_market_value_model"

# def ensure_hdfs_directories(spark, paths):
#     """Kiểm tra và tạo thư mục HDFS nếu chưa tồn tại."""
#     try:
#         conf = spark._jsc.hadoopConfiguration()
#         for path_str in paths:
#             uri = spark._jvm.java.net.URI(path_str)
#             fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
#             hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
#             if not fs.exists(hdfs_path):
#                 fs.mkdirs(hdfs_path)
#                 logger.info(f"Created HDFS directory: {path_str}")
#             else:
#                 logger.info(f"HDFS directory already exists: {path_str}")
#     except Exception as e:
#         logger.error(f"ERROR ensuring HDFS directories: {e}")
#         raise

# def main():
#     logger.info("Starting Spark ML job: Training RandomForest model for Football Players with Preprocessing and Tuning")

#     # Khởi tạo SparkSession
#     spark = SparkSession \
#         .builder \
#         .appName("FootballPlayerMarketValuePrediction") \
#         .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
#         .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
#         .getOrCreate()

#     spark.sparkContext.setLogLevel("INFO")
#     logger.info("SparkSession created.")

#     try:
#         # Tạo thư mục HDFS cho model
#         ensure_hdfs_directories(spark, [MODEL_PATH])

#         # Đọc dữ liệu từ HDFS
#         logger.info(f"Reading data from HDFS path: {HDFS_DATA_PATH}")
#         df = spark.read.parquet(HDFS_DATA_PATH)
#         logger.info(f"Loaded {df.count()} records from HDFS.")

#         # Chọn các cột đặc trưng và nhãn
#         feature_columns = [
#             "age", "appearances", "PPG", "goals", "assists", "own_goals",
#             "substitutions_on", "substitutions_off", "yellow_cards",
#             "second_yellow_cards", "red_cards", "penalty_goals",
#             "minutes_per_goal", "minutes_played", "player_height"
#         ]
#         label_column = "market_value"

        

#         # Xử lý outlier bằng biến đổi log cho các cột có giá trị lớn
#         log_transform_columns = ["appearances", "goals", "assists", "minutes_played", "minutes_per_goal"]
#         for col_name in log_transform_columns:
#             df = df.withColumn(f"{col_name}_log", log1p(col(col_name)))
#         feature_columns = [f"{col_name}_log" if col_name in log_transform_columns else col_name 
#                            for col_name in feature_columns]
#         logger.info("Applied log transformation to handle outliers.")

#         # Kiểm tra dữ liệu null
#         for col_name in feature_columns + [label_column]:
#             null_count = df.filter(col(col_name).isNull()).count()
#             if null_count > 0:
#                 logger.warning(f"Column {col_name} has {null_count} null values.")
#                 df = df.filter(col(col_name).isNotNull())

#         # Lọc dữ liệu (loại bỏ thủ môn và strong_foot không hợp lệ)
#         df = df.filter((col("position") != "Goalkeeper") & 
#                        (col("strong_foot").isin("left", "right", "both")))
#         logger.info(f"After filtering, {df.count()} records remain.")

#         # Chuyển đổi dữ liệu thành vector đặc trưng
#         assembler = VectorAssembler(
#             inputCols=feature_columns,
#             outputCol="features",
#             handleInvalid="skip"
#         )
#         feature_df = assembler.transform(df)
#         logger.info("Features assembled.")

#         # Chuẩn hóa dữ liệu với StandardScaler
#         # Mặc dù RF không yêu cầu chuẩn hóa, nhưng làm vậy để đồng bộ với pipeline trước
#         scaler = StandardScaler(
#             inputCol="features",
#             outputCol="scaled_features",
#             withStd=True,
#             withMean=True
#         )
#         scaler_model = scaler.fit(feature_df)
#         scaled_df = scaler_model.transform(feature_df)
#         logger.info("Features standardized using StandardScaler.")

#         # Lưu scaler_model để dùng trong dự đoán
#         scaler_model.write().overwrite().save("hdfs://namenode-h2dn:9000/user/spark/models/scaler_model")
#         logger.info("Scaler model saved to HDFS.")

#         # Chọn cột scaled_features và label
#         final_df = scaled_df.select("scaled_features", col(label_column).alias("label"))
#         logger.info("Final DataFrame prepared for training.")

#         # Chia dữ liệu thành tập huấn luyện và kiểm tra
#         train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)
#         logger.info(f"Training set: {train_df.count()} records, Test set: {test_df.count()} records.")

#         # Khởi tạo mô hình RandomForestRegressor
#         rf = RandomForestRegressor(featuresCol="scaled_features",
#                                     labelCol="label",
#                                     numTrees=70,
#                                     maxDepth=20,
#                                     maxBins=32,
#                                     seed=42)
#         logger.info("RandomForestRegressor model initialized.")

#         # Xây dựng lưới tham số để điều chỉnh
#         # paramGrid = ParamGridBuilder() \
#         #     .addGrid(rf.numTrees, [70]) \
#         #     .addGrid(rf.maxDepth, [20]) \
#         #     .addGrid(rf.maxBins, [32]) \
#         #     .build()
#         # logger.info(f"Parameter grid created with {len(paramGrid)} combinations.")

#         # Khởi tạo CrossValidator
#         evaluator = RegressionEvaluator(
#             labelCol="label",
#             predictionCol="prediction",
#             metricName="rmse"
#         )
#         # crossval = CrossValidator(
#         #     estimator=rf,
#         #     estimatorParamMaps=paramGrid,
#         #     evaluator=evaluator,
#         #     numFolds=3,
#         #     seed=42
#         # )
#         logger.info("CrossValidator initialized with 3 folds.")

#         # Huấn luyện mô hình với CrossValidator
#         logger.info("Starting model training with cross-validation...")
#         cv_model = crossval.fit(train_df)
#         logger.info("Model training with cross-validation completed.")

#         # Lấy mô hình tốt nhất
#         best_model = cv_model.bestModel
#         logger.info("Best model selected.")

#         # In các tham số tốt nhất
#         logger.info(f"Best numTrees: {best_model._java_obj.getNumTrees()}")
#         logger.info(f"Best maxDepth: {best_model._java_obj.getMaxDepth()}")
#         logger.info(f"Best maxBins: {best_model._java_obj.getMaxBins()}")

#         # Dự đoán trên tập kiểm tra với mô hình tốt nhất
#         predictions = best_model.transform(test_df)
#         logger.info("Predictions generated on test set.")

#         # Đánh giá mô hình tốt nhất
#         rmse = evaluator.evaluate(predictions)
#         logger.info(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

#         # Lưu mô hình tốt nhất vào HDFS
#         logger.info(f"Saving best model to {MODEL_PATH}")
#         best_model.write().overwrite().save(MODEL_PATH)
#         logger.info("Best model saved successfully.")

#         # Hiển thị một số dự đoán mẫu
#         predictions.select("prediction", "label").show(5)

#         # In các kết quả cross-validation (RMSE trung bình cho mỗi tổ hợp)
#         avg_metrics = cv_model.avgMetrics
#         for i, params in enumerate(paramGrid):
#             logger.info(f"Params: {params}, Avg RMSE: {avg_metrics[i]}")

#     except Exception as e:
#         logger.error(f"ERROR during Spark ML job: {e}", exc_info=True)
#     finally:
#         logger.info("Stopping SparkSession...")
#         spark.stop()
#         logger.info("Spark ML job finished.")

# if __name__ == "__main__":
#     main()



from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, log1p
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
HDFS_DATA_PATH = "hdfs://namenode-h2dn:9000/user/data/football_players_raw_parquet"
MODEL_PATH = "hdfs://namenode-h2dn:9000/user/spark/models/football_market_value_model"

def ensure_hdfs_directories(spark, paths):
    """Kiểm tra và tạo thư mục HDFS nếu chưa tồn tại."""
    try:
        conf = spark._jsc.hadoopConfiguration()
        for path_str in paths:
            uri = spark._jvm.java.net.URI(path_str)
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
            hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
            if not fs.exists(hdfs_path):
                fs.mkdirs(hdfs_path)
                logger.info(f"Created HDFS directory: {path_str}")
            else:
                logger.info(f"HDFS directory already exists: {path_str}")
    except Exception as e:
        logger.error(f"ERROR ensuring HDFS directories: {e}")
        raise

def main():
    logger.info("Starting Spark ML job: Training RandomForest model with Best Parameters and Increased numTrees")

    # Khởi tạo SparkSession
    spark = SparkSession \
        .builder \
        .appName("FootballPlayerMarketValuePrediction") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    logger.info("SparkSession created.")

    try:
        # Tạo thư mục HDFS cho model
        ensure_hdfs_directories(spark, [MODEL_PATH])

        # Đọc dữ liệu từ HDFS
        logger.info(f"Reading data from HDFS path: {HDFS_DATA_PATH}")
        df = spark.read.parquet(HDFS_DATA_PATH)
        logger.info(f"Loaded {df.count()} records from HDFS.")

        # Chọn các cột đặc trưng và nhãn
        feature_columns = [
            "age", "appearances", "PPG", "goals", "assists", "own_goals",
            "substitutions_on", "substitutions_off", "yellow_cards",
            "second_yellow_cards", "red_cards", "penalty_goals",
            "minutes_per_goal", "minutes_played", "player_height"
        ]
        label_column = "market_value"

        

        # Xử lý outlier bằng biến đổi log cho các cột có giá trị lớn
        log_transform_columns = ["appearances", "goals", "assists", "minutes_played", "minutes_per_goal"]
        for col_name in log_transform_columns:
            df = df.withColumn(f"{col_name}_log", log1p(col(col_name)))
        feature_columns = [f"{col_name}_log" if col_name in log_transform_columns else col_name 
                           for col_name in feature_columns]
        logger.info("Applied log transformation to handle outliers.")

        # Kiểm tra dữ liệu null
        for col_name in feature_columns + [label_column]:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                logger.warning(f"Column {col_name} has {null_count} null values.")
                df = df.filter(col(col_name).isNotNull())

        # Lọc dữ liệu (loại bỏ thủ môn và strong_foot không hợp lệ)
        df = df.filter((col("position") != "Goalkeeper") & 
                       (col("strong_foot").isin("left", "right", "both")))
        logger.info(f"After filtering, {df.count()} records remain.")

        # Chuyển đổi dữ liệu thành vector đặc trưng
        assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="features",
            handleInvalid="skip"
        )
        feature_df = assembler.transform(df)
        logger.info("Features assembled.")

        # Chuẩn hóa dữ liệu với StandardScaler
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        scaler_model = scaler.fit(feature_df)
        scaled_df = scaler_model.transform(feature_df)
        logger.info("Features standardized using StandardScaler.")

        # Lưu scaler_model để dùng trong dự đoán
        scaler_model.write().overwrite().save("hdfs://namenode-h2dn:9000/user/spark/models/scaler_model")
        logger.info("Scaler model saved to HDFS.")

        # Chọn cột scaled_features và label
        final_df = scaled_df.select("scaled_features", col(label_column).alias("label"))
        logger.info("Final DataFrame prepared for training.")

        # Chia dữ liệu thành tập huấn luyện và kiểm tra
        train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)
        logger.info(f"Training set: {train_df.count()} records, Test set: {test_df.count()} records.")

        # Khởi tạo mô hình RandomForestRegressor với tham số tốt nhất
        rf = RandomForestRegressor(
            featuresCol="scaled_features",
            labelCol="label",
            numTrees=100,  # Tăng từ 50 lên 100 để cải thiện hiệu suất
            maxDepth=20,   # Từ tổ hợp tốt nhất
            maxBins=32     # Từ tổ hợp tốt nhất
        )
        logger.info("RandomForestRegressor model initialized with numTrees=100, maxDepth=10, maxBins=32.")

        # Huấn luyện mô hình
        logger.info("Starting model training...")
        model = rf.fit(train_df)
        logger.info("Model training completed.")

        # Dự đoán trên tập kiểm tra
        predictions = model.transform(test_df)
        logger.info("Predictions generated on test set.")

        # Đánh giá mô hình
        evaluator = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        logger.info(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

        # Lưu mô hình vào HDFS
        logger.info(f"Saving model to {MODEL_PATH}")
        model.write().overwrite().save(MODEL_PATH)
        logger.info("Model saved successfully.")

        # Hiển thị một số dự đoán mẫu
        predictions.select("prediction", "label").show(5)

    except Exception as e:
        logger.error(f"ERROR during Spark ML job: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Spark ML job finished.")

if __name__ == "__main__":
    main()