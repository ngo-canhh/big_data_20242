# --- START OF FILE train_player_model_rf.py ---

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor # Import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lit
from pyspark.sql import DataFrame
import logging
import os

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
# Đường dẫn HDFS chứa dữ liệu cầu thủ thô (đã được lưu từ Kafka)
HDFS_RAW_DATA_PATH = os.getenv("HDFS_RAW_DATA_PATH", "hdfs://namenode-h2dn:9000/user/data/player_raw_parquet")
# Đường dẫn HDFS để lưu mô hình đã huấn luyện
# Đổi tên thư mục lưu mô hình để phân biệt với mô hình LR trước đó
MODEL_PATH = os.getenv("MODEL_PATH", "hdfs://namenode-h2dn:9000/user/models/player_market_value_prediction_model_rf")

# Định nghĩa cột mục tiêu (Label)
LABEL_COL = "market_value"

def main():
    logger.info(f"Starting Spark job to train {LABEL_COL} prediction model for player data using RandomForestRegressor")

    # Khởi tạo Spark Session
    logger.info("Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName(f"TrainPlayerModelRF_{LABEL_COL}") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") # Giảm bớt log từ Spark
    logger.info("SparkSession created.")

    try:
        # Đọc dữ liệu từ HDFS
        logger.info(f"Reading Parquet data from {HDFS_RAW_DATA_PATH}")
        # Spark sẽ tự động đọc các partition (year, month, day)
        df = spark.read.parquet(HDFS_RAW_DATA_PATH)

        # Hiển thị schema và một vài dòng dữ liệu
        logger.info("Schema of the DataFrame:")
        df.printSchema()
        logger.info("Sample raw data (5 rows):")
        df.show(5, truncate=False)

        # --- Tiền xử lý dữ liệu ---

        # Loại bỏ các dòng có giá trị null trong cột mục tiêu hoặc các cột đặc trưng quan trọng
        logger.info(f"Filtering out rows with null {LABEL_COL}...")
        df = df.filter(col(LABEL_COL).isNotNull())
        # Có thể thêm các bộ lọc khác nếu cần thiết, ví dụ:
        # df = df.filter(col("age").isNotNull() & col("appearances").isNotNull())
        initial_row_count = df.count()
        logger.info(f"Remaining rows after filtering for null {LABEL_COL}: {initial_row_count}")


        # 1. Xác định các cột đặc trưng
        # Cột số
        numeric_columns = [
            "age",
            "player_height",
            "contract_value_time", # Giả sử đây là giá trị số
            "appearances",
            "PPG",
            "goals",
            "assists",
            "own_goals",
            "substitutions_on",
            "substitutions_off",
            "yellow_cards",
            "second_yellow_cards",
            "red_cards",
            "penalty_goals",
            "minutes_per_goal", # Cẩn thận với cột này, có thể có giá trị Inf/NaN nếu goals=0
            "minutes_played"
            # Không sử dụng player_id vì nó chỉ là ID, không phải đặc trưng
            # Không sử dụng processing_timestamp, source_file vì là metadata
        ]

        # Cột phân loại
        categorical_columns = [
            "player_club",
            "position",
            "nationality",
            "player_agent",
            "strong_foot"
            # Không sử dụng name vì quá nhiều giá trị duy nhất
        ]

        # Lọc các dòng có null trong các cột số quan trọng có thể được sử dụng
        # Hoặc sử dụng Imputer nếu muốn điền giá trị thiếu (Xem gợi ý ở câu trả lời trước)
        # logger.info("Filtering out rows with nulls in key numeric features...")
        # df_filtered = df.na.drop(subset=numeric_columns)
        # rows_after_numeric_filter = df_filtered.count()
        # logger.info(f"Rows after filtering numeric nulls: {rows_after_numeric_filter}")
        # df = df_filtered # Sử dụng DataFrame đã lọc


        # 2. Chuyển đổi các cột phân loại thành dạng số (StringIndexer)
        indexers = [
            StringIndexer(inputCol=col, outputCol=f"{col}_indexed", handleInvalid="keep") # keep: giữ null và gán index cuối cùng
            for col in categorical_columns
        ]
        logger.info(f"Created {len(indexers)} StringIndexers for categorical columns.")


        # 3. Tạo vector đặc trưng từ các cột số và cột phân loại đã được mã hóa
        indexed_columns_output = [f"{col}_indexed" for col in categorical_columns]
        # Kết hợp cột số và cột phân loại đã được mã hóa
        all_feature_columns = numeric_columns + indexed_columns_output

        # VectorAssembler sẽ bỏ qua các dòng nếu bất kỳ cột đầu vào nào là null và handleInvalid="skip"
        # Hoặc giữ nguyên null và gán giá trị đặc biệt nếu handleInvalid="keep" (đối với sparse vectors)
        # Với dense vector, handleInvalid="skip" là phổ biến hơn. Cần đảm bảo dữ liệu không có null
        # trong các cột đầu vào của assembler hoặc sử dụng Imputer trước đó.
        assembler = VectorAssembler(
            inputCols=all_feature_columns,
            outputCol="raw_features",
            handleInvalid="skip" # skip: bỏ qua dòng nếu có bất kỳ giá trị null nào trong inputCols
        )
        logger.info(f"VectorAssembler created with {len(all_feature_columns)} input columns.")

        # 4. Chuẩn hóa các đặc trưng số (Scaler)
        # StandardScaler yêu cầu inputCol (raw_features) không chứa giá trị Inf hoặc NaN.
        # handleInvalid="skip" trong VectorAssembler giúp loại bỏ các dòng có nulls.
        # Cần kiểm tra và xử lý Inf/NaN trong các cột số gốc (đặc biệt là minutes_per_goal).
        # Lưu ý: Random Forest ít nhạy cảm với scaling hơn Linear Regression,
        # nhưng giữ lại scaler trong pipeline vẫn là một practice tốt.
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features", # Cột đặc trưng cuối cùng sau khi chuẩn hóa
            withStd=True,
            withMean=True
        )
        logger.info("StandardScaler created.")


        # 5. Pipeline tiền xử lý
        preprocessing_stages = indexers + [assembler, scaler]
        preprocessing_pipeline = Pipeline(stages=preprocessing_stages)
        logger.info(f"Preprocessing pipeline created with {len(preprocessing_stages)} stages.")

        # Áp dụng pipeline tiền xử lý
        # fit() cần được gọi trên toàn bộ DataFrame (df) trước khi chia tập huấn luyện/kiểm tra
        # để đảm bảo StringIndexer và StandardScaler học được tất cả các giá trị/thống kê.
        logger.info("Fitting preprocessing pipeline...")
        pipeline_model = preprocessing_pipeline.fit(df)
        logger.info("Applying preprocessing pipeline transform...")
        preprocessed_df = pipeline_model.transform(df)

        # Sau khi transform, cột "features" chứa vector đặc trưng đã sẵn sàng cho mô hình
        # Tuy nhiên, handleInvalid="skip" trong assembler có thể đã loại bỏ một số dòng.
        rows_after_preprocessing = preprocessed_df.count()
        logger.info(f"Rows remaining after preprocessing (potential skip by VectorAssembler): {rows_after_preprocessing}")
        if rows_after_preprocessing == 0:
             logger.critical("FATAL: No rows remaining after preprocessing. Check data quality and handleInvalid options.")
             spark.stop()
             exit(1)


        # --- Chia dữ liệu thành tập huấn luyện và tập kiểm tra ---
        # Chia ngẫu nhiên theo tỷ lệ
        train_df, test_df = preprocessed_df.randomSplit([0.8, 0.2], seed=42) # seed để kết quả chia nhất quán

        logger.info(f"Training data size: {train_df.count()}")
        logger.info(f"Testing data size: {test_df.count()}")

        # --- Huấn luyện mô hình ---
        # Sử dụng RandomForestRegressor
        prediction_col_name = f"{LABEL_COL}_prediction" # Tên cột dự đoán
        rf = RandomForestRegressor(
            featuresCol="features",      # Cột đặc trưng đã được tiền xử lý
            labelCol=LABEL_COL,          # Cột mục tiêu
            predictionCol=prediction_col_name, # Tên cột kết quả dự đoán
            numTrees=100,                # Hyperparameter: Số lượng cây trong rừng (có thể tinh chỉnh)
            maxDepth=10,                 # Hyperparameter: Độ sâu tối đa của mỗi cây (có thể tinh chỉnh)
            featureSubsetStrategy="auto",# Hyperparameter: Cách chọn đặc trưng cho mỗi cây
            seed=42                      # Seed để đảm bảo kết quả huấn luyện nhất quán
        )
        logger.info(f"Training Random Forest Regressor model for {LABEL_COL}...")
        # Fit mô hình trên tập huấn luyện
        rf_model = rf.fit(train_df)
        logger.info("Random Forest Regressor model training complete.")


        # --- Đánh giá mô hình trên tập kiểm tra ---
        logger.info(f"Evaluating model on test set...")
        # Áp dụng mô hình đã fit lên tập kiểm tra để có các dự đoán
        predictions = rf_model.transform(test_df)

        # Các chỉ số đánh giá cho hồi quy
        evaluator_rmse = RegressionEvaluator(labelCol=LABEL_COL, predictionCol=prediction_col_name, metricName="rmse")
        evaluator_mse = RegressionEvaluator(labelCol=LABEL_COL, predictionCol=prediction_col_name, metricName="mse")
        evaluator_r2 = RegressionEvaluator(labelCol=LABEL_COL, predictionCol=prediction_col_name, metricName="r2")

        rmse = evaluator_rmse.evaluate(predictions)
        mse = evaluator_mse.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)

        logger.info(f"Model evaluation metrics on test set for {LABEL_COL}:")
        logger.info(f"  RMSE (Root Mean Squared Error): {rmse}")
        logger.info(f"  MSE (Mean Squared Error): {mse}")
        logger.info(f"  R² (Coefficient of Determination): {r2}") # R2 càng gần 1 càng tốt

        # --- Hiển thị một vài dự đoán (Chỉ nhãn thực tế và dự đoán) ---
        logger.info(f"Sample predictions (10 rows) showing only {LABEL_COL} (Actual) and {prediction_col_name} (Predicted):")
        # Chỉ chọn cột nhãn thực tế và cột dự đoán
        predictions.select(LABEL_COL, prediction_col_name).show(10, truncate=False)


        # --- Lưu mô hình ---
        # Lưu pipeline model tiền xử lý và mô hình Random Forest đã huấn luyện
        # Việc lưu pipeline model giúp áp dụng cùng tiền xử lý cho dữ liệu mới khi dự đoán
        logger.info(f"Saving pipeline model and trained RF model to {MODEL_PATH}")

        # Lưu mô hình RandomForestRegressor đã fit
        rf_model_save_path = os.path.join(MODEL_PATH, "random_forest_model") # Sử dụng thư mục con "random_forest_model"
        logger.info(f"Saving RF model to {rf_model_save_path}")
        rf_model.write().overwrite().save(rf_model_save_path)
        logger.info(f"RF model saved successfully.")

        # Lưu pipeline model tiền xử lý (để sử dụng khi dự đoán)
        pipeline_model_save_path = os.path.join(MODEL_PATH, "preprocessing_pipeline_model")
        logger.info(f"Saving preprocessing pipeline model to {pipeline_model_save_path}")
        pipeline_model.write().overwrite().save(pipeline_model_save_path)
        logger.info(f"Preprocessing pipeline model saved successfully.")

        logger.info(f"Model training and saving finished. Models saved to {MODEL_PATH}")

    except Exception as e:
        logger.critical(f"CRITICAL ERROR during model training: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("Spark job finished.")

if __name__ == "__main__":
    main()
# --- END OF FILE train_player_model_rf.py ---