from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.ml.linalg import Vectors  # Chỉ import cần thiết
from pyspark.sql.types import DoubleType
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("TrainGoldPriceANN_SeparateModels") \
        .master("spark://spark-master-h2dn:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-h2dn:9000") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    try:
        # Đọc dữ liệu từ HDFS
        logger.info("Loading data from HDFS...")
        data_df = spark.read.option("mergeSchema", "true").parquet("hdfs://namenode-h2dn:9000/user/data/gold_raw_parquet")
        logger.info("Data loaded successfully. Schema:")
        data_df.printSchema()
        
        data_df.show(5)
        
        # Chuẩn bị dữ liệu
        logger.info("Preprocessing data...")
        
        # Chuyển đổi price_date từ string sang date và trích xuất các đặc trưng
        data_with_features = data_df \
            .withColumn("price_date", to_date(col("price_date"), "yyyy-MM-dd")) \
            .withColumn("year", year("price_date").cast(DoubleType())) \
            .withColumn("month", month("price_date").cast(DoubleType())) \
            .withColumn("day", dayofmonth("price_date").cast(DoubleType())) \
            .withColumn("buy_price", col("buy_price").cast(DoubleType())) \
            .withColumn("sell_price", col("sell_price").cast(DoubleType()))
            
        # Mã hóa gold_type bằng StringIndexer
        indexer = StringIndexer(inputCol="gold_type", outputCol="gold_type_indexed")
        
        # Áp dụng OneHotEncoder cho gold_type_indexed
        encoder = OneHotEncoder(inputCol="gold_type_indexed", outputCol="gold_type_encoded")
        
        # Loại bỏ các hàng có giá trị null
        data_with_features = data_with_features.na.drop()
        
        # Tạo pipeline cho việc tiền xử lý dữ liệu
        preprocessing_pipeline = Pipeline(stages=[indexer, encoder])
        preprocessing_model = preprocessing_pipeline.fit(data_with_features)
        processed_data = preprocessing_model.transform(data_with_features)
        
        # Tạo vector đặc trưng đầu vào (input features)
        feature_assembler = VectorAssembler(
            inputCols=["year", "month", "day", "gold_type_encoded"],
            outputCol="features"
        )
        
        # Áp dụng feature_assembler
        data_with_features_only = feature_assembler.transform(processed_data)
        
        # Kiểm tra schema
        logger.info("Checking features schema...")
        data_with_features_only.printSchema()
        
        # Chia dữ liệu thành tập huấn luyện và tập kiểm tra
        train_data, test_data = data_with_features_only.randomSplit([0.8, 0.2], seed=42)
        logger.info(f"Training data count: {train_data.count()}, Test data count: {test_data.count()}")
        
        # Lấy số features từ vector đặc trưng
        sample_features = train_data.select("features").limit(1)
        input_size = len(sample_features.collect()[0][0].toArray()) if sample_features.collect() else 0
        if input_size == 0:
            raise ValueError("No features found in sample data")
        
        # Định nghĩa kiến trúc mạng neural với 1 đầu ra (cho buy_price)
        layers_buy = [input_size, 16, 12, 1]
        ann_buy = MultilayerPerceptronClassifier(
            featuresCol="features",
            labelCol="buy_price",
            layers=layers_buy,
            maxIter=150,
            blockSize=128,
            solver="l-bfgs",
            seed=42
        )
        
        # Định nghĩa kiến trúc mạng neural với 1 đầu ra (cho sell_price)
        layers_sell = [input_size, 16, 12, 1]
        ann_sell = MultilayerPerceptronClassifier(
            featuresCol="features",
            labelCol="sell_price",
            layers=layers_sell,
            maxIter=150,
            blockSize=128,
            solver="l-bfgs",
            seed=42
        )
        
        # Huấn luyện mô hình cho buy_price
        logger.info("Training ANN model for buy_price...")
        ann_buy_model = ann_buy.fit(train_data)
        
        # Huấn luyện mô hình cho sell_price
        logger.info("Training ANN model for sell_price...")
        ann_sell_model = ann_sell.fit(train_data)
        
        # Dự đoán
        buy_predictions = ann_buy_model.transform(test_data).withColumnRenamed("prediction", "predicted_buy_price")
        predictions = ann_sell_model.transform(buy_predictions).withColumnRenamed("prediction", "predicted_sell_price")
        
        # Đánh giá RMSE và R² cho buy_price
        buy_evaluator = RegressionEvaluator(labelCol="buy_price", predictionCol="predicted_buy_price", metricName="rmse")
        buy_rmse = buy_evaluator.evaluate(predictions)
        buy_r2 = buy_evaluator.setMetricName("r2").evaluate(predictions)
        logger.info(f"Buy price prediction - RMSE: {buy_rmse}, R²: {buy_r2}")

        # Đánh giá RMSE và R² cho sell_price
        sell_evaluator = RegressionEvaluator(labelCol="sell_price", predictionCol="predicted_sell_price", metricName="rmse")
        sell_rmse = sell_evaluator.evaluate(predictions)
        sell_r2 = sell_evaluator.setMetricName("r2").evaluate(predictions)
        logger.info(f"Sell price prediction - RMSE: {sell_rmse}, R²: {sell_r2}")

        # Hiển thị một số dự đoán
        logger.info("Sample predictions:")
        sample_predictions = predictions.select(
            "price_date", "gold_type", "buy_price", "sell_price", "predicted_buy_price", "predicted_sell_price"
        ).limit(10)
        sample_predictions.show()

        # Lưu các mô hình vào HDFS
        logger.info("Saving models to HDFS...")
        preprocessing_model_path = "hdfs://namenode-h2dn:9000/user/models/gold_price_preprocessing_model"
        ann_buy_model_path = "hdfs://namenode-h2dn:9000/user/models/gold_price_ann_buy_model"
        ann_sell_model_path = "hdfs://namenode-h2dn:9000/user/models/gold_price_ann_sell_model"

        preprocessing_model.save(preprocessing_model_path)
        ann_buy_model.save(ann_buy_model_path)
        ann_sell_model.save(ann_sell_model_path)

        logger.info(f"Models saved successfully to HDFS.")

    except Exception as e:
        logger.error(f"Error in training process: {str(e)}", exc_info=True)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")

if __name__ == "__main__":
    main()