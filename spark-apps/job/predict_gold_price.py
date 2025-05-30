from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel

def main():
    # 1) Khởi SparkSession
    spark = SparkSession.builder \
        .appName("UseGoldModel") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 3) Load model (sử dụng đường dẫn trong container)
    model_path = "/opt/exported_model/model_export"

    # 3) Load model
    model = LinearRegressionModel.load(model_path)

    # 4) Tạo hoặc load một DataFrame để dự đoán
    from pyspark.ml.linalg import Vectors
    df = spark.createDataFrame([
        (Vectors.dense([90000.0, 2025.0, 5.0, 8.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0]), )
    ], ["features"])

    # 5) Dự đoán
    preds = model.transform(df)
    preds.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()