from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("FinalProject_DriverSafety_BigData") \
    .config("spark.sql.parquet.compression.codec", "uncompressed") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("="*65)
print("1. ĐỌC DỮ LIỆU TRỰC TIẾP TỪ HDFS & KIỂM TRA SCHEMA")
print("="*65)
raw_df = spark.read.option("header", True).option("inferSchema", True).csv("hdfs:///raw/Dataset.csv")
total_raw = raw_df.count()
print(f"-> Đường dẫn nguồn: hdfs:///raw/Dataset.csv")
print(f"-> Tổng số bản ghi thô ban đầu: {total_raw}")
raw_df.printSchema()

print("\n" + "="*65)
print("2. LÀM SẠCH VÀ BIẾN ĐỔI DỮ LIỆU (DATA CLEANING & TRANSFORMATION)")
print("="*65)
# Lọc bỏ giá trị Null, tốc độ âm và góc nghiêng bất thường (>= 90)
clean_df = raw_df.dropna(subset=["eye_aspect_ratio", "mouth_opening_ratio", "head_pitch"]) \
                 .filter((F.col("vehicle_speed") > 0) & (F.col("head_pitch") < 90)) \
                 .withColumn("risk_level", 
                             F.when((F.col("is_drowsy") == 1) & (F.col("vehicle_speed") > 80), "CRITICAL")
                              .when(F.col("is_drowsy") == 1, "WARNING")
                              .otherwise("NORMAL"))

total_clean = clean_df.count()
print(f"-> Số bản ghi hợp lệ sau làm sạch: {total_clean} (Đã loại bỏ {total_raw - total_clean} bản ghi lỗi/nhiễu)")

# Lưu dữ liệu sạch vào thư mục processed
clean_df.write.mode("overwrite").option("header", True).csv("hdfs:///processed/clean_driver_data")
print("-> Đã lưu dữ liệu sạch vào: hdfs:///processed/clean_driver_data")

print("\n" + "="*65)
print("3. PHÂN TÍCH DỮ LIỆU BẰNG SPARK SQL (DATA ANALYTICS & INSIGHTS)")
print("="*65)
clean_df.createOrReplaceTempView("driver_telemetry")

sql_query = """
    SELECT 
        driver_id,
        COUNT(*) AS total_events,
        SUM(is_drowsy) AS drowsy_count,
        ROUND(SUM(is_drowsy) * 100.0 / COUNT(*), 2) AS drowsy_rate_pct,
        ROUND(AVG(vehicle_speed), 1) AS avg_speed_kmh,
        ROUND(AVG(eye_aspect_ratio), 3) AS avg_ear
    FROM driver_telemetry
    GROUP BY driver_id
    ORDER BY drowsy_rate_pct DESC
"""
analytics_df = spark.sql(sql_query)
print("-> Bảng thống kê tỷ lệ buồn ngủ và tốc độ trung bình theo từng tài xế:")
analytics_df.show(10, truncate=False)

# Lưu bảng thống kê ra HDFS để làm đầu vào cho Dashboard
analytics_df.write.mode("overwrite").option("header", True).csv("hdfs:///output/driver_analytics_summary")

print("\n" + "="*65)
print("4. HUẤN LUYỆN MÔ HÌNH MACHINE LEARNING & ĐÁNH GIÁ METRICS")
print("="*65)
feature_cols = ["vehicle_speed", "eye_aspect_ratio", "mouth_opening_ratio", "head_pitch"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
ml_df = assembler.transform(clean_df).select("driver_id", "timestamp", "vehicle_speed", "risk_level", "features", F.col("is_drowsy").alias("label"))

train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)
print(f"-> Chia tập dữ liệu: Train = {train_df.count()} dòng | Test = {test_df.count()} dòng")

dt = DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=5)
model = dt.fit(train_df)
predictions = model.transform(test_df)

acc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
f1_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
prec_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
rec_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")

print(f"-> Accuracy  : {acc_eval.evaluate(predictions)*100:.2f}%")
print(f"-> F1-Score  : {f1_eval.evaluate(predictions)*100:.2f}%")
print(f"-> Precision : {prec_eval.evaluate(predictions)*100:.2f}%")
print(f"-> Recall    : {rec_eval.evaluate(predictions)*100:.2f}%")

print("\n-> Mẫu kết quả dự đoán trên tập Test:")
predictions.select("timestamp", "driver_id", "vehicle_speed", "risk_level", "label", "prediction").show(8, truncate=False)

# Lưu kết quả dự đoán xuống HDFS
predictions.select("timestamp", "driver_id", "vehicle_speed", "risk_level", "label", "prediction") \
    .write.mode("overwrite").option("header", True).csv("hdfs:///output/driver_ml_predictions")

print("-> Đã xuất kết quả dự đoán ML ra HDFS: hdfs:///output/driver_ml_predictions")
spark.stop()