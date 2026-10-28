# Driver Safety & Drowsiness Monitoring - Big Data Pipeline

**Tác giả:** Võ Quyền Anh  
**Học phần:** Kiến trúc và Công nghệ Big Data  
**Môi trường:** Hadoop HDFS, Apache Spark 3.5.1 (Spark SQL & MLlib), Apache Kafka 2.2.3, Apache Airflow 3.3.2.

## 1. Tổng quan kiến trúc Pipeline
1. **Data Generation & Ingestion:** Sinh 50,000 bản ghi cảm biến (`Dataset.csv` ~ 2.45 MB), truyền qua Kafka Topic `__STUDENT_TOPIC__` và lưu trữ vào `hdfs:///raw/Dataset.csv`.
2. **Data Cleaning (PySpark):** Lọc bỏ các bản ghi khuyết thiếu (Null) và nhiễu vật lý, lưu bản sạch vào `hdfs:///processed/clean_driver_data`.
3. **Data Analytics (Spark SQL):** Thống kê tỷ lệ buồn ngủ và vận tốc trung bình theo tài xế, xuất ra `hdfs:///output/driver_analytics_summary`.
4. **Machine Learning (Spark MLlib):** Huấn luyện mô hình `DecisionTreeClassifier` (80% Train / 20% Test), xuất dự đoán ra `hdfs:///output/driver_ml_predictions`.
5. **Orchestration (Airflow 3):** Điều phối tự động chuỗi 5 bước qua DAG `final_driver_safety_end_to_end_pipeline`.
6. **Dashboard:** Trực quan hóa kết quả bằng giao diện tương tác `dashboard.html`.

## 2. Hướng dẫn cài đặt và chạy (Reproducibility Guide)
```bash
# Bước 1: Tạo tập dữ liệu 50,000 bản ghi (hoặc chạy script tạo CSV)
python generate_final_dataset.py

# Bước 2: Upload Dataset.csv lên HDFS (/raw/Dataset.csv) trên BigData Lab

# Bước 3: Thực thi luồng truyền nhận Kafka
spark-submit samples/kafka_producer.py

# Bước 4: Thực thi làm sạch, Spark SQL và Machine Learning
spark-submit main.py

# Bước 5: Thực thi điều phối DAG Airflow
spark-submit samples/airflow_orders_dag.py

# Bước 6: Khởi chạy giao diện Dashboard
python run_dashboard.py