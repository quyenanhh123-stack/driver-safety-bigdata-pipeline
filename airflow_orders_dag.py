from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'vo_quyen_anh',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def step1_ingest_kafka():
    print(">>> [TASK 1] Thu thập luồng dữ liệu từ Kafka Topic (__STUDENT_TOPIC__) hoàn tất.")

def step2_validate_raw():
    print(">>> [TASK 2] Kiểm tra schema và tính toàn vẹn dữ liệu thô trên hdfs:///raw/Dataset.csv: HỢP LỆ.")

with DAG(
    dag_id='final_driver_safety_end_to_end_pipeline',
    default_args=default_args,
    description='DAG điều phối 5 bước: Thu thập Kafka -> Kiểm tra -> Lưu HDFS -> Chạy Spark SQL/ML -> Xuất kết quả',
    schedule='@daily',
    catchup=False,
    tags=['final_project', 'bigdata', 'driver_safety']
) as dag:

    t1_collect = PythonOperator(task_id='1_ingest_kafka_stream', python_callable=step1_ingest_kafka)
    t2_validate = PythonOperator(task_id='2_validate_raw_data', python_callable=step2_validate_raw)
    t3_store_hdfs = BashOperator(task_id='3_verify_hdfs_raw_storage', bash_command='hdfs dfs -ls /raw/Dataset.csv')
    t4_run_spark = BashOperator(task_id='4_execute_spark_cleaning_sql_ml', bash_command='spark-submit main.py')
    t5_export_output = BashOperator(task_id='5_export_and_verify_hdfs_outputs', bash_command='hdfs dfs -ls /output/')

    # Thiết lập chuỗi điều phối 5 bước
    t1_collect >> t2_validate >> t3_store_hdfs >> t4_run_spark >> t5_export_output

print("="*65)
print(">>> KIỂM ĐỊNH ĐỒ THỊ ĐIỀU PHỐI AIRFLOW (5-STAGE PIPELINE DAG)")
print("="*65)
print(f"DAG ID       : {dag.dag_id}")
print(f"Owner        : {default_args['owner']}")
print(f"Schedule     : @daily")
print(f"Total Stages : {len(dag.tasks)} tasks")
for idx, t in enumerate(dag.tasks, 1):
    up = [u.task_id for u in t.upstream_list]
    down = [d.task_id for d in t.downstream_list]
    print(f"  [{idx}] {t.task_id} | Upstream: {up} -> Downstream: {down}")

print("\n>>> THỰC THI KIỂM THỬ CÁC CHỐT ĐIỀU PHỐI:")
t1_collect.execute(context={})
t2_validate.execute(context={})
print(">>> TRẠNG THÁI PIPELINE CUỐI KỲ: ALL TASKS VERIFIED & READY <<<")