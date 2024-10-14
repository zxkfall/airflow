from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import glob
import csv
import time
import psycopg
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import datetime as dt

# PostgreSQL 连接设置
conn_params = {
    "dbname": "mydatabase",
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost",
    "port": "5432"
}

# 默认 DAG 参数
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=2)
}

# 日志功能
def log(message):
    """打印带有时间戳的日志信息"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def log_with_time(message, start_time):
    """打印日志信息并计算耗时"""
    elapsed_time = time.time() - start_time
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message} - Elapsed time: {elapsed_time:.2f} seconds")


# 步骤1：上传 CSV 文件到 PostgreSQL
def process_csv_file(file_path, **kwargs):
    conn = psycopg.connect(**conn_params)
    with conn.cursor() as cur:
        file_name = os.path.basename(file_path)
        date_str = file_name.split(".")[0]
        file_date = datetime.strptime(date_str, '%Y-%m-%d').date()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS smart_data (
                id SERIAL PRIMARY KEY,
                date DATE,
                serial_number VARCHAR(255),
                model VARCHAR(255),
                capacity_bytes BIGINT,
                failure BIGINT
            )
        """)

        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
                row = [None if v == '' else v for v in row]
                cur.execute("""
                    INSERT INTO smart_data (date, serial_number, model, capacity_bytes, failure)
                    VALUES (%s, %s, %s, %s, %s)
                """, (file_date, row[1], row[2], int(row[3]) if row[3] else None, int(row[4]) if row[4] else None))

        conn.commit()
    conn.close()

# 步骤2：从 PostgreSQL 中读取数据并进行分析
def analyze_data_from_postgres(**kwargs):
    # 1. 读取数据
    start_time = time.time()
    log("Starting data extraction from PostgreSQL")
    conn = psycopg.connect(**conn_params)
    query = "SELECT date, serial_number, model, failure FROM smart_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()
    log_with_time("Data extraction completed", start_time)

    # 2. 数据转换为 PySpark DataFrame
    start_time = time.time()
    log("Converting data to PySpark DataFrame")
    spark = SparkSession.builder.appName("Yearly Drive Summary").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    # 3. 数据清理和处理
    log("Cleaning data")
    df_spark = df_spark.select("date", "serial_number", "model", "failure").dropna()
    df_spark = df_spark.filter(F.col("failure").isin(0, 1))
    df_spark = df_spark.filter(F.col("date") >= "2023-01-01")
    df_spark = df_spark.dropDuplicates()

    total_records = df_spark.count()
    log_with_time(f"Total records after cleaning: {total_records}", start_time)

    # 4. 品牌分类和按年份汇总
    start_time = time.time()
    log("Classifying drive brands and aggregating yearly data")
    df_spark = df_spark.withColumn("year", F.year(df_spark.date))
    df_spark = df_spark.withColumn("brand",
                                   F.when(df_spark.model.startswith("CT"), "Crucial")
                                    .when(df_spark.model.startswith("DELLBOSS"), "Dell BOSS")
                                    .when(df_spark.model.startswith("HGST"), "HGST")
                                    .when(df_spark.model.startswith("Seagate"), "Seagate")
                                    .when(df_spark.model.startswith("ST"), "Seagate")
                                    .when(df_spark.model.startswith("TOSHIBA"), "Toshiba")
                                    .when(df_spark.model.startswith("WDC"), "Western Digital")
                                    .otherwise("Others"))

    yearly_summary = df_spark.groupBy("year", "brand").agg(
        F.sum(F.col("failure")).alias("drive_failures"),
        F.collect_set("model").alias("models")
    )
    yearly_summary = yearly_summary.withColumn("models", F.concat_ws(", ", "models"))
    log_with_time("Aggregation completed", start_time)

    # 5. 保存结果到 CSV
    start_time = time.time()
    log("Saving results to CSV")
    yearly_summary.write.csv("output/yearly_summary.csv", header=True)
    log_with_time("Saving to CSV completed", start_time)

    # 停止 Spark 会话
    start_time = time.time()
    log("Stopping Spark session")
    spark.stop()
    log_with_time("Spark session stopped", start_time)


# 搜索 CSV 文件
def find_csv_files(**kwargs):
    return glob.glob('mydata/**/*.csv', recursive=True)


# 定义 DAG
with DAG(
        dag_id='upload_and_analyze_yearly_csv_to_postgres',
        default_args=default_args,
        catchup=False,
) as dag:

    # 搜索 CSV 文件任务
    search_csv_task = PythonOperator(
        task_id='search_csv_files',
        python_callable=find_csv_files,
        provide_context=True
    )

    # 动态生成上传 CSV 数据的任务
    def create_csv_import_tasks(file_path):
        return PythonOperator(
            task_id=f'process_{os.path.basename(file_path)}',
            python_callable=process_csv_file,
            op_kwargs={'file_path': file_path},
            provide_context=True
        )

    csv_files = find_csv_files()
    process_csv_tasks = [create_csv_import_tasks(file) for file in csv_files]

    # 分析数据的任务
    analyze_data_task = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_data_from_postgres,
        provide_context=True
    )

    # 设置任务依赖
    search_csv_task >> process_csv_tasks >> analyze_data_task
