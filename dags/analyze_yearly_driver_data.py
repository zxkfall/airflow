from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import psycopg
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import datetime as dt
import time

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


# 步骤1：从 PostgreSQL 读取数据并进行数据清洗
def clean_data(**kwargs):
    # 1. 从 PostgreSQL 中读取数据
    start_time = time.time()
    log("Starting data extraction from PostgreSQL")
    conn = psycopg.connect(**conn_params)
    query = "SELECT date, serial_number, model, failure FROM smart_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()
    log_with_time("Data extraction completed", start_time)

    # 2. 将 Pandas DataFrame 转换为 PySpark DataFrame
    start_time = time.time()
    log("Converting data to PySpark DataFrame")
    spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    # 3. 数据清理和处理
    log("Cleaning data")
    df_spark = df_spark.select("date", "serial_number", "model", "failure").dropna()
    df_spark = df_spark.filter(F.col("failure").isin(0, 1))
    df_spark = df_spark.filter(F.col("date") >= "2023-01-01")
    df_spark = df_spark.dropDuplicates()

    total_records = df_spark.count()
    log_with_time(f"Total records after cleaning: {total_records}", start_time)

    # 4. 使用 psycopg 直接插入清洗后的数据到新的 PostgreSQL 表
    start_time = time.time()
    log("Saving cleaned data to PostgreSQL using psycopg")
    conn = psycopg.connect(**conn_params)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS yearly_cleaned_data (
                id SERIAL PRIMARY KEY,
                date DATE,
                serial_number VARCHAR(255),
                model VARCHAR(255),
                failure BIGINT
            )
        """)
        # 批量插入数据
        for row in df_spark.collect():
            cur.execute("""
                INSERT INTO yearly_cleaned_data (date, serial_number, model, failure)
                VALUES (%s, %s, %s, %s)
            """, (row.date, row.serial_number, row.model, row.failure))

    conn.commit()
    conn.close()
    log_with_time("Cleaned data saved to PostgreSQL", start_time)

    # 停止 Spark 会话
    start_time = time.time()
    log("Stopping Spark session")
    spark.stop()
    log_with_time("Spark session stopped", start_time)


# 步骤2：分析清洗后的数据
def analyze_cleaned_data(**kwargs):
    # 1. 从 PostgreSQL 中读取清洗后的数据
    start_time = time.time()
    log("Starting data extraction from yearly_cleaned_data table")
    conn = psycopg.connect(**conn_params)
    query = "SELECT date, serial_number, model, failure FROM yearly_cleaned_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()
    log_with_time("Data extraction from yearly_cleaned_data completed", start_time)

    # 2. 将 Pandas DataFrame 转换为 PySpark DataFrame
    start_time = time.time()
    log("Converting cleaned data to PySpark DataFrame")
    spark = SparkSession.builder.appName("Data Analysis").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    # 3. 品牌分类和按年份汇总
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

    # 4. 保存结果到 CSV
    start_time = time.time()
    log("Saving results to CSV")
    yearly_summary.write.csv("output/yearly_summary.csv", header=True)
    log_with_time("Saving to CSV completed", start_time)

    # 停止 Spark 会话
    start_time = time.time()
    log("Stopping Spark session")
    spark.stop()
    log_with_time("Spark session stopped", start_time)


# 定义 DAG
with DAG(
        dag_id='analyze_yearly_driver_data',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        tags=['analyse', 'postgresql', 'practice'],
) as dag:
    # 数据清洗的任务
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True
    )

    # 数据分析的任务
    analyze_data_task = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_cleaned_data,
        provide_context=True
    )

    # 设置任务依赖
    clean_data_task >> analyze_data_task
