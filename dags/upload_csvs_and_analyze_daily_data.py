from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import datetime as dt
import os
import csv
import psycopg
import glob
import pendulum
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

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
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=2),
    'execution_timeout': dt.timedelta(minutes=1),  # 任务超时设置
}


# 步骤1：上传 CSV 文件数据到 PostgreSQL
def process_csv_file(file_path, **kwargs):
    conn = psycopg.connect(**conn_params)
    with conn.cursor() as cur:
        file_name = os.path.basename(file_path)
        date_str = file_name.split(".")[0]
        file_date = dt.datetime.strptime(date_str, '%Y-%m-%d').date()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS smart_data (
                id SERIAL PRIMARY KEY,
                date DATE,
                serial_number VARCHAR(255),
                model VARCHAR(255),
                capacity_bytes BIGINT,
                failure BIGINT,
                datacenter VARCHAR(255),
                cluster_id BIGINT,
                vault_id BIGINT,
                pod_id BIGINT,
                pod_slot_num BIGINT,
                is_legacy_format BOOLEAN,
                smart_1_normalized BIGINT,
                smart_1_raw BIGINT
            )
        """)

        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
                row = [None if v == '' else v for v in row]
                cur.execute("""
                    INSERT INTO smart_data (date, serial_number, model, capacity_bytes, failure, datacenter, cluster_id, 
                    vault_id, pod_id, pod_slot_num, is_legacy_format, smart_1_normalized, smart_1_raw)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (file_date, row[1], row[2], int(row[3]) if row[3] else None, int(row[4]) if row[4] else None,
                      row[5], int(row[6]) if row[6] else None, int(row[7]) if row[7] else None,
                      int(row[8]) if row[8] else None, int(row[9]) if row[9] else None,
                      row[10] == 'True', int(row[11]) if row[11] else None, int(row[12]) if row[12] else None))

        conn.commit()
    conn.close()


# 步骤2：从 PostgreSQL 中读取数据并进行分析
def analyze_data_from_postgres(**kwargs):
    # 使用 psycopg2 从 PostgreSQL 中读取数据
    conn = psycopg.connect(**conn_params)
    query = "SELECT date, serial_number, failure FROM smart_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()

    # 将 Pandas DataFrame 转换为 PySpark DataFrame
    spark = SparkSession.builder.appName("Daily Drive Summary").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    # 数据清洗和处理
    df_spark = df_spark.select("date", "serial_number", "failure").dropna()
    df_spark = df_spark.filter(col("failure").isin(0, 1))  # 过滤无效数据

    # 汇总每日硬盘数量和故障数量
    daily_summary = df_spark.groupBy("date").agg(
        count("serial_number").alias("drive_count"),
        sum(col("failure")).alias("drive_failures")
    )

    # 将结果保存到 CSV
    daily_summary.write.csv("output/daily_summary.csv", header=True)
    spark.stop()


# 搜索 CSV 文件
def find_csv_files(**kwargs):
    return glob.glob('mydata/**/*.csv', recursive=True)


# 定义 DAG
with DAG(
        dag_id='upload_and_analyze_daily_csv_to_postgres',
        default_args=default_args,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=60),
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
