import csv
import os
import shutil
import zipfile
import glob
import pandas as pd
import psycopg
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, year
import logging

# sqlalchemy 连接设置
conn_params = {
    "dbname": "mydatabase",
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost",
    "port": "5432"
}

# target_dir
custom_target_base_dir = './mydata'

# source_dir
custom_source_dir = './original_drive_data'


# 获取 PostgreSQL 连接
def get_postgres_conn():
    return psycopg.connect(
        dbname=os.getenv("POSTGRES_DB", conn_params["dbname"]),
        user=os.getenv("POSTGRES_USER", conn_params["user"]),
        password=os.getenv("POSTGRES_PASSWORD", conn_params["password"]),
        host=os.getenv("POSTGRES_HOST", conn_params["host"]),
        port=os.getenv("POSTGRES_PORT", conn_params["port"])
    )


# 解压缩文件
def unzip_files(source_dir=custom_source_dir, target_base_dir=custom_target_base_dir, **kwargs):
    os.makedirs(target_base_dir, exist_ok=True)
    for filename in os.listdir(source_dir):
        if filename.endswith('.zip'):
            zip_path = os.path.join(source_dir, filename)
            target_dir = os.path.join(target_base_dir, os.path.splitext(filename)[0])

            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            os.makedirs(target_dir, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(target_dir)
                logging.info(f"解压完成: {zip_path} 到 {target_dir}")


# 处理 CSV 文件并写入数据库
def process_csv_file(file_path, **kwargs):
    conn = get_postgres_conn()
    file_name = os.path.basename(file_path)
    file_date = datetime.strptime(file_name.split(".")[0], '%Y-%m-%d').date()

    with conn.cursor() as cur, open(file_path, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)  # 跳过标题行

        # drop if existed
        cur.execute("DROP TABLE IF EXISTS smart_data")
        # ensure table existed
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

        for row in reader:
            row = [None if v == '' else v for v in row]
            cur.execute("""
                INSERT INTO smart_data (date, serial_number, model, capacity_bytes, failure, datacenter, 
                                        cluster_id, vault_id, pod_id, pod_slot_num, is_legacy_format, 
                                        smart_1_normalized, smart_1_raw)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (file_date, row[1], row[2], int(row[3]) if row[3] else None, int(row[4]) if row[4] else None,
                  row[5], int(row[6]) if row[6] else None, int(row[7]) if row[7] else None,
                  int(row[8]) if row[8] else None, int(row[9]) if row[9] else None,
                  row[10] == 'True', int(row[11]) if row[11] else None, int(row[12]) if row[12] else None))

        conn.commit()
    conn.close()


# 搜索 CSV 文件
def find_csv_files(target_dir='mydata', **kwargs):
    return glob.glob(f'{target_dir}/**/*.csv', recursive=True)


# 清洗数据
def clean_data(**kwargs):
    conn = get_postgres_conn()
    query = "SELECT date, serial_number, model, failure FROM smart_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()

    spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    df_cleaned = df_spark.select("date", "serial_number", "model", "failure") \
        .dropna() \
        .filter(col("failure").isin(0, 1)) \
        .dropDuplicates()

    df_cleaned_pandas = df_cleaned.toPandas()

    with conn.cursor() as cur:
        # drop if existed
        cur.execute("DROP TABLE IF EXISTS cleaned_data")

        # ensure table existed
        cur.execute("""
                   CREATE TABLE IF NOT EXISTS cleaned_data (
                       id SERIAL PRIMARY KEY,
                       date DATE,
                       serial_number VARCHAR(255),
                       model VARCHAR(255),
                       failure BIGINT
                   )
               """)

        for index, row in df_cleaned_pandas.iterrows():
            cur.execute("""
                INSERT INTO cleaned_data (date, serial_number, model, failure)
                VALUES (%s, %s, %s, %s)
            """, (row['date'], row['serial_number'], row['model'], row['failure']))
        conn.commit()
    conn.close()
    spark.stop()


# Daily 分析
def analyze_daily_data(**kwargs):
    conn = get_postgres_conn()
    query = "SELECT date, serial_number, failure FROM cleaned_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()

    spark = SparkSession.builder.appName("Daily Drive Summary").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    daily_summary = df_spark.groupBy("date").agg(
        count("serial_number").alias("drive_count"),
        sum(col("failure")).alias("drive_failures")
    )

    daily_summary.write.csv("output/daily_summary.csv", header=True)
    spark.stop()


# Yearly 分析
def analyze_yearly_data(**kwargs):
    conn = get_postgres_conn()
    query = "SELECT date, serial_number, model, failure FROM cleaned_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()

    spark = SparkSession.builder.appName("Yearly Drive Summary").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    df_spark = df_spark.withColumn("year", year("date"))
    yearly_summary = df_spark.groupBy("year", "model").agg(
        sum("failure").alias("drive_failures")
    )

    yearly_summary.write.csv("output/yearly_summary.csv", header=True)
    spark.stop()


# 支持多选的分析任务
def choose_analysis(**kwargs):
    analysis_types = kwargs['dag_run'].conf.get('analysis_type', ['daily'])
    return ['analyze_daily_data'] if 'daily' in analysis_types else ['analyze_yearly_data']


# 定义 DAG
with DAG(
        dag_id='analyze_driver_data',
        default_args={
            'owner': 'airflow',
            'start_date': days_ago(1),
            'retries': 1,
            'retry_delay': pd.Timedelta(minutes=1)
        },
        catchup=False,
        schedule_interval=None,
        tags=['data_analysis', 'practice', 'driver_data', 'daily', 'yearly'],
        params={'analysis_types': ['daily', 'yearly']}
) as dag:
    unzip_task = PythonOperator(
        task_id='unzip_files',
        python_callable=unzip_files
    )

    search_csv_task = PythonOperator(
        task_id='search_csv_files',
        python_callable=find_csv_files
    )

    # 并发处理每个 CSV 文件
    csv_files = find_csv_files()
    process_csv_tasks = [
        PythonOperator(
            task_id=f'process_{os.path.basename(file)}',
            python_callable=process_csv_file,
            op_kwargs={'file_path': file}
        ) for file in csv_files
    ]

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    branch_task = BranchPythonOperator(
        task_id='choose_analysis',
        python_callable=choose_analysis
    )

    analyze_daily_task = PythonOperator(
        task_id='analyze_daily_data',
        python_callable=analyze_daily_data
    )

    analyze_yearly_task = PythonOperator(
        task_id='analyze_yearly_data',
        python_callable=analyze_yearly_data
    )

    end_task = EmptyOperator(task_id='end')

    unzip_task >> search_csv_task >> process_csv_tasks >> clean_data_task
    clean_data_task >> branch_task >> [analyze_daily_task, analyze_yearly_task]
    [analyze_daily_task, analyze_yearly_task] >> end_task
