import csv
import datetime as dt
import glob
import os
import shutil
import zipfile
from datetime import datetime

import pandas as pd
import psycopg
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, year

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
    'retry_delay': dt.timedelta(minutes=1),
    'execution_timeout': dt.timedelta(minutes=10),
}


# 解压缩文件步骤
def unzip_files(**kwargs):
    source_dir = './original_drive_data'  # 压缩文件所在目录
    target_base_dir = './mydata'  # 解压缩目标基础目录

    # 确保目标基础目录存在
    os.makedirs(target_base_dir, exist_ok=True)

    # 遍历 source_dir 中的所有 zip 文件
    for filename in os.listdir(source_dir):
        if filename.endswith('.zip'):
            zip_path = os.path.join(source_dir, filename)
            zip_name = os.path.splitext(filename)[0]  # 获取不带扩展名的文件名
            target_dir = os.path.join(target_base_dir, zip_name)  # 目标文件夹以 zip 文件名命名

            # 如果目标目录存在，删除旧目录及内容
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)

            # 确保解压目录存在
            os.makedirs(target_dir, exist_ok=True)

            # 打开 zip 文件
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # 获取 zip 文件中的所有文件名
                for member in zip_ref.namelist():
                    # 跳过 __MACOSX 文件夹及其内容
                    if member.startswith('__MACOSX/'):
                        continue

                    # 构造解压目标路径
                    target_file_path = os.path.join(target_dir, member)
                    target_folder = os.path.dirname(target_file_path)

                    # 如果目标文件是目录，确保目录存在
                    if not os.path.exists(target_folder):
                        os.makedirs(target_folder, exist_ok=True)

                    # 解压文件
                    with zip_ref.open(member) as source, open(target_file_path, "wb") as target:
                        shutil.copyfileobj(source, target)

    print("所有 zip 文件已解压到以 zip 文件名命名的文件夹，__MACOSX 文件夹已被跳过。")


# CSV 文件处理步骤
def process_csv_file(file_path, **kwargs):
    conn = psycopg.connect(**conn_params)
    with conn.cursor() as cur:
        # 读取文件名中的日期
        file_name = os.path.basename(file_path)
        date_str = file_name.split(".")[0]
        file_date = datetime.strptime(date_str, '%Y-%m-%d').date()

        # 确保表存在
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

        # 打开 CSV 文件
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


# 搜索符合条件的 CSV 文件步骤
def find_csv_files(**kwargs):
    csv_files = glob.glob('mydata/**/*.csv', recursive=True)
    return csv_files


# 数据清洗步骤
def clean_data(**kwargs):
    conn = psycopg.connect(**conn_params)
    query = "SELECT date, serial_number, model, failure FROM smart_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()

    # 转换为 PySpark DataFrame
    spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    # 数据清洗：选择需要的列、去掉无效数据、去重
    df_cleaned = df_spark.select("date", "serial_number", "model", "failure").dropna()
    df_cleaned = df_cleaned.filter(col("failure").isin(0, 1))
    df_cleaned = df_cleaned.dropDuplicates()

    # 将清洗后的数据转换为 Pandas DataFrame
    df_cleaned_pandas = df_cleaned.toPandas()

    # 使用 psycopg2 写入清洗后的数据到 PostgreSQL
    conn = psycopg.connect(**conn_params)
    with conn.cursor() as cur:
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


# Daily分析步骤
def analyze_daily_data(**kwargs):
    conn = psycopg.connect(**conn_params)
    query = "SELECT date, serial_number, failure FROM cleaned_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()

    # 转换为 PySpark DataFrame
    spark = SparkSession.builder.appName("Daily Drive Summary").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    # 汇总每日硬盘数量和故障数量
    daily_summary = df_spark.groupBy("date").agg(
        count("serial_number").alias("drive_count"),
        sum(col("failure")).alias("drive_failures")
    )

    # 将结果保存到 CSV
    daily_summary.write.csv("output/daily_summary.csv", header=True)
    spark.stop()


# Yearly分析步骤
def analyze_yearly_data(**kwargs):
    conn = psycopg.connect(**conn_params)
    query = "SELECT date, serial_number, model, failure FROM cleaned_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()

    # 转换为 PySpark DataFrame
    spark = SparkSession.builder.appName("Yearly Drive Summary").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    # 汇总每年按品牌统计故障情况
    df_spark = df_spark.withColumn("year", year("date"))
    yearly_summary = df_spark.groupBy("year", "model").agg(
        sum("failure").alias("drive_failures")
    )

    # 将结果保存到 CSV
    yearly_summary.write.csv("output/yearly_summary.csv", header=True)
    spark.stop()


# 支持多选的分析选择函数
def choose_analysis(**kwargs):
    # 获取传入的 analysis_type 列表，默认值为 ['daily']
    analysis_types = kwargs['dag_run'].conf.get('analysis_type', ['daily'])

    # 返回所有需要执行的任务ID
    task_ids = []
    if 'daily' in analysis_types:
        task_ids.append('analyze_daily_data')
    if 'yearly' in analysis_types:
        task_ids.append('analyze_yearly_data')

    return task_ids  # 返回需要执行的任务列表


# 定义 DAG
with DAG(
        dag_id='analyze_driver_data',
        default_args=default_args,
        catchup=False,
        dagrun_timeout=dt.timedelta(days=1),
        schedule_interval=None,
        tags=['analyse', 'postgresql', 'practice'],
        params={
            'analysis_types': ['daily', 'yearly']  # 默认值是一个包含 'daily' 和 'yearly' 的列表
        }
) as dag:
    # 解压 zip 文件的任务
    unzip_task = PythonOperator(
        task_id='unzip_files',
        python_callable=unzip_files,
        provide_context=True
    )

    # 搜索 CSV 文件的任务
    search_csv_task = PythonOperator(
        task_id='search_csv_files',
        python_callable=find_csv_files,
        provide_context=True
    )


    # 导入每个 CSV 文件的任务
    def create_csv_import_tasks(file_path):
        return PythonOperator(
            task_id=f'process_{os.path.basename(file_path)}',
            python_callable=process_csv_file,
            op_kwargs={'file_path': file_path},
            provide_context=True
        )


    # 清洗数据任务
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True
    )

    # 分支选择任务
    branch_task = BranchPythonOperator(
        task_id='choose_analysis',
        python_callable=choose_analysis,
        provide_context=True
    )

    # Daily分析任务
    analyze_daily_task = PythonOperator(
        task_id='analyze_daily_data',
        python_callable=analyze_daily_data,
        provide_context=True
    )

    # Yearly分析任务
    analyze_yearly_task = PythonOperator(
        task_id='analyze_yearly_data',
        python_callable=analyze_yearly_data,
        provide_context=True
    )

    # Dummy结束任务
    end_task = EmptyOperator(task_id='end')

    csv_files = find_csv_files()
    process_csv_tasks = [create_csv_import_tasks(file) for file in csv_files]

    # 设置任务依赖
    # 解压 -> 搜索 CSV 文件 -> 处理 CSV 文件 -> 清洗数据 -> 分支选择 -> Daily/Yearly 分析 -> 结束
    unzip_task >> search_csv_task >> process_csv_tasks >> clean_data_task
    clean_data_task >> branch_task >> [analyze_daily_task, analyze_yearly_task]
    analyze_daily_task >> end_task
    analyze_yearly_task >> end_task
