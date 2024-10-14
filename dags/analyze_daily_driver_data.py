from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import datetime as dt
import pandas as pd
import psycopg
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
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'execution_timeout': dt.timedelta(minutes=10),  # 任务超时设置
}


# 步骤1：数据清洗
def clean_data(**kwargs):
    # 使用 psycopg2 从 PostgreSQL 中读取数据
    conn = psycopg.connect(**conn_params)
    query = "SELECT date, serial_number, model, failure FROM smart_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()

    # 将 Pandas DataFrame 转换为 PySpark DataFrame
    spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    # 数据清洗：选择需要的列、去掉无效数据、去重
    df_cleaned = df_spark.select("date", "serial_number", "model", "failure").dropna()
    df_cleaned = df_cleaned.filter(col("failure").isin(0, 1))  # 过滤无效数据
    df_cleaned = df_cleaned.dropDuplicates()  # 去掉重复数据

    # 将清洗后的数据转换为 Pandas DataFrame，准备写入 PostgreSQL
    df_cleaned_pandas = df_cleaned.toPandas()

    # 使用 psycopg2 写入清洗后的数据到 PostgreSQL
    conn = psycopg.connect(**conn_params)
    with conn.cursor() as cur:
        # 创建新表 (如果不存在)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_cleaned_data (
                id SERIAL PRIMARY KEY,
                date DATE,
                serial_number VARCHAR(255),
                model VARCHAR(255),
                failure BIGINT
            )
        """)

        # 插入清洗后的数据
        for index, row in df_cleaned_pandas.iterrows():
            cur.execute("""
                INSERT INTO daily_cleaned_data (date, serial_number, model, failure)
                VALUES (%s, %s, %s, %s)
            """, (row['date'], row['serial_number'], row['model'], row['failure']))

        conn.commit()
    conn.close()
    spark.stop()


# 步骤2：数据分析
def analyze_data_from_postgres(**kwargs):
    # 使用 psycopg2 从 PostgreSQL 中读取清洗后的数据
    conn = psycopg.connect(**conn_params)
    query = "SELECT date, serial_number, failure FROM daily_cleaned_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()

    # 将 Pandas DataFrame 转换为 PySpark DataFrame
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


# 定义 DAG
with DAG(
        dag_id='analyze_daily_driver_data',
        default_args=default_args,
        catchup=False,
        dagrun_timeout=dt.timedelta(minutes=60),
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
        python_callable=analyze_data_from_postgres,
        provide_context=True
    )

    # 设置任务依赖关系
    clean_data_task >> analyze_data_task
