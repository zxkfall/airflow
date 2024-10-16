import csv
import datetime as dt
import glob
import logging
import os
import shutil
import zipfile
from datetime import datetime, date

import pandas as pd
import psycopg
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum

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

# result_dir
custom_result_dir = './output'


# 获取 PostgreSQL 连接
def get_postgres_conn():
    return psycopg.connect(
        dbname=os.getenv("POSTGRES_DB", conn_params["dbname"]),
        user=os.getenv("POSTGRES_USER", conn_params["user"]),
        password=os.getenv("POSTGRES_PASSWORD", conn_params["password"]),
        host=os.getenv("POSTGRES_HOST", conn_params["host"]),
        port=os.getenv("POSTGRES_PORT", conn_params["port"])
    )


def remove_macosx_dirs(target_dir):
    """递归删除所有 __MACOSX 文件夹"""
    for root, dirs, files in os.walk(target_dir):
        if "__MACOSX" in dirs:
            macosx_path = os.path.join(root, "__MACOSX")
            shutil.rmtree(macosx_path)
            logging.info(f"已删除 __MACOSX 文件夹: {macosx_path}")


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

            # 递归删除所有层级的 __MACOSX 文件夹
            remove_macosx_dirs(target_dir)


# 删除表的函数
def drop_driver_data_table(**kwargs):
    conn = get_postgres_conn()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS driver_data")
        conn.commit()
    conn.close()
    logging.info("已删除 driver_data 表")


# 处理 CSV 文件
def process_csv_file(file_path, **kwargs):
    file_name = os.path.basename(file_path)
    file_date = datetime.strptime(file_name.split(".")[0], '%Y-%m-%d').date()
    # 定义时间范围
    start_date = date(2019, 1, 1)  # 2019年Q1的开始
    end_date = date(2023, 9, 30)  # 2023年Q3的结束

    # 检查日期是否在范围内
    if not (start_date <= file_date <= end_date):
        logging.info(f"{file_date} 超出时间范围，返回。")
        return

    conn = get_postgres_conn()
    try:
        # 创建表的操作单独放在一个事务中
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS driver_data (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    serial_number VARCHAR(255),
                    model VARCHAR(255),
                    failure INT
                )
            """)
            conn.commit()
            logging.info("已创建 driver_data 表")

        # 批量插入数据的操作保证在一个事务中
        with conn.cursor() as cur, open(file_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)  # 跳过标题行

            batch_size = 1000
            batch_data = []
            insert_count = 0  # 记录总插入数

            for row in reader:
                row = [None if v == '' else v for v in row]  # 替换空值为 None
                batch_data.append((
                    file_date,
                    row[1],
                    row[2],
                    int(row[4]) if row[4] else None,
                ))

                if len(batch_data) >= batch_size:
                    cur.executemany("""
                        INSERT INTO driver_data (date, serial_number, model, failure)
                        VALUES (%s, %s, %s, %s)
                    """, batch_data)

                    insert_count += len(batch_data)
                    logging.info(f"已插入 {len(batch_data)} 条记录，总插入数: {insert_count}")
                    batch_data = []

            # 插入剩余的批次数据
            if batch_data:
                cur.executemany("""
                    INSERT INTO driver_data (date, serial_number, model, failure)
                    VALUES (%s, %s, %s, %s)
                """, batch_data)

                insert_count += len(batch_data)
                logging.info(f"已插入剩余 {len(batch_data)} 条记录，总插入数: {insert_count}")

            conn.commit()  # 提交插入事务

    except Exception as e:
        logging.error(f"处理文件 {file_name} 时出错: {e}")
        conn.rollback()  # 如果发生错误，回滚事务

    finally:
        conn.close()


def clear_clean_data_table(**kwargs):
    conn = get_postgres_conn()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS cleaned_data")
        cur.execute("""
                           CREATE TABLE IF NOT EXISTS cleaned_data (
                               id SERIAL PRIMARY KEY,
                               date DATE,
                               serial_number VARCHAR(255),
                               model VARCHAR(255),
                               failure BIGINT
                           )
                       """)
        conn.commit()
        logging.info("已删除并创建 cleaned_data 表")
    conn.close()


# 清洗数据函数
# def clean_data(**kwargs):
#     conn = get_postgres_conn()
#
#     logging.info("开始清洗数据")
#     # 查询所有数据
#     query = "SELECT date, serial_number, model, failure FROM driver_data"
#     df_postgres = pd.read_sql(query, conn)
#     logging.info(f"已读取 {len(df_postgres)} 条记录")
#
#     # 创建 SparkSession
#     spark = SparkSession.builder.appName(
#         "Data Cleaning").config(
#         "spark.driver.memory", "8g").config(
#         "spark.executor.memory", "8g").getOrCreate()
#
#     # 加载数据到 Spark DataFrame
#     df_spark = spark.createDataFrame(df_postgres)
#
#     # 分区以并行处理, 可以根据集群资源调整分区数目
#     df_spark = df_spark.repartition(8)
#     logging.info(f"原始数据量: {df_spark.count()}")
#
#     # 数据清理和品牌分类
#     df_spark = df_spark.select("date", "serial_number", "model", "failure") \
#         .dropna(subset=["date", "serial_number", "model", "failure"]) \
#         .filter(F.col("date").rlike(r"\d{4}-\d{2}-\d{2}")) \
#         .filter((F.col("failure") == 0) | (F.col("failure") == 1)) \
#         .dropDuplicates()
#
#     # 打印 failure 列的分布情况，方便调试
#     df_spark.groupBy("failure").count().show()
#     logging.info(f"清洗后的数据量: {df_spark.count()}")
#
#     # 将清洗后的数据分批插入 PostgreSQL
#     batch_size = 10000
#     df_cleaned_pandas = df_spark.select("date", "model", "failure").toPandas()
#
#     # 创建或清空 cleaned_data 表
#     with conn.cursor() as cur:
#         cur.execute("DROP TABLE IF EXISTS cleaned_data")
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS cleaned_data (
#                 id SERIAL PRIMARY KEY,
#                 date DATE,
#                 model VARCHAR(255),
#                 failure INT
#             )
#         """)
#         conn.commit()
#         logging.info("已创建 cleaned_data 表")
#
#     try:
#         # 批量插入数据，保证所有插入操作在一个事务中
#         with conn.cursor() as cur:
#             for i in range(0, len(df_cleaned_pandas), batch_size):
#                 batch = df_cleaned_pandas.iloc[i:i + batch_size]
#                 cur.executemany("""
#                     INSERT INTO cleaned_data (date, model, failure)
#                     VALUES (%s, %s, %s)
#                 """, batch.values.tolist())
#
#                 logging.info(f"已插入 {i + len(batch)} 条清洗后的数据")
#
#             conn.commit()  # 提交批量插入
#     except Exception as e:
#         logging.error(f"清洗数据插入时出错: {e}")
#         conn.rollback()
#     finally:
#         conn.close()
#         spark.stop()


# Daily 分析
def analyze_daily_data(**kwargs):
    conn = get_postgres_conn()
    query = "SELECT date, failure FROM cleaned_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()
    logging.info(f"已读取 {len(df_postgres)} 条记录")
    spark = SparkSession.builder.appName(
        "Daily Drive Summary").config(
        "spark.driver.memory", "12g").config(
        "spark.executor.memory", "12g").getOrCreate()
    df_spark = spark.createDataFrame(df_postgres)

    daily_summary = df_spark.groupBy("date").agg(
        count("*").alias("drive_count"),
        sum(col("failure")).alias("drive_failures")
    )

    daily_summary.show(100)
    output_folder = f"{custom_result_dir}/daily_summary_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    daily_summary.write.csv(output_folder, header=True)
    logging.info(f"已保存每日汇总数据到: {output_folder}")
    spark.stop()


def analyze_yearly_data(**kwargs):
    conn = get_postgres_conn()
    query = "SELECT date, model, failure FROM cleaned_data"
    df_postgres = pd.read_sql(query, conn)
    conn.close()
    logging.info(f"已读取 {len(df_postgres)} 条记录")
    # 初始化 Spark 会话
    spark = SparkSession.builder.appName("Yearly Drive Summary").config(
        "spark.driver.memory", "8g").config(
        "spark.executor.memory", "8g").getOrCreate()

    df_spark = spark.createDataFrame(df_postgres)
    logging.info(f"已创建 Spark DataFrame: {df_spark.count()} 条记录")
    # 提取年份和品牌分类
    df_spark = df_spark.withColumn("year", F.year(F.col("date")))

    df_spark = df_spark.withColumn("brand", F.when(df_spark.model.startswith("CT"), "Crucial")
                                   .when(df_spark.model.startswith("DELLBOSS"), "Dell BOSS")
                                   .when(df_spark.model.startswith("HGST"), "HGST")
                                   .when(df_spark.model.startswith("Seagate"), "Seagate")
                                   .when(df_spark.model.startswith("ST"), "Seagate")
                                   .when(df_spark.model.startswith("TOSHIBA"), "Toshiba")
                                   .when(df_spark.model.startswith("WDC"), "Western Digital")
                                   .otherwise("Others"))

    # 按年份和品牌汇总故障数
    yearly_summary = df_spark.groupBy("year", "brand").agg(
        F.sum(F.col("failure")).alias("drive_failures"),
        # F.collect_set("model").alias("models")
    )

    # 将模型列转换为逗号分隔的字符串
    # yearly_summary = yearly_summary.withColumn("models", F.concat_ws(", ", "models"))

    # 显示结果
    yearly_summary.show(20)

    output_folder = f"{custom_result_dir}/yearly_summary_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    # 删除文件夹如果已经存在
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)

    # 保存结果到 CSV
    yearly_summary.write.csv(output_folder, header=True)
    logging.info(f"已保存年度汇总数据到: {output_folder}")

    # 关闭 Spark 会话
    spark.stop()


# 支持多选的分析任务
def choose_analysis(**kwargs):
    analysis_types = kwargs['dag_run'].conf.get('analysis_types', ['daily'])
    logging.info(f"选择的分析类型: {analysis_types}")
    if 'daily' in analysis_types and 'yearly' in analysis_types:
        return ['analyze_daily_data', 'analyze_yearly_data']
    elif 'yearly' in analysis_types and 'daily' not in analysis_types:
        return ['analyze_yearly_data']
    else:
        return ['analyze_daily_data']


def find_csv_files(target_dir='mydata', **kwargs):
    files = glob.glob(f'{target_dir}/**/*.csv', recursive=True)
    logging.info(f"找到 {len(files)} 个 CSV 文件")
    files.sort(key=lambda x: os.path.basename(x)[:10])
    return [[file] for file in files]


def generate_intervals_for_month():
    conn = get_postgres_conn()
    total_query = "SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM driver_data"
    date_range = pd.read_sql(total_query, conn)
    min_date = pd.Timestamp(date_range['min_date'].iloc[0])
    max_date = pd.Timestamp(date_range['max_date'].iloc[0])
    logging.info(f"日期范围: {min_date} 到 {max_date}")
    current_date = min_date
    intervals = []
    while current_date <= max_date:
        next_month = current_date + pd.DateOffset(months=1)
        # ranges = [[]]
        intervals.append([[current_date.strftime('%Y-%m-%d'), next_month.strftime('%Y-%m-%d')]])
        current_date = next_month
    logging.info(f"生成的日期范围: {intervals}")
    return intervals


def clean_month_data(interval):
    logging.info(f"正在处理 {interval} 之间的数据")
    start_date = interval[0]
    end_date = interval[1]
    conn = get_postgres_conn()

    query = "SELECT date, serial_number, model, failure FROM driver_data WHERE date >= %s AND date < %s"
    df_postgres = pd.read_sql(query, conn, params=(start_date, end_date))
    if df_postgres.empty:
        logging.info(f"在 {start_date} 和 {end_date} 之间没有数据")
        return

    logging.info(f"已读取 {len(df_postgres)} 条记录")
    spark = SparkSession.builder.appName(
        "Monthly Drive Summary").config(
        "spark.driver.memory", "8g").config(
        "spark.executor.memory", "8g").getOrCreate()

    df_spark = spark.createDataFrame(df_postgres)
    logging.info(f"已创建 Spark DataFrame: {df_spark.count()} 条记录")

    df_spark = df_spark.select("date", "serial_number", "model", "failure") \
        .dropna(subset=["date", "serial_number", "model", "failure"]) \
        .filter(F.col("date").rlike(r"\d{4}-\d{2}-\d{2}")) \
        .filter((F.col("failure") == 0) | (F.col("failure") == 1)) \
        .dropDuplicates()
    df_spark.groupBy("failure").count().show()
    logging.info(f"清洗后的数据量: {df_spark.count()}")
    batch_size = 10000
    df_cleaned_pandas = df_spark.select("date", "model", "failure").toPandas()
    with conn.cursor() as cur:
        for i in range(0, len(df_cleaned_pandas), batch_size):
            batch = df_cleaned_pandas.iloc[i:i + batch_size]
            cur.executemany("""
                    INSERT INTO cleaned_data (date, model, failure)
                    VALUES (%s, %s, %s)
                """, batch.values.tolist())
            logging.info(f"已插入 {i + len(batch)} 条清洗后的数据")
    conn.commit()
    conn.close()


# 定义 DAG
with (DAG(
        dag_id='analyze_driver_data',
        default_args={
            'owner': 'airflow',
            'start_date': days_ago(1),
            'retries': 1,
            'retry_delay': dt.timedelta(seconds=20),
            'execution_timeout': dt.timedelta(hours=8),  # 设置任务超时时间
        },
        catchup=False,
        schedule_interval=None,
        dagrun_timeout=dt.timedelta(days=1),
        tags=['data_analysis', 'practice', 'driver_data', 'daily', 'yearly'],
        params={'analysis_types': ['daily', 'yearly']}
) as dag):
    unzip_task = PythonOperator(
        task_id='unzip_files',
        python_callable=unzip_files
    )

    # 删除表任务
    drop_table_task = PythonOperator(
        task_id='drop_driver_data_table',
        python_callable=drop_driver_data_table,
        dag=dag,
    )

    drop_clean_table_task = PythonOperator(
        task_id='drop_clean_data_table',
        python_callable=clear_clean_data_table,
        dag=dag,
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

    # 动态生成后续任务
    process_csvs_task = PythonOperator.partial(
        task_id='process_csvs_task',
        python_callable=process_csv_file,
        map_index_template="Input file_path={{ task.op_args[0] }}"
    ).expand(op_args=find_csv_files())

    # 生成输入数据的任务
    generate_month_intervals_task = PythonOperator(
        task_id='generate_intervals_for_month',
        python_callable=generate_intervals_for_month,
        # provide_context=True,
    )

    # 动态生成后续任务
    process_month_clean_tasks = PythonOperator.partial(
        task_id='process_month_clean_tasks',
        python_callable=clean_month_data,
        # op_args="{{ task_instance.xcom_pull(task_ids='generate_data')[0] }}"
        # ).expand(op=generate_data_task.output)
    ).expand(op_args=generate_month_intervals_task.output)

    # 设置任务依赖
    unzip_task >> drop_table_task >> process_csvs_task >> drop_clean_table_task

    drop_clean_table_task >> generate_month_intervals_task >> process_month_clean_tasks >> branch_task

    branch_task >> [analyze_daily_task, analyze_yearly_task]
    [analyze_daily_task, analyze_yearly_task] >> end_task
