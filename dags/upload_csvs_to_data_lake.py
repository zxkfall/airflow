import os
import glob
import logging
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from pyspark.sql import SparkSession

# 默认参数
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
    'execution_timeout': datetime.timedelta(minutes=1),  # 设置任务超时时间
}


# 创建支持 Delta Lake 的 Spark 会话
def create_spark_session():
    spark = SparkSession.builder \
        .appName("AirflowDeltaLakeUpload") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark


# 定义上传到 Delta Lake 的函数
def upload_to_delta(csv_file):
    try:
        logging.info(f'Starting to upload {csv_file} to Delta Lake...')
        print(f'Starting to upload {csv_file} to Delta Lake...')

        # 初始化 Spark 会话
        spark = create_spark_session()

        # 读取 CSV 文件
        df = spark.read.csv(csv_file, header=True, inferSchema=True)

        # Delta Lake 目标路径
        delta_table_path = "/delta/tables/driver_table"  # 本地或 HDFS 路径

        # 将数据写入 Delta Lake 表
        df.write.format("delta").mode("append").save(delta_table_path)

        logging.info(f'Finished uploading {csv_file} to Delta Lake at {delta_table_path}')
        print(f'Finished uploading {csv_file} to Delta Lake at {delta_table_path}')
    except Exception as e:
        logging.error(f"Error uploading {csv_file} to Delta Lake: {str(e)}")
        raise


# 定义 DAG
with DAG(
        dag_id='upload_csvs_to_data_lake',
        schedule='@once',  # 立即执行一次
        default_args=default_args,
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=['example', 'upload_csvs'],
        params={"example_key": "example_value"},
) as dag:
    # 定义空任务，作为最后的占位任务
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # 遍历指定目录下的所有 CSV 文件
    csv_files = glob.glob('mydata/*/*.csv', recursive=True)

    # 如果没有找到 CSV 文件，记录日志并添加一个空任务
    if not csv_files:
        logging.warning("No CSV files found to upload.")
        no_files_task = EmptyOperator(task_id='no_files_found')
        no_files_task >> run_this_last
    else:
        # 为每个 CSV 文件创建上传任务
        upload_tasks = []
        for i, csv_file in enumerate(csv_files):
            task = PythonOperator(
                task_id=f'upload_{os.path.basename(csv_file)}',
                python_callable=upload_to_delta,
                op_args=[csv_file],  # 将 CSV 文件路径传递给任务
                execution_timeout=datetime.timedelta(minutes=1),  # 给每个上传任务添加超时
            )
            upload_tasks.append(task)

        # 设置任务依赖，确保最后一个上传任务连接到 run_this_last
        if upload_tasks:
            for i in range(1, len(upload_tasks)):
                upload_tasks[i - 1] >> upload_tasks[i]

            upload_tasks[-1] >> run_this_last

    # 模拟额外的操作任务
    also_run_this = PythonOperator(
        task_id='also_run_this',
        python_callable=lambda: logging.info("All CSV files have been uploaded to Delta Lake successfully!"),
    )

    also_run_this >> run_this_last
