from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import datetime as dt
import csv
import os
import psycopg
import glob
import shutil
import zipfile
from airflow.utils.dates import days_ago

# PostgreSQL 连接设置
conn_params = {
    "dbname": "mydatabase",
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost",
    "port": "5432"
}

# 默认参数
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # 设置 DAG 的开始时间为当前时间减去一天
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=2),
    'execution_timeout': dt.timedelta(minutes=1),  # 设置任务超时时间
}

# 解压缩文件的函数
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


# CSV 文件处理函数
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


# 搜索符合条件的 CSV 文件
def find_csv_files(**kwargs):
    csv_files = glob.glob('mydata/**/*.csv', recursive=True)
    return csv_files


# 定义 Airflow DAG
with DAG(
        dag_id='load_csvs_to_postgresql',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,  # 禁止调度
        dagrun_timeout=dt.timedelta(days=1),
        tags=['load', 'csv', 'postgresql', 'practice'],
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

    # 解压 -> 搜索 CSV 文件 -> 处理 CSV 文件
    csv_files = find_csv_files()
    process_csv_tasks = [create_csv_import_tasks(file) for file in csv_files]

    # 设置任务依赖
    unzip_task >> search_csv_task >> process_csv_tasks
