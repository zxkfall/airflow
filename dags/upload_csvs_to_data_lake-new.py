import csv
import os
import psycopg
import glob
from datetime import datetime

# PostgreSQL 连接设置
conn = psycopg.connect("dbname=mydatabase user=myuser password=mypassword host=localhost port=5432")

def process_csv_file(file_path):
    with conn.cursor() as cur:
        # 读取文件名中的日期
        file_name = os.path.basename(file_path)
        date_str = file_name.split(".")[0]
        file_date = datetime.strptime(date_str, '%Y-%m-%d').date()

        # 确保表存在，并使用合适的字段类型
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
                -- 添加所有必要的 SMART 字段...
            )
        """)

        # 打开 CSV 文件
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)  # 跳过表头

            # 遍历每一行数据并插入到表中
            for row in reader:
                # print(row)
                # 转换空字符串为 None
                row = [None if v == '' else v for v in row]

                # 插入数据
                cur.execute("""
                    INSERT INTO smart_data (date, serial_number, model, capacity_bytes, failure, datacenter, cluster_id, 
                    vault_id, pod_id, pod_slot_num, is_legacy_format, smart_1_normalized, smart_1_raw)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (file_date, row[1], row[2], int(row[3]) if row[3] else None, int(row[4]) if row[4] else None,
                      row[5], int(row[6]) if row[6] else None, int(row[7]) if row[7] else None,
                      int(row[8]) if row[8] else None, int(row[9]) if row[9] else None,
                      row[10] == 'True', int(row[11]) if row[11] else None, int(row[12]) if row[12] else None))

        conn.commit()

# 使用 glob 匹配所有符合条件的 CSV 文件
csv_files = glob.glob('../mydata/**/*.csv', recursive=True)

# 处理每个匹配到的文件
for csv_file in csv_files:
    print(f"Processing file: {csv_file}")
    process_csv_file(csv_file)

conn.close()
