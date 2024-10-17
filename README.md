## base environment

- Python 3.9
- Airflow 2.10.2
- Data Lake: Postgres, can be replaced by other data lake, such as S3, HDFS, etc.

## create postgres container

```bash
docker run --name postgres_container -e POSTGRES_USER=myuser -e POSTGRES_PASSWORD=mypassword -e POSTGRES_DB=mydatabase -p 5432:5432 -d postgres:latest
```

## file structure

```baash
airflow
├── dags
│   ├── analyze_driver_data.py  # main dag
├── original_driver_data # original zip files
│   ├── data_Q1_2019.zip
│   ├── data_Q2_2019.zip
│   ├── ...
├── mydata # unzipped csv files, will generate by the dag
│   ├── data_Q1_2019
│   │   ├── 2019-01-01.csv
│   │   ├── 2019-01-02.csv
│   │   ├── ...
│   ├── data_Q2_2019
│   ├── ...
├── output # result files, will generate by the dag
│   ├── daily_summary_[datetime] # daily driver count and failure count summary
│   ├── yearly_summary_[datetime] # yearly driver by brand failure count summary
```

## process

```zxk
unzip zipfiles -> save data to db -> load data from db then clean data, save to db
-> load data from db then generate daily/yearly summary, save as csv
```

## performance

- each csv file almost use 15s to process
- each task for data clean by month almost use 8m to process(first task use 15m)
- for daily summary, when use three month, almost use 3m to process
- for yearly summary, when use three month, almost use 3m to process

## errors fix

- broken pipe: `lsof -i tcp:8746` -> `kill -9 [PID]` -> `airflow standalone`
- clear failed job to rerun
