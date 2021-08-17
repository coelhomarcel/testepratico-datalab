from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta
from encodings.aliases import aliases
from os import listdir, makedirs, rename
from os.path import isfile, join, exists
from utils.mongo import get_criteria, insert_criteria, update_criteria
from utils.constants import EXTRACT_CSVS_PATH, EXTRACTED_CSVS_PATH, LANDING_ZONE_PATH
import pandas as pd

DAG_ID = 'ingest_csvs'

default_args = {
    "owner": "Marcel Coelho",
    "depends_on_past": False,
    "start_date": datetime(2021, 8, 14),
    "email": ["coelhosmarcel@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def get_files(**kwargs):
    return [f for f in listdir(EXTRACT_CSVS_PATH) if f.endswith('.csv') and isfile(join(EXTRACT_CSVS_PATH, f))]

def extract_data(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(task_ids='get_files')

    extract_date = str(date.today())
    date_split = extract_date.split('-')
    year = date_split[0]
    month = date_split[1]
    day = date_split[2][:2]

    print(f"Extracting {extract_date}")

    for file in files:
        print(file)
        s3_dir = join(LANDING_ZONE_PATH, DAG_ID, file[:-4], year, month, day)
        s3_file = join(s3_dir, f'{file[:-4]}.parquet.gzip')
        extract_file = join(EXTRACT_CSVS_PATH, file)
        extracted_dir = join(EXTRACTED_CSVS_PATH, year, month, day)
        extracted_file = join(extracted_dir, file)
        if not exists(s3_dir):
            makedirs(s3_dir)
        if not exists(extracted_dir):
            makedirs(extracted_dir)
        for encoding in set(aliases.values()):
            try:
                df = pd.read_csv(extract_file, encoding=encoding)
                df.to_parquet(s3_file, engine='fastparquet', compression='gzip')
                print("Inserted in Landing Zone: ", s3_file)
                rename(extract_file, extracted_file)
            except:
                pass

    if not bool(get_criteria(DAG_ID)):
        insert_criteria(DAG_ID, extract_date)
    else:
        update_criteria(DAG_ID, extract_date)

with DAG(DAG_ID,
        default_args = default_args,
        schedule_interval = '0 0 * * *',
        catchup=False) as dag:

        get_files = PythonOperator(
            task_id='get_files',
            python_callable=get_files,
            provide_context=True)
        extract_data = PythonOperator(
            task_id='extract_data',
            python_callable=extract_data,
            provide_context=True)
        
        get_files >> extract_data