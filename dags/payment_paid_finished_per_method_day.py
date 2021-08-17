from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta
from fastparquet import ParquetFile
from os import listdir
from os.path import join, exists
from utils.mongo import get_criteria, insert_criteria, update_criteria
from utils.constants import LANDING_ZONE_PATH, PRESENTATION_ZONE_PATH
import pandas as pd

DAG_ID = 'payment_paid_finished_per_method_day'

default_args = {
    'owner': 'Marcel Coelho',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 14),
    'email': ['coelhosmarcel@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def extract_data(**kwargs):
    dag_vars = get_criteria(DAG_ID)
    if not bool(dag_vars):
        start_date = default_args['start_date'].date()
    else:
        start_date = datetime.strptime(dag_vars['date'], '%Y-%m-%d').date()
        
    date_range = pd.date_range(start=start_date, end=date.today(), freq='D').to_list()

    files = {
        'orders': [],
        'payments': []
    }
    for process_date in date_range:
        date_split = str(process_date).split('-')
        year = date_split[0]
        month = date_split[1]
        day = date_split[2][:2]

        for table in files:
            ingest_path = join(LANDING_ZONE_PATH, 'ingest_csvs', table, year, month, day)
            if exists(ingest_path):
                files[table].extend([join(ingest_path, f) for f in listdir(ingest_path)])
    return files

def transform_data(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(task_ids='extract_data')

    df = {}
    filter = {
        'orders': [
            'payment_order_id',
            'order_status',
            'order_moment_created',
            'order_created_day',
            'order_created_month',
            'order_created_year'
        ],
        'payments': [
            'payment_order_id',
            'payment_amount',
            'payment_method',
            'payment_fee',
            'payment_status'
        ]
    }

    for table in files:
        if files[table]:
            df[table] = ParquetFile(files[table]).to_pandas()
            df[table] = df[table][filter[table]]
        else:
            print('No data to transform')
    
    if len(df) == 2:
        df['orders'] = df['orders'][df['orders']['order_status'] == 'FINISHED']
        df['payments'] = df['payments'][df['payments']['payment_status'] == 'PAID']

        df['new'] = pd.merge(df['orders'], df['payments'], on='payment_order_id')

        df['new']['date'] = pd.to_datetime(df['new']['order_moment_created'], infer_datetime_format=True).dt.normalize()

        df = df['new'].drop(columns=['order_status', 'payment_status', 'payment_order_id', 'order_moment_created'])
        df = df.rename(columns={'order_created_day': 'day', 'order_created_month': 'month', 'order_created_year': 'year'})

        df = df.groupby(['year', 'month', 'day', 'date', 'payment_method']).agg(
            payment_amount_sum=('payment_amount' , 'sum'),
            payment_amount_mean=('payment_amount' , 'mean'),
            payment_amount_median=('payment_amount' , 'median'),
            payment_amount_min=('payment_amount' , 'min'),
            payment_amount_max=('payment_amount' , 'max'),
            payment_amount_std=('payment_amount' , 'std'),
            payment_amount_var=('payment_amount' , 'var'),
            payment_fee_sum=('payment_fee' , 'sum'),
            payment_fee_mean=('payment_fee' , 'mean'),
            payment_fee_median=('payment_fee' , 'median'),
            payment_fee_min=('payment_fee' , 'min'),
            payment_fee_max=('payment_fee' , 'max'),
            payment_fee_std=('payment_fee' , 'std'),
            payment_fee_var=('payment_fee' , 'var'),
        ).round(2).reset_index()
        
        print('Transformation completed:')
        print(df.dtypes)

        df.to_parquet(join(PRESENTATION_ZONE_PATH, DAG_ID), engine='fastparquet', compression='gzip', partition_cols=['year', 'month', 'day'], index=False)

    if not bool(get_criteria(DAG_ID)):
        insert_criteria(DAG_ID, str(date.today()))
    else:
        update_criteria(DAG_ID, str(date.today()))

with DAG(DAG_ID,
        default_args = default_args,
        schedule_interval = '0 3 * * *',
        catchup=False) as dag:

        extract_data = PythonOperator(
            task_id='extract_data',
            python_callable=extract_data,
            provide_context=True)
        transform_data = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data,
            provide_context=True)
        
        extract_data >> transform_data