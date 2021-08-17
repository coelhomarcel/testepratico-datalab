from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta
from fastparquet import ParquetFile
from os import listdir
from os.path import join, exists
from utils.mongo import get_criteria, insert_criteria, update_criteria
from utils.constants import LANDING_ZONE_PATH, PRESENTATION_ZONE_PATH
import pandas as pd

DAG_ID = 'orders_finished_delivered_per_channel_day'

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
        'channels': [],
        'deliveries': []
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
            'delivery_order_id',
            'channel_id',
            'order_status',
            'order_amount',
            'order_delivery_fee',
            'order_delivery_cost',
            'order_moment_created',
            'order_created_day',
            'order_created_month',
            'order_created_year'
        ],
        'channels': [
            'channel_id',
            'channel_type'
        ],
        'deliveries': [
            'delivery_order_id',
            'delivery_status',
            'delivery_distance_meters'
        ]
    }

    for table in files:
        if files[table]:
            df[table] = ParquetFile(files[table]).to_pandas()
            df[table] = df[table][filter[table]]
        else:
            print('No data to transform')
    
    if len(df) == 3:
        df['orders'] = df['orders'][df['orders']['order_status'] == 'FINISHED']
        df['deliveries'] = df['deliveries'][df['deliveries']['delivery_status'] == 'DELIVERED']

        df['new'] = pd.merge(df['orders'], df['channels'], on='channel_id')
        df['new'] = pd.merge(df['new'], df['deliveries'], on='delivery_order_id')

        df['new']['date'] = pd.to_datetime(df['new']['order_moment_created'], infer_datetime_format=True).dt.normalize()

        df = df['new'].drop(columns=['order_status', 'delivery_status', 'channel_id', 'delivery_order_id', 'order_moment_created'])
        df = df.rename(columns={'order_created_day': 'day', 'order_created_month': 'month', 'order_created_year': 'year'})

        df = df.groupby(['year', 'month', 'day', 'date', 'channel_type']).agg(
            order_amount_sum=('order_amount' , 'sum'),
            order_amount_mean=('order_amount' , 'mean'),
            order_amount_median=('order_amount' , 'median'),
            order_amount_min=('order_amount' , 'min'),
            order_amount_max=('order_amount' , 'max'),
            order_amount_std=('order_amount' , 'std'),
            order_amount_var=('order_amount' , 'var'),
            order_delivery_fee_sum=('order_delivery_fee' , 'sum'),
            order_delivery_fee_mean=('order_delivery_fee' , 'mean'),
            order_delivery_fee_median=('order_delivery_fee' , 'median'),
            order_delivery_fee_min=('order_delivery_fee' , 'min'),
            order_delivery_fee_max=('order_delivery_fee' , 'max'),
            order_delivery_fee_std=('order_delivery_fee' , 'std'),
            order_delivery_fee_var=('order_delivery_fee' , 'var'),
            order_delivery_cost_sum=('order_delivery_cost' , 'sum'),
            order_delivery_cost_mean=('order_delivery_cost' , 'mean'),
            order_delivery_cost_median=('order_delivery_cost' , 'median'),
            order_delivery_cost_min=('order_delivery_cost' , 'min'),
            order_delivery_cost_max=('order_delivery_cost' , 'max'),
            order_delivery_cost_std=('order_delivery_cost' , 'std'),
            order_delivery_cost_var=('order_delivery_cost' , 'var'),
            delivery_distance_meters_sum=('delivery_distance_meters' , 'sum'),
            delivery_distance_meters_mean=('delivery_distance_meters' , 'mean'),
            delivery_distance_meters_median=('delivery_distance_meters' , 'median'),
            delivery_distance_meters_min=('delivery_distance_meters' , 'min'),
            delivery_distance_meters_max=('delivery_distance_meters' , 'max'),
            delivery_distance_meters_std=('delivery_distance_meters' , 'std'),
            delivery_distance_meters_var=('delivery_distance_meters' , 'var')
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