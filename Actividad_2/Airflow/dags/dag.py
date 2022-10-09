

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.utils.decorators import apply_defaults

from airflow.models.baseoperator import BaseOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


db_target_fields = [
    "CO", "PT081", "NMHC", "C6H6", "PT082", 
    "NOx", "PT083", "NO2", "PT084", "PT085",
    "T", "RH", "AH"]

class MissingInsertOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 target_table=None,
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)
        self.target_table = target_table

    def execute(self, context):

        # Read
        df = pd.read_csv('dags/AirQualityUCI_NaN.csv', sep=';', index_col=0)

        df_list = [[getattr(i , "_0") for i in list(df.itertuples(index=False))[2:]]]

        target = PostgresHook('postgres_default')
        
        target.insert_rows(self.target_table,
                           df_list,
                           target_fields=db_target_fields,
                           replace=False)

class DailyInsertOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 target_table=None,
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)
        self.target_table = target_table

    def execute(self, context):

        # Read
        df = pd.read_csv('dags/AirQualityUCI_Daily.csv', sep=';', index_col=0)
        df = df.set_index("index")

        df_list = [[getattr(i , "_0") for i in list(df.itertuples(index=False))]]


        for j in range(1, len(df_list[0])):
            df_list[0][j] = float(f'{float(df_list[0][j]):.2f}')

        target = PostgresHook('postgres_default')
        
        target.insert_rows(self.target_table,
                           df_list,
                           target_fields=["DATE_ID"] + db_target_fields,
                           replace=False)


def clean_dataset():
    df = pd.read_csv('dags/AirQualityUCI.csv', sep=';')

    df = df.drop('Unnamed: 15', axis=1)
    df = df.drop('Unnamed: 16', axis=1)

    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    df['Time'] = pd.to_datetime(df['Time'], format='%H.%M.%S')

    df['CO(GT)'] = df['CO(GT)'].str.replace(',','.').astype('float64')
    df['C6H6(GT)'] = df['C6H6(GT)'].str.replace(',','.').astype('float64')
    df['T'] = df['T'].str.replace(',','.').astype('float64')
    df['RH'] = df['RH'].str.replace(',','.').astype('float64')
    df['AH'] = df['AH'].str.replace(',','.').astype('float64')


    df.replace(-200, np.nan, inplace=True)

    df = df.dropna(axis=0, how='all')

    df.to_csv('dags/AirQualityUCI_clean.csv', sep=';')


def count_empty():
    df = pd.read_csv('dags/AirQualityUCI_clean.csv', sep=";", index_col=0)
    
    counts = df.isna().sum() / len(df)

    counts.to_csv('dags/AirQualityUCI_NaN.csv', sep=';')


def daily_aggregate():
    df = pd.read_csv('dags/AirQualityUCI_clean.csv', sep=";", index_col=0)

    from random import randint

    # Get random day
    # Simulating a finishing day
    index = randint(24 + 1, len(df) - 25)

    day_df = df[df['Date'] == df['Date'].iloc[index]]

    day_df = day_df.mask(day_df.isnull()).mean()

    day_df = day_df.to_frame().reset_index() 
    day_df.loc[-1] = ['Date', df['Date'].iloc[index] ]  # adding a row

    day_df.index = day_df.index + 1  # shifting index
    day_df = day_df.sort_index() 

    day_df.to_csv('dags/AirQualityUCI_Daily.csv', sep=';')



default_args = {
    'owner': 'ariflowUser',    
    'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    'retry_delay': timedelta(minutes=5),
}
with DAG(
	dag_id = "Actividad_1",
	default_args=default_args,
	schedule_interval='@daily',	
	dagrun_timeout=timedelta(minutes=60),
	description='Clean data and get daily log',
	start_date = airflow.utils.dates.days_ago(1)) as dag_python:

    create_missing_table = PostgresOperator(
        task_id="create_missing_table",
        postgres_conn_id="postgres_default",
        sql="create_missing.sql")
    
    create_daily_table = PostgresOperator(
        task_id="create_daily_table",
        postgres_conn_id="postgres_default",
        sql="create_daily.sql")


    clean_dataset_py = PythonOperator(task_id='clean_dataset_py', python_callable=clean_dataset)
    count_empty_py = PythonOperator(task_id='count_empty_py', python_callable=count_empty)
    daily_aggregate_py = PythonOperator(task_id='daily_aggregate_py', python_callable=daily_aggregate)

    add_empty = MissingInsertOperator(
        task_id=f"start_empty",
        target_table='missing'
    )

    add_daily = DailyInsertOperator(
        task_id=f"start_daily",
        target_table='daily'
    )

    create_daily_table >> clean_dataset_py
    create_missing_table >> clean_dataset_py
    
    clean_dataset_py >> count_empty_py
    clean_dataset_py >> daily_aggregate_py

    count_empty_py >> add_empty
    daily_aggregate_py >> add_daily
