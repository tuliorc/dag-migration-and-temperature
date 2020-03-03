from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from stage_redshift import StageToRedshiftOperator
from data_quality_check import DataQualityOperator
from datetime import datetime, time, date, timedelta
import logging
import os
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sql_statements

os.environ['JAVA_HOME'] = '/Users/tuliocarreira/airflow/Java/'
os.environ['AWS_ACCESS_KEY_ID'] = Variable.get('aws_access_key')
os.environ['AWS_SECRET_ACCESS_KEY'] = Variable.get('aws_secret_key')

default_args = {
    'owner': 'Tulio Carreira',
    'start_date': datetime(2020, 3, 1),
    'past_dependencies': False,
    'retries_on_failure': 2,
    'retries_frequency_in_minutes': 5,
    'email_on_retry': True
    }

dag = DAG(
     'capstone',
     default_args=default_args,
     description='DAG for running ETLs for temperature and \
                                        migration rate analysis',
     schedule_interval='0 7 * * *'
     )


def etl_airports():
    logging.info("Preparing airports data...")
    df = pd.read_csv('/Users/tuliocarreira/airflow/datasets/\
                                                   airport-codes_csv.csv')
    df = df[df['iso_country'] == 'US'].drop_duplicates()
    df = df.dropna(subset=['ident'])
    df = df[['ident', 'type', 'name', 'elevation_ft', 'iso_region',
             'municipality', 'gps_code', 'local_code', 'coordinates']]
    df.to_csv('/Users/tuliocarreira/airflow/datasets/airport-codes_csv_v2.csv',
                index=False)
    logging.info("Airports data saved locally and ready to be uploaded to S3!")


def stage_airports():
    logging.info("Loading airports file to S3...")
    hook = S3Hook(aws_conn_id="aws_credentials")
    bucket = Variable.get('capstone_bucket')
    hook.load_file('/Users/tuliocarreira/airflow/datasets/\
                                                 airport-codes_csv_v2.csv',
                                                 'df_airports.csv',
                                                 bucket_name=bucket,
                                                 replace=True)
    logging.info("Airports data successfully uploaded to S3!")


def etl_demographics():
    logging.info("Preparing demographics data...")
    df = pd.read_csv('/Users/tuliocarreira/airflow/datasets/\
                                                   us-cities-demographics.csv',
                                                   sep=";")
    df = df[~df['Foreign-born'].isna()]
    df = df[['City', 'State', 'Median Age', 'Male Population',
             'Female Population', 'Total Population', 'Foreign-born',
             'State Code', 'Race', 'Count']]
    df.to_csv('/Users/tuliocarreira/airflow/datasets/\
                                            us-cities-demographics_v2.csv',
                                            index=False)
    logging.info("Demographics data saved locally and ready\
                                                    to be uploaded to S3!")


def stage_demographics():
    logging.info("Loading demographics file to S3...")
    hook = S3Hook(aws_conn_id="aws_credentials")
    bucket = Variable.get('capstone_bucket')
    hook.load_file('/Users/tuliocarreira/airflow/datasets/\
                                                 us-cities-demographics_v2.csv',
                                                 'df_demographics.csv',
                                                 bucket_name=bucket,
                                                 replace=True)
    logging.info("Demographics data successfully uploaded to S3!")


def etl_temperatures():
    logging.info("Preparing temperatures data...")
    df = pd.read_csv('/Users/tuliocarreira/airflow/datasets/\
                                             GlobalLandTemperaturesByCity.csv')
    df = df[df['Country'] == 'United States'].dropna()
    df.to_csv('/Users/tuliocarreira/airflow/datasets/\
                                           GlobalLandTemperaturesByCity_v2.csv',
                                           index=False)
    logging.info("Temperatures data saved locally and ready\
                                                        to be uploaded to S3!")


def stage_temperatures():
    logging.info("Loading temperatures file to S3...")
    hook = S3Hook(aws_conn_id="aws_credentials")
    bucket = Variable.get('capstone_bucket')
    hook.load_file('/Users/tuliocarreira/airflow/datasets/\
                                        GlobalLandTemperaturesByCity_v2.csv',
                                        'df_temperatures.csv',
                                        bucket_name=bucket,
                                        replace=True)
    logging.info("Temperatures data successfully uploaded to S3!")


def etl_and_stage_migration():
    logging.info("Preparing migration data...")
    spark = SparkSession.builder.config("spark.jars.packages",
                                        "org.apache.hadoop:hadoop-aws:2.7.0")\
                                .getOrCreate()
    df = spark.read.parquet('/Users/tuliocarreira/airflow/datasets/\
                                                          sas_data/*.parquet')
    df = df.selectExpr(['i94yr as i94_year', 'i94mon as i94_month',
                        'i94cit as city_origin', 'i94res as city_destination',
                        'i94port as airport_code', 'arrdate as arrival_date',
                        'depdate as departure_date', 'i94visa as i94_visa',
                        'visatype as visa_type', 'biryear as year_birth'])

    df = df.dropna(subset=('departure_date'))
    df = df.withColumn("id", F.monotonically_increasing_id())

    bucket = Variable.get('capstone_bucket')
    logging.info("Loading migration file to S3...")
    logging.info(df.printSchema())
    df.write.format('parquet').mode('overwrite')\
                              .save("s3a://" + bucket + '/df_migration_files')
    logging.info("Migration data successfully uploaded to S3!")

start_operator = DummyOperator(task_id='trigger_execution', dag=dag)

create_staging_airports_table = PostgresOperator(
    task_id = 'create_staging_airports_table',
    postgres_conn_id = 'redshift',
    sql = sql_statements.CREATE_STAGING_AIRPORTS_REDSHIFT,
    dag = dag
)
create_staging_temperatures_table = PostgresOperator(
    task_id = 'create_staging_temperatures_table',
    postgres_conn_id = 'redshift',
    sql = sql_statements.CREATE_STAGING_TEMPERATURES_REDSHIFT,
    dag = dag
)

create_staging_demographics_table = PostgresOperator(
    task_id = 'create_staging_demographics_table',
    postgres_conn_id = 'redshift',
    sql = sql_statements.CREATE_STAGING_DEMOGRAPHICS_REDSHIFT,
    dag = dag
)
#
create_staging_migration_table = PostgresOperator(
    task_id = 'create_staging_migration_table',
    postgres_conn_id = 'redshift',
    sql = sql_statements.CREATE_STAGING_MIGRATION_REDSHIFT,
    dag = dag
)

create_dim_weather = PostgresOperator(
    task_id = 'create_dim_weather',
    postgres_conn_id = 'redshift',
    sql = sql_statements.CREATE_DIM_WEATHER,
    dag = dag
)

create_dim_cities = PostgresOperator(
    task_id = 'create_dim_cities',
    postgres_conn_id = 'redshift',
    sql = sql_statements.CREATE_DIM_CITIES,
    dag = dag
)

create_fact_migration = PostgresOperator(
    task_id = 'create_fact_migration',
    postgres_conn_id = 'redshift',
    sql = sql_statements.CREATE_FACT_MIGRATION,
    dag = dag
)

create_stats_migration = PostgresOperator(
    task_id = 'create_stats_migration',
    postgres_conn_id = 'redshift',
    sql = sql_statements.CREATE_STATS_MIGRATION,
    dag = dag
)

prepare_airports_dataset = PythonOperator(
    task_id = 'prepare_airports_dataset',
    python_callable = etl_airports,
    dag = dag
)

prepare_demographics_dataset = PythonOperator(
    task_id = 'prepare_demographics_dataset',
    python_callable = etl_demographics,
    dag = dag
)

prepare_temperatures_dataset = PythonOperator(
    task_id = 'prepare_temperatures_dataset',
    python_callable = etl_temperatures,
    dag = dag
)
#
prepare_and_stage_migration_to_s3 = PythonOperator(
    task_id = 'prepare_and_stage_migration_to_s3',
    python_callable = etl_and_stage_migration,
    dag = dag
)
#

stage_airports_to_s3 = PythonOperator(
    task_id = 'stage_airports_to_s3',
    python_callable = stage_airports,
    dag = dag
)


stage_demographics_to_s3 = PythonOperator(
    task_id = 'stage_demographics_to_s3',
    python_callable = stage_demographics,
    dag = dag
)

stage_temperatures_to_s3 = PythonOperator(
    task_id = 'stage_temperatures_to_s3',
    python_callable = stage_temperatures,
    dag = dag
)

stage_demographics_to_redshift = StageToRedshiftOperator(
    task_id='stage_demographics_to_redshift',
    aws_credentials_id="aws_credentials",
    table='public.df_demographics',
    s3_bucket=Variable.get('capstone_bucket'),
    s3_key="df_demographics.csv",
    dag=dag
)

stage_temperatures_to_redshift = StageToRedshiftOperator(
    task_id='stage_temperatures_to_redshift',
    aws_credentials_id="aws_credentials",
    table='public.df_temperatures',
    s3_bucket=Variable.get('capstone_bucket'),
    s3_key="df_temperatures.csv",
    dag=dag
)
stage_airports_to_redshift = StageToRedshiftOperator(
    task_id='stage_airports_to_redshift',
    aws_credentials_id="aws_credentials",
    table='public.df_airports',
    s3_bucket=Variable.get('capstone_bucket'),
    s3_key="df_airports.csv",
    dag=dag
)

stage_migration_to_redshift = StageToRedshiftOperator(
    task_id='stage_migration_to_redshift',
    aws_credentials_id="aws_credentials",
    table='public.df_migration',
    s3_bucket=Variable.get('capstone_bucket'),
    s3_key="df_migration_files/part",
    dag=dag
)

run_staging_quality_checks = DataQualityOperator(
     task_id='Run_staging_quality_checks',
     dag=dag,
     tables=["public.df_airports", "public.df_temperatures",
             "public.df_demographics", "public.df_migration"]
 )

run_schema_quality_checks = DataQualityOperator(
    task_id='Run_schema_quality_checks',
    dag=dag,
    tables=["public.dim_weather", "public.dim_cities", "public.fact_migration",
            "public.stats_migration"]
)

final_operator = DummyOperator(task_id='complete_execution', dag=dag)

start_operator >> create_staging_airports_table
start_operator >> create_staging_temperatures_table
start_operator >> create_staging_demographics_table
start_operator >> create_staging_migration_table

start_operator >> prepare_airports_dataset >> stage_airports_to_s3
stage_airports_to_s3 >> stage_airports_to_redshift
start_operator >> prepare_temperatures_dataset >> stage_temperatures_to_s3
stage_temperatures_to_s3 >> stage_temperatures_to_redshift
start_operator >> prepare_demographics_dataset >> stage_demographics_to_s3
stage_demographics_to_s3 >> stage_demographics_to_redshift
start_operator >> prepare_and_stage_migration_to_s3
prepare_and_stage_migration_to_s3 >> stage_migration_to_redshift

create_staging_airports_table >> stage_airports_to_redshift
create_staging_temperatures_table >> stage_temperatures_to_redshift
create_staging_demographics_table >> stage_demographics_to_redshift
create_staging_migration_table >> stage_migration_to_redshift

stage_airports_to_redshift >> run_staging_quality_checks
stage_temperatures_to_redshift >> run_staging_quality_checks
stage_demographics_to_redshift >> run_staging_quality_checks
stage_migration_to_redshift >> run_staging_quality_checks

run_staging_quality_checks >> create_dim_weather >> run_schema_quality_checks
run_staging_quality_checks >> create_dim_cities >> run_schema_quality_checks
run_staging_quality_checks >> create_fact_migration >> run_schema_quality_checks

create_dim_weather >> create_stats_migration
create_dim_cities >> create_stats_migration
create_fact_migration >> create_stats_migration

create_stats_migration >> run_schema_quality_checks
run_schema_quality_checks >> final_operator
