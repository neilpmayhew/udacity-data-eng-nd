from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries,get_data_quality_queries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'NeilPMayhew',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past':False,
    'retries':3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'email_on_retry':False
}

redshift_conn_id = 'redshift'
s3_region='us-west-2'
s3_bucket='udacity-dend'
aws_credentials_id='aws_credentials'

# use context manager for dag
with DAG('Sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False,
        ) as dag:
    
    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        s3_region=s3_region,
        s3_bucket=s3_bucket,
        s3_key='log_data/{{ execution_date.strftime("%Y") }}/{{ execution_date.strftime("%m") }}/{{ ds }}-events.json',
        #s3_key='log_data',
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_format='JSON',
        s3_format_args="'s3://udacity-dend/log_json_path.json'",
        staging_table='staging_events'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        s3_region=s3_region,
        s3_bucket=s3_bucket,
        s3_key='song_data',
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_format='JSON',
        s3_format_args="'auto'",
        staging_table='staging_songs',
        execution_date = None
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id=redshift_conn_id,
        sql_query=SqlQueries.songplay_table_insert,
        target_table='public.songplays'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id=redshift_conn_id,
        sql_query=SqlQueries.user_table_insert,
        target_table='public.users',
        truncate=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id=redshift_conn_id,
        sql_query=SqlQueries.song_table_insert,
        target_table='public.songs',
        truncate=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id=redshift_conn_id,
        sql_query=SqlQueries.artist_table_insert,
        target_table='public.artists',
        truncate=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id=redshift_conn_id,
        sql_query=SqlQueries.time_table_insert,
        target_table='public.time',
        truncate=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=redshift_conn_id,
        data_quality_queries=get_data_quality_queries()
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]

    [stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]

    [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks

    run_quality_checks >> end_operator


