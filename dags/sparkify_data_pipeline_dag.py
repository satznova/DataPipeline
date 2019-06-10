from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.udacity_plugin import (StageToRedshiftOperator, LoadFactOperator,
                                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# default arguments used when creating tasks
default_args = {
    'owner': 'sathish',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup' : True
}

# defining dag object
dag = DAG('sparkify_dwh_pipeline_dag',
		  catchup=False,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 */1 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# staging from S3 to Redshift stage tables

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials = 'aws_credentials',
    redshift_conn_id = 'aws_redshift',
    s3_bucket = Variable.get('s3_bucket'),
    s3_key = Variable.get('s3_prefix_events'),
    table_name = 'staging_events',
    table_format = 'JSON'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials = 'aws_credentials',
    redshift_conn_id = 'aws_redshift',
    s3_bucket = Variable.get('s3_bucket'),
    s3_key = Variable.get('s3_prefix_events'),
    table_name = 'staging_songs',
    table_format = 'JSON'
)

# loading fact and dimension tables

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'aws_redshift',
    table_name = 'songplays',
    sql_load_query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'aws_redshift',
    table_name = 'users',
    sql_load_query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'aws_redshift',
    table_name = 'songs',
    sql_load_query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'aws_redshift',
    table_name = 'artists',
    sql_load_query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'aws_redshift',
    table_name = 'time',
    sql_load_query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table_list = ['songplays', 'users', 'songs', 'artists', 'time'],
    redshift_conn_id = 'aws_redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# configuring the Task Dependencies

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift  >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table   >> run_quality_checks
load_song_dimension_table   >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table   >>  run_quality_checks

run_quality_checks >> end_operator
