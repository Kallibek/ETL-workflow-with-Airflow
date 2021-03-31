from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'kallibekk',
    'start_date': datetime(2021, 3, 30),
    'depends_on_past': False,
    'retries': 3,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}
# Define a dag
dag = DAG('data_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          template_searchpath = ['/home/workspace/airflow/']
        )

# Dummy operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Creates tables two staging, one fact 
# and four dimensional tables in the Redshift cluster
create_tables_task = PostgresOperator(
          task_id="create_tables",
          dag=dag,
          postgres_conn_id="redshift",
          sql="create_tables.sql"
          )
# Copies data from json log files located in S3 to the staging_events table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data"
)

# Copies data from json song files located in S3 to the staging_songs table
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

# Inserts data from the two staging tables to the songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songplays",
    query=SqlQueries.songplay_table_insert
)

# Inserts user-related data from staging_events to the users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.users",
    query=SqlQueries.user_table_insert
)

# Inserts song data from staging_songs to the songs table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songs",
    query=SqlQueries.song_table_insert
)

# Inserts artist data from staging_songs to the artists table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.artists",
    query=SqlQueries.artist_table_insert
)

# Inserts time-related data from staging_events to the time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='public."time"',
    query=SqlQueries.time_table_insert
)

# Checks whether tables contain any rows and logs count of rows
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["public.songplays","public.users","public.songs","public.artists",'public."time"']
)

# Dummpy end operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Organize DAG structure 
start_operator >> create_tables_task

create_tables_task >> stage_events_to_redshift >> load_songplays_table
create_tables_task >> stage_songs_to_redshift >> load_songplays_table


load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
