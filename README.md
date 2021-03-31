# ETL Pipelines with Airflow

by Kallibek Kazbekov

Date: 3/31/2021

# Project summary

## Purpose

The project addresses an ETL needs of an imaginary music streaming company. 
The code builds scheduled Airflow data pipelines to copy songs and log data from S3 into Redshift star-schema tables.
In addition, basic data quality checks are implemented within the pipeline.


## Airflow Pipeline

![Alt text](/dag_data_pipeline_project.png "Airflow Pipeline")

The pipeline (DAG) consists of five tasks:

1. Create tables in the Redshift cluster (two staging, one fact, and four dimensional tables);
2. Copy song and log data from jSON file stored in S3 to corresponding staging tables;
3. Insert data from the two staging tables to the songplays fact table;
4. Insert data from the two staging tables to the four dimensional tables;
5. Basic data quality check.

For each step, a custom Airflow operator was developed. 

# Project instructions on how to run the Python scripts

Before executing the code, add Redshift and AWS IAM connections in the Airflow UI.

# An explanation of the files in the repository

## `airflow/dags/data_pipeline_dag.py` 

The file defines the DAG.

`dag = DAG('data_pipeline_dag', default_args=default_args,...)` - DAG initialization

`create_tables_task = PostgresOperator(...)` - create tables in a Redshift cluster

`stage_events_to_redshift = StageToRedshiftOperator(...)` - copies data from JSON (in S3 bucket) to staging tables

`load_songplays_table = LoadFactOperator(...)` - inserts data from staging tables to a fact table

`load_user_dimension_table = LoadDimensionOperator(...)` - inserts data from staging tables to a dimensional table

`run_quality_checks = DataQualityOperator(...)` - checks whether tables contain any rows and logs count of rows

##  `airflow/create_tables.sql`

Contains queries to create tables

## `airflow/plugins/helpers/sql_queries.py`

Contains queries to insert data to tables

## `airflow/plugins/operators/stage_redshift.py`

Custom operator to copy data from JSON (in S3 bucket) to staging tables

`StageToRedshiftOperator(
                          redshift_conn_id="",
                          aws_credentials_id="",
                          table="",
                          s3_bucket="",
                          s3_key="",
                          delimiter=",",
                          ignore_headers=1,
                          *args, **kwargs)`

Arguments:

`redshift_conn_id` - String - Redshift connection ID;

`aws_credentials_id` - String - AWS IAM connection ID;

`table` - String - name of a table to copy data to;

`s3_bucket` - String - S3 bucket name containing data;

`s3_key` - String - prefix of files.

## `airflow/plugins/operators/load_fact.py`

Custom operator that inserts data from staging tables to a fact table

`LoadFactOperator(redshift_conn_id="",
                 table="",
                 query="",
                 truncate=True,
                 *args, **kwargs)`
                 
Arguments:

`redshift_conn_id` - String - Redshift connection ID;

`table` - String - name of a table to copy data to;

`query` - String - SQL query to select data to be copied;

`truncate` - Boolean -  if True, truncated tables instead of merge.

## `airflow/plugins/operators/load_dimension.py`

Custom operator that inserts data from staging tables to a dimensional table

`LoadDimensionOperator(redshift_conn_id="",
                       table="",
                       query="",
                       truncate=True,
                       *args, **kwargs)`
                       
Arguments:

`redshift_conn_id` - String - Redshift connection ID;

`table` - String - name of a table to copy data to;

`query` - String - SQL query to select data to be copied;

`truncate` - Boolean -  if True, truncated tables instead of merge.

## `airflow/plugins/operators/data_quality.py`

Custom operator that runs basic data quality checks (e.g. count of rows). 

`DataQualityOperator(redshift_conn_id="",
                 tables=[],
                 *args, **kwargs)`
Arguments:

`redshift_conn_id` - String - Redshift connection ID;


`tables` - List of Strings - list of tables to be checked for data quality;
