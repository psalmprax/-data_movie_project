# film_data_dag.py
import logging
import os
import re
import yaml
import pyodbc

# from airflow.operators.sql import DataQualityOperator
import pandas as pd
from airflow import DAG
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlite3 import ProgrammingError
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

# Load environment variables from the specified .env file
load_dotenv(dotenv_path='./dags/docker_job/env/.env')


# Define the SQL Server credentials
SOURCE_DATABASE_SERVER = os.getenv('SOURCE_DATABASE_HOST')
SOURCE_DATABASE_NAME = os.getenv('SOURCE_DATABASE_NAME')
SOURCE_DATABASE_USER = os.getenv('SOURCE_DATABASE_USER')
SOURCE_DATABASE_PASSWORD = os.getenv('SOURCE_DATABASE_PASSWORD')

TARGET_DATABASE_SERVER = os.getenv('TARGET_DATABASE_HOST')
TARGET_DATABASE_NAME = os.getenv('TARGET_DATABASE_NAME')
TARGET_DATABASE_USER = os.getenv('TARGET_DATABASE_USER')
TARGET_DATABASE_PASSWORD = os.getenv('TARGET_DATABASE_PASSWORD')

def copy_data(
        source_conn_str,
        target_conn_str,
        src_table,
        tgt_table,
        target_db_name,
        target_schema,
        order_column,
        batch_size=100
):
    print(source_conn_str)
    print(order_column)
    print(target_schema)
    last_record_value = None
    # Create a connection to the source database
    source_conn = pyodbc.connect(source_conn_str)

    # Create a connection to the target database
    engine = create_engine(target_conn_str)
    try:
        # Check if the database already exists
        with engine.connect() as connection:
            result = connection.execute(f"SELECT 1 FROM pg_database WHERE datname = '{target_db_name.lower()}'")
            database_exists = result.scalar()

        # Create the database if it does not exist
        if not database_exists:
            with engine.connect() as connection:
                # Set isolation level to autocommit
                connection.execution_options(isolation_level="AUTOCOMMIT").execute(f"CREATE DATABASE {target_db_name}")

    except ProgrammingError as e:
        # If an error occurs, print the error message
        print(e)
        raise

    engine = create_engine(target_conn_str + target_db_name.lower(), isolation_level="AUTOCOMMIT")
    target_conn = engine.connect()

    try:
        # Create the target schema if it does not exist
        with engine.connect() as connection:
            result = connection.execute(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{target_schema.lower()}'")
            schema_exists = result.scalar()

        if not schema_exists:
            with engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT").execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
        try:
            # Determine the last record in the target table based on the order_column
            with engine.connect() as connection:
                last_record_query = f"SELECT {order_column} FROM {target_schema}.{tgt_table} ORDER BY {order_column} DESC LIMIT 1"
                last_record_result = connection.execute(last_record_query)
                last_record_value = last_record_result.scalar()
            # df = pd.read_sql(query, source_conn)
        except Exception as e:
            logging.error(f"Error during data transfer: {e}")

        offset = 0
        while True:
            # Fetch data from source in batches
            if last_record_value:
                # Fetch data from source where order_column is greater than the last record value
                query = f"SELECT * FROM {src_table} WHERE {order_column} > {last_record_value} ORDER BY {order_column} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            else:
                query = f"SELECT * FROM {src_table} ORDER BY {order_column} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            # batch_size} ROWS ONLY"
            df = pd.read_sql(query, source_conn)

            # Break the loop if no more rows
            if df.empty:
                break

            # Write data to the target database
            df.to_sql(tgt_table, target_conn, schema=target_schema, index=False, if_exists='append')

            offset += batch_size

    except Exception as e:
        logging.error(f"Error during data transfer: {e}")

    finally:
        source_conn.close()
        target_conn.close()


# Function to perform data quality check
def data_quality_check(server, database, username, password):
    # Define a list of queries to check data quality
    queries = [
        "SELECT COUNT(*) FROM your_table",  # Check if the table has rows
        # Add more queries as needed
    ]

    # Execute queries and check results
    for query in queries:
        with pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}') as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                if not result or result[0] == 0:
                    raise ValueError(f"Data quality check failed for query: {query}")


# Function to monitor data quality
def monitor_data_quality(src_server, src_database, src_username, src_password, src_table_name, src_order_column ):
    # Create a connection to SQL Server
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={src_server};DATABASE={src_database};UID={src_username};PWD={src_password}'
    conn = pyodbc.connect(conn_str)

    try:
        # Check Characteristics: Count of rows in the 'film' table
        characteristics_query = f"SELECT COUNT(*) FROM {src_table_name}"
        with conn.cursor() as cursor:
            cursor.execute(characteristics_query)
            result = cursor.fetchone()
            logging.info(f"Characteristics Check: {result[0]} rows found in the '{src_table_name}' table.")

        # Check Uniqueness: Ensure unique film titles
        uniqueness_query = f"SELECT title, COUNT(*) FROM {src_table_name} GROUP BY title HAVING COUNT(*) > 1"
        uniqueness_df = pd.read_sql(uniqueness_query, conn)
        if not uniqueness_df.empty:
            logging.warning(f"Uniqueness Check: Duplicate {src_table_name} titles found:\n{uniqueness_df}")

        # Check Integrity: Ensure all films have a corresponding entry in 'film_category' table
        integrity_query = (f"SELECT f.film_id FROM {src_table_name} f LEFT JOIN film_category fc ON f.film_id = "
                           f"fc.film_id WHERE fc.film_id IS NULL")
        integrity_df = pd.read_sql(integrity_query, conn)
        if not integrity_df.empty:
            logging.error(
                f"Integrity Check: Films without corresponding entries in 'film_category' found:\n{integrity_df}")

        # Check Consistency: Ensure the same actor does not act in the same film twice
        consistency_query = ("SELECT actor_id, film_id, COUNT(*) FROM film_actor GROUP BY actor_id, film_id HAVING "
                             "COUNT(*) > 1")
        consistency_df = pd.read_sql(consistency_query, conn)
        if not consistency_df.empty:
            logging.error(f"Consistency Check: Actors appearing more than once in the same film:\n{consistency_df}")

    except Exception as e:
        logging.error(f"Error during data quality monitoring: {e}")

    finally:
        conn.close()


def create_dag(dag_id, schedule, task_id, description, start_date, default_args, target_schema, source_schema,
               tables, primary_id):
    if schedule is "None":
        schedule = None
    dag = DAG(
        schedule_interval=schedule,
        default_args=default_args,
        description=description,
        dag_id=dag_id,
        start_date=start_date,
        catchup=False,
        max_active_runs=1,
        template_searchpath=['/opt/airflow/dags']
    )
    with dag:
        for table_name in tables:
            source_conn_str = source_database_connection_string = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={SOURCE_DATABASE_SERVER};"
                f"DATABASE={SOURCE_DATABASE_NAME};"
                f"UID={SOURCE_DATABASE_USER};"
                f"PWD={SOURCE_DATABASE_PASSWORD}"
            )

            target_conn_str = (
                F"postgresql://{TARGET_DATABASE_USER}:{TARGET_DATABASE_PASSWORD}@{TARGET_DATABASE_SERVER}:5432/"
            )

            # Task to copy data
            copy_data_task = PythonOperator(
                task_id=f'copy_data_task_{table_name}',
                python_callable=copy_data,
                op_args=[
                    source_conn_str,
                    target_conn_str,
                    table_name,
                    f"raw_{table_name}",
                    TARGET_DATABASE_NAME,
                    target_schema,
                    primary_id[table_name]["unique_key_id"][0],

                ],
                dag=dag,
            )

            # # Task to perform data quality check
            # data_quality_check_task = PythonOperator(
            #     task_id='data_quality_check_task',
            #     python_callable=target_db,
            #     op_args=[DATABASE_SERVER, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD],
            #     dag=dag,
            # )
            #
            # # Task to perform data quality check using DataQualityOperator (replace the queries)
            # data_quality_check_operator_task = DataQualityOperator(
            #     task_id='data_quality_check_operator_task',
            #     sql=[
            #         {"check_sql": "SELECT COUNT(*) FROM your_table", "expected_result": 1},
            #     ],
            #     dag=dag,
            # )
            #
            # Task to monitor data quality
            # monitor_data_quality_task = PythonOperator(
            #     task_id='monitor_data_quality_task',
            #     python_callable=monitor_data_quality,
            #     op_args=[DATABASE_SERVER, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD],
            #     dag=dag,
            # )
            #
            # # Set task dependencies
            # copy_data_task >> [data_quality_check_task, data_quality_check_operator_task] >> monitor_data_quality_task


task_id = "test_dwh_orc_v2_copy"

default_args = {
    'depends_on_past': False,
    'email': ['samuelolle@yahoo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=16),
}
# schedule_interval = "0 0-16 * * 1-5"  # timedelta(minutes=60)
start_date = datetime(2023, 12, 26)

with open("dags/docker_job/yaml_files/data_sources.yml") as file_loc:
    source_table = yaml.load_all(file_loc, Loader=yaml.FullLoader)
    for tbl in source_table:
        for key, val in tbl.items():
            print(key, " -> ", val['table_name'])
            dag_id = f'Job-{key}-orc-copy-v2'
            description = f'A {key} DAG '

            create_dag(
                dag_id=dag_id,
                schedule=val['schedule'],
                task_id=task_id,
                description=description,
                start_date=start_date,
                default_args=default_args,
                target_schema=val["target_schema"],
                source_schema=val['source_schema'],
                tables=val['table_name'],
                primary_id=tbl[key]["table_name"],
            )
