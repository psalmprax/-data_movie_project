import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import pyodbc
import yaml
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from sqlalchemy import create_engine
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtDocsGenerateOperator, DbtTestOperator

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


def monitor_data_quality(
        src_server,
        src_database,
        src_username,
        src_password,
        src_table_name,
        src_order_column,
):
    # Create a connection to SQL Server
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={src_server};DATABASE={src_database};UID={src_username};PWD={src_password}'
    conn = pyodbc.connect(conn_str)

    try:
        # Check Characteristics: Count of rows in the source table
        characteristics_query = f"SELECT COUNT(*) FROM {src_table_name}"
        with conn.cursor() as cursor:
            cursor.execute(characteristics_query)
            result = cursor.fetchone()
            logging.info(f"Characteristics Check: {result[0]} rows found in the '{src_table_name}' table.")

        # Check Uniqueness: Ensure unique values in a specific column (e.g., title)
        uniqueness_query = f"SELECT {src_order_column}, COUNT(*) FROM {src_table_name} GROUP BY {src_order_column} HAVING COUNT(*) > 1"
        uniqueness_df = pd.read_sql(uniqueness_query, conn)
        if not uniqueness_df.empty:
            logging.warning(f"Uniqueness Check: Duplicate {src_order_column} values found:\n{uniqueness_df}")

    except Exception as e:
        logging.error(f"Error during data quality monitoring: {e}")

    finally:
        conn.close()

def perform_advanced_data_quality_check(
        source_conn_str,
        table,
        cross_chk_table,
        check_type,
        columns=None,
        cross_chk_column=None,
        check_condition=None
):
    if columns is None:
        columns = list()
    column = columns
    source_conn = pyodbc.connect(source_conn_str)
    source_cursor = source_conn.cursor()
    print(columns)
    print(cross_chk_column)
    try:
        query = ""

        if 'uniqueness' in check_type:
            query = f"SELECT {column}, COUNT(*) FROM {table} GROUP BY {column} HAVING COUNT(*) > 1;"

        elif 'integrity' in check_type:
            # Adjust the query based on the specific integrity checks required
            if table == 'actor':
                # Example: Check if actor references in the film_actor table are valid
                query = f"SELECT {column} FROM {table} WHERE {column} NOT IN (SELECT {column} FROM {cross_chk_table});" # film_actor
            elif table == 'film':
                # Example: Cross-reference language IDs with the language table
                query = f"SELECT {columns} FROM {table} WHERE {cross_chk_column[0]} NOT IN (SELECT {cross_chk_column[0]} FROM {cross_chk_table[0]});"
                print(query)
            # ... Other integrity checks ...

        elif 'consistency' in check_type:
            # Example: Verify consistent naming conventions in category table
            if table == 'category':
                query = f"SELECT {column}, COUNT(*) FROM {table} GROUP BY {column} HAVING COUNT(DISTINCT {column}) != COUNT({column});"

        elif 'characteristics' in check_type:
            # Validate data types in table
            query = f"SELECT {column} FROM {table} WHERE {check_condition};"

        # Execute the query if it's set
        if query:
            source_cursor.execute(query)
            results = source_cursor.fetchall()

            if results:
                raise ValueError(f"Data quality issues found in {table} for check {check_type}: {results}")
            else:
                print(f"No data quality issues found in {table} for check {check_type}.")
        else:
            print(f"No query defined for check type: {check_type} on table: {table}")

    except Exception as e:
        print(f"Error during data quality check on {table}: {e}")
    finally:
        source_cursor.close()


def create_database_if_not_exists(engine, target_db_name):
    try:
        with engine.connect() as connection:
            result = connection.execute(f"SELECT 1 FROM pg_database WHERE datname = '{target_db_name.lower()}'")
            database_exists = result.scalar()

        if not database_exists:
            with engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT").execute(f"CREATE DATABASE {target_db_name}")
    except Exception as e:
        logging.error(f"Error creating database: {e}")
        raise


def create_schema_if_not_exists(engine, target_schema):
    try:
        with engine.connect() as connection:
            result = connection.execute(
                f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{target_schema.lower()}'")
            schema_exists = result.scalar()

        if not schema_exists:
            with engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT").execute(
                    f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    except Exception as e:
        logging.error(f"Error creating schema: {e}")
        raise


def get_last_record_value(engine, target_schema, tgt_table, order_column):
    try:
        with engine.connect() as connection:
            last_record_query = f"SELECT {order_column} FROM {target_schema}.{tgt_table} ORDER BY {order_column} DESC LIMIT 1"
            last_record_result = connection.execute(last_record_query)
            return last_record_result.scalar()
    except Exception as e:
        logging.error(f"Error getting last record value: {e}")
        raise


def fetch_data_from_source(source_conn, src_table, order_column, last_record_value, offset, batch_size):
    if last_record_value:
        query = f"SELECT * FROM {src_table} WHERE {order_column} > {last_record_value} ORDER BY {order_column} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
    else:
        query = f"SELECT * FROM {src_table} ORDER BY {order_column} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
    return pd.read_sql(query, source_conn)


def write_data_to_target(df, target_conn, target_schema, tgt_table):
    df.to_sql(tgt_table, target_conn, schema=target_schema, index=False, if_exists='append')


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

    source_conn = pyodbc.connect(source_conn_str)
    engine = create_engine(target_conn_str + target_db_name.lower(), isolation_level="AUTOCOMMIT")
    target_conn = engine.connect()

    try:
        create_database_if_not_exists(engine, target_db_name)
        create_schema_if_not_exists(engine, target_schema)
        try:
            last_record_value = get_last_record_value(engine, target_schema, tgt_table, order_column)
        except Exception as e:
            logging.error(f"Information during data transfer: {e}")

        offset = 0
        while True:
            df = fetch_data_from_source(source_conn, src_table, order_column, last_record_value, offset, batch_size)

            if df.empty:
                break

            write_data_to_target(df, target_conn, target_schema, tgt_table)

            offset += batch_size

    except Exception as e:
        logging.error(f"Error during data transfer: {e}")

    finally:
        source_conn.close()
        target_conn.close()


def create_dag(dag_id, schedule, task_id, description, start_date, default_args, target_schema, source_schema,
               tables, table_metadata):
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

            # Define the checks
            data_quality_checks = [
                {
                    'type': 'Integrity',
                    'query': f"EXEC RunCheckTableWithMsg '{table_name}';",
                    'log_level': 'error'
                },
            ]

            # Task to copy data
            copy_data_task = PythonOperator(
                task_id=f'copy_data_from_table_{table_name}_task',
                python_callable=copy_data,
                op_args=[
                    source_conn_str,
                    target_conn_str,
                    table_name,
                    f"raw_{table_name}",
                    TARGET_DATABASE_NAME,
                    target_schema,
                    table_metadata[table_name]["unique_key_id"][0],

                ],
                dag=dag,
            )

            # Define the task that runs the data quality monitoring function
            monitor_quality_task = PythonOperator(
                task_id=f'monitor_data_quality_for_table_{table_name}',
                python_callable=monitor_data_quality,
                op_kwargs={
                    # 'src_conn': source_conn_str,
                    'src_server': SOURCE_DATABASE_SERVER,
                    'src_database': SOURCE_DATABASE_NAME,
                    'src_username': SOURCE_DATABASE_USER,
                    'src_password': SOURCE_DATABASE_PASSWORD,
                    'src_table_name': table_name,
                    'src_order_column': table_metadata[table_name]["unique_key_id"][0],
                    'checks': data_quality_checks,
                },
                dag=dag,
            )

            monitor_quality_check_task = PythonOperator(
                task_id=f'monitor_quality_check_{table_name}_task',
                python_callable=perform_advanced_data_quality_check,
                op_kwargs={
                    'source_conn_str': source_conn_str,
                    'table': table_name,
                    'cross_chk_table': table_metadata[table_name]["cross_chk_table"],
                    'check_type': table_metadata[table_name]["check_type"],
                    'columns': table_metadata[table_name]["columns"][0],
                    'cross_chk_column': table_metadata[table_name]["cross_chk_column"],
                    'check_condition': table_metadata[table_name]["check_condition"][0]
                },
                dag=dag,
            )

            # # Set task dependencies
            var = copy_data_task >> monitor_quality_task >> monitor_quality_check_task


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

start_date = datetime(2023, 12, 26)

with open("dags/docker_job/yaml_files/data_sources.yml") as file_loc:
    source_table = yaml.load_all(file_loc, Loader=yaml.FullLoader)
    for tbl in source_table:
        for key, val in tbl.items():
            print(key, " -> ", val['table_name'])
            dag_id = f'Job-{key}-orc-v2'
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
                table_metadata=tbl[key]["table_name"],
            )
