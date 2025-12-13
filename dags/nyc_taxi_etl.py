from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# TODO
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta, time
import logging
from dotenv import load_dotenv
import os
import sys
import json
import clickhouse_connect
import socket


default_args = {
    'owner': 'dgorenoy',
    # Start at 1 am
    'start_date': datetime.combine((datetime.now() - timedelta(days=1)).date(), time(1, 0)),
    'retries': 0
}


# Set logging options
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# Path to current file
DAG_FOLDER = os.path.dirname(__file__)
# Path to config file
CONFIG_PATH = os.path.join(DAG_FOLDER, 'conf', 'config.json')

#Load environmental variables
load_dotenv(dotenv_path=f'{DAG_FOLDER}/conf/.env', override=True)

# Load config file
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)


def create_dataproc():
    """
    Create Yandex Dataproc cluster
    """
    logging.info(f'Creating Yandex DataProc cluster')


def delete_dataproc():
    """
    Delete Yandex Dataproc cluster
    """
    logging.info(f'Deleting Yandex Dataproc Cluster')


def check_spark_master():
    """
    Checks if Spark master is accessible
    """
    try:
        with socket.create_connection((CONFIG['spark']['master']['host'], CONFIG['spark']['master']['port']), timeout=3):
            logging.info(f"Spark master {CONFIG['spark']['master']['host']} доступен")
            return True
    except Exception as e:
        logging.error(f"Spark master {CONFIG['spark']['master']['host']} НЕДОСТУПЕН: {e}")
        raise AirflowException(f'ERROR')
    

def get_unprocessed_files_list():
    """
    Returns list of downloaded, but unprocessed raw data files
    """
    sql = f"""
        SELECT file_name, taxi_type_id
            FROM {CONFIG['clickhouse']['dbs']['staging_db']}.bronze_files
        WHERE processed = 0
            AND taxi_type_id IS NOT NULL
    """

    try:
        ch_client = clickhouse_connect.get_client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            username=os.getenv('CLICKHOUSE_USER'),
            password=os.getenv('CLICKHOUSE_PASS')
        )

        result = ch_client.query(sql)
        files_list = [dict(row) for row in result.named_results()]

        return files_list

    except Exception as e:
        logging.error(f'Error getting unprocessed files list from database {CONFIG['clickhouse']['dbs']['staging_db']}: {e}')
        raise AirflowException(f'ERROR')


with DAG(
    dag_id='nyc_taxi_etl',
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    # Get list of unprocessed files
    unprocessed_files_list = get_unprocessed_files_list()

    # If there are unprocessed files, create DAG tasks to process them
    if unprocessed_files_list:
        check_spark_master = PythonOperator(
            task_id='check_spark_master',
            python_callable=check_spark_master
        )

        create_dataproc = PythonOperator(
            task_id='create_dataproc',
            python_callable=create_dataproc
        )

        delete_dataproc = PythonOperator(
            task_id='delete_dataproc',
            python_callable=delete_dataproc
        )


        # Process data files
        process_bronze = SparkSubmitOperator(
            task_id=f'process_bronze',
            application='/opt/spark/spark_jobs/process_bronze.py',
            application_args=[json.dumps(CONFIG), json.dumps(unprocessed_files_list), os.getenv('CLICKHOUSE_USER'), os.getenv('CLICKHOUSE_PASS')],
            conn_id='spark_default',
            name=f'NYC_ETL',
            conf={
                'spark.jars': '/opt/spark/jars_ex/*.jar',
                'spark.driver.extraJavaOptions': '-Dlog4j.configuration=file:/opt/airflow/dags/conf/log4j.properties',
                'spark.executor.extraJavaOptions': '-Dlog4j.configuration=file:/opt/airflow/dags/conf/log4j.properties',

                'spark.hadoop.fs.s3a.access.key': os.getenv('STORAGE_USER'),
                'spark.hadoop.fs.s3a.secret.key': os.getenv('STORAGE_PASS'),
                'spark.hadoop.fs.s3a.endpoint': CONFIG['storage']['path'],
                'spark.hadoop.fs.s3a.path.style.access': 'true',          
                'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',

                'spark.sql.session.timeZone': 'UTC'
            }
        )

        check_spark_master >> create_dataproc >> process_bronze >> delete_dataproc

    else:
        # No files to process
        log_no_files = PythonOperator(
            task_id='log_no_files',
            python_callable=lambda: logging.info('No unprocessed files found.')
        )
