from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta, time, timezone
import logging
import sys
import os
from dotenv import load_dotenv
import json
import clickhouse_connect
import boto3
import requests


default_args = {
    # Start download at 2 am NY time
    'start_date': datetime.combine((datetime.now() - timedelta(days=1)).date(), time(7, 0), tzinfo=timezone.utc),
    'owner': 'dgorenoy'
}

# Set logging options
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# Path to current file
DAG_FOLDER = os.path.dirname(__file__)
# Path to config file
CONFIG_PATH = os.path.join(DAG_FOLDER, 'conf', 'config.json')

# Load environment variables
load_dotenv(dotenv_path='./conf/.env', override=True)

# Load settings
with open(CONFIG_PATH, 'r',encoding='utf-8') as f:
    CONFIG = json.load(f)


def get_downloaded_files(file_name=None):
    """
    Get list of already downloaded files from database
    (may be different from list of files in the bucket)
    If file_name parameter is present - returns only corresponding records
    """

    sql = f"""
        SELECT file_name
            FROM {CONFIG['clickhouse']['dbs']['staging_db']}.bronze_files
    """
    
    # If file_name is present - add filter by file name
    if file_name:
        sql += f" WHERE file_name = '{file_name}'"

    try:
        ch_client = clickhouse_connect.get_client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            username=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['pass']
        )

        result = ch_client.query(sql).result_rows
        files_list = [r[0] for r in result]

        return files_list
        
    except Exception as e:
        logging.error(f'Error getting downloadded files list: {e}')   
        raise AirflowException(f'ERROR')


def get_taxi_types():
    """
    Get ID and file prefix of taxi company from table taxi_type_details in silver layer
    """
    sql = f"""
        SELECT taxi_type_id, file_prefix
            FROM {CONFIG['clickhouse']['dbs']['silver_db']}.taxi_type_details
    """

    try:
        ch_client = clickhouse_connect.get_client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            username=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['pass']
        )

        result = ch_client.query(sql)
        taxi_types = tuple(result.named_results())

        return taxi_types
    except Exception as e:
        logging.error(f'Error getting taxi types from database {CONFIG['clickhouse']['dbs']['silver_db']}')
        raise AirflowException(f'ERROR')
    

def download_file(file_url):
    """
    Downloads file from given URL and saves to storage
    """
    try:
        headers = {
            'Referer': 'https://www.nyc.gov/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            #'Accept-encoding': 'gzip, deflate, br, zstd',
        }
        
        with requests.get(file_url, headers=headers, stream=True) as file:
            # If status is not 2xx - raise error
            file.raise_for_status()

            # Extract file name from URL
            file_name = file_url.split('/')[-1]

            try:
                s3 = boto3.client(
                    's3',
                    endpoint_url=CONFIG['storage']['path'],
                    aws_access_key_id=CONFIG['storage']['user'],
                    aws_secret_access_key=CONFIG['storage']['pass']
                )
                s3.upload_fileobj(file.raw, CONFIG['storage']['bucket'], file_name)
                logging.info(f"File {file_name} uploaded to storage {CONFIG['storage']['path']}{CONFIG['storage']['bucket']}")

                return True
            except Exception as ee:
                    logging.error(f"Error saving file {file_name} to storage {storage['path']}{storage['bucket']}. Error: {ee}")
                    raise AirflowException(f'ERROR')

    except Exception as e:
        logging.error(f'Error downloading file {file_url}: {e}')
        raise AirflowException(f'ERROR')
    
    return False


def write_file_info_to_db(file_name, file_url, id='NULL'):
    """
    Writes or updates information on downloaded file into staging database table
    Table bronze_files uses ReplacingMergeTree, so no need to check existance of the record
    """
    
    sql = f"""
        INSERT INTO {CONFIG['clickhouse']['dbs']['staging_db']}.bronze_files (file_name, source_url, taxi_type_id)
            VALUES ('{file_name}', '{file_url}', {id})
    """

    try:
        ch_client = clickhouse_connect.get_client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            username=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['pass']
        )

        ch_client.command(sql)
        logging.info(f'File {file_name} information saved to database {CONFIG['clickhouse']['dbs']['staging_db']}')
    except Exception as e:
        logging.error(f'Error while saving file {file_name} information to database {CONFIG['clickhouse']['dbs']['staging_db']}: {e}')
        raise AirflowException(f'ERROR')


def download_raw_data_file():
    """
    Download missing data files for chosen taxi types from NYC TLC site
    """

    # Download raw data file for each month from the last upload
    start_date = datetime.strptime('2017-01-01', '%Y-%m-%d')
    start_year = int(start_date.year)
    start_month = int(start_date.month)

    end_date = datetime.now()
    end_year = int(end_date.year)
    end_month = int(end_date.month)

    # Get the list of already downloaded files
    # Checking all files every time the DAG runs allows to download them in any order
    # and not rely on specific order (as with getting maximum date drom DB etc)
    # Keeping information in the table instead of checking existance in the bucket
    # allows to drop older processed files without them being downloaded again
    downloaded_files_list = get_downloaded_files()

    taxi_types = get_taxi_types()

    # TODO
    tmp_counter = 0

    # Construct a raw data file name for each month between start_date and end_date and download it
    for year in range(start_year, end_year + 1):
        # Set month boundary for the month iteration
        # If the last year in range - stop iterating at end_month
        if year == end_year:
            stop_month = end_month
        # If not the last year - iterate to December
        else:
            stop_month = 12

        # Get data file for each month
        for month in range(start_month, stop_month + 1):
            # Get data file for each taxi type (yellow, green etc)
            for taxi in taxi_types:
                raw_data_file_name = f"{taxi['file_prefix']}_tripdata_{year}-{month:02d}.parquet"

                # If that file has already been downloaded - get the next one
                if raw_data_file_name in downloaded_files_list:
                    continue;

                # Make full URL to data file
                raw_data_url = CONFIG['raw_data_base_link'] + raw_data_file_name
                logging.info(f'Downloading file {raw_data_url}')

                if download_file(raw_data_url):
                    write_file_info_to_db(raw_data_file_name, raw_data_url, taxi['taxi_type_id'])

                # TODO
                tmp_counter += 1

            # TODO
            if tmp_counter > 3:
                break;
        # TODO
        break;

        # For all years except start_year start_month = 1
        start_month = 1


with DAG(
    dag_id='nyc_taxi_download_raw',
    schedule='@weekly',
    catchup=False,
    max_active_runs=1,
    default_args=default_args
) as dag:
    
    download_raw_data_file = PythonOperator(
        task_id = 'download_raw_data_file',
        python_callable = download_raw_data_file
    )

    download_raw_data_file