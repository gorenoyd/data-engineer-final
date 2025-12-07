from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging
import sys
import json
from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
import clickhouse_connect
import requests
from io import BytesIO
import pandas as pd


default_args = {
    'start_date': datetime.now() - timedelta(minutes=5),
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

# Load config
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)


def check_storage():
    """
    Checks if storage exists and bucket is accessible with provided credentials.
    Tries to create bucket if it doesn't exist
    """
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=CONFIG['storage']['path'],
            aws_access_key_id=CONFIG['storage']['user'],
            aws_secret_access_key=CONFIG['storage']['pass']
        )

        # Check if bucket is present and accessible
        s3.head_bucket(Bucket=CONFIG['storage']['bucket'])
        logging.info(f"Bucket {CONFIG['storage']['bucket']} is accessible.")

    except EndpointConnectionError as e:
        logging.error(f'Cannot connect to endpoint: {e}')
        raise AirflowException(f'ERROR')
    
    except ClientError as e:
        logging.error(f'S3 error: {e}')

        # If bucket doesn't exist - try to create
        if int(e.response['Error']['Code']) == 404:
            logging.error(f"Bucket {CONFIG['storage']['bucket']} not found — creating")

            try:
                s3.create_bucket(Bucket=CONFIG['storage']['bucket'])
                logging.info(f"Bucket {CONFIG['storage']['bucket']} created.")
            except Exception as ee:
                logging.error(f"Cannot create bucket {CONFIG['storage']['bucket']}. Error: {ee}")
                raise AirflowException(f'ERROR')

    except Exception as e:
        logging.error(f'Error checking storage: {e}')
        raise AirflowException(f'ERROR')


def check_clickhouse():
    """
    Check if Clickhouse server is accessible
    """
    # Try to connect to ClickHouse instance
    try:
        clickhouse_connect.get_client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            username=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['pass']
        )
        logging.info(f'Connected to Clickhouse successfully.')
    except Exception as e:
        logging.error(f'Error connecting to Clickhouse: {e}')
        raise AirflowException(f'ERROR')


def check_db_existance(db_name):
    """
    Checks if database with db_name exists
    """
    sql = f"""
        SELECT COUNT(*)
            FROM system.databases
        WHERE name='{db_name}'
    """

    try:
        ch_client = clickhouse_connect.get_client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            username=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['pass']
        )

        result = bool(ch_client.query(sql).result_rows[0][0])
        logging.info(f'Database {db_name} exists: {result}')

        return result
    except Exception as e:
        logging.error(f'Error checking database: {e}')
        raise AirflowException(f'ERROR')

    return False


def create_db(db_name):
    """
    Creates database with name db_name
    """
    sql= f"""
        CREATE DATABASE iF NOT EXISTS {db_name}
    """
    
    try:
        ch_client = clickhouse_connect.get_client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            username=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['pass']
        )

        ch_client.command(sql)
        logging.info(f'Database {db_name} created successfully')
    except Exception as e:
        logging.error(f'Error creating database: {e}')
        raise AirflowException(f'ERROR')

        return False

    # Recheck db existance
    return check_db_existance(db_name)


def check_db_schema(db):
    """
    Creates neccessary tables in database db_name, if they don't exist
    Runs scripts to populate some tables
    """
    db_name = CONFIG['clickhouse']['dbs'][db]
    logging.info(f'Checking database {db_name} schema')

    # Make a list of SQL scripts to run
    if db == 'staging_db':
        sql_scripts = ['staging.sql']
    elif db == 'silver_db':
        sql_scripts = [
            'silver.sql',
            'populate_vendors.sql',
            'populate_rates.sql',
            'populate_payment_types.sql',
            'populate_trip_types.sql',
            'populate_taxi_types.sql'
        ]
    elif db == 'golden_db':
        sql_scripts = [
            'gold.sql',
            'populate_payment_gold.sql',
            'populate_distances.sql',
            'populate_durations.sql',
            'mv_top_zones.sql',
            'mv_rides_by_hour.sql',
            'mv_weekdays_by_hour.sql',
            'mv_weekends_by_hour.sql',
            'mv_trip_categories.sql',
            'mv_trip_vectors.sql'
        ]
    else:
        logging.error(f'Wrong database name!')
        return False
    
    # Select database
    try:
        ch_client = clickhouse_connect.get_client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            username=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['pass']
        )

        ch_client.command(f'USE {db_name};')
    except Exception as e:
        logging.error(f'Error selecting database {db_name}: {e}')
        raise AirflowException(f'ERROR')
    
    logging.info(f"Creating tables for database {db_name}, if they don't exist")

    # For each script load it from file and execute
    for script in sql_scripts:
        with open(f"{DAG_FOLDER}/{CONFIG['sql_scripts_folder']}/{script}", 'r', encoding='utf-8') as file:
            sql = file.read()

        # Get individual commands from script
        for cmd in sql.split(';'):
            cmd = cmd.strip()
            if cmd:
                try:
                    # Execute command
                    ch_client.command(cmd)
                except Exception as e:
                    logging.error(f'Error creating schema for database {db_name}: {e}')
                    raise AirflowException(f'ERROR')


def check_schema():
    """
    Check existance of each neccessary database and it's schema
    """
    # Check each database from config.json
    for db in CONFIG['clickhouse']['dbs']:
        # If database doesn't exist - try to create database
        db_name = CONFIG['clickhouse']['dbs'][db]

        if not check_db_existance(db_name):
            logging.info(f'Database {db_name} not found, trying to create.')

            if not create_db(db_name):
                logging.error(f'Cannot create database {db_name}!')

                return False

        # # Check database schema
        check_db_schema(db)


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


def download_taxi_zones():
    # Download lookup table if it is not downloaded already
    if not len(get_downloaded_files(CONFIG['lookup_table_file_name'])):
        try:
            response = requests.get(CONFIG['lookup_table_url'])
            # Raise error if response code is not not 2xx
            response.raise_for_status()

            # Save file to storage
            s3 = boto3.client(
                's3',
                endpoint_url=CONFIG['storage']['path'],
                aws_access_key_id=CONFIG['storage']['user'],
                aws_secret_access_key=CONFIG['storage']['pass']
            )

            s3.upload_fileobj(BytesIO(response.content), CONFIG['storage']['bucket'], CONFIG['lookup_table_file_name'])
            logging.info(f"File {CONFIG['lookup_table_file_name']} uploaded to storage {CONFIG['storage']['path']}{CONFIG['storage']['bucket']}")
            
            # Add information about file to table bronze_files
            write_file_info_to_db(CONFIG['lookup_table_file_name'], CONFIG['lookup_table_url'])

        except Exception as e:
            logging.error(f'Error downloading lookup table {CONFIG['lookup_table_url']}: {e}')
            raise AirflowException(f'ERROR')
    else:
        logging.info(f'Taxi zones lookup table is already downloaded')


def mark_file_processed(file_name):
    """
    Set field 'processed' in table bronze_files as True for given file
    The table uses ReplacingMergeTree engine, so we use INSERT to update
    """

    sql = f"""
        ALTER TABLE {CONFIG['clickhouse']['dbs']['staging_db']}.bronze_files
        UPDATE processed = True,
            processed_dt = now()
        WHERE file_name = '{file_name}';
    """

    try:
        ch_client = clickhouse_connect.get_client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            username=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['pass']
        )
        ch_client.command(sql)
        logging.info(f'File {file_name} marked as processed.')
    except Exception as e:
        logging.error(f'Error marking file {file_name} as processed: {e}')
        raise AirflowException(f'ERROR')


def save_taxi_zones_to_db():
    """
    Insert the data from file with taxi zones lookup table
    into silver and gold layers
    """
    logging.info(f"Inserting data from file {CONFIG['lookup_table_file_name']} into databases.")

    try:
        # Read the lookup table
        s3 = boto3.client(
            's3',
            endpoint_url=CONFIG['storage']['path'],
            aws_access_key_id=CONFIG['storage']['user'],
            aws_secret_access_key=CONFIG['storage']['pass']
        )

        file = s3.get_object(Bucket=CONFIG['storage']['bucket'], Key=CONFIG['lookup_table_file_name'])

        # Make panadas dataframe from csv for further processing
        csv_data = file['Body'].read()
        df = pd.read_csv(BytesIO(csv_data))

        # Drop duplicates
        df = df.drop_duplicates(subset=['LocationID'])
        # Add 'source' field
        df['source'] = CONFIG['lookup_table_url']
        # Cast data types just in case
        df['LocationID'] = df['LocationID'].astype(int)
        df['Borough'] = df['Borough'].astype(str)
        df['Zone'] = df['Zone'].astype(str)
        df['service_zone'] = df['service_zone'].astype(str)
        # Add boolean field "is_airport"
        df['is_airport'] = df['Zone'].str.contains('airport', case=False, na=False)

        # Write data into silver and gold databases
        for db in [CONFIG['clickhouse']['dbs']['silver_db'], CONFIG['clickhouse']['dbs']['golden_db']]:
            # Check existing taxi zone IDs in databases
            if db == CONFIG['clickhouse']['dbs']['silver_db']:
                table_name = 'taxi_zone_details'
                id_col = 'taxi_zone_id'
            else:
                table_name = 'dim_taxi_zone'
                id_col = 'taxi_zone_id'

            sql = f'SELECT {id_col} FROM {db}.{table_name}'

            ch_client = clickhouse_connect.get_client(
                host=CONFIG['clickhouse']['host'],
                port=CONFIG['clickhouse']['port'],
                username=CONFIG['clickhouse']['user'],
                password=CONFIG['clickhouse']['pass']
            )
            result = ch_client.query(sql)
            existing_ids = set(row[0] for row in result.result_rows)

            # In new dataframe leave only unique rows
            new_rows = df[~df['LocationID'].isin(existing_ids)]

            # If new rows are present - write them into tables
            if not new_rows.empty:
                # Insert new rows
                # For silver layer
                if db == CONFIG['clickhouse']['dbs']['silver_db']:
                    data_to_insert = [tuple(x) for x in new_rows[['LocationID', 'Borough', 'Zone', 'service_zone', 'source']].to_numpy()]
                    ch_client.insert(f'{db}.{table_name}', data_to_insert, column_names=[id_col, 'borough', 'zone', 'service_zone', 'record_source'])

                # For golden layer
                else:
                    data_to_insert = [tuple(x) for x in new_rows[['LocationID', 'Borough', 'Zone', 'is_airport', 'source']].to_numpy()]
                    ch_client.insert(f'{db}.{table_name}', data_to_insert, column_names=[id_col, 'borough', 'zone', 'is_airport', 'record_source'])

                logging.info(f'Taxi zones uploaded into database {db}')
            else:
                logging.info(f'There are no new records in lookup table for database {db}')

        # Mark lookup file as processed
        mark_file_processed(CONFIG['lookup_table_file_name'])

    except Exception as e:
        logging.error(f'Error loading taxi zones data into database {db}: {e}')
        raise AirflowException(f'ERROR')


with DAG(
    dag_id='nyc_taxi_initialize',
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:
    
    check_storage = PythonOperator(
        task_id='check_storage',
        python_callable=check_storage
    )

    check_clickhouse = PythonOperator(
        task_id='check_clickhouse',
        python_callable=check_clickhouse
    )

    check_schema = PythonOperator(
        task_id='check_schema',
        python_callable=check_schema
    )

    download_taxi_zones = PythonOperator(
        task_id='download_taxi_zones',
        python_callable=download_taxi_zones
    )

    save_taxi_zones_to_db = PythonOperator(
        task_id='save_taxi_zones_to_db',
        python_callable=save_taxi_zones_to_db
    )

    check_storage >> check_clickhouse >> check_schema >> download_taxi_zones >> save_taxi_zones_to_db
