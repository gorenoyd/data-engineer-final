import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim, lit, ceil, sha2, \
    concat_ws, unix_timestamp, round, to_date, year, month, dayofmonth, hour, minute, dayofweek
from pyspark.sql.types import FloatType, LongType
import sys
import json
import clickhouse_connect
import re


# Set logging level
logging.basicConfig(level=logging.WARNING)

# Read config from command line argument
CONFIG = json.loads(sys.argv[1])
unprocessed_files_list = json.loads(sys.argv[2])
taxi_type_id = None

# Get credentials
CLICKHOUSE_USER = sys.argv[3]
CLICKHOUSE_PASS = sys.argv[4]


def drop_outliers(df, column_name):
    """
    Drop values outside 1.5 IQR
    """
    # Try to calculate quantiles
    if df.filter(col(column_name).isNotNull()).count() > 0:
        quantiles = df.approxQuantile(column_name, [0.25, 0.75], 0.01)
        if len(quantiles) == 2:
            q1, q3 = quantiles
            iqr = q3 - q1

            # Calculate boundaries
            lower_boundary = q1 - 1.5 * iqr
            upper_boundary = q3 + 1.5 * iqr

            # Drop rows outside boudaries
            df_clean = df.filter((col(column_name) >= lower_boundary) & (col(column_name) <= upper_boundary))
            
            logging.warning(f'Deleted outliers for column {column_name}')

            return df_clean

    # If we cannot calculate quantiles, return original dataframe
    return df


def clean_data(df, file_name):
    """
    Cleans data in the dataframe
    """

    # Get year and month from file name
    match = re.search(r'(\d{4}-\d{2})', file_name)
    if match:
        year_month = match.group(1)
        file_year, file_month = map(int, year_month.split('-'))
    else:
        logging.error(f"Cannot get date from file name {file_name}. Skip this file.")
        return False
    
    # List of money amount columns - all can be 0 or null
    money_columns = ['fare_amount',
                    'extra',
                    'mta_tax',
                    'tip_amount',
                    'tolls_amount',
                    'improvement_surcharge',
                    'total_amount',
                    'congestion_surcharge',
                    'airport_fee',
                    'cbd_congestion_fee']

    logging.warning(f'Rows before cleaning: {df.count()}')

    # Check columns one by one
    for column in df.columns:
        # Information vendor
        if column == 'VendorID':
            # Convert columnt to int
            df = df.withColumn(column, col(column).cast('int'))
            # Drop negative and too big values
            df = df.filter(col(column).isNull() | ((col(column) > 0) & (col(column) <= 1000)))
        
        # Pickup datetime - this column is named differently for different taxis
        elif 'pickup_datetime' in column:
            # Drop rows with null values
            df = df.filter(col(column).isNotNull())
            # Convert to datetime
            df = df.withColumn(column, col(column).cast('timestamp'))
            # Rename column
            df = df.withColumnRenamed(column, 'pickup_datetime')

        # Dropoff datetime - this column is named differently for different taxis
        elif 'dropoff_datetime' in column:
            # Drop rows with null
            df = df.filter(col(column).isNotNull())
            # Convert to datetime
            df = df.withColumn(column, col(column).cast('timestamp'))
            # Rename column
            df = df.withColumnRenamed(column, 'dropoff_datetime')

        # Passenger count - can be 0 or null for voided trips etc
        elif column == 'passenger_count':
            # Convert to int
            df = df.withColumn(column, col(column).cast('int'))
            # Drop negative values and values greater than 10
            df = df.filter(col(column).isNull() | ((col(column) >= 0) & (col(column) <= 10)))


        # Trip distance - can be 0 or null for voided trips etc
        elif column == 'trip_distance':
            # Convert to float
            df = df.withColumn(column, col(column).cast(FloatType()))
            # Drop negative values and values greater than 1000 miles
            df = df.filter(col(column).isNull() | ((col(column) >= 0) & (col(column) <= 1000)))
            # Drop outliers
            df = drop_outliers(df, column)

        # Rate id
        elif column == 'RatecodeID':
            # Convert to int
            df = df.withColumn(column, col(column).cast('int'))
            # Drop negative and too big values
            df = df.filter(col(column).isNull() | ((col(column) > 0) & (col(column) <= 1000)))

        # Store and forward flag
        elif column == 'store_and_fwd_flag':
            # Convert to bool
            df = df.withColumn(column, \
                when(upper(trim(col(column))) == 'Y', 1) \
                .when(upper(trim(col(column))) == 'N', 0) \
                .otherwise(None)
                .cast('boolean'))

        # Taxi zone ID
        elif column == 'PULocationID' or column == 'DOLocationID':
            # Convert to int
            df = df.withColumn(column, col(column).cast('int'))
            # Drop rows with null and negative values and values greater than 500
            df = df.filter(col(column).isNotNull() & (col(column) > 0) & (col(column) <= 500))

        # Payment type
        elif column == 'payment_type':
            # Convert to int
            df = df.withColumn(column, col(column).cast('int'))
            # Drop negative and too big values
            df = df.filter(col(column).isNull() | ((col(column) > 0) & (col(column) <= 1000)))

        # Trip type
        elif column == 'trip_type_id':
            # Convert to int
            df = df.withColumn(column, col(column).cast('int'))
            # Drop negative and too big values
            df = df.filter(col(column).isNull() | ((col(column) > 0) & (col(column) <= 1000)))

        # All columns with money are treated the same way
        elif column in money_columns:
            # Convert to float
            df = df.withColumn(column, col(column).cast(FloatType()))
            # Drop negative values
            df = df.filter(col(column).isNull() | (col(column) >= 0))
            # Drop outliers
            df = drop_outliers(df, column)

        else:
            continue

    # Drop rows that are outside month in file name
    df = df.filter((year(col('pickup_datetime')) == file_year) & (month(col('pickup_datetime')) == file_month))

    # Drop duplicates by start date and location, end date and location and price
    df = df.dropDuplicates(['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID', 'total_amount'])

    # Add column with taxi type id
    df = df.withColumn('taxi_type_id', lit(taxi_type_id))

    # If column trip_type_id is not present, add it with value 1 (street-hail) - for yellow taxi, for example
    if 'trip_type_id' not in df.columns:
        df = df.withColumn('trip_type_id', lit(1))

    # Add column with calculated trip duration in minutes
    df = df.withColumn('trip_duration', ceil((col('dropoff_datetime').cast('long') - col('pickup_datetime').cast('long')) / 60))
    # Drop rows with negative duration and duration more than 24 hours
    df = df.filter((col('trip_duration') >= 0) & (col('trip_duration') < 1440))
    # Drop outliers for duration
    df = drop_outliers(df, 'trip_duration')
    
    logging.warning(f'Data Sample after cleaning:')
    df.show(5)
    logging.warning(f'Rows after cleaning: {df.count()}')
    
    return df


def insert_staging_into_silver(df):
    """
    Write dataframe into silver layer tables
    """
    # Set default ClickHouse connection parameters
    clickhouse_options = {
        'driver': 'com.clickhouse.jdbc.ClickHouseDriver',
        'url': f"jdbc:clickhouse://{CONFIG['clickhouse']['host']}:{CONFIG['clickhouse']['port']}/{CONFIG['clickhouse']['dbs']['silver_db']}?compress=false",
        'user': CLICKHOUSE_USER,
        'password': CLICKHOUSE_PASS,
        'batchsize': '100000',
        'isolationLevel': 'NONE',
        'rewriteBatchedStatements': 'true'
    }

    logging.warning(f'Start uploading data to database {CONFIG['clickhouse']['dbs']['silver_db']}')

    try:
        # Create hash key for trips
        df = df.withColumn(
            'trip_hashkey',
            sha2(concat_ws('|',
                col('pickup_datetime'),
                col('dropoff_datetime'),
                col('PULocationID'),
                col('DOLocationID'),
                col('total_amount')
                ),
            256))

        logging.warning(f'Data Sample with trip_hashkey:')
        df.show(5)

        # Write trip main details
        main_details = ('trip_hashkey',
                        'VendorID',
                        'pickup_datetime',
                        'dropoff_datetime',
                        'passenger_count',
                        'trip_distance',
                        'RatecodeID',
                        'store_and_fwd_flag',
                        'PULocationID',
                        'DOLocationID',
                        'payment_type',
                        'trip_type_id',
                        'taxi_type_id',
                        'fare_amount',
                        'total_amount',
                        'trip_duration',
                        'record_source')
        # Leave only columns that exist in the dataframe
        existing_columns = (c for c in main_details if c in df.columns)

        df_silver = df.select(*existing_columns)
        df_silver.write \
            .format('jdbc') \
            .option('driver', 'com.clickhouse.jdbc.ClickHouseDriver') \
            .option('url', f"jdbc:clickhouse://{CONFIG['clickhouse']['host']}:{CONFIG['clickhouse']['port']}/{CONFIG['clickhouse']['dbs']['silver_db']}?compress=false") \
            .option('dbtable', 'trip_main_details') \
            .option('user', CLICKHOUSE_USER) \
            .option('password', CLICKHOUSE_PASS) \
            .option('batchsize', '100000') \
            .option('isolationLevel', 'NONE') \
            .option('rewriteBatchedStatements', 'true') \
            .mode('append') \
            .save()

        # Write trip financial details
        fin_details = ('trip_hashkey',
                    'pickup_datetime',
                    'extra',
                    'mta_tax',
                    'tip_amount',
                    'tolls_amount',
                    'improvement_surcharge',
                    'congestion_surcharge',
                    'airport_fee',
                    'cbd_congestion_fee',
                    'record_source')
        
        # Leave only columns that exist in the dataframe
        existing_columns = (c for c in fin_details if c in df.columns)

        df_silver = df.select(*existing_columns)
        df_silver.write \
            .format('jdbc') \
            .option('driver', 'com.clickhouse.jdbc.ClickHouseDriver') \
            .option('url', f"jdbc:clickhouse://{CONFIG['clickhouse']['host']}:{CONFIG['clickhouse']['port']}/{CONFIG['clickhouse']['dbs']['silver_db']}?compress=false") \
            .option('dbtable', 'trip_fin_details') \
            .option('user', CLICKHOUSE_USER) \
            .option('password', CLICKHOUSE_PASS) \
            .option('batchsize', '100000') \
            .option('isolationLevel', 'NONE') \
            .option('rewriteBatchedStatements', 'true') \
            .mode('append') \
            .save()

        logging.warning(f"Succefully uploaded staging data to database {CONFIG['clickhouse']['dbs']['silver_db']}")

        return df
    
    except Exception as e:
        logging.error(f'Error uploading staging data to database {CONFIG['clickhouse']['dbs']['silver_db']}: {e}')
        raise


def insert_staging_into_gold(df):
    """
    Write dataframe into golden layer tables
    """
    logging.warning(f"Start uploading data to database {CONFIG['clickhouse']['dbs']['golden_db']}")

    try:
        # Leave only necessary columns that exist in dataframe
        main_details = ('trip_hashkey',
                        'pickup_datetime',
                        'dropoff_datetime',
                        'PULocationID',
                        'DOLocationID',
                        'payment_type',
                        'passenger_count',
                        'trip_distance',
                        'trip_duration',
                        'fare_amount',
                        'total_amount',
                        'record_source')
        existing_columns = (c for c in main_details if c in df.columns)
        df_gold = df.select(*existing_columns)

        # Get pickup datetime ID - in 5 minutes intervals (300 seconds)
        logging.warning('Getting pickup datetime bucket')
        df_gold = df_gold.withColumn(
            'pickup_datetime_id',
            (round(unix_timestamp(col('pickup_datetime')) / 300) * 300).cast(LongType()))

        # # Get dropoff datetime ID
        logging.warning('Getting dropoff datetime bucket')
        df_gold = df_gold.withColumn(
            'dropoff_datetime_id',
            (round(unix_timestamp(col('dropoff_datetime')) / 300) * 300).cast(LongType()))

        logging.warning('Data Sample for golden layer:')
        df_gold.show(5)

        # Write trip date and time dimension
        # Get pickup and dropoff datetime buckets ID from full dataframe
        pickup_df = df_gold.select(col('pickup_datetime_id').alias('datetime_id'))
        dropoff_df = df_gold.select(col('dropoff_datetime_id').alias('datetime_id'))

        # Unite them and leave only distinct
        dim_dt_df = pickup_df.unionByName(dropoff_df).distinct()
        # Add temporary column with type timestamp
        dim_dt_df = dim_dt_df.withColumn('ts', col('datetime_id').cast('timestamp'))
        # Add derived columns
        dim_dt_df = dim_dt_df.withColumn('date', to_date('ts')) \
            .withColumn('year', year('ts')) \
            .withColumn('month', month('ts')) \
            .withColumn('day_of_month', dayofmonth('ts')) \
            .withColumn('hour', hour('ts')) \
            .withColumn('minute', minute('ts')) \
            .withColumn('day_of_week', dayofweek('ts')) \
            .withColumn('is_weekend', (col('day_of_week') == 1) | (col('day_of_week') == 7)) \
            .withColumn('record_source', lit('Table fact_trip'))
        dim_dt_df = dim_dt_df.drop('ts', 'weekday')
        logging.warning('Data Sample for dim_datetime:')
        dim_dt_df.show(5)

        # First write new rows into dim_datetime
        dim_dt_df.write \
            .format('jdbc') \
            .option('driver', 'com.clickhouse.jdbc.ClickHouseDriver') \
            .option('url', f"jdbc:clickhouse://{CONFIG['clickhouse']['host']}:{CONFIG['clickhouse']['port']}/{CONFIG['clickhouse']['dbs']['golden_db']}?compress=false") \
            .option('dbtable', 'dim_datetime') \
            .option('user', CLICKHOUSE_USER) \
            .option('password', CLICKHOUSE_PASS) \
            .option('batchsize', '100000') \
            .option('isolationLevel', 'NONE') \
            .option('rewriteBatchedStatements', 'true') \
            .mode('append') \
            .save()
        
        # Now write trip facts into fact_trip
        df_gold.write \
            .format('jdbc') \
            .option('driver', 'com.clickhouse.jdbc.ClickHouseDriver') \
            .option('url', f"jdbc:clickhouse://{CONFIG['clickhouse']['host']}:{CONFIG['clickhouse']['port']}/{CONFIG['clickhouse']['dbs']['golden_db']}?compress=false") \
            .option('dbtable', 'fact_trip') \
            .option('user', CLICKHOUSE_USER) \
            .option('password', CLICKHOUSE_PASS) \
            .option('batchsize', '100000') \
            .option('isolationLevel', 'NONE') \
            .option('rewriteBatchedStatements', 'true') \
            .mode('append') \
            .save()

        logging.warning(f"Succefully uploaded staging data to database {CONFIG['clickhouse']['dbs']['golden_db']}")
    
    except Exception as e:
        logging.error(f"Error uploading staging data to database {CONFIG['clickhouse']['dbs']['golden_db']}: {e}")
        raise


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
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS
        )
        ch_client.command(sql)
        logging.warning(f'File {file_name} marked as processed.')
    except Exception as e:
        logging.error(f'Error marking file {file_name} as processed: {e}')
        raise


# Initialize Spark session
spark = SparkSession.builder \
    .getOrCreate()

# TODO
counter = 0
for file in unprocessed_files_list:
    try:
        # Load data file from storage
        df = spark.read.parquet(f"s3a://{CONFIG['storage']['bucket']}/{file['file_name']}")
        logging.warning(f"Got file {file['file_name']} from storage. Processing data for silver layer.")
        logging.warning(f"Data Schema: {df.schema}")
        logging.warning(f"Data Sample:")
        df.show(5)
        # Set taxi type id
        taxi_type_id = file['taxi_type_id']

        # Clean data
        df = clean_data(df, file['file_name'])
        # If file cannot be processed - get next one
        if not df:
            continue

        # Insert data into silver layer
        df = insert_staging_into_silver(df)

        # Insert data into golden layer
        insert_staging_into_gold(df)

        # Mark file as processed in staging database
        mark_file_processed(file['file_name'])

    except Exception as e:
        logging.error(f"Error processing file {file['file_name']}: {e}")
        raise

    # TODO
    counter += 1
    if counter >= 4:
        break