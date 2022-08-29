"""
main lambda function to carry out quality checks
"""
import os
import json
import logging
import boto3
import pandas as pd
import pymysql
import s3fs
import csv
pymysql.install_as_MySQLdb()
ENV = os.getenv('ENV')

def setup_logging():
    '''
    This method sets up the logger with required format
    '''
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    handler = logging.StreamHandler()
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

LOGGER = setup_logging()

def execute_query(query, connection, store_number, columns, bucket, upload_key):
    """
    Input - query:str, connection:pymysql.connect, store_number:str,
            columns:list, bucket:str, upload_path:str
    This method loads data from s3 at specified path to aurora
    in specified database and table
    """
    with connection.cursor() as cur:
        query = query.replace('__store_num__', str(store_number))
        cur.execute(query)
        data = cur.fetchall()
        final_data = pd.DataFrame(list(data), columns=columns)
        upload_path = f"s3://{bucket}/{upload_key}"
        print(upload_path)
        final_data.to_csv(upload_path, index=False)
        connection.commit()
    LOGGER.info("Query executed successfully")
    return "success"

def load_to_aurora(query, connection, bucket, upload_path, database, table):
    """
    Input - query:str, connection:pymysql.connect, bucket:str,
            upload_path:str, database:str, table:str
    This method executes the query with required parameters, saves it in local path
    and uploads the file on s3
    """
    with connection.cursor() as cur:
        query = query.replace('__s3_prefix__', f"s3://{bucket}/{upload_path}")
        query = query.replace('__database_name__', database)
        query = query.replace('__table__', table)
        cur.execute(query)
        #connection.commit()
    LOGGER.info("loaded results into table "+table+" from: " + upload_path)
    return "success"

def run_qc_query(connection, store, database, bucket_name, query_path, table_name,\
    forecast_from, forecast_to, column_name, upload_key):
    """
    Input - connection:pymysql.connect, store:int, database:str, bucket_name:str, query_path:str
    table_name:str, forecast_from:str, forecast_from:str, column_name:list, upload_key:str,    
    This method runs the forecast QC query and stores the result in a csv on S3 
    """

    query_read = open(query_path, 'r')
    query_name = query_read.read()
    query_name = query_name.replace('__forecast_database__',\
                                            database)
    query_name = query_name.replace('__forecast_table__',\
                                            table_name)
    query_name = query_name.replace('__forecast_from__',\
                                            forecast_from)
    query_name = query_name.replace('__forecast_to__',\
                                            forecast_to)
    query_read.close()
    execute_query(query_name, connection, store,\
        column_name, bucket_name,\
        upload_key.replace('__store_num__',str(store)))
    
    return "Read Successful"

def run_load_aurora(connection, bucket_name, upload_key, store, database, upload_table):
    """
    Input - connection:pymysql.connect,bucket_name:str,upload_key:str,store:int, 
    database:str, upload_table:str 
    This method takes the bucket and key of of the QC file and uploads
    it to a table in Aurora.
    """

    query_load = open('core_forecast_qc_baseline/load_data.sql', 'r')
    load_query = query_load.read()
    load_to_aurora(load_query,\
        connection, bucket_name,\
        '/'.join(upload_key.replace('__store_num__',str(store)).split('/')[:-1]),\
        database, upload_table)
    query_load.close()
    return "Data Loaded"

# def get_store_list(crawler_bucket, crawler_key):
#     """
#     Input crawler_bucket:str, crawler_key:str
#     This method takes the bucket and key of crawler file,
#     returns list of stores
#     """
#     client = boto3.client('s3')
#     csvfile = client.get_object(Bucket=crawler_bucket, Key=crawler_key)
#     csvcontent = csvfile['Body'].read().decode('utf-8').split()
#     reader = csv.DictReader(csvcontent)
#     data = [row['location_num'] for row in reader]
#     LOGGER.info("fetched store list")
#     return data

def lambda_handler(event, context):
    """
    Input - event:dictionary
    Main lambda Function to execute queries that does quality check on the data
    """
    store_list = event['store_list']
    ENV = os.getenv('ENV')

    with open('core_forecast_qc_baseline/config.json') as config_params:
        event = json.load(config_params)[ENV]

    
    LOGGER.info("Fetching credentials for rds")
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=event['secret_key'])
    credentials = json.loads(keys['SecretString'])

    LOGGER.info("Connecting to RDS")
    connection = pymysql.connect(host=event['custom_host_link'],
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=event['forecast_database'])

    connection_load_final = pymysql.connect(host=credentials['host'],
                                            user=credentials['username'],
                                            password=credentials['password'],
                                            db=event['qc_database'])
    LOGGER.info("Successfully connected to RDS")

    for store in store_list:
        LOGGER.info('Starting QC for: %s', str(store))

        run_qc_query(connection, store, event['forecast_database'], event['bucket'],\
                    'core_forecast_qc_baseline/dollartranscount_10days-15min.sql', event['15min_dollar_10days_table'],\
                    event['10day_forecast_from'], event['10day_forecast_to'], event['dollartranscount_columns'],\
                    event['15min_dollartranscount_10days_upload_key'])
        
        run_qc_query(connection, store, event['forecast_database'], event['bucket'],\
                    'core_forecast_qc_baseline/dollartranscount_10days-15min.sql', event['15min_dollar_30days_table'],\
                    event['30day_forecast_from'], event['30day_forecast_to'], event['dollartranscount_columns'],\
                    event['15min_dollartranscount_30days_upload_key'])

        LOGGER.info('Dollarsales and transcount 15 mins forecast QC done. Starting Itemcount QC')

        run_qc_query(connection, store, event['forecast_database'], event['bucket'],\
                    'core_forecast_qc_baseline/itemcount_forecast_qc.sql', event['15min_itemcount_10days_table'],\
                    event['10day_forecast_from'], event['10day_forecast_to'], event['itemcount_columns'],\
                    event['15min_itemcount_10days_upload_key'])

        run_qc_query(connection, store, event['forecast_database'], event['bucket'],\
                    'core_forecast_qc_baseline/itemcount_forecast_qc.sql', event['15min_itemcount_30days_table'],\
                    event['30day_forecast_from'], event['30day_forecast_to'], event['itemcount_columns'],\
                    event['15min_itemcount_30days_upload_key'])

        LOGGER.info('Itemcount 15 mins 10 days ahead forecast QC done')
        LOGGER.info('Loading the data')

        run_load_aurora(connection_load_final, event['bucket'], event['15min_dollartranscount_10days_upload_key'],\
            store, event['qc_database'], event['15min_dollartranscount_10days_upload_table'])

        run_load_aurora(connection_load_final, event['bucket'], event['15min_dollartranscount_30days_upload_key'],\
            store, event['qc_database'], event['15min_dollartranscount_30days_upload_table'])

        run_load_aurora(connection_load_final, event['bucket'], event['15min_itemcount_10days_upload_key'],\
            store, event['qc_database'], event['15min_itemcount_10days_upload_table'])

        run_load_aurora(connection_load_final, event['bucket'], event['15min_itemcount_30days_upload_key'],\
            store, event['qc_database'], event['15min_itemcount_30days_upload_table'])

        connection_load_final.commit()
        LOGGER.info('Data Loaded for: %s', str(store))
    
    connection.close()
    connection_load_final.close()
    LOGGER.info('Forecast QC process for all metrics completed successfully')
    return "success"