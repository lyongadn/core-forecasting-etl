"""
main lambda function to carry out quality checks for reduandancy pipeline - match actual locations with expected locations
"""
import os
import json
import logging
import boto3
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from config import Config

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

def upload_to_s3(local_path, prod_bucket, upload_path):
    """
    Input - local_path:str, prod_bucket:str, upload_path:str
    This method upload files from tmp folder to s3 at specified prod bucket and path
    """
    s_3 = boto3.client('s3')
    s_3.upload_file(local_path, prod_bucket, upload_path)
    LOGGER.info("uploaded file: " + local_path + " at : " + upload_path)
    return upload_path

def execute_query(query, connection, store_number, columns, local_path, prod_bucket, upload_key):
    """
    Input - query:str, connection:pymysql.connect, store_number:str, columns:list,
            local_path:str, prod_bucket:str, upload_path:str
    This method loads data from s3 at specified path to aurora
    in specified database and table
    """
    with connection.cursor() as cur:
        query = query.replace('__store_num__', str(store_number))
        cur.execute(query)
        data = cur.fetchall()
        final_data = pd.DataFrame(list(data), columns=columns)
        final_data.to_csv(local_path, index=False)
        upload_to_s3(local_path, prod_bucket, upload_key)
        connection.commit()
    LOGGER.info("Query executed successfully")
    return "success"

def load_to_aurora(query, connection, prod_bucket, upload_path, database, table):
    """
    Input - query:str, connection:pymysql.connect, prod_bucket:str,
            upload_path:str, database:str, table:str
    This method executes the query with required parameters, saves it in local path
    and uploads the file on s3
    """
    with connection.cursor() as cur:
        query = query.replace('__prod_bucket__', prod_bucket)
        query = query.replace('__upload_path__', upload_path)
        query = query.replace('__database_name__', database)
        query = query.replace('__table__', table)
        cur.execute(query)
    LOGGER.info("loaded results into table "+table+" from: " + upload_path)
    return "success"

def lambda_handler(event, context):
    """
    Input - event:dictionary
    Main lambda Function to execute queries that does quality check on the data
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_qc_redundancy_inference/config.json') as config_params:
        config_dict = json.load(config_params)[ENV]
        conf = Config.from_event(config_dict)

    store_list = event['store_list']
    LOGGER.info("Fetching credentials for rds")
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
    credentials = json.loads(keys['SecretString'])

    LOGGER.info("Connecting to RDS")
    connection = pymysql.connect(host=conf.get_replica_host_link(),
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf.get_database())

    connection_load_final = pymysql.connect(host=credentials['host'],
                                            user=credentials['username'],
                                            password=credentials['password'],
                                            db=conf.get_database())
    LOGGER.info("Successfully connected to RDS")

    for store in store_list:
        LOGGER.info('Starting QC for: %s', str(store))
        dollartranscount = open('core_forecast_qc_redundancy_inference/redundancy_dollartranscount.sql', 'r')
        dollartranscount_daily = dollartranscount.read()
        dollartranscount_daily = dollartranscount_daily.replace('__dollar_forecast_table__',\
                                            conf.get_daily_dollar_redundancy_table())
        dollartranscount_daily = dollartranscount_daily.replace('__transcount_forecast_table__',\
                                            conf.get_daily_transcount_redundancy_table())
        dollartranscount_daily = dollartranscount_daily.replace('__redundancy_forecast_from__',\
                                            conf.get_redundancy_forecast_from())
        dollartranscount_daily = dollartranscount_daily.replace('__redundancy_forecast_to__',\
                                            conf.get_redundancy_forecast_to())
        dollartranscount_daily = dollartranscount_daily.replace('__placeholder__',\
                                                conf.get_place_holder_daily())
        dollartranscount.close()
        execute_query(dollartranscount_daily, connection, store,\
            conf.get_dollartranscount_columns(), conf.get_dollartranscount_local_path(),\
            conf.get_prod_bucket(), conf.get_daily_dollartranscount_upload_key(str(store)))
        LOGGER.info('Dollarsales and transcount daily Redundancy forecast QC done.')

        dollartranscount = open('redundancy_dollartranscount-core_forecast_qc_redundancy_inference/15min.sql', 'r')
        dollartranscount_15min = dollartranscount.read()
        dollartranscount_15min = dollartranscount_15min.replace('__dollar_forecast_table__',\
                                            conf.get_15min_dollar_redundancy_table())
        dollartranscount_15min = dollartranscount_15min.replace('__transcount_forecast_table__',\
                                            conf.get_15min_transcount_redundancy_table())
        dollartranscount_15min = dollartranscount_15min.replace('__redundancy_forecast_from__',\
                                            conf.get_redundancy_forecast_from())
        dollartranscount_15min = dollartranscount_15min.replace('__redundancy_forecast_to__',\
                                            conf.get_redundancy_forecast_to())
        dollartranscount_15min = dollartranscount_15min.replace('__placeholder__',\
                                                conf.get_place_holder_15min())
        dollartranscount.close()
        execute_query(dollartranscount_15min, connection, store,\
            conf.get_dollartranscount_columns(), conf.get_dollartranscount_local_path(),\
            conf.get_prod_bucket(), conf.get_15min_dollartranscount_upload_key(str(store)))
        LOGGER.info('Dollarsales and transcount 15 mins Redundancy forecast QC done.')

        itemcount = open('core_forecast_qc_redundancy_inference/redundancy_itemcount.sql', 'r')
        itemcount_daily = itemcount.read()
        itemcount_daily = itemcount_daily.replace('__itemcount_forecast_table__',\
                                            conf.get_daily_itemcount_redundancy_table())
        itemcount_daily = itemcount_daily.replace('__redundancy_forecast_from__',\
                                            conf.get_redundancy_forecast_from())
        itemcount_daily = itemcount_daily.replace('__redundancy_forecast_to__',\
                                            conf.get_redundancy_forecast_to())
        itemcount_daily = itemcount_daily.replace('__placeholder__',\
                                                conf.get_place_holder_daily())
        itemcount.close()
        execute_query(itemcount_daily, connection, store,\
            conf.get_itemcount_columns(), conf.get_itemcount_local_path(),\
            conf.get_prod_bucket(), conf.get_daily_itemcount_upload_key(str(store)))
        LOGGER.info('Itemcount daily Redundancy forecast QC done.')

        itemcount = open('core_forecast_qc_redundancy_inference/redundancy_itemcount.sql', 'r')
        itemcount_15min = itemcount.read()
        itemcount_15min = itemcount_15min.replace('__itemcount_forecast_table__',\
                                            conf.get_15min_itemcount_redundancy_table())
        itemcount_15min = itemcount_15min.replace('__redundancy_forecast_from__',\
                                            conf.get_redundancy_forecast_from())
        itemcount_15min = itemcount_15min.replace('__redundancy_forecast_to__',\
                                            conf.get_redundancy_forecast_to())
        itemcount_15min = itemcount_15min.replace('__placeholder__',\
                                                conf.get_place_holder_15min())
        itemcount.close()
        execute_query(itemcount_15min, connection, store,\
            conf.get_itemcount_columns(), conf.get_itemcount_local_path(),\
            conf.get_prod_bucket(), conf.get_15min_itemcount_upload_key(str(store)))
        LOGGER.info('Itemcount 15 mins Redundancy forecast QC done. Loading the data')

        query_load = open('core_forecast_qc_redundancy_inference/load_data.sql', 'r')
        daily_dollartranscount = query_load.read()
        load_to_aurora(daily_dollartranscount, connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_daily_dollartranscount_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_daily_dollartranscount_redundancy_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_redundancy_inference/load_data.sql', 'r')
        load_15min_dollartranscount = query_load.read()
        load_to_aurora(load_15min_dollartranscount, connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_15min_dollartranscount_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_15min_dollartranscount_redundancy_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_redundancy_inference/load_data.sql', 'r')
        daily_itemcount = query_load.read()
        load_to_aurora(daily_itemcount, connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_daily_itemcount_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_daily_itemcount_redundancy_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_redundancy_inference/load_data.sql', 'r')
        load_15min_itemcount = query_load.read()
        load_to_aurora(load_15min_itemcount, connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_15min_itemcount_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_15min_itemcount_redundancy_upload_table())
        query_load.close()
        connection_load_final.commit()
        LOGGER.info('Data Loaded for: %s', str(store))

    connection.close()
    connection_load_final.close()
    
    LOGGER.info('Redundancy QC Completed for: %s', str(store_list)[1:-1])
