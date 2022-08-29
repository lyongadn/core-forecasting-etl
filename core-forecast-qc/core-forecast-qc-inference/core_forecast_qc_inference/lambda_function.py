"""
main lambda function to carry out quality checks
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
        #connection.commit()
    LOGGER.info("loaded results into table "+table+" from: " + upload_path)
    return "success"

def lambda_handler(event, context):
    """
    Input - event:dictionary
    Main lambda Function to execute queries that does quality check on the data
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_qc_inference/config.json') as config_params:
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
        dollartranscount_10 = open('core_forecast_qc_inference/dollartranscount_forecast_qc.sql', 'r')
        dollarsales10days = dollartranscount_10.read()
        dollarsales10days = dollarsales10days.replace('__dollar_forecast_table__',\
                                                conf.get_daily_dollar_10days_table())
        dollarsales10days = dollarsales10days.replace('__transcount_forecast_table__',\
                                                conf.get_daily_transcount_10days_table())
        dollarsales10days = dollarsales10days.replace('__forecast_from__',\
                                                conf.get_10day_forecast_from())
        dollarsales10days = dollarsales10days.replace('__forecast_to__',\
                                                conf.get_10day_forecast_to())
        dollarsales10days = dollarsales10days.replace('__placeholder__',\
                                                conf.get_place_holder_daily())
        
        dollartranscount_10.close()
        # query, connection, store_number, columns, local_path, prod_bucket, upload_key
        execute_query(dollarsales10days, connection, store, conf.get_dollartranscount_columns(),\
                        conf.get_dollartranscount_local_path(), conf.get_prod_bucket(),\
                        conf.get_daily_dollartranscount_10days_upload_key(str(store)))

        dollartranscount_30 = open('core_forecast_qc_inference/dollartranscount_forecast_qc.sql', 'r')
        dollarsales30days = dollartranscount_30.read()
        dollarsales30days = dollarsales30days.replace('__dollar_forecast_table__',\
                                                conf.get_daily_dollar_30days_table())
        dollarsales30days = dollarsales30days.replace('__transcount_forecast_table__',\
                                                conf.get_daily_transcount_30days_table())
        dollarsales30days = dollarsales30days.replace('__forecast_from__',\
                                                conf.get_30day_forecast_from())
        dollarsales30days = dollarsales30days.replace('__forecast_to__',\
                                                conf.get_30day_forecast_to())
        dollarsales30days = dollarsales30days.replace('__placeholder__',\
                                                conf.get_place_holder_daily())
        dollartranscount_30.close()
        execute_query(dollarsales30days, connection, store, conf.get_dollartranscount_columns(),\
                        conf.get_dollartranscount_local_path(), conf.get_prod_bucket(),\
                        conf.get_daily_dollartranscount_30days_upload_key(str(store)))
        LOGGER.info('Dollarsales and transcount daily forecast QC done')

        dollartranscount_10 = open('core_forecast_qc_inference/dollartranscount_10days-15min.sql', 'r')
        dollarsales10days = dollartranscount_10.read()
        dollarsales10days = dollarsales10days.replace('__dollar_forecast_table__',\
                                                conf.get_15min_dollar_10days_table())
        dollarsales10days = dollarsales10days.replace('__transcount_forecast_table__',\
                                                conf.get_15min_transcount_10days_table())
        dollarsales10days = dollarsales10days.replace('__forecast_from__',\
                                                conf.get_10day_forecast_from())
        dollarsales10days = dollarsales10days.replace('__forecast_to__',\
                                                conf.get_10day_forecast_to())
        dollarsales10days = dollarsales10days.replace('__placeholder__',\
                                                conf.get_place_holder_15min())
        dollartranscount_10.close()
        execute_query(dollarsales10days, connection, store, conf.get_dollartranscount_columns(),\
                        conf.get_dollartranscount_local_path(), conf.get_prod_bucket(),\
                        conf.get_15min_dollartranscount_10days_upload_key(str(store)))

        dollartranscount_30 = open('core_forecast_qc_inference/dollartranscount_10days-15min.sql', 'r')
        dollarsales30days = dollartranscount_30.read()
        dollarsales30days = dollarsales30days.replace('__dollar_forecast_table__',\
                                                conf.get_15min_dollar_30days_table())
        dollarsales30days = dollarsales30days.replace('__transcount_forecast_table__',\
                                                conf.get_15min_transcount_30days_table())
        dollarsales30days = dollarsales30days.replace('__forecast_from__',\
                                                conf.get_30day_forecast_from())
        dollarsales30days = dollarsales30days.replace('__forecast_to__',\
                                                conf.get_30day_forecast_to())
        dollarsales30days = dollarsales30days.replace('__placeholder__',\
                                                conf.get_place_holder_15min())
        dollartranscount_30.close()
        execute_query(dollarsales30days, connection, store, conf.get_dollartranscount_columns(),\
                        conf.get_dollartranscount_local_path(), conf.get_prod_bucket(),\
                        conf.get_15min_dollartranscount_30days_upload_key(str(store)))
        LOGGER.info('Dollarsales and transcount 15 mins forecast QC done')
        LOGGER.info('Dollarsales and Transcount forecast QC done. Starting Itemcount QC')

        itemcount_10 = open('core_forecast_qc_inference/itemcount_forecast_qc.sql', 'r')
        itemcount10days = itemcount_10.read()
        itemcount10days = itemcount10days.replace('__itemcount_forecast_table__',\
                                                conf.get_daily_itemcount_10days_table())
        itemcount10days = itemcount10days.replace('__forecast_from__',\
                                                conf.get_10day_forecast_from())
        itemcount10days = itemcount10days.replace('__forecast_to__',\
                                                conf.get_10day_forecast_to())
        itemcount10days = itemcount10days.replace('__placeholder__',\
                                                conf.get_place_holder_daily())
        itemcount_10.close()
        execute_query(itemcount10days, connection, store, conf.get_itemcount_columns(),\
                        conf.get_itemcount_local_path(), conf.get_prod_bucket(),\
                        conf.get_daily_itemcount_10days_upload_key(str(store)))

        itemcount_30 = open('core_forecast_qc_inference/itemcount_forecast_qc.sql', 'r')
        itemcount30days = itemcount_30.read()
        itemcount30days = itemcount30days.replace('__itemcount_forecast_table__',\
                                                conf.get_daily_itemcount_30days_table())
        itemcount30days = itemcount30days.replace('__forecast_from__',\
                                                conf.get_30day_forecast_from())
        itemcount30days = itemcount30days.replace('__forecast_to__',\
                                                conf.get_30day_forecast_to())
        itemcount30days = itemcount30days.replace('__placeholder__',\
                                                conf.get_place_holder_daily())
        itemcount_30.close()
        execute_query(itemcount30days, connection, store, conf.get_itemcount_columns(),\
                        conf.get_itemcount_local_path(), conf.get_prod_bucket(),\
                        conf.get_daily_itemcount_30days_upload_key(str(store)))
        LOGGER.info('Itemcount daily forecast QC done')

        itemcount_10 = open('core_forecast_qc_inference/itemcount_forecast_qc.sql', 'r')
        itemcount10days = itemcount_10.read()
        itemcount10days = itemcount10days.replace('__itemcount_forecast_table__',\
                                                conf.get_15min_itemcount_10days_table())
        itemcount10days = itemcount10days.replace('__forecast_from__',\
                                                conf.get_10day_forecast_from())
        itemcount10days = itemcount10days.replace('__forecast_to__',\
                                                conf.get_10day_forecast_to())
        itemcount10days = itemcount10days.replace('__placeholder__',\
                                                conf.get_place_holder_15min())
        itemcount_10.close()
        execute_query(itemcount10days, connection, store, conf.get_itemcount_columns(),\
                        conf.get_itemcount_local_path(), conf.get_prod_bucket(),\
                        conf.get_15min_itemcount_10days_upload_key(str(store)))

        itemcount_30 = open('core_forecast_qc_inference/itemcount_forecast_qc.sql', 'r')
        itemcount30days = itemcount_30.read()
        itemcount30days = itemcount30days.replace('__itemcount_forecast_table__',\
                                                conf.get_15min_itemcount_30days_table())
        itemcount30days = itemcount30days.replace('__forecast_from__',\
                                                conf.get_30day_forecast_from())
        itemcount30days = itemcount30days.replace('__forecast_to__',\
                                                conf.get_30day_forecast_to())
        itemcount30days = itemcount30days.replace('__placeholder__',\
                                                conf.get_place_holder_15min())
        itemcount_30.close()
        execute_query(itemcount30days, connection, store, conf.get_itemcount_columns(),\
                        conf.get_itemcount_local_path(), conf.get_prod_bucket(),\
                        conf.get_15min_itemcount_30days_upload_key(str(store)))
        LOGGER.info('Itemcount 15 mins 10 days ahead forecast QC done')
        LOGGER.info('Loading the data')

        query_load = open('core_forecast_qc_inference/load_data.sql', 'r')
        daily_dollartranscount_10days = query_load.read()
        load_to_aurora(daily_dollartranscount_10days,\
          connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_daily_dollartranscount_10days_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_daily_dollartranscount_10days_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_inference/load_data.sql', 'r')
        daily_dollartranscount_30days = query_load.read()
        load_to_aurora(daily_dollartranscount_30days,\
          connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_daily_dollartranscount_30days_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_daily_dollartranscount_30days_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_inference/load_data.sql', 'r')
        load_15min_dollartranscount_10days = query_load.read()
        load_to_aurora(load_15min_dollartranscount_10days,\
          connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_15min_dollartranscount_10days_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_15min_dollartranscount_10days_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_inference/load_data.sql', 'r')
        load_15min_dollartranscount_30days = query_load.read()
        load_to_aurora(load_15min_dollartranscount_30days,\
          connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_15min_dollartranscount_30days_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_15min_dollartranscount_30days_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_inference/load_data.sql', 'r')
        daily_itemcount_10days = query_load.read()
        load_to_aurora(daily_itemcount_10days,\
          connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_daily_itemcount_10days_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_daily_itemcount_10days_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_inference/load_data.sql', 'r')
        daily_itemcount_30days = query_load.read()
        load_to_aurora(daily_itemcount_30days,\
          connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_daily_itemcount_30days_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_daily_itemcount_30days_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_inference/load_data.sql', 'r')
        load_15min_itemcount_10days = query_load.read()
        load_to_aurora(load_15min_itemcount_10days,\
          connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_15min_itemcount_10days_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_15min_itemcount_10days_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_inference/load_data.sql', 'r')
        load_15min_itemcount_30days = query_load.read()
        load_to_aurora(load_15min_itemcount_30days,\
          connection_load_final, conf.get_prod_bucket(),\
          '/'.join(conf.get_15min_itemcount_30days_upload_key(str(store)).split('/')[:-1]),\
          conf.get_database(), conf.get_15min_itemcount_30days_upload_table())
        query_load.close()
        connection_load_final.commit()
        LOGGER.info('Data Loaded for: %s', str(store))
    LOGGER.info('Forecast QC process for all metrics completed successfully')
