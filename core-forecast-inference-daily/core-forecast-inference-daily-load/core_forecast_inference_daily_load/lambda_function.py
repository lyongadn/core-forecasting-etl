"""
main lambda function to carry out quality checks for datalake tables
"""
import json
import os
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
        connection.commit()
    LOGGER.info("loaded results into table "+table+" from: " + upload_path)
    return "success"
def lambda_handler(event, context):
    """
    Input - event:dictionary
    Main lambda Function to execute queries that does quality check on the data in dollartranscount
    and itemlevelcount of datalake database.
    Inserts distinct products in expected forecasted products from initial itemcount table
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_inference_input_dollarsandtrans/config.json') as config_params:
        config_dict = json.load(config_params)[ENV]
        config_dict['store_list'] = event['store_list']
        conf = Config.from_event(config_dict)

    store_list = conf.get_store_list()
    LOGGER.info("Fetching credentials for rds")
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
    credentials = json.loads(keys['SecretString'])

    LOGGER.info("Connecting to RDS")

    LOGGER.info("Successfully connected to RDS")

    for store in store_list:
        connection_load_final = pymysql.connect(host=credentials['host'],
                                            user=credentials['username'],
                                            password=credentials['password'],
                                            db=conf.get_database())
        LOGGER.info("Connection Succeded")
        LOGGER.info('Running QC for: %s', str(store))

        load_query = open('core_forecast_inference_daily_load/load_data.sql', 'r')
        load_all = load_query.read()
        print (conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_dollars_14day_key(str(store)).split('/')[:-1]))
        # query, connection, store_number, columns, local_path, prod_bucket, upload_key
        load_to_aurora(load_all, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_dollars_14day_key(str(store)).split('/')[:-1]),\
            conf.get_15min_database(), conf.get_ten_days_upload_table_name_dollars_10())
    
        
        load_to_aurora(load_all, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_dollars_30day_key(str(store)).split('/')[:-1]),\
            conf.get_15min_database(), conf.get_twenty_days_upload_table_name_dollars_30())
            
        load_to_aurora(load_all, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_dollars_redundancy_key(str(store)).split('/')[:-1]),\
            conf.get_database(), conf.get_thirty_days_upload_table_name_dollars_red())
            
        load_to_aurora(load_all, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_trans_14day_key(str(store)).split('/')[:-1]),\
            conf.get_15min_database(), conf.get_ten_days_upload_table_name_trans_10())
        
        load_to_aurora(load_all, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_trans_30day_key(str(store)).split('/')[:-1]),\
            conf.get_15min_database(), conf.get_twenty_days_upload_table_name_trans_30())
        
        load_to_aurora(load_all, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_trans_redundancy_key(str(store)).split('/')[:-1]),\
            conf.get_database(), conf.get_thirty_days_upload_table_name_trans_red())
            
        load_to_aurora(load_all, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_item_14day_key(str(store)).split('/')[:-1]),\
            conf.get_15min_database(), conf.get_ten_days_upload_table_name_item_10())
        
        load_to_aurora(load_all, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_item_30day_key(str(store)).split('/')[:-1]),\
            conf.get_15min_database(), conf.get_twenty_days_upload_table_name_item_30())
            
        load_to_aurora(load_all, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_daily_item_redundancy_key(str(store)).split('/')[:-1]),\
            conf.get_database(), conf.get_thirty_days_upload_table_name_item_red())
        load_query.close()
        connection_load_final.commit()
        
