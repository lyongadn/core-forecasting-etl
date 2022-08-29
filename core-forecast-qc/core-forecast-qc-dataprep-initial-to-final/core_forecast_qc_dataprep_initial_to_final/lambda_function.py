"""
main lambda function to carry out data prep quality check - match initial data with final data
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
        query = query.replace('__location_num__', str(store_number))
        cur.execute(query)
        data = cur.fetchall()
        final_data = pd.DataFrame(list(data), columns=columns)
        final_data.to_csv(local_path, index=False)
        upload_to_s3(local_path, prod_bucket, upload_key)
        connection.commit()
    LOGGER.info("Query Executed Successfully")
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
    Main lambda function to execute queries that does quality check on the data
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_qc_dataprep_initial_to_final/config.json') as config_params:
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
        LOGGER.info('Running QC for: %s', str(store))
        dollartranscount_qc_query = open('core_forecast_qc_dataprep_initial_to_final/dataprep_dollartranscount_qc.sql', 'r')
        dollartranscount_qc = dollartranscount_qc_query.read()
        # query, connection, store_number, columns, local_path, prod_bucket, upload_key
        execute_query(dollartranscount_qc, connection, store, conf.get_dollartranscount_columns(),\
                        conf.get_dollartranscount_local_path(), conf.get_prod_bucket(),\
                        conf.get_dollartranscount_upload_key(str(store)))
        dollartranscount_qc_query.close()

        itemcount_qc_query = open('core_forecast_qc_dataprep_initial_to_final/dataprep_itemcount_qc.sql', 'r')
        itemcount_qc = itemcount_qc_query.read()
        execute_query(itemcount_qc, connection, store, conf.get_itemcount_columns(),\
                        conf.get_itemcount_local_path(), conf.get_prod_bucket(),\
                        conf.get_itemcount_upload_key(str(store)))
        itemcount_qc_query.close()
        LOGGER.info('Querying Done. Loading the data')

        query_load = open('core_forecast_qc_dataprep_initial_to_final/load_data.sql', 'r')
        dollartranscount = query_load.read()
        load_to_aurora(dollartranscount, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_dollartranscount_upload_key(str(store)).split('/')[:-1]),\
            conf.get_database(), conf.get_dollartranscount_qc_upload_table())
        query_load.close()

        query_load = open('core_forecast_qc_dataprep_initial_to_final/load_data.sql', 'r')
        itemcount = query_load.read()
        load_to_aurora(itemcount, connection_load_final, conf.get_prod_bucket(),\
            '/'.join(conf.get_itemcount_upload_key(str(store)).split('/')[:-1]),\
            conf.get_database(), conf.get_itemcount_qc_upload_table())
        query_load.close()
        connection_load_final.commit()
        LOGGER.info('Data Loaded')
    return "Success"
