"""
This code breaks down the redundancy forecast for dollar transcount
and triggers the ingredient itemcount redundancy forecast breakdown
"""
import os
import json
import logging
import boto3
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from config import Config
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

date = str(datetime.date.today())

def upload_to_s3(local_path, prod_bucket, upload_path):
    """
    Input - local_path:str, prod_bucket:str, upload_path:str
    This method upload files from tmp folder to s3 at specified prod bucket and path
    """
    s_3 = boto3.client('s3')
    s_3.upload_file(local_path, prod_bucket, upload_path)
    LOGGER.info("uploaded file: " + local_path + " at : " + upload_path)
    return "success"

def execute_query(query, connection, store_number, columns, local_path, prod_bucket, upload_key,\
                 local_path_staging, upload_key_staging):
    """
    Input - query:str, connection:pymysql.connect, store_number:str, columns:list, local_path:str
            prod_bucket:str, upload_key:str
    This method loads data from s3 at specified path to aurora
    in specified database and table
    """
    LOGGER.info("executing query: \n" + query)
    with connection.cursor() as cur:
        query = query.replace('__store_number__', str(store_number))
        print(query)
        cur.execute(query)
        data = cur.fetchall()
        final_data = pd.DataFrame(list(data), columns=columns)
        final_data.to_csv(local_path, index=False)
        upload_to_s3(local_path, prod_bucket, upload_key)
        final_data.to_parquet(local_path_staging, index=False)
        upload_to_s3(local_path_staging, prod_bucket, upload_key_staging)
        connection.commit()
    LOGGER.info("executed query : " + query)
    return upload_key

def load_to_aurora(query, connection, prod_bucket, upload_path, database, table):
    """
    Input - query:str, connection:pymysql.connect, prod_bucket, upload_path:str
            database:str, table:str
    This method execute the query with required parameters, saves it in local path
    and uploads the file on s3
    """
    with connection.cursor() as cur:
        query = query.replace('__prod_bucket__', prod_bucket)
        query = query.replace('__upload_path__', upload_path)
        query = query.replace('__database_name__', database)
        query = query.replace('__table__', table)
        print(query)
        cur.execute(query)
        connection.commit()
    LOGGER.info("loaded the data into : " + database + "." + table)
    return "success"

def lambda_handler(event, context):
    """
    Input - event:dictionary
    This method is the main function which gets triggered by lambda,
    connects to aurora and executes the query and loads the data
    to aurora after processing
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_15min_breakdown_redundancy_dollarsandtrans/config.json') as config_params:
        config_dict = json.load(config_params)[ENV]
        conf = Config.from_event(config_dict)

    LOGGER.info("Fetching credentials for rds")
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
    credentials = json.loads(keys['SecretString'])

    LOGGER.info("Connecting to RDS")
    connection = pymysql.connect(host=conf.get_replica_host_link(),
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf.get_database())
    LOGGER.info("Connection succeeded")

    dollarsales = open('core_forecast_15min_breakdown_redundancy_dollarsandtrans/final_query_dollartranscount.sql', 'r')
    query_dollarsales = dollarsales.read()
    query_dollarsales = query_dollarsales.replace('__ratio_table__',
    conf.get_dollarsales_ratio_table()).replace('__forecast__trans__',\
    conf.get_transcount_forecast_table()).replace('__ratio__trans__',\
    				 conf.get_transcount_ratio_table())
    query_dollarsales = query_dollarsales.replace('__forecast_table__',
                                                  conf.get_dollarsales_forecast_table())
    execute_query(query_dollarsales, connection, event['store_num'],\
                conf.get_dollartranscount_columns(), conf.get_dollarsales_local_path(),\
                conf.get_prod_bucket(), conf.get_dollarsales_upload_key(event['store_num']),\
                  conf.get_dollarsales_local_path_stg(),\
                  conf.get_dollarsales_upload_key_stg(event['store_num']).replace('_date_', date))
    dollarsales.close()


    LOGGER.info('loading data')
    connection_load = pymysql.connect(host=credentials['host'],
                                      user=credentials['username'],
                                      password=credentials['password'],
                                      db=conf.get_database())

    load_data = open('core_forecast_15min_breakdown_redundancy_dollarsandtrans/load_data.sql', 'r')
    load_query = load_data.read()
    load_to_aurora(load_query, connection_load, conf.get_prod_bucket(),
                   '/'.join(conf.get_dollarsales_upload_key(event['store_num']).split('/')[:-1]),
                   conf.get_database(), conf.get_dollarsales_upload_table())
    load_data.close()

    LOGGER.info('process completed for dollars. Triggering ingredient/itemcount lambda')
    client = boto3.client('lambda', region_name='us-east-1')
    try:
        resp = client.invoke(FunctionName=conf.get_itemcount_function_name(),\
                                InvocationType='Event',\
                                Payload=json.dumps(conf.get_config()))
    except Exception as invoke_error:
        LOGGER.error(invoke_error)
    return "Success"
