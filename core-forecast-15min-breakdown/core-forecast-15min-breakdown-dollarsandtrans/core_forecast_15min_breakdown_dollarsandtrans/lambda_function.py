"""
main lambda function to breakdown the daily forecast to 15min level
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
    return upload_path


def execute_query(query, connection, store_number, columns, local_path, prod_bucket, upload_key,\
                 local_path_staging, upload_key_staging):
    """
    Input - query:str, connection:pymysql.connect, store_number:str, columns:list,
            local_path:str, prod_bucket:str, upload_path:str
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
        final_data['timequarter'] = final_data['timequarter'].astype(str)
        final_data['timequarter'] = final_data.timequarter.str[7:15]
        final_data.to_csv(local_path, index=False)
        upload_to_s3(local_path, prod_bucket, upload_key)
        final_data.to_parquet(local_path_staging, index=False)
        upload_to_s3(local_path_staging, prod_bucket, upload_key_staging)
        connection.commit()
    LOGGER.info("executed query: \n" + query)
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
        print(query)
        cur.execute(query)
        #connection.commit()
    LOGGER.info("loaded results into table "+table+" of query:\n" + query + "at: " + upload_path)
    return "success"

def lambda_handler(event, context):
    """
    Input - event:dictionary
    This method is the main function which gets triggered by lambda,
    connects to aurora and executes the query and loads the data
    to aurora after processing
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_15min_breakdown_dollarsandtrans/config.json') as config_params:
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
    LOGGER.info("Connection RDS replica succeeded")

    LOGGER.info("executing forecast_breakdown query for: " + event['store_num'])

    query_dollar_10days = open('core_forecast_15min_breakdown_dollarsandtrans/final_query_dollartranscount.sql', 'r')
    dollar_10days = query_dollar_10days.read()
    dollar_10days = dollar_10days.replace('__forecast_table__', conf.get_dollar_10_table()).replace('__forecast__trans__',\
    				 conf.get_transcount_10_table())
    dollar_10days = dollar_10days.replace('__ratio_table__', conf.get_dollar_ratio_table()).replace('__ratio__trans__',\
    				 conf.get_transcount_ratio_table())
    execute_query(dollar_10days, connection, event['store_num'],
                  conf.get_forecast_columns(), conf.get_dollar_lstm10_localpath(),
                  conf.get_prod_bucket(), conf.get_dollar_lstm10_key(event['store_num']),\
                  conf.get_dollar_lstm10_localpath_stg(),\
                  conf.get_dollar_lstm10_key_stg(event['store_num']).replace('_date_', date))
    query_dollar_10days.close()

    query_dollar_30days = open('core_forecast_15min_breakdown_dollarsandtrans/final_query_dollartranscount.sql', 'r')
    dollar_30days = query_dollar_30days.read()
    dollar_30days = dollar_30days.replace('__forecast_table__', conf.get_dollar_30_table()).replace('__forecast__trans__',\
    				 conf.get_transcount_30_table())
    dollar_30days = dollar_30days.replace('__ratio_table__', conf.get_dollar_ratio_table()).replace('__ratio__trans__',\
    				 conf.get_transcount_ratio_table())
    execute_query(dollar_30days, connection, event['store_num'],
                  conf.get_forecast_columns(), conf.get_dollar_lstm30_localpath(),
                  conf.get_prod_bucket(), conf.get_dollar_lstm30_key(event['store_num']),\
                  conf.get_dollar_lstm30_localpath_stg(),\
                  conf.get_dollar_lstm30_key_stg(event['store_num']).replace('_date_', date))
    query_dollar_30days.close()

    LOGGER.info('query executed successfully')
    connection.close()

    LOGGER.info("Connecting to Aurora RDS Writer")

    connection_load_final = pymysql.connect(host=credentials['host'],
                                            user=credentials['username'],
                                            password=credentials['password'],
                                            db=conf.get_database())

    query_load = open('core_forecast_15min_breakdown_dollarsandtrans/load_data.sql', 'r')
    dollar_10days = query_load.read()
    load_to_aurora(dollar_10days, connection_load_final, conf.get_prod_bucket(),\
        '/'.join(conf.get_dollar_lstm10_key(event['store_num']).split('/')[:-1]),\
        conf.get_database(), conf.get_dollar_10_upload_table())
    query_load.close()

    query_load = open('core_forecast_15min_breakdown_dollarsandtrans/load_data.sql', 'r')
    dollar_30days = query_load.read()
    load_to_aurora(dollar_30days, connection_load_final, conf.get_prod_bucket(),\
        '/'.join(conf.get_dollar_lstm30_key(event['store_num']).split('/')[:-1]),\
        conf.get_database(), conf.get_dollar_30_upload_table())
    query_load.close()
    connection_load_final.commit()
    connection_load_final.close()

    LOGGER.info("Forecast Breakdown Complete for: " + event['store_num'])
    return "success"
