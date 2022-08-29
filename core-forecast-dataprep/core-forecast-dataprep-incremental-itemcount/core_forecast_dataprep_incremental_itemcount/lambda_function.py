"""
main lambda function to add features to data from initial table
and dump to final tables in  aurora
"""
import os
import json
import logging
import pandas as pd
import boto3
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
    LOGGER.info("uploaded file: " + local_path + " at : " + upload_path)
    s_3.upload_file(local_path, prod_bucket, upload_path)
    return "success"

def execute_query(query, connection, store_number, columns, local_path, prod_bucket, upload_key,\
                 local_path_staging, upload_key_staging):
    """
    Input - query:str, connection:pymysql.connect, store_number:str, columns:list,
            local_path:str, prod_bucket:str, upload_path:str
    This method execute the query with required parameters along with columns of dataframe,
    saves it in local path, and uploads the file on s3
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
        final_data.drop('location_num', axis=1, inplace=True)
        final_data.to_parquet(local_path_staging, index=False)
        upload_to_s3(local_path_staging, prod_bucket, upload_key_staging)
        #connection.commit()
    LOGGER.info("loaded results of query : \n" + query + "at: " + upload_key)
    return upload_key

def load_to_aurora(query, connection, prod_bucket, upload_path, database, table):
    """
    Input - query:str, connection:pymysql.connect, prod_bucket:str,
            upload_path:str, database:str, table:str
    This method loads data from s3 at specified path to aurora
    in specifeid database and table
    """
    with connection.cursor() as cur:
        query = query.replace('__prod_bucket__', prod_bucket)
        query = query.replace('__upload_path__', upload_path)
        query = query.replace('__database_name__', database)
        query = query.replace('__table__', table)
        print(query)
        cur.execute(query)
        #connection.commit()
    LOGGER.info("loaded results of query : \n" + query + "at: " + upload_path)
    return "success"

def lambda_handler(event, context):
    """
    Input - event:dictionary
    This method is the main function which gets triggered by lambda,
    connects to aurora and executes the query and loads the data
    to aurora after processing
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_dataprep_incremental_itemcount/config.json') as config_params:
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
    LOGGER.info("executing data_prep query for: " + event['store_num'])
    query_15min = open('core_forecast_dataprep_incremental_itemcount/final_item_15min.sql', 'r')
    final_item_15min = query_15min.read()
    query_15min.close()
    execute_query(final_item_15min, connection, event['store_num'], conf.get_15min_columns(),\
    conf.get_15min_local_path(), conf.get_prod_bucket(), conf.get_15min_key(event['store_num']),\
                  conf.get_15min_local_path_staging(),\
                  conf.get_15min_key_staging(event['store_num']).replace('_date_', date))

    query_daily = open('core_forecast_dataprep_incremental_itemcount/final_item_daily.sql', 'r')
    final_item_daily = query_daily.read()
    query_daily.close()
    execute_query(final_item_daily, connection, event['store_num'], conf.get_daily_columns(),\
    conf.get_daily_local_path(), conf.get_prod_bucket(), conf.get_daily_key(event['store_num']),\
                  conf.get_daily_local_path_staging(),\
                  conf.get_daily_key_staging(event['store_num']).replace('_date_', date))
    connection.commit()
    LOGGER.info("Connecting to Aurora RDS Writer")

    connection_load_final = pymysql.connect(host=credentials['host'],#credentials['host'],
                                            user=credentials['username'],
                                            password=credentials['password'],
                                            db=conf.get_database())

    LOGGER.info("connection final load succeeded")
    LOGGER.info("Loading results for store : " + event['store_num'])
    query_load = open('core_forecast_dataprep_incremental_itemcount/load_data.sql', 'r')
    load_data_15min = query_load.read()
    load_to_aurora(load_data_15min, connection_load_final, conf.get_prod_bucket(),\
                    '/'.join(conf.get_15min_key(event['store_num']).split('/')[:-1]) \
                    , conf.get_15min_database(), conf.get_15min_table())
    query_load.close()

    query_load = open('core_forecast_dataprep_incremental_itemcount/load_data.sql', 'r')
    load_data_qc = query_load.read()
    load_to_aurora(load_data_qc, connection_load_final, conf.get_prod_bucket(),\
                    '/'.join(conf.get_15min_key(event['store_num']).split('/')[:-1]),\
                    conf.get_qc_database(), conf.get_qc_table())
    query_load.close()
    
    query_load = open('core_forecast_dataprep_incremental_itemcount/load_data.sql', 'r')
    load_data_daily = query_load.read()
    load_to_aurora(load_data_daily, connection_load_final, conf.get_prod_bucket(),\
                   '/'.join(conf.get_daily_key(event['store_num']).split('/')[:-1]),\
                   conf.get_daily_database(), conf.get_daily_table())
    query_load.close()
    connection_load_final.commit()
    connection_ingre = pymysql.connect(host=conf.get_replica_host_link(),
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf.get_database())
    query_ingredient_15min = open('core_forecast_dataprep_incremental_itemcount/final_ingredient_15min.sql', 'r')
    final_ingredient_15min = query_ingredient_15min.read()
    query_daily.close()
    execute_query(final_ingredient_15min, connection_ingre, event['store_num'],\
                  conf.get_ingredient_columns(), conf.get_ingredient_local_path(),\
                  conf.get_prod_bucket(), conf.get_ingredient_key(event['store_num']),\
    conf.get_ingredient_local_path_stag(),\
    conf.get_ingredient_key_stag(event['store_num']).replace('_date_', date))
    #connection_ingre.commit()
    query_load = open('core_forecast_dataprep_incremental_itemcount/load_data.sql', 'r')
    load_data_ingredient = query_load.read()
    load_to_aurora(load_data_ingredient, connection_load_final, conf.get_prod_bucket(),\
                   '/'.join(conf.get_ingredient_key(event['store_num']).split('/')[:-1]),\
                   conf.get_ingredient_database(), conf.get_ingredient_table())
    connection_load_final.commit()
    connection_ingre.commit()
    query_load.close()
    LOGGER.info("Completed Dataprep for: " + event['store_num'])
    return "success"
