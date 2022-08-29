'''
Main lambda function to generate 15min forecast for ingredients
'''
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

def get_business_date(query, connection, store_number):
    """
    Input - query:str, connection:pymysql.connect, store_numer:str
    This method returns list of business_dates for each store
    """
    with connection.cursor() as cur:
        query = query.replace('__store_number__', store_number)
        cur.execute(query)
        business_dates = cur.fetchall()
        dates = pd.DataFrame(list(business_dates), columns=['business_date'])
        date = pd.to_datetime(dates['business_date']).apply(lambda x: x.date())
        connection.commit()
    LOGGER.info("List of dates : " + str(dates) +" for store_Num : "+str(store_number))
    return date
def execute_query(query, connection, store_number, columns):
    """
    Input - query:str, connection:pymysql.connect, store_number:str, columns:list,
            local_path:str, prod_bucket:str, upload_path:str
    This method loads data from specified prod_bucket and path of s3 to aurora
    in specified database and table
    """
    LOGGER.info("executing query: \n" + query.replace('__store_number__', str(store_number)))
    with connection.cursor() as cur:
        query = query.replace('__store_number__', str(store_number))
        print(query)
        cur.execute(query)
        data = cur.fetchall()
        final_data = pd.DataFrame(list(data), columns=columns)
        #final_data.to_csv(local_path, index=False)
        #upload_to_s3(local_path, prod_bucket, upload_key)
        connection.commit()
    LOGGER.info("executed query: \n" + query.replace('__store_number__', str(store_number)))
    return final_data


def load_to_aurora(query, connection, prod_bucket, upload_path, database, table):
    """
    Input - query:str, connection:pymysql.connect, prod_bucket:str,
            upload_path:str, database:str, table:str
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
        #connection.commit()
    LOGGER.info("loaded results into table "+table+" of query : \n" + query + "at: " + upload_path)
    return "success"


def lambda_handler(event, context):
    """
    Input - event:dictionary
    This method is the main function which gets triggered by lambda,
    connects to aurora and executes the query and loads the data
    to aurora after processing
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_15min_breakdown_ingredie/config.json') as config_params:
        config_dict = json.load(config_params)[ENV]
        conf = Config.from_event(config_dict)

    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
    credentials = json.loads(keys['SecretString'])
    LOGGER.info("Connecting to RDS replica")

    connection = pymysql.connect(host=conf.get_replica_host_link(),
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf.get_database())

    LOGGER.info("Connection to RDS replica succeded")
    LOGGER.info("Starting Ingredient Forecast breakdown for : " + event['store_num'])
    query_dates = open('core_forecast_15min_breakdown_ingredie/get_dates14days.sql', 'r').read()
    ingre10_dates = get_business_date(query_dates, connection, event['store_num'])
    LOGGER.info("Got the 14 days dates")
    final_data_14 = pd.DataFrame()
    for dates in ingre10_dates:
        query_final_10days = open('core_forecast_15min_breakdown_ingredie/final_query_ingredient.sql', 'r')
        final_10days = query_final_10days.read()
        final_10days = final_10days.replace(
        '__forecast_table__', conf.get_10days_forecast_table())
        final_10days = final_10days.replace(
        '__lookup_table__', conf.get_lookup_table())
        final_10days = final_10days.replace('__date__', str(dates))
        data = execute_query(final_10days, connection, event['store_num'],
                  conf.get_forecast_columns())
        final_data_14 = final_data_14.append(data, ignore_index = True)
    final_data_14.to_csv(conf.get_lstm10_local_path(), index=False, header=False)
    final_data_14.to_parquet(conf.get_lstm10_local_path_stg(), index=False)
    upload_to_s3(conf.get_lstm10_local_path(),\
                conf.get_prod_bucket(),\
                conf.get_lstm10_upload_key(event['store_num']))
    upload_to_s3(conf.get_lstm10_local_path_stg(),\
                conf.get_prod_bucket(),\
                conf.get_lstm10_upload_key_stg(event['store_num']).replace('_date_', date))
    
    query_final_10days.close()

    query_dates = open('core_forecast_15min_breakdown_ingredie/get_dates30days.sql', 'r').read()
    ingre30_dates = get_business_date(query_dates, connection, event['store_num'])
    LOGGER.info("Got the 30 days dates")
    final_data_30 = pd.DataFrame()
    for dates in ingre30_dates:
        query_final_30days = open('core_forecast_15min_breakdown_ingredie/final_query_ingredient.sql', 'r')
        final_30days = query_final_30days.read()
        final_30days = final_30days.replace(
        '__forecast_table__', conf.get_30days_forecast_table())
        final_30days = final_30days.replace(
        '__lookup_table__', conf.get_lookup_table())
        final_30days = final_30days.replace('__date__', str(dates))
        data = execute_query(final_30days, connection, event['store_num'],
                  conf.get_forecast_columns())
        final_data_30 = final_data_30.append(data, ignore_index = True)
    final_data_30.to_csv(conf.get_lstm30_local_path(), index=False, header=False)
    final_data_30.to_parquet(conf.get_lstm30_local_path_stg(), index=False)
    upload_to_s3(conf.get_lstm30_local_path(),\
                conf.get_prod_bucket(),\
                conf.get_lstm30_upload_key(event['store_num']))
    upload_to_s3(conf.get_lstm30_local_path_stg(),\
                conf.get_prod_bucket(),\
                conf.get_lstm30_upload_key_stg(event['store_num']).replace('_date_', date))
    query_final_30days.close()

    connection.close()
    LOGGER.info("Connecting to RDS writer")

    connection_load = pymysql.connect(host=credentials['host'],
                                      user=credentials['username'],
                                      password=credentials['password'],
                                      db=conf.get_database())

    load_query = open('core_forecast_15min_breakdown_ingredie/load_data.sql', 'r')
    ingredient_10days = load_query.read()
    load_query.close()
    load_to_aurora(ingredient_10days, connection_load, conf.get_prod_bucket(),
                   '/'.join(conf.get_lstm10_upload_key(event['store_num']).split('/')[:-1]),
                   conf.get_database(), conf.get_lstm10_upload_table())

    load_query = open('core_forecast_15min_breakdown_ingredie/load_data.sql', 'r')
    ingredient_30days = load_query.read()
    load_query.close()
    load_to_aurora(ingredient_30days, connection_load, conf.get_prod_bucket(),
                   '/'.join(conf.get_lstm30_upload_key(event['store_num']).split('/')[:-1]),
                   conf.get_database(), conf.get_lstm30_upload_table())

    LOGGER.info("Finished Ingredient Forecast breakdown for : " + event['store_num'])
    connection_load.commit()
    connection_load.close()
    return "success"