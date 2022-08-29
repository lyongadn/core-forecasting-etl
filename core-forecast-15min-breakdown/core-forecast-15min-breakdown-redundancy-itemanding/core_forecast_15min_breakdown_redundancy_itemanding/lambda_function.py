"""
main lambda function to breakdown the itemcount daily forecast to 15min level
"""
import os
import json
import time
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

def execute_query(query, connection, store_number, columns):
    """
    Input - query:str, connection:pymysql.connect, store_number:str, columns:list,
            local_path:str, prod_bucket:str, upload_path:str
    This method execute the query with required parameters, saves it in local path
    and uploads the file on s3
    """
    LOGGER.info("executing query: \n" + query)
    with connection.cursor() as cur:
        query = query.replace('__store_number__', str(store_number))
        print(query)
        cur.execute(query)
        data = cur.fetchall()
        final_data = pd.DataFrame(list(data), columns=columns)
        #final_data.to_csv(local_path, index=False, header=False)
        #upload_to_s3(local_path, prod_bucket, upload_key)
        connection.commit()
    LOGGER.info("executed query : " + query)
    return final_data

def load_to_aurora(query, connection, prod_bucket, upload_path, database, table):
    """
    Input - query:str, connection:pymysql.connect, prod_bucket:str,
            upload_path:str, database:str, table:str
    This method loads data from s3 at specified path to aurora
    in specified database and table
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

def lambda_handler(event, context):
    """
    Input -  event:dictionary
    Main lambda Function to generate forecast at 15min level for itemcount and ingredient redundancy
    with ratios table
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_15min_breakdown_redundancy_itemanding/config.json') as config_params:
        config_dict = json.load(config_params)[ENV]
        conf = Config.from_event(config_dict)

    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
    credentials = json.loads(keys['SecretString'])

    connection = pymysql.connect(host=conf.get_replica_host_link(),
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf.get_database())
    print("Connection succeeded")

    query_dates = open('core_forecast_15min_breakdown_redundancy_itemanding/get_dates.sql', 'r').read()
    itemcount_dates = get_business_date(query_dates, connection, event['store_num'])
    file = 1
    final_data = pd.DataFrame()
    for dates in itemcount_dates:
        itemcount_query = open('core_forecast_15min_breakdown_redundancy_itemanding/final_query_itemcount.sql', 'r').read()
        itemcount_query = itemcount_query.replace('__date__', str(dates))
        data = execute_query(itemcount_query, connection, event['store_num'],\
                conf.get_itemcount_columns())
        final_data = final_data.append(data, ignore_index = True)

    final_data.to_csv(conf.get_itemcount_local_path(), index=False, header=False)
    final_data.to_parquet(conf.get_itemcount_local_path_stg(), index=False)
    upload_to_s3(conf.get_itemcount_local_path(),\
                conf.get_prod_bucket(),\
                conf.get_itemcount_upload_key(event['store_num']).replace('__file__', str(file)))
    upload_to_s3(conf.get_itemcount_local_path_stg(),\
                conf.get_prod_bucket(),\
                conf.get_itemcount_upload_key_stg(event['store_num']).replace('_date_', date))
        #file = file+1

    LOGGER.info('query completed for itemcoun. Loading the data')
    connection_load = pymysql.connect(host=credentials['host'],
                                      user=credentials['username'],
                                      password=credentials['password'],
                                      db=conf.get_database())

    # query, connection, prod_bucket, upload_path, database, table
    try:
        load_itemcount = open('core_forecast_15min_breakdown_redundancy_itemanding/load_data_item.sql', 'r').read()
        load_to_aurora(load_itemcount, connection_load, conf.get_prod_bucket(),\
                '/'.join(conf.get_itemcount_upload_key(event['store_num']).split('/')[:-1]),\
                conf.get_database(), conf.get_itemcount_upload_table())
        LOGGER.info("ItemCount Loading Done. Starting Ingredient")
        time.sleep(5)
    except:
        LOGGER.info("Data duplication issue found")
    file = 1
    final_data_ingre = pd.DataFrame()
    for dates in itemcount_dates:
        query_ingredient = open('core_forecast_15min_breakdown_redundancy_itemanding/final_query_ingredient.sql', 'r').read()
        query_ingredient = query_ingredient.replace('__date__', str(dates))
        data = execute_query(query_ingredient, connection, event['store_num'],\
            conf.get_ingredient_columns())
        final_data_ingre = final_data_ingre.append(data, ignore_index = True)
    final_data_ingre.to_csv(conf.get_ingredient_local_path(), index=False, header=False)
    final_data_ingre.to_parquet(conf.get_ingredient_local_path_stg(), index=False)
    upload_to_s3(conf.get_ingredient_local_path(),\
                conf.get_prod_bucket(),\
                conf.get_ingredient_upload_key(event['store_num']))
    upload_to_s3(conf.get_ingredient_local_path_stg(),\
                conf.get_prod_bucket(),\
                conf.get_ingredient_upload_key_stg(event['store_num']).replace('_date_', date))
    load_ingredient = open('core_forecast_15min_breakdown_redundancy_itemanding/load_data_item.sql', 'r').read()
    load_to_aurora(load_ingredient, connection_load, conf.get_prod_bucket(),\
            '/'.join(conf.get_ingredient_upload_key(event['store_num']).split('/')[:-1]),\
            conf.get_database(), conf.get_ingredient_upload_table())
    LOGGER.info("Ingredient Loaded.\n Complete Process Done")
