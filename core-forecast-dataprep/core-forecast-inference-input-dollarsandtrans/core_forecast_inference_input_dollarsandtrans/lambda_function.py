"""
code for inference input dollarsales
"""
import os
import json
import logging
from datetime import datetime
import pandas as pd
import boto3
from pytz import timezone
import pymysql
pymysql.install_as_MySQLdb()
from config import Config
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def upload_to_s3(local_path, prod_bucket, upload_path):
    """
    upload files to s3 from local temporary folder to prod bucket
    """
    s_3 = boto3.client('s3')
    s_3.upload_file(local_path, prod_bucket, upload_path)

def find_max_date(query, connection, store_number):
    """
    This query is to get the max business_date from the dollars data
    """
    with connection.cursor() as cur:
        query = query.replace('store_number', str(store_number))
        print(query)
        cur.execute(query)
        max_date = cur.fetchall()
        df = pd.DataFrame(list(max_date), columns=['MaxDate'])
        MaxDate=df['MaxDate'][0]
        connection.commit()
    return MaxDate

#def find_max_date(query, connection, store_number, status, period):
#    """
#    This query is to get the max business_date from the dollars data
#    """
#    if status == 1:
#        with connection.cursor() as cur:
#            query = query.replace('store_number', str(store_number))
#            print(query)
#            cur.execute(query)
#            max_date = cur.fetchall()
#            df = pd.DataFrame(list(max_date), columns=['MaxDate'])
#            MaxDate=df['MaxDate'][0]
#            connection.commit()
#    #if status == 0:
#    #    MaxDate=period
#    #    print(MaxDate)
#
#           return MaxDate




def execute_query(query, connection, numberdays_topredict, actual_days, store_number,\
                     columns, local_path, prod_bucket, upload_key):
    """
    execute the query with required parameters to generate a dollars and transcount input
    file for inference on aurora and uploads to s3
    """
    with connection.cursor() as cur:
        query = query.replace('store_number', str(store_number)).replace('actual_days', \
             str(actual_days)).replace('numberOfFeatures', str(numberdays_topredict))
        LOGGER.info(query)
        cur.execute(query)
        data = cur.fetchall()
        d_f = pd.DataFrame(list(data), columns=columns)
        d_f.to_csv(local_path, index=False)
        upload_to_s3(local_path, prod_bucket, upload_key)
        connection.commit()


def lambda_handler(event, context):
    """
    main lambda function handler which calls each and every
    function defined in the script to get the results
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_inference_input_dollarsandtrans/config.json') as config_params:
        config_dict = json.load(config_params)[ENV]
        conf = Config.from_event(config_dict)

    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
    credentials = json.loads(keys['SecretString'])

    connection = pymysql.connect(host=conf.get_custom_host_link(),
                                            user=credentials['username'],
                                            password=credentials['password'],
                                            db=conf.get_database())
    LOGGER.info("Connection of replica final succeeded")
    print(event['status'], 'status')
    print(event['period'], 'period')
    if event['status'] == '99':
        print ("exiting the process since store is closed for remodeling ", event['store_num'])
    else:
        if event['status'] == '0':
            MaxDate=event['period']
            print(MaxDate)
        else:
            inference_daily_dollarsales_max = open(
                'core_forecast_inference_input_dollarsandtrans/Inference_daily_dollarsales_max_date.sql', 'r')
            inference_daily_dollarsales_max_date = inference_daily_dollarsales_max.read()
            inference_daily_dollarsales_max.close()
            MaxDate = find_max_date(inference_daily_dollarsales_max_date, connection, \
                                    event['store_num'])
        
        inference_daily_dollarsales = open('core_forecast_inference_input_dollarsandtrans/Inference_daily_dollarsales_input.sql', 'r')
        inference_daily_dollarsales_input = inference_daily_dollarsales.read()
        inference_daily_dollarsales.close()
        inference_daily_dollarsales_input = inference_daily_dollarsales_input.replace('MaxDate', str(MaxDate))
        execute_query(inference_daily_dollarsales_input, connection, conf.get_numberdays_topredict(),\
            conf.get_actual_days(), event['store_num'], \
            conf.get_inference_dollars_input_columns(), conf.get_inference_dollars_input_local_path(),\
            conf.get_prod_bucket(), conf.get_inference_dollars_input_key(event['store_num']))
        LOGGER.info('Input is generated for '+event['store_num'])
