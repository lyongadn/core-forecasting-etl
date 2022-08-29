"""
code for inference input itemcount
"""
import os
import json
import logging
import boto3
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from config import Config
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def upload_to_s3(local_path, prod_bucket, upload_path):
    """
    upload files to s3 from local temp folder to prod bucket
    """
    s_3 = boto3.client('s3')
    s_3.upload_file(local_path, prod_bucket, upload_path)
    LOGGER.info('uploaded to s3')

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

def execute_query(query, connection, store_number, feature_days, features, actual_days, columns,
                  local_path, prod_bucket, upload_key):
    """
    execute the query with required parameters on aurora which basically uploads
    the inference item count input csv file to s3
    """
    with connection.cursor() as cur:
        query = query.replace('store_number', str(store_number)).replace(\
                    'numberOfFeatures', str(feature_days)).replace(\
                    'actual_days', str(actual_days)).replace(\
                    'features', str(features))

        LOGGER.info(query)
        cur.execute(query)
        data = cur.fetchall()
        final_data = pd.DataFrame(list(data), columns=columns)
        final_data.to_csv(local_path, index=False)
        upload_to_s3(local_path, prod_bucket, upload_key)
        connection.commit()


def lambda_handler(event, context):
    """
    main lambda function handler
    runs the above execute_query method with all the parameters required
    """
    ENV = os.getenv('ENV')
    
    with open('core_forecast_inference_input_itemcount/config.json') as config_params:
        config_dict = json.load(config_params)[ENV]
        conf = Config.from_event(config_dict)

    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
    credentials = json.loads(keys['SecretString'])

    connection = pymysql.connect(host=conf.get_replica_host_link(),
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
            inference_daily_itemcount_max = open(
            'core_forecast_inference_input_itemcount/Inference_daily_itemcount_max_date.sql', 'r')
            inference_daily_itemcount_max_date = inference_daily_itemcount_max.read()
            inference_daily_itemcount_max.close()
            MaxDate = find_max_date(inference_daily_itemcount_max_date, connection,\
            event['store_num'])

        inferencedaily_itemcount = open('core_forecast_inference_input_itemcount/Aurora-Inference-ItemCount-Input.sql', 'r')
        inferencedaily_itemcountinput = inferencedaily_itemcount.read()
        inferencedaily_itemcount.close()
        inferencedaily_itemcountinput = inferencedaily_itemcountinput.replace('MaxDate', str(MaxDate))
        execute_query(inferencedaily_itemcountinput, connection, event['store_num'],\
                        conf.get_feature_days(), conf.get_features(), conf.get_actual_days(),\
                        conf.get_inference_iteminputcolumns(),\
                        conf.get_inferenceinput_itemlocalpath(), conf.get_prod_bucket(),\
                        conf.get_inferenceinput_itemkey(event['store_num']))
        LOGGER.info('Input is generated for '+event['store_num'])
