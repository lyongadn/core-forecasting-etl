import os
import time
import json
import logging
import pandas as pd
import boto3
import pymysql
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from config import Config
pymysql.install_as_MySQLdb()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
# handler = logging.StreamHandler(sys.stdout)
# LOGGER.addHandler(handler)

client = boto3.client('secretsmanager')

def upload_file_to_s3(local_path, prod_bucket, upload_path):
    
    """
    Input - local_path:str, prod_bucket:str, upload_path:str
    This method upload files from tmp folder to s3 at specified prod bucket and path
    """
    s_3 = boto3.client('s3')
    s_3.upload_file(local_path, prod_bucket, upload_path)
    LOGGER.info("uploaded file: " + local_path + " at : " + upload_path)
    return upload_path
    
def upload_to_s3(slack_message, prod_bucket, key):
    """
    Input - Slack_message:str, prod_bucket:str, key:str
    This method creates a text file with the slack message on s3 bucket
    at specified key
    """
    s_3 = boto3.resource('s3')
    s_3.Bucket(prod_bucket).put_object(Key=key, Body=slack_message)
    return()

def send_slack_email(conf, message):
    """
    Input - conf:Config class object, message:str
    This method sends the message to slack
    """
    # The Slack channel to send a message to stored in the slackChannel environment variable
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key_slack())
    credentials = json.loads(keys['SecretString'])
    slack_channel = conf.get_slack_channel_QC()
    hook_url = credentials['hook_url']

    slack_message = {
        'channel': slack_channel,
        'text': message,
        "username": conf.get_slack_username(),
        "icon_emoji": conf.get_slack_emoji()
    }
    # print(slack_message)
    req = Request(hook_url, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        LOGGER.info("Message posted to %s", slack_message['channel'])
    except HTTPError as error:
        LOGGER.error("Request failed: %d %s", error.code, error.reason)
    except URLError as error:
        LOGGER.error("Server connection failed: %s", error.reason)

def execute_query(query, connection, columns):
    """
    Input - query:str, connection:pymysql.connect, store_number:str, columns:list,
            local_path:str, prod_bucket:str, upload_path:str
    This method loads data from s3 at specified path to aurora
    in specified database and table
    """
    with connection.cursor() as cur:
        cur.execute(query)
        try:

            data = cur.fetchall()
        except:
            data = ''
        data = pd.DataFrame(list(data), columns=columns)
    LOGGER.info("Query Executed Successfully")
    return (data)


def lambda_handler(event, context):
    ENV = os.getenv('ENV')
    
    with open('core_forecast_slack_alert_redundancy_inference_qc/config.json') as config_params:
        config_dict = json.load(config_params)[ENV]
        conf = Config.from_event(config_dict)

    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf.get_secret_key())
    credentials = json.loads(keys['SecretString'])

    
    LOGGER.info("Connecting to RDS")
    connection = pymysql.connect(host=conf.get_replica_host_link(),
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf.get_database())
    
    LOGGER.info("Successfully connected to RDS")

    goodstores_list = open('core_forecast_slack_alert_redundancy_inference_qc/forecastqc_passed_stores.sql', 'r')
    goodstores = goodstores_list.read()
    print (goodstores, conf.get_columns_forecast_qc())
    goodstores = execute_query(goodstores, connection, conf.get_columns_forecast_qc()
                                    )
    goodstores_list.close()

    failedstores_list = open('core_forecast_slack_alert_redundancy_inference_qc/forecastqc_failed_stores.sql', 'r')
    failedstores = failedstores_list.read()

    failedstores = execute_query(failedstores, connection, conf.get_columns_forecast_qc()
                                         )
    failedstores_list.close()

    connection.commit()
    print(len(failedstores))
    if len(goodstores.index)==0:
        send_slack_email(conf, 'All the stores has failed. Pipeline will be stopped')
        upload_to_s3("Failed", conf.get_prod_bucket(), conf.get_forecast_qc_slack_upload_msg_key())
    if len(failedstores)>0:
        failedstores_list = failedstores['store_number'].values.tolist()
        send_slack_email(conf, 'Number of stores FAILED in Redundancy QC are- ' + str(len(failedstores_list)))
        send_slack_email(conf, 'Number of stores Passed in Redundancy QC are- ' + str(len(goodstores)))
        upload_to_s3("Success", conf.get_prod_bucket(), conf.get_forecast_qc_slack_upload_msg_key())
        goodstores.to_csv(conf.get_local_path_forecast_qc(), index=False)
        upload_file_to_s3(conf.get_local_path_forecast_qc(), conf.get_prod_bucket(), conf.get_upload_key_forecast_qc())
    
    else:
        upload_to_s3("Success", conf.get_prod_bucket(), conf.get_forecast_qc_slack_upload_msg_key())
        send_slack_email(conf, 'Redundancy Forecast QC has passed for all the stores. Triggering CSVtoJSON')
        print("Done")
        goodstores.to_csv(conf.get_local_path_forecast_qc(), index=False)
        upload_file_to_s3(conf.get_local_path_forecast_qc(), conf.get_prod_bucket(), conf.get_upload_key_forecast_qc())
   
    print("succeeded")
