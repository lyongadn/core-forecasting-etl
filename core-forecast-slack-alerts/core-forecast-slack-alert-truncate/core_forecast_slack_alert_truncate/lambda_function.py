import os
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
def upload_to_s3(slack_message, prod_bucket, key):
    """
    Input - Slack_message:str, prod_bucket:str, key:str
    This method creates a text file with the slack message on s3 bucket
    at specified key
    """
    s_3 = boto3.resource('s3')
    s_3.Bucket(prod_bucket).put_object(Key=key, Body=slack_message)

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

def execute_query(query, connection):
    """
    Input - query:str, connection:pymysql.connect, store_number:str, columns:list,
            local_path:str, prod_bucket:str, upload_path:str
    This method loads data from s3 at specified path to aurora
    in specified database and table
    """
    with connection.cursor() as cur:
        cur.execute(query)
        try:
            data = str(cur.fetchall()[0][0])
        except:
            data = '0'
    LOGGER.info("Query Executed Successfully")
    return (data)


def lambda_handler(event, context):
    ENV = os.getenv('ENV')
    
    with open('core_forecast_slack_alert_truncate/config.json') as config_params:
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

    itemlevelcount_15min_QC = open('core_forecast_slack_alert_truncate/itemlevelcount_15min_QC.sql', 'r')
    itemlevelcount_QC = itemlevelcount_15min_QC.read()
    item_qc = execute_query(itemlevelcount_QC, connection)
    itemlevelcount_15min_QC.close()

    expected_products_QC = open('core_forecast_slack_alert_truncate/expected_products_QC.sql', 'r')
    expected_products = expected_products_QC.read()
    expected_pro = execute_query(expected_products, connection)
    expected_products_QC.close()

    lookup_ingredients_QC = open('core_forecast_slack_alert_truncate/lookup_ingredients_QC.sql', 'r')
    lookup_ingredients = lookup_ingredients_QC.read()
    lookup_ingre = execute_query(lookup_ingredients, connection)
    lookup_ingredients_QC.close()
    
    if lookup_ingre != '1':
        lookup_backup = open('core_forecast_slack_alert_truncate/lookup_ingredients_backup.sql','r')
        lookup_backup_read = lookup_backup.read()
        print(lookup_backup_read)
        lookup_backup_execute = execute_query(lookup_backup_read, connection)
        print("loaded lookup_backup data")
        lookup_backup.close()

    connection.commit()


    if (item_qc == '0'
            and expected_pro == '0'
            and lookup_ingre == '1'):

        upload_to_s3("Success", conf.get_prod_bucket(), conf.get_truncate_slack_upload_key())
        print ("Data was Truncated successfully ")
        send_slack_email(conf, "Data was Truncated successfully")

    else:
        upload_to_s3("Failed", conf.get_prod_bucket(), conf.get_truncate_slack_upload_key())
        print ("Data was not Truncated successfully")
        send_slack_email(conf, "Data was not Truncated successfully")
    print("success")
    
    client = boto3.client('lambda', region_name='us-east-1')
    resp = client.invoke(
           FunctionName= 'baseline_lookup_ycf',
           InvocationType='Event'
       )