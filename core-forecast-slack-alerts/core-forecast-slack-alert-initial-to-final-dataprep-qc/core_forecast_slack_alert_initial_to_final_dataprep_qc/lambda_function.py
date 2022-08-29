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

def load_to_aurora(query, connection, prod_bucket, upload_path, database, table):
    """
    Input - query:str, connection:pymysql.connect, prod_bucket:str,
            upload_path:str, database:str, table:str
    This method loads data from s3 at specified path to aurora in specifeid database and table
    """
    with connection.cursor() as cur:
        query = query.replace('__prod_bucket__', prod_bucket)
        query = query.replace('__upload_path__', upload_path)
        query = query.replace('__database_name__', database)
        query = query.replace('__table__', table)
        cur.execute(query)
        connection.commit()
    LOGGER.info("loaded results from : \n" + upload_path + "at: " + table)
    return "success"

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
        data = cur.fetchall()
        data = pd.DataFrame(list(data), columns=columns)
    LOGGER.info("Query Executed Successfully")
    return (data)


def lambda_handler(event, context):
    ENV = os.getenv('ENV')
    
    with open('core_forecast_slack_alert_initial_to_final_dataprep_qc/config.json') as config_params:
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

    initial_final_QC = open('core_forecast_slack_alert_initial_to_final_dataprep_qc/initial_final_dollars.sql', 'r')
    Dollar_QC = initial_final_QC.read()
    Dollar_QC = execute_query(Dollar_QC, connection, conf.get_columns_initial_final_slack())
    initial_final_QC.close()

    initial_final_items_QC = open('core_forecast_slack_alert_initial_to_final_dataprep_qc/initial_final_items.sql', 'r')
    Item_QC = initial_final_items_QC.read()
    Item_QC = execute_query(Item_QC, connection, conf.get_columns_initial_final_slack())
    initial_final_items_QC.close()

    connection.commit()

    print(Dollar_QC.head())
    print("Dollar_QC check columns")
    if (((Dollar_QC['dailyqc'] == 0).any()) or
            ((Item_QC['dailyqc'] == 0).any())):

        failed_ds_location_num = Dollar_QC[Dollar_QC['dailyqc'] == 0]['location_num'].values.tolist()
        failed_ic_location_num = Item_QC[Item_QC['dailyqc'] == 0]['location_num'].values.tolist()
        upload_to_s3("Failed", conf.get_prod_bucket(), conf.get_initial_final_slack_upload_msg_key())
        print("Pipeline Failed")            
        print("failed location_num for DS:")
        print(failed_ds_location_num)

        print("failed location_num for IC:")
        print(failed_ic_location_num)
        send_slack_email(conf, "Initial and final data is not matching")
        failed_final = list(set(failed_ic_location_num+failed_ds_location_num))
        send_slack_email(conf,"Initial-Final Data QC failed for stores are :" + str(failed_final))
        #if len(failed_ic_location_num) >0 :
        #    send_slack_email(conf,"Initial-Final-Item-Data QC failed for number of stores are :" + str(len(failed_ic_location_num)))
        #if len(failed_ds_location_num) >0 :
        #    send_slack_email(conf,"Initial-Final-Dollars-Data QC failed for number of stores are :" + str(len(failed_ic_location_num)))

    else:
        upload_to_s3("Success", conf.get_prod_bucket(), conf.get_initial_final_slack_upload_msg_key())
        print("Pipeline Success")
        send_slack_email(conf, "Initial and final data is matching")
    print("Succeded")
