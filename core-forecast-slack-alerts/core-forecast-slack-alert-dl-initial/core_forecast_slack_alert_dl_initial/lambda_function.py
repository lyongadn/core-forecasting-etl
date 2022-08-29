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
import time
from time import sleep
# handler = logging.StreamHandler(sys.stdout)
# LOGGER.addHandler(handler)

client = boto3.client('secretsmanager')

def load_to_aurora(query, connection_load_final, prod_bucket, upload_path, database, table):
    """
    Input - query:str, connection:pymysql.connect, prod_bucket:str,
            upload_path:str, database:str, table:str
    This method executes the query with required parameters, saves it in local path
    and uploads the file on s3
    """
    with connection_load_final.cursor() as cur:
        query = query.replace('__prod_bucket__', prod_bucket)
        query = query.replace('__upload_path__', upload_path)
        query = query.replace('__database_name__', database)
        query = query.replace('__table__', table)
        cur.execute(query)
        connection_load_final.commit()
    LOGGER.info("loaded results into table "+table+" from: " + upload_path)
    return "success"

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
        data = cur.fetchall()
        data = pd.DataFrame(list(data), columns=columns)
        connection.commit()
    LOGGER.info("Query Executed Successfully")
    return (data)


def lambda_handler(event, context):
    ENV = os.getenv('ENV')
    
    with open('core_forecast_slack_alert_dl_initial/config.json') as config_params:
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
    
    connection_load_final = pymysql.connect(host=credentials['host'],
                                            user=credentials['username'],
                                            password=credentials['password'],
                                            db=conf.get_database())
    
    LOGGER.info("Successfully connected to RDS")

    crawler_fixed_load = open('core_forecast_slack_alert_dl_initial/load_query.sql', 'r')
    load_crawler = crawler_fixed_load.read()
    load_to_aurora(load_crawler, connection_load_final, conf.get_prod_bucket(),\
                    (conf.get_fixed_crawler_csv_key()),\
                    conf.get_daily_database(), conf.get_fixed_crawler_table())
    connection_load_final.commit()
    crawler_fixed_load.close()
    
    print("sleeping")
    crawler_update = open('core_forecast_slack_alert_dl_initial/inference_input_crawler.sql', 'r')
    crawler_query = crawler_update.read()
    sleep(10)
    print(crawler_query)
    crawler = execute_query(crawler_query, connection, conf.get_columns_inference_input_crawler())
    crawler_inference = pd.DataFrame(columns=['store_number'])
    crawler_inference['store_number'] = crawler[crawler['status']<=int(conf.get_status_to_run_forecast_for())]['store_number']
    print (crawler_inference.head() ,"printing head")
    crawler.to_csv(conf.get_local_crawler_path() , sep=',',index= False)
    crawler_inference.to_csv(conf.get_local_crawler_path_forecast() , sep=',',index= False)
    if len(crawler.dropna()) == 0:
        print("crawler file is empty")
    else:
        upload_file_to_s3( conf.get_local_crawler_path() , conf.get_prod_bucket(), \
                               conf.get_upload_crawler_path())
    if len(crawler_inference.dropna()) == 0:
        print("crawler inference file is empty")
    else:
        upload_file_to_s3( conf.get_local_crawler_path_forecast() , conf.get_prod_bucket(), \
                               conf.get_upload_forecast_crawler_path())
    crawler_update.close()
    
    initial_datalake_QC = open('core_forecast_slack_alert_dl_initial/initial_datalake_dollars.sql', 'r')
    initial_datalake = initial_datalake_QC.read()
    print (initial_datalake, conf.get_columns_datalake_initial_slack())
    initial_datalake = execute_query(initial_datalake, connection, conf.get_columns_datalake_initial_slack()
                                    )
    initial_datalake_QC.close()

    initial_datalake_item_QC = open('core_forecast_slack_alert_dl_initial/initial_datalake_items.sql', 'r')
    initial_datalake_item_query = initial_datalake_item_QC.read()
    initial_datalake_item = execute_query(initial_datalake_item_query, connection, conf.get_columns_datalake_initial_slack()
                                         )
    initial_datalake_item_QC.close()

    if (((initial_datalake['dailyqc'] == 0).any()) or
            ((initial_datalake_item['dailyqc'] == 0).any())) :

        failed_ds_location_num = initial_datalake[initial_datalake['dailyqc'] == 0]['location_num'].values.tolist()
        failed_ic_location_num = initial_datalake_item[initial_datalake_item['dailyqc'] == 0]['location_num'].values.tolist()
        print("Pipeline Failed")            
        print("failed location_num for DS:")
        print(failed_ds_location_num)

        print("failed location_num for IC:")
        print(failed_ic_location_num)

        if len(failed_ic_location_num) >0 :
            print("test")
            send_slack_email(conf,"Datalake data is not matching with initial load at Item level count for stores :" + str(len(failed_ic_location_num)))
        if len(failed_ds_location_num) >0:
            print("test1")
            send_slack_email(conf,"Datalake data is not matching with initial load at Dollarsales and transcount level for stores :" + str(len(failed_ds_location_num)))

    else:
        print("Pipeline Success")
        send_slack_email(conf,"Datalake data is matching with initial load")

    crawler_parameter = pd.DataFrame()
    success_ds_location_num = set(initial_datalake[initial_datalake['dailyqc'] == 1]['location_num'].tolist())
    print(success_ds_location_num, 'Print success')
    success_ic_location_num = set(initial_datalake_item[initial_datalake_item['dailyqc'] == 1]['location_num'].tolist())
    print(success_ic_location_num, 'Print success item')
    crawler_parameter['store_number'] = list(success_ds_location_num.intersection(success_ic_location_num))
    crawler_parameter['store_number'] = crawler_parameter['store_number'].astype(str)
    crawler_parameter['store_number'] = crawler_parameter['store_number'].str.zfill(5)
    print(crawler_parameter.head())
    print("Starting final crawler upload")
    crawler_parameter.to_csv(conf.get_local_path_slack_datalake(), sep=',' , header=True, index=False)
    upload_file_to_s3(conf.get_local_path_slack_datalake() ,conf.get_prod_bucket(), conf.get_datalake_initial_slack_upload_crawler_path())
    upload_file_to_s3(conf.get_local_path_slack_datalake() ,conf.get_prod_bucket(),\
                      conf.get_datalake_initial_slack_upload_crawler_path_remod())
    print("Succeded")
    print("This has completed")
    print("starting  business_dates upload")
    if len(crawler_parameter.index)>0:
        initial_dollars_dates = open('core_forecast_slack_alert_dl_initial/dollars_max_business_dates.sql', 'r')
        initial_dollars_max_dates = initial_dollars_dates.read()
        initial_dollars_max = execute_query(initial_dollars_max_dates, connection, conf.get_max_business_date_columns()
                                    )
        #initial_dollars_max = pd.DataFrame(columns=conf.get_max_business_date_columns())
        initial_dollars_max.to_csv(conf.get_local_bd_path() , sep=',',index= False)
        upload_file_to_s3( conf.get_local_bd_path() , conf.get_prod_bucket(), \
                           conf.get_upload_max_bd_path())
        initial_dollars_dates.close()

        initial_item_dates = open('core_forecast_slack_alert_dl_initial/item_max_business_dates.sql', 'r')
        initial_item_max_dates = initial_item_dates.read()
        initial_item_max = execute_query(initial_item_max_dates, connection, conf.get_max_business_date_columns()
                                    )
        #initial_item_max = pd.DataFrame(columns=conf.get_max_business_date_columns())
        initial_item_max.to_csv(conf.get_local_bd_item_path() , sep=',',index= False)
        upload_file_to_s3( conf.get_local_bd_item_path() , conf.get_prod_bucket(), \
                           conf.get_upload_max_item_path())
        initial_item_dates.close()
        connection.commit()
        upload_to_s3("Success", conf.get_prod_bucket(), conf.get_datalake_initial_slack_upload_msg_key())
    else:
        upload_to_s3("Failed", conf.get_prod_bucket(), conf.get_datalake_initial_slack_upload_msg_key())
        send_slack_email(conf,"All the stores has failed the initial datalake QC- Pipeline will be using last correct data points to make forecast")