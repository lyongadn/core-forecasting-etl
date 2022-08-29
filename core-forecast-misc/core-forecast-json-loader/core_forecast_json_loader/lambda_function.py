import boto3
from boto3.s3.transfer import S3Transfer
from datetime import datetime,timedelta
import pandas as pd
import pymysql
import time
import os
import json
import logging
import pytz

ENV = os.getenv('ENV')

def setup_logging():
    '''
    This method sets up the logger with required format
    '''
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    handler = logging.StreamHandler()
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

LOGGER = setup_logging()

def insert_into_dynamodb(table_name,path,generation_date):
    db = boto3.client('dynamodb')
    EST = pytz.timezone('America/New_York') 
    curdate = datetime.now(EST) + timedelta(hours=60) # Adding 60 hours to the Time To Live column 
    curdate = curdate.strftime("%d.%m.%Y %H:%M:%S")
    pattern = '%d.%m.%Y %H:%M:%S'
    epoch = str(int(time.mktime(time.strptime(curdate, pattern))))

    try:
        db.put_item(
            TableName=table_name,
            Item={
                "file_name":{
                    "S":path
                } ,
                "generation_date":{
                    "S":generation_date
                } ,
                "ttl_epoch":{
                    "N":epoch
                } 
            }
        )
        LOGGER.info("Data upload to DynamoDB successful for path %s", path)
        return True
    except Exception as e:
        LOGGER.info("Data upload to DynamoDB failed for path %s", path)
        LOGGER.info("Upload Error is %s", e)
        return False

def load_to_aurora(conf, s3_file):
    
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf['secret_key'])
    credentials = json.loads(keys['SecretString'])
    LOGGER.info("Fetching credentials for rds")

    connection_write_demand = pymysql.connect(host=credentials['host'],
                                            user=credentials['username'],
                                            password=credentials['password'],
                                            db=conf['database'])
    print("connection final load succeeded")
     
    try:
        with connection_write_demand.cursor() as final:        
            queryToLoadIntoAurora = f"""LOAD DATA FROM S3 file '{s3_file}'
                                    INTO TABLE {conf['database']}.{conf['table_name']}
                                    FIELDS TERMINATED BY ','
                                        ENCLOSED BY '"'
                                            LINES TERMINATED BY '\\n'
                                                IGNORE 1 LINES;"""
            
            final.execute(queryToLoadIntoAurora)
            connection_write_demand.commit()
        connection_write_demand.close()
        LOGGER.info("loading succeeded for file %s", s3_file)
        return True
    except Exception as e:
        LOGGER.info("Loading failed for file %s . Error in upload %s", s3_file, e)
        return False



def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = (event['Records'][0]['s3']['object']['key'])
    location_num = key.split('/')[2]
    business_date = key.split('/')[-1].split('demand-')[1][:10]
    #ENV = 'test'
    print(ENV)
    with open('core_forecast_json_loader/config.json') as config_params:
        conf = json.load(config_params)[ENV]
    
    s3 = boto3.client('s3')
    file_x = s3.get_object(Bucket=bucket, Key=key)

    #Shortening DynamoDB file name
    file_name = f"{location_num}/demand-{business_date}.json"

    business_date = datetime.strptime(business_date, "%m-%d-%Y")

    business_date = datetime.strftime(business_date, "%Y-%m-%d")

    # Inserts the json key, generation date and TTL param into the DynamoDB
    # for consumption by Postgres DB loader
    insert_into_dynamodb(
        table_name=conf['dynamodb_table'],
        path=file_name,
        generation_date=file_x['ResponseMetadata']['HTTPHeaders']['x-amz-meta-generation_date'].split('T')[0]
    )
    
    # Creates the csv with data on the latest demand json's.
    data = pd.DataFrame(columns =['location_num','business_date','forecast_type','generation_date','row_entry_date'],\
                        data={'location_num':location_num, 'business_date':business_date,\
                             'forecast_type':file_x['ResponseMetadata']['HTTPHeaders']['x-amz-meta-forecast_type'],\
                             'generation_date':file_x['ResponseMetadata']['HTTPHeaders']['x-amz-meta-generation_date'].split('T')[0],
                            'row_entry_date':datetime.strftime(datetime.now(pytz.timezone('America/New_York')), "%Y-%m-%d")},index=[0])

    local_path = conf['local_path'].replace('__business_date__',business_date)
    data.to_csv(local_path, sep=',', index=False)

    transfer = S3Transfer(s3)

    # Upload key of the file we created above with the data about the latest demand json's.
    key_1=conf['upload_path'].replace('__location_num__',location_num)
    key_1=key_1.replace('__business_date__',business_date)
    print("upload key is", key_1)
    transfer.upload_file(local_path, conf['output_bucket'], \
                     key_1)
    
    s3_file = f"s3-{conf['region_name']}://{conf['output_bucket']}/{key_1}"
    
    # Loads the csv created in the above process to the Aurora table for logging.
    insert_status = load_to_aurora(conf, s3_file)
    
    # If insert is successful, we will delete the file we created earlier.
    if insert_status:
        time.sleep(0.03)
        s3.delete_object(Bucket=conf['output_bucket'], Key=key_1)
        return "Load Successful"

    return "Load Unsuccessful. File not deleted"
