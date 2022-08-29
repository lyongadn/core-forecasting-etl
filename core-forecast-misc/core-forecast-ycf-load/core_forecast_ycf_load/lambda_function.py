import json
import pandas as pd
import csv
import boto3
import json
import os
import logging
import sys
from pytz import timezone
import pymysql
import time
import datetime
from boto3.s3.transfer import S3Transfer
pymysql.install_as_MySQLdb()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    print("event", event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = (event['Records'][0]['s3']['object']['key'])
    #bucket = 'prod-q-forecasting-artifacts'
    #key = 'forecastycf/00001/ycf-00001.json'
    
    print ('bucket name ', bucket)
    print ('Key name', key)
    
    a,b,c = key.split("/")
    c = c.split('.')[0] + '.csv'
    client = boto3.client('s3')
    client.download_file(bucket, key, '/tmp/file.json')
    #client = boto3.client('s3')

    print ('bucket name', bucket)
    print ('Key name', key)
    logger.info('FILE DOWNLOADED')
    with open('/tmp/file.json', 'r') as js:
        json_object = json.load(js)
        print ("JSON Object was loaded")
        print (json_object)
        #generation_date = datetime.datetime.now().strftime("%Y-%m-%d")
        df = pd.DataFrame(columns=['StoreNumber','ItemCode','YieldCorrectionFactor'],data=json_object)
        df.columns = ['location_num','ingredient_id','ycf']
        df['generation_date'] = datetime.datetime.now(tz = timezone('US/Pacific')).strftime("%Y-%m-%d")
        df['location_num'] = df['location_num'].apply(lambda x : x.lstrip("0"))

        df.to_csv('/tmp/'+ c ,index=False)
        print("Read Body")

    prodbucket = 'prod-q-forecasting-artifacts'
    #s3://test-q-forecasting-artifacts/ycf/00001/ycf-00001.csv
    client.put_object(Bucket = prodbucket,Body=open('/tmp/'+ c, 'rb') ,Key= "ycf/" + b+"/"+c)
    print('JSON file was uploaded to S3')

    connection = pymysql.connect(host=os.environ['host_link'],
                                 user=os.environ['user_name'],
                                 password=os.environ['rds_password'],
                                 db='ml_preprod'
                                 )
    print("Connection succeeded")

    with connection.cursor() as cursor:

        str = '''LOAD DATA FROM S3 prefix 's3-us-east-1://prod-q-forecasting-artifacts/ycf/''' + \
              b + ''''
						        INTO TABLE ml_preprod.ycf_ingredients_monthly
						                  FIELDS TERMINATED BY ','
							                   ENCLOSED BY '"'
								                 LINES TERMINATED BY '\\n'
									               IGNORE 1 LINES;'''


        print(str)
        cursor.execute(str)
        connection.commit()
