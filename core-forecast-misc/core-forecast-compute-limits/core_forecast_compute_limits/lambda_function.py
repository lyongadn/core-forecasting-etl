import sys
import logging
import time
import re
import os
import pandas as pd
import io
from io import BytesIO
import boto3
from boto3 import client
from collections import OrderedDict
import csv
import json
import time
import datetime
import numpy as np
from boto3.s3.transfer import S3Transfer
import pymysql
pymysql.install_as_MySQLdb()
from decimal import Decimal
from boto3.s3.transfer import S3Transfer
from io import StringIO

#from multiprocessing import Pool, cpu_count

import glob
import shutil

def execute_query(query_name, connection, store_number, limit, columns):
    """
    This method executes the limits Query and returns it as a Dataframe
    """
    query = open(query_name, 'r').read()
    query = query.replace('__store_num__', str(store_number)).replace('__limit__', limit)

    with connection.cursor() as cur:
        #print (query)
        cur.execute(query)
        data = cur.fetchall()
        final_data = pd.DataFrame(list(data), columns=columns)
        connection.commit()
    
    return final_data

def compute_limits(demand,storenumber):
    """
    This method calculates the statistics like iqr, mean, median, max, min and std
    for each weekday based on the past 56 days of data
    """

    data= demand.groupby(['weekday'])
    limits = pd.DataFrame()
    print('location_num',storenumber)
    for name,loc_data in data :
        demand = loc_data['demand']        
        location_num = storenumber
        weekday = loc_data['weekday'].unique()
        min_value = pd.to_numeric(demand).min()
        max_value = pd.to_numeric(demand).max()
        mean = pd.to_numeric(demand).mean()
        median = pd.to_numeric(demand).median()
        std = pd.to_numeric(demand).std()
        iqr = np.subtract(*np.percentile(demand,[75, 25]))  
        min_max_limit = pd.DataFrame(columns=['location_num','weekday','min_value','max_value','mean','median','std','iqr'],
                                data={'location_num':location_num,'weekday':weekday,'min_value':min_value,'max_value':max_value,'mean':mean,'median':median,'std':std,'iqr':iqr},index=[0])
        limits = limits.append(min_max_limit, ignore_index=True) 
    
    ################## Updating values for all the months and weekdays ###################################
    print(limits.head())
    wday_df = pd.DataFrame({'weekday': [0,1,2,3,4,5]})
    Median_value = limits[limits['median'] != 0]['median'].median()
    print('Median_value',Median_value)
    limits = pd.merge(wday_df,limits,on=['weekday'],how='outer')
    limits = limits.fillna(0)
    min_limit = 0.15*(pd.to_numeric(Median_value))
    print('min_limit',min_limit)
    print('length',len(limits.loc[:,'median'][limits['median']<=min_limit]))
    if len(limits.loc[:,'median'][limits['median']<=min_limit])>0:
            print('inside if')
            limits.loc[:,'median'][limits['median']<=min_limit] = Median_value 
    limits['location_num'] = storenumber
    limits = limits[['location_num','weekday','min_value','max_value','mean','median','std','iqr']]
    print(len(limits))
    return(limits)


def lambda_handler(event, context):
    start = time.time()

    ENV = os.getenv('ENV')

    # Store number is passed through the event of the lambda function.
    store_number = event['store_number']
    
    with open('core_forecast_compute_limits/config.json') as config_params:
        conf = json.load(config_params)[ENV]
    
    # We get the secrets from the Secrets Manager to create an RDS connection
    kms_manager = boto3.client('secretsmanager', region_name=conf['region_name'])
    keys = kms_manager.get_secret_value(SecretId=conf['secret_key'])
    credentials = json.loads(keys['SecretString'])

    connection = pymysql.connect(host=conf['reader_host_link'],
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf['database'])
    print("Connection of Replica suceeded")
    
    # We are defining the lookback period based on the forecast type. 8 weeks for LSTM, 5 weeks for Baseline
    limit = '56'
    if event['forecast_type'] == 'baseline':
        limit = '35'

    print(event['forecast_type'], limit)
    
    # Create an S3 connection using boto3
    conn = boto3.client('s3')
    transfer = S3Transfer(conn)
    # We iterate over the Query Names of the 4 metrics and compute the limits of each metric for the store
    for query in conf['query_list']:
        # Get the metric name from the name of the Query
        metric_name = query.split('/')[1].split('_')[0]
        
        # Execute the Query that returns the weekday and salestotal for the metric from the ml_preprod daily actuals 
        # tables grouped by weekday for the past 56 days
        limit_data = execute_query(query, connection, store_number, limit, conf['limit_columns'])
        print('location_num',store_number)
        print(f"{metric_name}_data", len(limit_data))

        # Compute limits like median, max, min, iqr, mean, standard deviation and store it in a dataframe
        final_limits_data = compute_limits(limit_data,store_number)
        local_path = conf['local_path'].replace('__metric__', metric_name).replace('__store_num__',store_number)
        
        # Upload the limits dataframe as a csv to the concerned location for every metric
        final_limits_data.to_csv(local_path, index=False, header=False)
        upload_path = conf['upload_path'].replace('__metric__', metric_name).replace('__store_num__',store_number)
        transfer.upload_file(local_path, conf['output_bucket'], upload_path)

    end = time.time()
    print("Total time to run the script", end - start)