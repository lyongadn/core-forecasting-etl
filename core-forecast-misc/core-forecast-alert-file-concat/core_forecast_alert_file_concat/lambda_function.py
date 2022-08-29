import sys
import boto3
import pandas as pd
import json
import os
import datetime
import pytz

def join_files(bucket,files):
    """
    If there are multiple files under same prefix than all the files will
    be combined into single dataframe and then returned.
    """
    print(f"{len(files)} files found.")
    final_data = pd.DataFrame()
    for file_x in files:
        try:
            if not file_x.endswith('/'):
                print("Reading file :- ",file_x)
                data = pd.read_csv(f"s3://{bucket}/{file_x}")
                final_data = final_data.append(data)
        except Exception as e:
            print("No data for file",file_x, e)
            continue
    return final_data

def upload_alerts_s3(conf, alerts_df, batch, forecast_type):
    """
    Input is a dataframe, which we convert to a parquet file and 
    upload it to the desired S3 path
    """
    local_path = conf['alert_local_path'].replace('__batch__',batch)
    alerts_df.to_parquet(local_path,index=False)
    # print(alerts_df.head())
    # getting the EST date to create folder for partitioning. 
    EST = pytz.timezone('America/New_York') 
    gen_date = str(datetime.datetime.now(EST).date()) 
    
    # Replace the upload path with the corresponding variables
    upload_path = conf['alert_upload_path'].replace('__forecast__',forecast_type) \
            .replace('__gen_date__',gen_date) \
            .replace('__batch__',batch)
    print(upload_path)
    s_3 = boto3.client('s3')
    s_3.upload_file(local_path, conf['prod_bucket'], upload_path)
    return True

def lambda_handler(event, context):
    ENV = os.getenv('ENV')

    with open('core_forecast_alert_file_concat/config.json') as config_params:
        conf = json.load(config_params)[ENV]
    
    # We concatenate the csv files into a single dataframe
    final_data = join_files(conf['prod_bucket'],event['file_list'])
    # print(final_data.head())
    # Upload the file into an S3 path
    upload_alerts_s3(conf, final_data, event['batch_name'], event['forecast_type'])
    print("Sent successfully")
    
    return {
        "Status":"200",
        "Message":"Files concatenated successfully"
        }