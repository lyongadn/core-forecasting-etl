import os
import json
from datetime import datetime,date,timedelta
import pandas as pd
import boto3
import pymysql
import pytz
pymysql.install_as_MySQLdb()

ENV = os.getenv('ENV')

def get_secret(secret_name):
    """
      This function fetches the required secrets from AWS Secrets Manager to get the
      RDS url, username and password to connect to Aurora Database.
    """
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret_string = get_secret_value_response['SecretString']

    except Exception as exc:
        logger.info(f'Error: {exc}')

    return json.loads(secret_string)

def load_to_aurora(query, connection, s3_prefix, dbtable):
    """
        Input - query:str, connection:pymysql.connect, prod_bucket:str,
                upload_path:str, database:str, table:str
        This method executes the query with required parameters, saves it in local path
        and uploads the file on s3
    """
    with connection.cursor() as cur:
        query = query.replace('__s3_prefix__', s3_prefix)
        query = query.replace('__dbtable__', dbtable)
        cur.execute(query)
        connection.commit()
    print("loaded results into table "+dbtable+" from: " + s3_prefix)
    return "success"

def fetch_saturdayoff_stores(connection,dbtable):
    """
    This function will fetch the list of stores that will be off on saturdays from Aurora database.
    """
    query = f"select * from {dbtable}"

    with connection.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        stores_list = [x[0] for x in list(data)]
        connection.commit()
    return stores_list

def saturdays_check(store_date,store_list):
    """
    For the given date and store_num, this function will check
    if the date is saturday for saturday off stores or not.
    """
    bdate = datetime.strptime(store_date['prediction_date'],"%Y-%m-%d").date()
    return bdate.weekday()==5 & store_date['location_num'] in store_list

def holiday_check(bdate):
    """
    This function will check whether the given day is a holiday or not
    """
    bdate = datetime.strptime(bdate,"%Y-%m-%d").date()
    year, month, day = bdate.year, bdate.month, bdate.day
    # Checking for thanksgiving which is fourth thursday in November month
    if month == 11 and 28>=day>=22 and date(year,month,day).weekday() == 3:
        check = True
    # Checking for Christmas
    elif month == 12 and day==25:
        check = True
    else:
        check = False
    return check

def sunday_check(bdate):
    """
    This function will check whether the given day is a sunday or not
    """
    bdate = datetime.strptime(bdate,"%Y-%m-%d").date()
    return bdate.weekday() == 6

def list_files(bucket,prefix):
    """
    This function will list all the files in one prefix and return a list of those files
    """
    s3_client = boto3.client('s3')
    files = []
    res = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
    is_truncated = res['IsTruncated']
    # print(res)

    files.extend(res.get('Contents',[]))
    while is_truncated:
        res = s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    ContinuationToken=res['NextContinuationToken']
                )
        is_truncated = res['IsTruncated']
        files.extend(res.get('Contents',[]))
    return files

def join_files(bucket,files):
    """
    If there are multiple files under same prefix than all the files will
    be combined into single dataframe and then returned.
    """
    print(f"{len(files)} files found.")
    final_data = pd.DataFrame()
    for file_x in files:
        if not file_x['Key'].endswith('/'):
            print("Reading file :- ",file_x['Key'])
            data = pd.read_csv(f"s3://{bucket}/{file_x['Key']}")
            final_data = final_data.append(data)
    return final_data

def send_message_to_sns(message,topic_name):
    """
    This function will send a message on SNS topic.
    """
    print(f'sending sns message: {message}')
    sns     = boto3.resource('sns')
    topic   = sns.Topic(topic_name)
    topic.publish(Message=json.dumps(message))

def compose_slack_message(conf,message,icon_emoji):
    """
    This function will compose a message to be sent on slack channel and sends it on SNS topic.
    """
    slack_message ={
        'channel': conf['slack_channel'],
        'username': f"{ENV}-baseline-daily-sales-algorithm-qc",
        'icon_emoji': f"{icon_emoji}",
        "blocks": [
        {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"{message}"
            }
        }
    ]
    }
    send_message_to_sns(slack_message,conf['slack_alerts_topic'])

def lambda_handler(event,context):
    EST = pytz.timezone('America/New_York') 

    with open('core_forecast_qc_baseline_daily_sales_algorithm/config.json') as config_params:
        conf = json.load(config_params)[ENV]

    secret = get_secret(conf['secret_name'])

    conf['prediction_dates'] = {
            "2weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(16,20)],
            "3weeks_ahead": [str(datetime.now(EST).date()+timedelta(days=x)) for x in range(21,25)]
        }
        
    print(conf['prediction_dates'])
    
    connection = pymysql.connect(host=secret['host'],
                                 user=secret['username'],
                                 password=secret['password'],
                                 database=conf['database'])

    pred_dates = conf['prediction_dates']['2weeks_ahead'].copy()
    pred_dates.extend(conf['prediction_dates']['3weeks_ahead'])
    pred_dates_df = pd.DataFrame([[x,1] for x in pred_dates],columns=['prediction_date','x'])

    stores_df = pd.read_csv(conf['store_list_filepath'])
    stores_df['x'] = 1

    qc_df = stores_df.merge(pred_dates_df,on='x').drop('x',axis=1)
    print("Fetching Saturday Off Stores.")
    saturday_off_stores = fetch_saturdayoff_stores(connection,conf['saturday_off_stores_table'])

    print("Fetching BASELINE_dates data.")
    dates_bucket = conf['bucket']
    dates_prefix = conf['baseline_dates_path']
    files = list_files(dates_bucket,dates_prefix)
    dates_df = join_files(dates_bucket,files)

    dates_qc = dates_df.groupby([
                    'location_num',
                    'prediction_date',
                    'lookup_weeks',
                    'forecast_avg_dr'
                ]).nunique()['business_date']
    dates_qc.name = 'row_count'
    dates_qc = dates_qc.reset_index()
    dates_qc['dates_qc'] = (  (dates_qc['row_count'] <= (dates_qc['lookup_weeks']-3))  & \
                          (dates_qc['row_count'] == dates_qc['forecast_avg_dr']) & \
                          (dates_qc['prediction_date'].isin(pred_dates)) & \
                          ~ dates_qc.apply(saturdays_check,axis=1,args=(saturday_off_stores,)) & \
                          ~ dates_qc['prediction_date'].apply(sunday_check) & \
                          ~ dates_qc['prediction_date'].apply(holiday_check)
                        )
    # dates_qc1 = dates_qc.groupby('location_num').all()['dates_qc'].reset_index()
    print("Final dates qc results:-")
    print(dates_qc)

    print("Fetching HS_trend data.")
    trend_bucket = conf['bucket']
    trend_prefix = conf['baseline_trend_path']
    files = list_files(trend_bucket,trend_prefix)
    trend_df = join_files(trend_bucket,files)

    trend_qc = trend_df.groupby(['location_num','prediction_date']).count()['trend'].reset_index()
    trend_qc['trend_qc'] = (trend_qc['trend']==1) & trend_qc['prediction_date'].isin(pred_dates)
    # trend_qc1 = trend_qc.groupby('location_num').all()['trend_qc'].reset_index()
    print("Final trend qc results:-")
    print(trend_qc)

    dates_qc = dates_qc[['location_num','prediction_date','dates_qc']]
    trend_qc = trend_qc[['location_num','prediction_date','trend_qc']]

    final_qc = qc_df.merge(dates_qc,on=['location_num','prediction_date'],how='left')
    final_qc['dates_qc'] = final_qc['dates_qc'].fillna(True)

    final_qc = final_qc.merge(trend_qc,on=['location_num','prediction_date'],how='left')
    final_qc['trend_qc'] = final_qc['trend_qc'].fillna(False)
    final_qc['forecast_qc'] = final_qc['dates_qc'] & final_qc['trend_qc']
    final_qc['generation_date'] = str(date.today())
    final_qc = final_qc[['location_num','prediction_date','dates_qc',
                         'trend_qc','forecast_qc','generation_date']]
    final_qc = final_qc.rename({'prediction_date':"business_date"},axis=1)
    final_qc['dates_qc'] = final_qc['dates_qc'].apply(lambda x: 1 if x else 0)
    final_qc['trend_qc'] = final_qc['trend_qc'].apply(lambda x: 1 if x else 0)
    final_qc['forecast_qc'] = final_qc['forecast_qc'].apply(lambda x: 1 if x else 0)
    print("final_qc df:-")
    print(final_qc)
    final_qc.to_csv(conf['qc_file_s3_path'],index=False,header=True)

    print("Calculating final_qc dataframe.")
    # final_qc1 = trend_qc1.merge(dates_qc1,on='location_num',how='left')
    # final_qc1['dates_qc'] = final_qc1['dates_qc'].fillna(True)
    # final_qc1['forecast_qc'] = final_qc1['trend_qc'] & final_qc1['dates_qc']
    final_qc1 = final_qc.groupby('location_num').all()['forecast_qc'].reset_index()

    passed_stores = final_qc1.loc[final_qc1['forecast_qc']==1,['location_num']]\
                             .reset_index(drop=True)
    failed_stores = final_qc1.loc[final_qc1['forecast_qc']==0,['location_num']]\
                             .reset_index(drop=True)
    print("Passed stores are :- ",passed_stores)
    print("Failed stores are :- ",failed_stores)

    print("Storing passed stores and failed stores dataframes in S3.")
    passed_stores.to_csv(conf['qc_passed_stores_path'],index=False,header=True)
    failed_stores.to_csv(conf['qc_failed_stores_path'],index=False,header=True)

    passed_message = f"Number of Passed stores: {len(passed_stores.index)}"
    failed_message = f"Number of Failed stores: {len(failed_stores.index)}"

    compose_slack_message(conf, passed_message, icon_emoji='white_check_mark')
    compose_slack_message(conf, failed_message, icon_emoji='rotating_light')

    with open('core_forecast_qc_baseline_daily_sales_algorithm/load_data.sql','r') as query_file:
        query = query_file.read()

    print("Loading the QC results into RDS.")
    load_to_aurora(query,connection,conf['qc_file_s3_path'],conf['qc_table_name'])

    print("Loading the dates results into RDS.")
    load_to_aurora(query,connection,f"s3://{dates_bucket}/{dates_prefix}",conf['dates_table_name'])

    return {
        'statusCode': 200,
        'body': "QC done for Step 1 of BASELINE!!"
    }