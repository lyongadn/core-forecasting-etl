import json
import logging
import os
import os.path
from datetime import datetime, timedelta
import boto3
import pandas as pd
import pymysql
import opsgenie_sdk
import time

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
pymysql.install_as_MySQLdb()

ENV = os.getenv('ENV')

def send_opsgenie_alert(conf, message, attachments):
    """
    Input - conf:Config class object, message:str, attachments:list
    This method sends the failure message to opsgenie and adds attachments (if any)
    """
    try:
    #if True:
        sns_client = boto3.client('sns', region_name='us-east-1')
        
        response = sns_client.list_subscriptions_by_topic(
                        TopicArn = conf['opsgenie_topic_arn']
                  )
        apiKey = response['Subscriptions'][0]['Endpoint'].split('?apiKey=')[1]
        LOGGER.info('Got API key for OpsGenie')
        
        opsg_config = opsgenie_sdk.configuration.Configuration()
        opsg_config.api_key['Authorization'] = apiKey

        base_client = opsgenie_sdk.api_client.ApiClient(configuration = opsg_config)
            
        alert_client = opsgenie_sdk.AlertApi(api_client = base_client)
        LOGGER.info('Set up OpsGenie Alert API client')
            
        body = opsgenie_sdk.CreateAlertPayload(
                        message=f"{conf['env']} Missing JSONs",
                        description=message,
                        responders=[{
                            'name': conf['responder_name'],
                            'type': conf['responder_type']
                            }],
                        visible_to=[{
                            'name': conf['visible_name'],
                            'type': conf['visible_type']
                            }],
                        source=conf['source'],
                        priority=conf['priority_level'],
                        tags=conf['tags']
                        )
        
        create_response = alert_client.create_alert(create_alert_payload = body)
        requestId = create_response.request_id
        LOGGER.info('Creating OpsGenie alert..')
        
        time.sleep(2)
        get_request_response = alert_client.get_request_status(request_id = requestId)
        LOGGER.info('Got OpsGenie alert response')
                    
        if get_request_response.data.is_success == True:
                        
            alertId = get_request_response.data.alert_id
            LOGGER.info('OpsGenie alert created successfully!')
            
            time.sleep(2)
            num_attachs = len(attachments)
            if num_attachs > 0:
                for i in range(num_attachs):
                    add_attachment_response = alert_client.add_attachment(identifier = alertId, file = attachments[i])            
                    LOGGER.info('Added attachment {} to OpsGenie alert'.format(attachments[i]))
                    time.sleep(2)
                            
        else:           
            LOGGER.info('OpsGenie alert creation failed')
        
    except Exception as err:
    #else:    
        LOGGER.info('Could not create OpsGenie alert')
        LOGGER.error(err)

# Uploads the files to S3
def uploadToS3(slack_message, env, csv_bucket):
    s3 = boto3.resource('s3')
    s3.Bucket(csv_bucket).put_object(Key='QC-Flag/ProdJSON/notification.txt', Body=slack_message)

# Pulls the saturday closed stores from RDS
def get_saturday_off_stores(conn, env, sat_off_table):
    query = "select * from "+sat_off_table
    with conn.cursor() as cur:
        cur.execute(query)
        data = list(cur.fetchall())
    return data

# Sends a message to slack
def send_message_to_sns(slack_alerts_topic, message):
    print(message)
    sns     = boto3.resource('sns')
    topic   = sns.Topic(slack_alerts_topic)
    topic.publish(Message=json.dumps(message))
    return

def add_field(slack_message, block_index, metric_name, metric_value):
    # Here we add the field for every data metric
    emoji = ':aws-logo:'
    new_field = {
                    "type": "mrkdwn",
                    "text": f"{emoji} *{metric_name}*\n```{metric_value}``` "
                }

    slack_message['blocks'][block_index]['elements'].append(new_field)

    return slack_message

def get_metric_datapoint(metric_name, statistic, units=""):
    # Create the Metic heading
    metric_datapoint = {
        "name": f"{metric_name}"
    }

    #Store slo        
    # We use the tabulate library to convert the list of list into a text tabular format 
    metric_datapoint['value'] = statistic
    
    return metric_datapoint

def compose_slack_message(slack_channel, env, icon_emoji):
    slack_message ={
        'channel': slack_channel,
        'username': f"{env}-missing-jsons",
        'icon_emoji': f"{icon_emoji}",
        "blocks": [
            {
                "type": "context",
                "elements": []
            }
        ]
    }

    return slack_message

# Lists all the S3 files which are modified in last 12 days.
def get_keys(bucket, prefix, requester_pays=False):
    """Get s3 objects from a bucket/prefix
    optionally use requester-pays header
    """
    s3_client = boto3.client('s3', region_name='us-east-1')

    extra_kwargs = {}
    if requester_pays:
        extra_kwargs = {'RequestPayer': 'requester'}

    next_token = 'init'
    while next_token:
        kwargs = extra_kwargs.copy()
        if next_token != 'init':
            kwargs.update({'ContinuationToken': next_token})
        # prefix_key = prefix.replace('__store_num__', store)
        resp = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, MaxKeys=10000, **kwargs)
    
        try:
            next_token = resp['NextContinuationToken']
        except KeyError:
            next_token = None
    
        for contents in resp['Contents']:
            # Returning only the JSON's that were added in the last 2 PROD runs
            if ((datetime.today().date() - contents['LastModified'].date()).days <= 12):
                key = contents['Key']
                #print("key is", key)
                yield key


# Lists down all the missing jsons for the respective days
def get_missing_jsons_list(date_list, key, env, conf):
    
    secret_key = conf["kms-key"]
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=secret_key)
    credentials = json.loads(keys['SecretString'])
    conn = pymysql.connect(host=credentials['host'],
                            user=credentials['username'],
                            password=credentials['password'],
                            db='ml_preprod')

    saturday_off_stores = get_saturday_off_stores(conn, env, conf['saturday_off_stores_table'])
    # Removing sundays
    date_list = [dt for dt in date_list if dt.weekday() != 6]
    
    # Till here
    # Changing the format of the date
    date_list_str = list(map(lambda p: datetime.strftime(p, "%m-%d-%Y"), date_list))
    print(date_list_str)

    client = boto3.client('s3')
    client.download_file(conf['csvbucket'], conf['csvkey'], '/tmp/file.csv')
    TotalStores = []
    # Reading the file that contains the LSTM Store numbers
    with open('/tmp/file.csv', 'rt') as f:
        reader = f.readlines()
        for i in reader:
            if 'store_number' in i:
                pass
            else:
                TotalStores.append(i[:5])
    
    # Getting the most recent JSON's from the bucket
    listOfJSONS = list(get_keys(conf['jsonbucket'], key, requester_pays=True))
    print("Total stores: {}".format(len(TotalStores)))

    TotalJSONS = []
    MissingJSONS = []
    missing_stores_pd = []


    dateList = []
    storeList =[]
    count_list=0
    counter_json=0
    print("first: {}".format(listOfJSONS[0]))
    #print(count_list)
    json_dict={}

    # Iterating over the list of JSONs to create a repository of the store and dates for which the forecast was generated.
    for eachJson in listOfJSONS[1:]:
        count_list+=1
        #if count_list<5:
            #print(eachJson)
        # Extracting date from the JSON name
        date_value = list(eachJson.split('demand-'))

        # Extracting the store number from the JSON Key
        store_value = list(eachJson.split('/'))

        dateList.append(date_value[-1][:10])
        storeList.append(store_value[2])

        if 'summary' not in store_value[1]:
            if date_value[-1][:10] in date_list_str: # Check if date belongs to the forecast dates
                if store_value[2] in TotalStores: # Check if store is an LSTM store or not
                    counter_json+=1
                    if store_value[2] in json_dict: # If store is already present in the dictionary, append the new date to the value
                        tmp=json_dict[store_value[2]]
                        tmp.append(date_value[-1][:10])
                        json_dict[store_value[2]]=tmp
                    else:
                        tmp=[date_value[-1][:10]] # If a new LSTM store then add the date as the value and store number as key
                        json_dict[store_value[2]]=tmp

                    TotalJSONS.append(eachJson) # A list of all the JSON's generated for every LSTM store.
    #print("dateList len: {}, storeList: {}".format(len(set(dateList)),len(set(storeList))))
    
    ### Finding the Missing JSON's
    status_flag=True
    missing_store_list=[]
    missing_date_list={}
    store_missing=False
    date_missing=False
    print("Totalstores: "+str(len(TotalStores)))
    print("len of json_dict: "+str(len(json_dict)))
    
    # Checking the if the json generated equals the total number of LSTM stores
    if len(json_dict)!=len(TotalStores):
        print("Less store(s): {}".format(set(TotalStores)-set(json_dict.keys())))
        missing_store_list+=list(set(TotalStores)-set(json_dict.keys()))
        print("missing_store_list")
        print(missing_store_list)
        status_flag=False
        store_missing=True # Sets Flag if there is a store missing.

    # Finding the missing dates of JSON's for LSTM stores
    for k in json_dict:
        print("json_dict_k: "+str(json_dict[k]))
        print("date_list_str: "+str(len(date_list_str)))
        if len(json_dict[k])!=len(date_list_str):
            print("missing date: {}, store: {}".format(set(date_list_str)-set(json_dict[k]),k))
            # A dict containing store number and missing date of JSON
            if k not in missing_date_list:
                missing_date_list[k]=list(set(date_list_str)-set(json_dict[k]))
            status_flag=False
            date_missing=True # Setting date flag to true

    missing_json_metrics = []
    missing_json_metrics.append(get_metric_datapoint('Total number of stores with JSONS generated are:                                 ', str(len(TotalStores))))
    # send_message_to_sns(env, slack_message_TotalStores)

    # The JSON's for all stores and all dates are available
    if status_flag:
        uploadToS3("Success", env, conf['csvbucket'])
        missing_json_metrics.append(get_metric_datapoint("Total number of JSONs present for each store are            ", str(len(date_list_str))))


    else:
        uploadToS3("Failed", env, conf['csvbucket'])
        # If there was a Store that had no JSON's generated for any dates
        if store_missing:

            stores_missing = sorted(missing_store_list)
            store_missing_list =  (','.join(stores_missing))
        #   missing_json_metrics.append(get_metric_datapoint("All the JSONs missing for the following Locations(s):                ", store_missing_list))

            # Added this code to append all the store and date combinations into a list
            for store in stores_missing:
                for dt in date_list:
                    if not (dt.weekday() == 5 and store in saturday_off_stores):
                        missing_stores_pd.append({
                            "location_num":store,
                            "business_date":datetime.strftime(dt,"%m-%d-%Y")
                        })
                    
            # table_data = tabulate(missing_stores_pd, tablefmt="grid")
            # missing_json_metrics.append(get_metric_datapoint("All the JSONs missing for the following Locations(s) and Dates(s):         ", table_data))

        if date_missing:
            #print("Some JSONs missing for the following store(s):")
            # missing_dates_pd = []
            all_values = []
            # Creating a list of list of all the missing dates of all stores
            for i in missing_date_list:
                all_values.append(missing_date_list[i])
            # Flattening the list of list to contain all the unique dates
            flattened = [val for sublist in all_values for val in sublist]
            # Removing duplicates by converting it into a SET
            unique_missing_date_list_1 = set(flattened)
            unique_missing_date_list =sorted( unique_missing_date_list_1)
            # Iteraing over the unique dates to create a date and Store combination
            for i  in unique_missing_date_list:

                print("Date: "+str(i))
                store_number_list=[]
                for j in missing_date_list:

                    for k in range(len(missing_date_list[j])):
                        # Checking if the store is missing the forecast for the unique business date
                        if missing_date_list[j][k] == i:
                            store_number_list.append(j)
                            # Added this code
                            dt = datetime.strptime(i,"%m-%d-%Y")
                            if not (dt.weekday() == 5 and j in saturday_off_stores):
                                # missing_dates_pd.append({
                                #     "location_num":j,
                                #     "business_date":datetime.strftime(dt,"%m-%d-%Y")
                                # })
                                missing_stores_pd.append({
                                    "location_num":j,
                                    "business_date":datetime.strftime(dt,"%m-%d-%Y")
                                })

                actual_list= sorted(store_number_list)
                actual_location_list = (','.join(actual_list))
                print("Location Number(s): ")
                print(str(actual_location_list))

        # table_data = tabulate(missing_dates_pd, tablefmt="grid")
        # # Adding the stats to send to a Slack channel
        # missing_json_metrics.append(get_metric_datapoint("Some JSONs missing for the following store(s) and date(s):             ", table_data))

    slack_message = compose_slack_message(conf['slack_channel'], env, 'rotating_light')
    block_index = 0

    if len(missing_json_metrics)>0:
        # If there are any limit errors, then format the data and post the message to the slack channel
        for metric in missing_json_metrics:
            slack_message = add_field(slack_message, block_index, metric['name'], metric['value'])
    
    send_message_to_sns(conf['slack_alerts_topic'], slack_message)

    # Added this code
    missing_jsons_df = pd.DataFrame(missing_stores_pd)
    missing_jsons_df['business_date'] = pd.to_datetime(missing_jsons_df['business_date'])
    # missing_jsons_df['forecast_type'] = forecast_type
    return missing_jsons_df

def threshold_check(conf, missing_jsons_df):
    date_agg = missing_jsons_df.groupby(by='business_date', as_index=False)['location_num'].count()
    date_agg.rename(columns={'location_num':'count'},inplace=True)
    
    threshold = int(conf['threshold'])
    dates = date_agg.loc[date_agg['count'] > threshold, 'business_date'].tolist()
    
    dates_loc_threshold = date_agg.where(date_agg['business_date'].isin(dates)).dropna()
    dates_loc_threshold['count'] = dates_loc_threshold['count'].astype(int)

    alert_msg = ""
    att = []
    
    if len(dates_loc_threshold.index) > 0:
        dates_invalid_df = missing_jsons_df.where(missing_jsons_df['business_date'].isin(dates)).dropna()
        dates_invalid_df['location_num'] = dates_invalid_df['location_num'].astype(int).astype(str).str.zfill(5)
        # print("InValid DF", len(dates_invalid_df.index))

        alert_msg += '''\n More than expected number of stores were not able to generate forecast files for the below business dates.'''
        
        for date in list(dates_loc_threshold['business_date']):
            alert_msg += '\n - {} : {} stores with missing JSON files'.format(date, \
                        dates_loc_threshold.loc[dates_loc_threshold['business_date'] == date]['count'].values[0])

        dates_invalid_df.to_csv('/tmp/above_threshold_missing_jsons.csv',index=False,header=True)

        att += ['/tmp/above_threshold_missing_jsons.csv']

        conf['env'] = env.upper()
        conf['priority_level'] = 'P1'
        send_opsgenie_alert(conf, alert_msg, att)
    
    dates_valid_df = missing_jsons_df.where(~missing_jsons_df['business_date'].isin(dates)).dropna()
    dates_valid_df['location_num'] = dates_valid_df['location_num'].astype(int).astype(str).str.zfill(5)
    
    return dates_valid_df

#Execution starts here
def lambda_handler(event,context):

    with open('core_forecast_fallback_missing_json_list/config.json') as config_params:
        conf = json.load(config_params)[ENV]

    dates_list = pd.read_csv(f"s3://{conf['dates_bucket']}/{conf['dates_file']}")
    # Get the forecast dates for 14 day and 30 day out .
    dates_list['business_date'] = pd.to_datetime(dates_list['business_date'])
    # Converting the dates into a list
    datesAlldays = list(dates_list['business_date'])
    # dates30days = list(dates_list[dates_list['forecast_type']=='30_days']['business_date'])

    # This function creates a dataframe of the location number and the business-date of the missing JSONs
    missing_jsons_14days = get_missing_jsons_list(datesAlldays, conf['json_key'], ENV, conf)
    
    # Merging the missing forecast type with the location number and business date
    missing_jsons_df = missing_jsons_14days.merge(dates_list, on=['business_date'], how='left') 
    
    if len(missing_jsons_df.index) > 0:
        missing_jsons_valid_df = threshold_check(conf, missing_jsons_df)
        print(missing_jsons_valid_df.head())
    else:
        missing_jsons_valid_df = pd.DataFrame({'location_num':[],'business_date':[],'forecast_type':[]})
   
    # Storing the missing jsons to a csv on S3
    missing_jsons_valid_df.to_csv(f"s3://{conf['csvbucket']}/{conf['missing_jsons_csv_path']}",index=False,header=True)

    return {
        "status":"200",
        "Message":"Missing JSONs CSV created successfully"
    }