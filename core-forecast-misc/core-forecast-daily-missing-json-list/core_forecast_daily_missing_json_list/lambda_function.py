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
from time import sleep

logger = logging.getLogger(__name__)
pymysql.install_as_MySQLdb()

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

#Define the different buckets for each environment.

ENV = os.getenv('ENV')

def uploadToS3(slack_message, csv_bucket, env):
    s3 = boto3.resource('s3')
    s3.Bucket(csv_bucket).put_object(Key='QC-Flag/ProdJSON/notification.txt', Body=slack_message)

# Added this code
def execute_query(conn, query_type, **kwargs):
    if query_type == 'SatOff':
        query = "select * from "+kwargs['sat_off_table']
    else:
        business_date = pd.to_datetime(kwargs['business_date']).strftime('%F')
        query = kwargs['query'].replace('__business_date__', business_date)
    
    with conn.cursor() as cur:
        cur.execute(query)
        data = list(cur.fetchall())
        if query_type == 'SatOff':
            return data
        
        location_list = pd.DataFrame(data, columns=['location_num'])
        return location_list

def send_message_to_sns(env, slack_alert_topics, message):
    print(env)
    print(message)
    sns     = boto3.resource('sns')
    topic   = sns.Topic(slack_alert_topics)
    topic.publish(Message=json.dumps(message))
    return True

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

def compose_slack_message(env, slack_channel, icon_emoji):
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


def get_missing_jsons_list(date_list, key, conn, conf):
    
    saturday_off_stores = execute_query(conn, "SatOff", sat_off_table=conf['saturday_off_stores_table'])
    # saturday_off_stores = list(saturday_off_stores_df['location_num'])
    print(saturday_off_stores)
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
    # send_message_to_sns(ENV, slack_message_TotalStores)

    # The JSON's for all stores and all dates are available
    if status_flag:
        uploadToS3("Success", conf['csvbucket'], ENV)
        missing_json_metrics.append(get_metric_datapoint("Total number of JSONs present for each store are            ", str(len(date_list_str))))


    else:
        uploadToS3("Failed", conf['csvbucket'], ENV)
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
                    
            missing_json_metrics.append(get_metric_datapoint("All the JSONs missing for the following Locations(s):                 ", store_missing_list))

        if date_missing:
            #print("Some JSONs missing for the following store(s):")
            all_values = []
            # Creating a list of list of all the missing dates of all stores

            for i in missing_date_list:
                all_values.append(missing_date_list[i])
            # Flattening the list of list to contain all the unique dates
            flattened = [val for sublist in all_values for val in sublist]
            # Removing duplicates by converting it into a SET
            unique_missing_date_list_1 = set(flattened)
            unique_missing_date_list =sorted( unique_missing_date_list_1)
            missing_json_metrics.append(get_metric_datapoint("Some JSONs missing for the following date(s):             ", unique_missing_date_list))

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
                                missing_stores_pd.append({
                                    "location_num":j,
                                    "business_date":datetime.strftime(dt,"%m-%d-%Y")
                                })

                actual_list= sorted(store_number_list)
                actual_location_list = (','.join(actual_list))
                print("Location Number(s): ")
                print(str(actual_location_list))
                location_str = f"Location(s): {str(actual_location_list)}"
                missing_json_metrics.append(get_metric_datapoint(f"Date: {i}                                        ", location_str))


        # Adding the stats to send to a Slack channel

    slack_message = compose_slack_message(ENV, conf['slack_channel'], 'rotating_light')
    block_index = 0

    if len(missing_json_metrics)>0:
        # If there are any limit errors, then format the data and post the message to the slack channel
        for metric in missing_json_metrics:
            slack_message = add_field(slack_message, block_index, metric['name'], metric['value'])
    
    print(slack_message)
    send_message_to_sns(ENV, conf['slack_alerts_topic'], slack_message)

    # Added this code
    missing_jsons_df = pd.DataFrame(missing_stores_pd)
    missing_jsons_df['business_date'] = pd.to_datetime(missing_jsons_df['business_date'])
    # missing_jsons_df['forecast_type'] = forecast_type
    return missing_jsons_df


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

def check_opsgenie_alert(missing_json_df, conf):
    alert_msg = ""
    att = []

    missing_jsons_count = missing_json_df.groupby(['business_date'])['location_num'].count().reset_index()
    missing_jsons_count.rename(columns={'location_num':'count'},inplace=True)

    alert_msg += '\n There are MISSING JSONs for the expected store list during the below business dates :'

    for i in range(len(list(missing_jsons_count['business_date']))):
        alert_msg += '\n - {} : {} missing JSON files'.format(missing_jsons_count['business_date'][i], missing_jsons_count['count'][i])

    alert_msg += '\n Please find the list of store-business date combinations attached. All stores need to be notified of the missing files'

    missing_json_df.to_csv('/tmp/missing_jsons.csv',index=False,header=True)

    att += ['/tmp/missing_jsons.csv']

    conf['env'] = ENV.upper()
    conf['priority_level'] = 'P1'
    send_opsgenie_alert(conf, alert_msg, att)
    return True

def dynamic_store_list_check(missing_jsons_df, conn):
    # Reading the Query that pulls the unique location list that contains data for the past 56 days
    query_read = open('core_forecast_daily_missing_json_list/dates_loc.sql', 'r')
    date_query = query_read.read()
    filtered_df = pd.DataFrame({'location_num':[],'business_date':[]})

    # Get the unique dates present in the dataframe in a list
    date_list = list(missing_jsons_df['business_date'].unique())
    
    print("The length of the missing df is", len(missing_jsons_df.index))
    # Iterate over the unique date list present in the missing jsons data
    for biz_date in date_list:
        # Defining the variables required for processing
        locs_df = pd.DataFrame()
        loc_date_filtered = pd.DataFrame()
        locs_list = []
        
        locs_df = execute_query(conn, "Date", business_date=biz_date, query=date_query) # Execute the qurey that returns the data 
        locs_list = list(locs_df['location_num']) # Converting the locations to list
        # Filtering the missing jsons df on business_date and the locations pulled from the Query
        loc_date_filtered = missing_jsons_df.where(missing_jsons_df['business_date'] == biz_date).dropna() \
                            .where(missing_jsons_df['location_num'].isin(locs_list)).dropna()
        
        # Formatting the filtered df and appending the new df to be returned as a csv 
        if len(loc_date_filtered.index) > 0: 
            loc_date_filtered['location_num'] = loc_date_filtered['location_num'].astype(int).astype(str).str.zfill(5)
            filtered_df = filtered_df.append(loc_date_filtered)

    print("Length of Filtered DF", len(filtered_df.index))
    return filtered_df


def lambda_handler(event,context):
    
    # env = context.function_name.split('-')[0]
    #dates_list = pd.read_csv(f"s3://{PARAMS[env]['dates_bucket']}/{PARAMS[env]['dates_file']}")
    with open('core_forecast_daily_missing_json_list/config.json') as config_params:
        conf = json.load(config_params)[ENV]
     
    datelist = [(datetime.now() + timedelta(days=x+16)).strftime('%Y-%m-%d') for x in range(0,6)]
    dates_list = pd.DataFrame({'business_date': datelist})

    # Get the forecast dates for 14 day and 30 day out .
    dates_list['business_date'] = pd.to_datetime(dates_list['business_date'])
    # Converting the dates into a list
    datesAlldays = list(dates_list['business_date'])
    # dates30days = list(dates_list[dates_list['forecast_type']=='30_days']['business_date'])
   
    # Creating the RDS connection string
    secret_key = conf["kms-key"]
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=secret_key)
    credentials = json.loads(keys['SecretString'])
    conn = pymysql.connect(host=credentials['host'],
                            user=credentials['username'],
                            password=credentials['password'],
                            db='ml_preprod')

    # This function creates a dataframe of the location number and the business-date of the missing JSONs
    missing_jsons_14days = get_missing_jsons_list(datesAlldays, conf['json_key'], conn, conf)
    
    # Merging the missing forecast type with the location number and business date
    missing_jsons_df = missing_jsons_14days.merge(dates_list, on=['business_date'], how='left') 
    
    print(missing_jsons_df.head())

    if len(missing_jsons_df.index) > 0:
        # Checking the dynamic store list based on business date to reduce noise in OpsGenie
        missing_json_df_to_be_sent = dynamic_store_list_check(missing_jsons_df, conn)
        # Send OpsGenie Alert for missing store and business date combinations
        if len(missing_json_df_to_be_sent.index) > 0:
            check_opsgenie_alert(missing_json_df_to_be_sent, conf)
    
    else:
        missing_json_df_to_be_sent = pd.DataFrame({'location_num':[],'business_date':[]})
    
    # Storing the missing jsons to a csv on S3
    missing_json_df_to_be_sent.to_csv(f"s3://{conf['csvbucket']}/{conf['missing_jsons_csv_path']}",index=False,header=True)

    return {
        "status":"200",
        "Message":"Missing JSONs CSV created successfully"
    }