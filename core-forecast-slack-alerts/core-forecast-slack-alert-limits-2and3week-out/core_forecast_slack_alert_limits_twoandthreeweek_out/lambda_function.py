from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json
import logging
import pandas as pd
import numpy as np
from tabulate import tabulate
import boto3
import time
import os
import datetime
import pytz
import opsgenie_sdk
import time
from time import sleep
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def send_slack_message(conf, slack_message):
    # Sends the slack message to the channel, gets the hook url from secrets manager
    
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf['secret_key_slack'])
    credentials = json.loads(keys['SecretString'])
    hook_url = credentials['hook_url']

    req = Request(hook_url, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        LOGGER.info("Message posted to %s", slack_message['channel'])
    except HTTPError as error:
        LOGGER.error("Request failed: %d %s", error.code, error.reason)
    except URLError as error:
        LOGGER.error("Server connection failed: %s", error.reason)


def get_base_slack_message(slack_channel, forecast_type):
    # Creates the base slack message, with channel name
    slack_message = {
        "channel": slack_channel,
        "username": f"{forecast_type} Prod Run Alerts",
        "icon_emoji": "bar_chart",
        "blocks": [
            {
                "type": "context",
                "elements": []
            }
        ]
    }

    return slack_message


def add_field(slack_message, block_index, metric_name, metric_slo, metric_value):
    # Here we add the field for every data metric
    emoji = ':aws-logo:'
    new_field = {
                    "type": "mrkdwn",
                    "text": f"{emoji} *{metric_name}*\n```{metric_value}``` "
                }

    slack_message['blocks'][block_index]['elements'].append(new_field)

    return slack_message

def get_metric_datapoint(metric_name, gen_date, statistic, units=""):
    # Create the Metic heading
    metric_datapoint = {
        "name": f"{metric_name} ( {gen_date} )"
    }

    #Store slo
    metric_datapoint['slo'] = gen_date
        
    # We use the tabulate library to convert the list of list into a text tabular format 
    metric_datapoint['value'] = tabulate(statistic, headers="firstrow", tablefmt="pretty")
    metric_datapoint['units'] = units  
    
    return metric_datapoint

def get_query(query_path, table14days="", table30days="", gen_date=""):
    query_read = open(query_path, 'r')
    query_name = query_read.read()
    query_name = query_name.replace('__gen_date__',\
        gen_date)
    query_name = query_name.replace('__table_14days__',\
        table14days)
    query_name = query_name.replace('__table_30days__',\
        table30days)
    
    query_read.close()
    return query_name

def execute_query(query, conf):
    # Execution
    client = boto3.client('athena')

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': conf['database']
        },
        ResultConfiguration={
            'OutputLocation': conf['query_output_path'],
        }
    )

    # get query execution id
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)

    # get execution status
    for i in range(1, 1 + conf['retry_count']):

        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i+10)

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    return result

def athena_query_results(conf, query, gen_date, forecast_type):
    # created query
    if forecast_type == 'lstm':
        query_name = get_query(query, conf['14days_table'], conf['30days_table'], gen_date)
    else:
        query_name = get_query(query, conf['Redundancy_table'], conf['Redundancy_table'], gen_date)
    # athena client
    
    result = execute_query(query_name, conf)
    # get data

    # If the query return data other than the column names then create a list of list of the data
    if len(result['ResultSet']['Rows']) > 1:
        final_data = []
        for rows in result['ResultSet']['Rows']:
            data_list = [data_row['VarCharValue'] for data_row in rows['Data']]
            final_data.append(data_list)

        # Check to see if the data returned by the Query does not contain all zeros 
        if count_non_zeros(final_data):
            return final_data
        
        return None

    else:
    # If not, then return None    
        print(None)
        return None

def count_non_zeros(final_data):
    """This function returns False if all the rows and columns in the dataframe contains zeros."""
    
    final_df = pd.DataFrame(final_data[1:], columns=final_data[0]) #Convert the list of list to a DF
    print(final_df.head())
    final_df = final_df.drop(['Business Date'], axis=1) #Remove the string column
    final_df = final_df.applymap(int) #Convert all the elements of the df into integer datatype 

    # Use the count_nonzero numpy function to count the total zeros in each row.
    # This function returns a list of the num of zeros in each row.
    # If the sum of the list is greater than zero, it means we need to display the stats

    if sum(np.count_nonzero(final_df.values, axis=1)) > 0:
        return True
    
    return False

def load_partitions(conf, table_name):
    # We will send table to name as a replacement string to avoid adding another function
    patition_load_query = get_query(conf['load_partition_query'], table_name)
    
    result = execute_query(patition_load_query, conf)
    
    return result


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
                        message="{} Prod Run Alerts".format(conf['forecast_type']),
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

def get_gen_date(result):
    try:    
        gen_date = ''
        if len(result['ResultSet']['Rows']) > 1:
            final_data = []
            for rows in result['ResultSet']['Rows']:
                data_list = [data_row['VarCharValue'] for data_row in rows['Data']]
                final_data.append(data_list)
            
            print("Data list is",final_data)

            if len(final_data) > 1:
                gen_date = final_data[-1][0].split(":")[2].split("=")[1]
                print("Parsed gendate is", gen_date)
                return gen_date
            else:
                raise ValueError("Empty Row returned from Athena")
                
        else:
            raise ValueError("Empty Row returned from Athena")
            
    except Exception as e:
        print("Exception is", e)
        EST = pytz.timezone('America/New_York') 
        gen_date = str(datetime.datetime.now(EST).date())
        print("Standard gen date is", gen_date)
        return gen_date

def lambda_handler(event, context):
    ENV = os.getenv('ENV')
    t1 = time.time()
    
    with open('core_forecast_slack_alert_limits_twoandthreeweek_out/config.json') as config_params:
        conf = json.load(config_params)[ENV]
    
    gen_date = ''
    # Loading the partitions from the latest run
    if event['forecast_type'] == 'lstm':
        load_partition_14days = load_partitions(conf, conf['14days_table'])
        print(load_partition_14days)
        time.sleep(5)
        load_partition_30days = load_partitions(conf, conf['30days_table'])
        print(load_partition_30days)
        time.sleep(5)
        gen_date = get_gen_date(load_partition_30days)
        total_stores = conf['total_stores']
    else:
        load_partition_14days = load_partitions(conf, conf['Redundancy_table'])
        time.sleep(5)
        gen_date = get_gen_date(load_partition_14days)
        total_stores = conf['total_stores_lstm']

    daily_metrics = []
    
    alert_msg = ""
    att = []
    
    # Iterate over the 4 Athena Queries to execute it one at a time
    for index, query in enumerate(conf['query_list']):
        # Get the data from the Athena Query
        
        final_data = athena_query_results(conf, query, gen_date, event['forecast_type'])
        
        if final_data is not None:
            # If the Query returns data, then append it to the daily metrics list for flat and zero forecasts
            
            # Converting the list of lists into a dataframe for OpsGenie Alerting
            final_df = pd.DataFrame(final_data[1:], columns=final_data[0])
            final_df['Business Date'] = pd.to_datetime(final_df['Business Date'])
            cols = [col for col in final_df.columns if col != 'Business Date']
            for col in cols:
                final_df[col] = final_df[col].astype('int')
        
            if "Flat" in conf['stats_list'][index]:
                daily_metrics.append(get_metric_datapoint(conf['stats_list'][index], gen_date, final_data, 'Num of Stores'))
                
                final_data_req = final_df[(final_df.select_dtypes(include=['number']) != 0).any(1)]
                
                dates_affected = list(final_data_req['Business Date'].astype('str'))
                alert_msg += "\n Dates affected by flat forecasts: {}".format(dates_affected)
                
                final_data_req.to_csv('/tmp/flat_forecast.csv',index=False,header=True)
                att += ['/tmp/flat_forecast.csv']
                
            elif "Zero" in conf['stats_list'][index]:
                daily_metrics.append(get_metric_datapoint(conf['stats_list'][index], gen_date, final_data, 'Num of Stores'))
                
                final_data_req = final_df[(final_df.select_dtypes(include=['number']) != 0).any(1)]
                
                dates_affected = list(final_data_req['Business Date'].astype('str'))
                alert_msg += "\n Dates affected by zero forecasts: {}".format(dates_affected)
                
                final_data_req.to_csv('/tmp/zero_forecast.csv',index=False,header=True)
                att += ['/tmp/zero_forecast.csv']
            
            # If data returned is more than threshold for ds/tc or ic/ing, then append it to daily metrics

            else:
                final_data_copy = final_df.copy()
                
                if "Dollar" in conf['stats_list'][index]:
                    #final_data_copy['dollars_affected'] = ((final_data_copy['Dollar > 1.5'] + final_data_copy['Dollar < 0.9']).astype('float')/total_stores)*100
                    #final_data_copy['trans_affected'] = ((final_data_copy['Trans > 1.5'] + final_data_copy['Trans < 0.9']).astype('float')/total_stores)*100
                    
                    #final_data_req = final_data_copy[(final_data_copy['dollars_affected'] > conf['threshold']) | (final_data_copy['trans_affected'] > conf['threshold'])]
                    
                    final_data_req = final_data_copy[((final_data_copy['Dollar > 1.5'].astype('float')/total_stores)*100 > conf['threshold']) | ((final_data_copy['Dollar < 0.9'].astype('float')/total_stores)*100 > conf['threshold']) | ((final_data_copy['Trans > 1.5'].astype('float')/total_stores)*100 > conf['threshold']) | ((final_data_copy['Trans < 0.9'].astype('float')/total_stores)*100 > conf['threshold'])]
                    daily_metrics.append(get_metric_datapoint(conf['stats_list'][index], gen_date, final_data, 'Num of Stores'))

                    if len(final_data_req) > 0:
                        
                        dates_affected = list(final_data_req['Business Date'].astype('str'))
                        alert_msg += "\n Dates affected by out-of-limit dollars and trans: {}".format(dates_affected)
                        
                        (final_data_req.iloc[:,:-2]).to_csv('/tmp/dollars_trans_limits.csv', index=False, header = True)
                        att += ['/tmp/dollars_trans_limits.csv']
                        
                elif "Item" in conf['stats_list'][index]:
                    #final_data_copy['item_affected'] = ((final_data_copy['Item > 1.5'] + final_data_copy['Item < 0.9']).astype('float')/total_stores)*100
                    #final_data_copy['ingre_affected'] = ((final_data_copy['Ingre > 1.5'] + final_data_copy['Ingre < 0.9']).astype('float')/total_stores)*100
                    
                    #final_data_req = final_data_copy[(final_data_copy['item_affected'] > conf['threshold']) | (final_data_copy['ingre_affected'] > conf['threshold'])]
                    
                    final_data_req = final_data_copy[((final_data_copy['Item > 1.5'].astype('float')/total_stores)*100 > conf['threshold']) | ((final_data_copy['Item < 0.9'].astype('float')/total_stores)*100 > conf['threshold']) | ((final_data_copy['Ingre > 1.5'].astype('float')/total_stores)*100 > conf['threshold']) | ((final_data_copy['Ingre < 0.9'].astype('float')/total_stores)*100 > conf['threshold'])]
                    daily_metrics.append(get_metric_datapoint(conf['stats_list'][index], gen_date, final_data, 'Num of Stores'))

                    if len(final_data_req) > 0:
                        
                        dates_affected = list(final_data_req['Business Date'].astype('str'))
                        alert_msg += "\n Dates affected by out-of-limit items and ingredients: {}".format(dates_affected)
                        
                        (final_data_req.iloc[:,:-2]).to_csv('/tmp/item_ingre_limits.csv', index=False, header = True)
                        att += ['/tmp/item_ingre_limits.csv']
                
          
    # Create the base slack message
    slack_message = get_base_slack_message(conf['slack_channel'], event['forecast_type'].upper())
    block_index = 0

    if len(daily_metrics)>0:
        # If there are any limit errors, then format the data and post the message to the slack channel
        for metric in daily_metrics:
            slack_message = add_field(slack_message, block_index, metric['name'], metric['slo'], metric['value'])
    else:
        # If no limit errors, then this posts a generic message to the slack channel
        no_limit_msg = f"Statistics for {event['forecast_type'].upper()} Prod Run ({gen_date})"
        slack_message = add_field(slack_message, block_index, no_limit_msg, gen_date, "All Statistics are in Range" )
        block_index += 2
    
    print(slack_message)
    
    # Sends the slack message to the specific channel
    send_slack_message(conf, slack_message)
    
    if len(alert_msg) > 0:
        
        conf['forecast_type'] = event['forecast_type'].upper()
        conf['priority_level'] = 'P3'
        # send_opsgenie_alert(conf, alert_msg, att)
    
    
    t3 = time.time()
    print("Total time of execution:- ",t3-t1)
    return {
        "Status":"200",
        "Message":"Slack message posted successfully"
    }
