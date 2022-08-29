import sys
import os
import pymysql
import boto3
import json
import logging
import time
import opsgenie_sdk
import pandas as pd
ENV = os.environ['ENV']
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        logger.info('Got API key for OpsGenie')
        
        opsg_config = opsgenie_sdk.configuration.Configuration()
        opsg_config.api_key['Authorization'] = apiKey

        base_client = opsgenie_sdk.api_client.ApiClient(configuration = opsg_config)
            
        alert_client = opsgenie_sdk.AlertApi(api_client = base_client)
        logger.info('Set up OpsGenie Alert API client')
            
        body = opsgenie_sdk.CreateAlertPayload(
                        message=f"{conf['env']} Missing Partitions",
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
        logger.info('Creating OpsGenie alert..')
        
        time.sleep(2)
        get_request_response = alert_client.get_request_status(request_id = requestId)
        logger.info('Got OpsGenie alert response')
                    
        if get_request_response.data.is_success == True:
                        
            alertId = get_request_response.data.alert_id
            logger.info('OpsGenie alert created successfully!')
            
            time.sleep(2)
            num_attachs = len(attachments)
            if num_attachs > 0:
                for i in range(num_attachs):
                    add_attachment_response = alert_client.add_attachment(identifier = alertId, file = attachments[i])            
                    logger.info('Added attachment {} to OpsGenie alert'.format(attachments[i]))
                    time.sleep(2)
                            
        else:           
            logger.info('OpsGenie alert creation failed')

        return True
        
    except Exception as err:
    #else:    
        logger.info('Could not create OpsGenie alert')
        logger.error(err)

def create_opsgenie_msg(failed_tables, conf):
    ''' 
    This function creates the OpsGenie Message and the csv file Attachment
    that contains the list of all failed tables.
    '''
    alert_msg = ""
    att = []
    
    alert_msg += '''\n Adding New Partitions has failed for the attached tables.'''
    with open('/tmp/failed_tables.csv','w') as f:
        for row in failed_tables:
            f.write(str(row) + '\n')

    att.append('/tmp/failed_tables.csv')
    conf['env'] = ENV.upper()
    conf['priority_level'] = 'P1'

    ops_status = send_opsgenie_alert(conf ,alert_msg, att)
    return ops_status

def send_message_to_sns(env, message, slack_alerts_topic):
    ''' 
    This function sends the message to be posted to the Slack Channel.
    '''
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
    metric_datapoint['value'] = statistic
    
    return metric_datapoint

def compose_slack_message(env, slack_channel, icon_emoji):
    slack_message ={
        'channel': slack_channel,
        'username': f"{env}-new-partitions",
        'icon_emoji': f"{icon_emoji}",
        "blocks": [
            {
                "type": "context",
                "elements": []
            }
        ]
    }

    return slack_message

def execute_query(conn, query):
    ''' 
    This function executes the Query that adds new partitions to the
    Aurora Tables.
    '''
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        
    return data

def add_partition(connection, query_path, location_num="", database="", table_name="", partition_name=""):
    ''' 
    This function read the Add Partition Query and returns the status of the Query Execution
    '''
    query_read = open(query_path, 'r')
    query_name = query_read.read()
    query_name = query_name.replace('__database__',\
        database)
    query_name = query_name.replace('__table_name__',\
        table_name)
    query_name = query_name.replace('__partition_name__',\
        partition_name)
    query_name = query_name.replace('__location_num__',\
        str(location_num))
    
    query_read.close()
    partition_status = execute_query(connection, query_name)
    return partition_status

def insert_new_locations(conn, query_path, location_num):
    '''This function will insert the new location into the 
    LSTM_Baseline table with forecast type as baseline'''
    
    query_status = add_partition(conn, query_path, location_num) # Execute the Insert Query for the new location
    conn.commit()
    logger.info(f"{query_status} for {location_num} to LSTM Baseline Table")

    return True

def create_new_location_list_file(conn, conf):
    """This function will query the updated table with the updated location_list
    and save the new location list in 3 different locations"""

    location_list_data = add_partition(conn, conf['location_query'])
    location_list = pd.DataFrame(list(location_list_data), columns=conf['location_columns']) #Create a dataframe of new locations
    location_list.to_csv(f"s3://{conf['prod_bucket']}/{conf['baseline_upload_path']}",index=False) # Upload this dataframe to the baseline folder
    location_list.rename(columns={"location_num":"store_number"}, inplace=True) # Rename column
    location_list.to_csv(f"s3://{conf['prod_bucket']}/{conf['lstm_upload_path']}",index=False) # Upload this df to the LSTM folder
    location_list.drop(columns=['forecast_type'],inplace=True,axis=1) # Drop the forecast type column
    location_list.to_csv(f"s3://{conf['prod_bucket']}/{conf['fallback_upload_path']}",index=False) # Upload the updated df to fallback location list
    
    logger.info("Added files to all locations containing new stores")

    return True

def lambda_handler(event, context):

    location_list = event['locations']
    # Read the config json. This json contains the details of all the parameters that are required for the function.
    with open('core_forecast_add_partitions/config.json') as config_params:
        conf = json.load(config_params)[ENV]

    # Get the secret key from KMS to create the RDS connection
    kms_manager = boto3.client('secretsmanager', region_name='us-east-1')
    keys = kms_manager.get_secret_value(SecretId=conf['secret_key'])
    credentials = json.loads(keys['SecretString'])
    
    # Creates the RDS connection object
    connection = pymysql.connect(host=credentials['host'],
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf['database'])
    logger.info("Successfully connected to Aurora")

    failed_tables = [] # This is a list that will populate the tables for which add partition has failed.
    partition_status = [] # This list will contain the Slack Message.
    for location in location_list: # Iterate over all locations
        insert_new_locations(connection, conf['insert_query'], location)
        for db in conf['databases']: # Iterate over all 4 databases
            for table_name in conf[db]: # Iterate over each table in every database
                try:
                    partition_name = f"p{str(location).zfill(5)}" # Convert partition name into a string
                    query_status = add_partition(connection, conf['partition_query'], location, db, table_name, partition_name) # Execute the add partition Query for the new location
                    logger.info(f"{query_status} for {location} to {db}.{table_name}")
                except Exception as e:
                    table = f"{db}.{table_name}"
                    logger.error(f"{e} for {location} to {table}") 
                    failed_tables.append(table) # If an exception has occured, append the table name and move on to the next table.
                    continue

    # Creating the latest location list file and storing it in different locations
    create_new_location_list_file(connection, conf)
    
    # Create the Slack alert and OpsGenie messaging.
    if len(failed_tables) > 0:
        partition_status.append(get_metric_datapoint("Adding New Partitions has failed for the following number of tables.             ", str(len(failed_tables)))) # Create the Slack alert message for failures
        create_opsgenie_msg(failed_tables, conf) # Call the OpsGenie function to send the list of failed tables.
    else:
        partition_status.append(get_metric_datapoint("Added New Partitions to the following number of tables successfully.             ", "41")) # Create the Slack alert message for success

    slack_message = compose_slack_message(ENV, conf['slack_channel'], 'rotating_light')
    block_index = 0

    if len(partition_status)>0:
        # Format the data and post the message to the slack channel
        for metric in partition_status:
            slack_message = add_field(slack_message, block_index, metric['name'], metric['value'])
    
    # Send the Slack Message to the appropriate channel
    send_message_to_sns(ENV, slack_message, conf['slack_alerts_topic'])
    connection.close()

    return {
        "Status": "200",
        "Message": "Successfully Added Partitions"
    }