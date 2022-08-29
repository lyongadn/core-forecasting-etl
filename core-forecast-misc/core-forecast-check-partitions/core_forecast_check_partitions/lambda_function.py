import boto3
import json
import logging
import os
import pymysql
import pandas as pd
import time

ENV = os.environ['ENV']
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    # We use the tabulate library to convert the list of list into a text tabular format 
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


def execute_query(conn, query, columns):
    ''' 
    This function executes the Query that returns the list of exisitng partition in
    Aurora Tables.
    '''
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        partition_list = pd.DataFrame(list(data), columns=columns)
        
    return partition_list

def aurora_partitions(connection, conf):
    ''' 
    This function read the Partition Query and returns the list of existing Aurora Partitions
    to be compared with the list of Athena Partitions.
    '''
    query_read = open(conf['partition_query'], 'r')
    query_name = query_read.read()
    query_name = query_name.replace('__database__',\
        conf['database'])
    query_name = query_name.replace('__table_name__',\
        conf['table_name'])
    
    query_read.close()
    partition_list = execute_query(connection, query_name, conf['columns'])
    existing_partitions = sorted(list(partition_list['location_num'].astype(int)))
    logger.info(f"Total number of existing partitions are {len(existing_partitions)}")
    return existing_partitions

def datalake_partitions(message, context):
    ''' 
    This function read the message from the SNS topic that contains the list of locations
    and creates a sorted list of all locations for the comparison.
    '''
    messageJson = json.loads(message)
    bucketList = messageJson['blocks'][1]['text']['text']

    theKeyList = bucketList.split('|')
    logger.info(f"Total number of athena partitions are {len(theKeyList)}")
    theKeyList = list(dict.fromkeys(theKeyList))
    finalList = sorted([int(x.split("=")[1][0:5]) for x in theKeyList])

    return finalList

def invoke_add_partitions(location_list, region, lambda_function):
    ''' 
    This function invokes the add partitions lambda if there is a new location that is added.
    It sends the location list as a Payload to the add-partitions lambda.
    '''
    thePayload = json.dumps({
        'locations': location_list
    })

    lb = boto3.client('lambda', region_name=region)

    return_response = lb.invoke(
        FunctionName=lambda_function,
        InvocationType='Event',
        Payload=thePayload
    )

    logger.info(f"{lambda_function} invoked with response {return_response}")

    return True

def lambda_handler(event, context):
    # print('event is {}'.format(event))
    # Get the message that is published via the SNS Topic
    subject = event['Records'][0]['Sns']['Subject']
    message = event['Records'][0]['Sns']['Message']

    print('subject is {}'.format(subject))

    # Read the config json. This json contains the details of all the parameters that are required for the function.
    with open('core_forecast_check_partitions/config.json') as config_params:
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
    partition_status = []
    # Check the kind of message that is posted via the SNS topic.
    if 'Prod EMR Pipeline' in message:
        # If the message contains instructions for Refreshing partitions invoke the specifc function to get the existing DB and Athena partitions.
        existing_partitions = aurora_partitions(connection, conf) # This function gets the existing Partitions in the Aurora DB
        athena_partitions = datalake_partitions(message, context) # This function gets the existing Partitions in the Athena Table 
        new_locations = list(set(athena_partitions) - set(existing_partitions)) # This calculates the difference between the 2 partitions.
        if len(new_locations) > 0: # If there is a difference, it means a new store was adddd and we need to add the partition for the same.
            logger.info(f"Partitions being added for locations {new_locations}")
            invoke_add_partitions(new_locations, conf['region'], conf['partitions_lambda']) # This will invoke the add partition lambda.
            partition_status.append(get_metric_datapoint("New Partitions are being added for the following new stores.             ", (new_locations)))
        else:
            partition_status.append(get_metric_datapoint("No New stores being added, hence no new partitions needed.               ", "0"))    
            logger.info(f"No new Partitions are being added")

    # Create the Slack Message for status of partitions
    slack_message = compose_slack_message(ENV, conf['slack_channel'], 'rotating_light')
    block_index = 0

    if len(partition_status)>0:
        for metric in partition_status:
            slack_message = add_field(slack_message, block_index, metric['name'], metric['value'])
    
    # Send the Slack Message to the appropriate channel
    send_message_to_sns(ENV, slack_message, conf['slack_alerts_topic'])
    
    return {
        "Status": "200",
        "Message": "Successfully Added Partitions"
    }