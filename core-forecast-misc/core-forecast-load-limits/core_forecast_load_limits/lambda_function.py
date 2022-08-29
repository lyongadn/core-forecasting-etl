import os
import json
import boto3
import pymysql
pymysql.install_as_MySQLdb()

ENV = os.getenv('ENV')

def execute_query(query_name, connection, database, table_name, s_3_path=None):
    """
    This method executes the Deletes the old limits and uploads the new limits.
    """
    query = open(query_name, 'r')
    query_name = query.read()
    query_name = query_name.replace('__database__', database)
    query_name = query_name.replace('__table_name__', table_name)
    if s_3_path:
        query_name = query_name.replace('__s3_path__', s_3_path)
    
    print(query_name)
    query.close()

    with connection.cursor() as cur:
        #print (query)
        data = cur.execute(query_name)
        connection.commit()
    
    return data

def get_secret(region_name, secret_name):
    
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
        secret = get_secret_value_response['SecretString']

    except Exception as e:
        print(f'Error: {e}')
        
    return json.loads(secret)

def lambda_handler(event, context):
    
    with open('core_forecast_load_limits/config.json') as config_params:
        conf = json.load(config_params)[ENV]

    print("Fetching credentials for rds")
    credentials = get_secret(conf['region_name'], conf['secret_key'])

    print("Connecting to RDS")
    connection = pymysql.connect(host=credentials['host'],
                                 user=credentials['username'],
                                 password=credentials['password'],
                                 db=conf['database'])
    print("Successfully connected to RDS")

    metric = event['metric']
    print(f"Metric is {metric}")
    delete_status = execute_query(conf['delete_query'],connection,conf['database'],conf[metric])
    print(f"Deleted from {conf[metric]}", delete_status)
    upload_path = conf['upload_path'].replace('__metric__', metric)
    s3_path = f"s3-{conf['region_name']}://{conf['output_bucket']}/{upload_path}"
    print(s3_path)
    data = execute_query(conf['load_query'],connection,conf['database'],conf[metric],s_3_path=s3_path)
    print(f"Loading data into aurora by using {data}")
    
    connection.close()