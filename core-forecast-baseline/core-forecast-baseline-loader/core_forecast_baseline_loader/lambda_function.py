import os
import json
import boto3
import pymysql
import logging
pymysql.install_as_MySQLdb()

ENV = os.getenv('ENV')
region_name = "us-east-1"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name):
    # secret_name = PARAMS[ENV]['secret_key']
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
        logger.info(f'Error: {e}')
        
    return json.loads(secret)

def lambda_handler(event, context):
    store_num = event['store_number']
    forecast_types = ['dollartrans', 'ingredient', 'itemcount']
    time_lines = ['2weeks_ahead', '3weeks_ahead']
    logger.info("Fetching credentials for rds")
    
    with open('core_forecast_baseline_loader/config.json') as config_params:
        conf = json.load(config_params)[ENV]
    
    credentials = get_secret(conf['secret_key'])

    logger.info("Connecting to RDS")
    connection = pymysql.connect(host=credentials['host'],
                                 user=credentials['username'],
                                 password=credentials['password']
                                )
    cursor = connection.cursor()
    logger.info("Successfully connected to RDS")
    
    for forecast_type in forecast_types:
        for time_line in time_lines:
            logger.info(forecast_type, time_line)
            s3_path = f"s3-{region_name}://{conf['s3_bucket']}/baseline/forecast/{forecast_type}/{time_line}/store_num={store_num}/"
            logger.info(f'table_name is {conf[forecast_type][time_line]["table_name"]}')
            data = f"""LOAD DATA FROM S3 prefix '{s3_path}' INTO TABLE {conf['database']}.{conf[forecast_type][time_line]['table_name']}
                        FIELDS TERMINATED BY ','
                        ENCLOSED BY '"'
                        LINES TERMINATED BY '\\n'
                        IGNORE 1 LINES; """
            logger.info(f"Loading data into aurora by using {data}")
            
            cursor.execute(data)
    connection.commit()

    return {
        'statusCode': 200,
        'body': f"Baseline Data Loaded successfully for store {store_num}"
    }