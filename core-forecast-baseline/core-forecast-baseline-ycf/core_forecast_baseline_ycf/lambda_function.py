import logging
import os
import awswrangler as wr
from datetime import datetime,timedelta
import boto3
import json
import time
import pymysql
import pandas as pd
import csv
from boto3.s3.transfer import S3Transfer
pymysql.install_as_MySQLdb()

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()

#Get environment
ENV = os.getenv('ENV')
REGION_NAME = 'us-east-1'
athena_client = boto3.client('athena', region_name=REGION_NAME)
#Define the different buckets for each environment.
PARAMS = {
    'dev': {
        "secret_key":"cfa-rdskey-dev",
        'database': 'baseline',
        'output_bucket': 'dev-q-forecasting-artifacts',
        'athena_bucket': 'aws-athena-query-results-186277090777-us-east-1'
    },
    'test': {
        "secret_key":"cfa-rdskey-test",
        'database': 'baseline',
        'output_bucket': 'test-q-forecasting-artifacts',
        'athena_bucket': 'aws-athena-query-results-725706091682-us-east-1',
        'crawler_key': 'test-parameterization/crawler_parameters/LSTM/LSTM_Baseline_List.csv',
        's3_path':'baseline/ycf/store_num=__store_number__/__store_number__-ycf.csv'
    },
    'prod': {
        "secret_key":"cfa-rdskey-prod",
        'database': 'baseline',
        'output_bucket': 'prod-q-forecasting-artifacts',
        'athena_bucket': 'aws-athena-query-results-725706091682-us-east-1',
        'crawler_key': 'prod-parameterization/crawler_parameters/LSTM/LSTM_Baseline_List.csv',
        's3_path':'baseline/ycf/store_num=__store_number__/__store_number__-ycf.csv'
    }
}

ENV='prod'
region_name = "us-east-1"

def get_store_list(crawler_bucket, crawler_key):
    """
    Input crawler_bucket:str, crawler_key:str
    This method takes the bucket and key of crawler file,
    returns list of stores
    """
    client = boto3.client('s3')
    client.download_file(crawler_bucket, crawler_key,\
                         '/tmp/Data_Prep_Store_List.csv')
    client = boto3.client('lambda', region_name='us-east-1')
    file_reader = open('/tmp/Data_Prep_Store_List.csv', "r")
    reader = csv.DictReader(file_reader)
    data = [row for row in reader]
    # logger.info("fetched store list")
    return [loc['store_number'] for loc in data]

def write_to_locations(final_df, location):
    loc = location.lstrip("0")
    upload_path = PARAMS[ENV]['s3_path'].replace('__store_number__', loc)
    output_file = f"s3://{PARAMS[ENV]['output_bucket']}/{upload_path}"
    print("File written at loc", output_file)
    wr.s3.to_csv(final_df, output_file, index=False)

    
def get_result(response, QEID):
    if athena_client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])['QueryExecution']['Status']['State'] != 'SUCCEEDED':
        #wait for 30 seconds then re-call the function
        time.sleep(30)
        return get_result(response, response['QueryExecutionId'])
    else:
        print('---------Process Completed for Daily---------')
        print(athena_client.get_query_execution(QueryExecutionId = QEID))
        return athena_client.get_query_execution(QueryExecutionId = QEID)['QueryExecution']['ResultConfiguration']['OutputLocation']

def run_query(query, database, output_location):
    try:
        response = athena_client.start_query_execution(
            QueryString = query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
            'OutputLocation': f's3://{output_location}/results'
            },
        )
        return response, response['QueryExecutionId']
    except Exception as error:
        print(f'something bad happened: {error}')
        
        
def get_secret():
    secret_name = PARAMS[ENV]['secret_key']
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
    LOGGER.info(f"Running with event: {event}")

    if 'location_query_db' in event:
        database = event['location_query_db']
    else:
        database = PARAMS[ENV]['database']

    #Running QC query
    query = open('query.sql').read()
    LOGGER.info(f"Running with query: {query}")

    generation_date = datetime.today().strftime('%Y-%m-%d')
    output_file = f"s3://{PARAMS[ENV]['output_bucket']}/baseline/ycf/ycf.csv"
    #Query Athena to get location list w/ attributes
    loc_df = wr.athena.read_sql_query(query, database, ctas_approach=False)
    #Format loc_nums
    
    LOGGER.info("Write data to S3")
    wr.s3.to_csv(loc_df, output_file, index=False)
    #LOGGER.error("Query returned no results")
    time.sleep(2)

    # #This process fetches ookup ingredients data from aurora and save it in s3
    print("Fetching credentials for rds")
    credentials = get_secret()

    print("Connecting to RDS")
    connection = pymysql.connect(host=credentials['host'],
                                 user=credentials['username'],
                                 password=credentials['password']
                                )
    cursor = connection.cursor()
    print("Successfully connected to RDS")
    with connection.cursor() as rep2:
        
        lookup_data = '''select DISTINCT * from ml_preprod.lookup_ingredients 
        where generation_date=(select max(generation_date) from ml_preprod.lookup_ingredients) ;'''
        print(lookup_data)
        rep2.execute(lookup_data)
        data2 = rep2.fetchall()
        df3 = pd.DataFrame(list(data2), columns=['pin', 'recipe_item_id', 'recipe_name', 'ingredient_name', 'ingredient_id',  'unit_of_measure', 'ingredient_quantity_actual', 'ingredient_quantity', 'generation_date']  # ,  index = False
                          )
        df3.to_csv('/tmp/final_data.csv', sep=',', index=False)
        s3 = boto3.client('s3')
        transfer = S3Transfer(s3)
        target_dir = '/tmp/'
        transfer.upload_file('/tmp/final_data.csv', 'prod-q-forecasting-artifacts', \
                         'baseline/lookup_ingredients/lookup_ingredients.csv')
        connection.commit()
        